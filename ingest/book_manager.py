from sortedcontainers import SortedDict
from typing import Dict, List, Tuple, Optional
import logging
import math
import time

from config import config


logger = logging.getLogger(__name__)


class OrderBookManager:
    """Maintain a validated level-2 order book with Binance-compatible snapshots and diffs."""
    def __init__(self, symbol: str, tick_size: Optional[float] = None,
                 max_depth: Optional[int] = None):
        self.symbol = symbol
        self.tick_size = float(tick_size or config.exchange.get('tick_size', 0.1))
        self.max_depth = int(max_depth or config.orderbook.get('max_depth', 500))

        self.bids = SortedDict()
        self.asks = SortedDict()

        self.last_update_id: Optional[int] = None
        self.last_event_time: float = 0.0
        self.initialized = False
        self.update_buffer: List[Dict] = []

        self.desync_detected = False
        self.last_validation = 0.0
        self.last_resync = 0.0
        self.last_compare = 0.0

        ob_cfg = config.orderbook
        self.stale_tolerance = ob_cfg.get('stale_tolerance_s', 3)
        self.resync_interval = ob_cfg.get('resync_interval_s', 30)
        self.compare_depth = ob_cfg.get('compare_depth', 20)
        self.deviation_threshold = ob_cfg.get('deviation_threshold_pct', 0.0005)

    # Snapshot handling --------------------------------------------------
    def process_snapshot(self, snapshot: Dict):
        bids = snapshot.get('bids') or snapshot.get('b') or []
        asks = snapshot.get('asks') or snapshot.get('a') or []

        self.bids.clear()
        self.asks.clear()

        for price, qty in bids[: self.max_depth]:
            self._write_level(self.bids, float(price), float(qty))
        for price, qty in asks[: self.max_depth]:
            self._write_level(self.asks, float(price), float(qty))

        lid = snapshot.get('lastUpdateId') or snapshot.get('u') or snapshot.get('nonce')
        self.last_update_id = int(lid) if lid is not None else None
        self.initialized = True
        self.last_event_time = time.time()
        self.desync_detected = False
        self._apply_buffered_updates()

    def process_update(self, update: Dict) -> bool:
        if not self.initialized:
            self.update_buffer.append(update)
            return False

        update_id = update.get('u') or update.get('lastUpdateId') or update.get('nonce')
        first_update_id = update.get('U') or update.get('firstUpdateId')
        prev_update_id = update.get('pu')

        if self.last_update_id is not None:
            if update_id and update_id <= self.last_update_id:
                return True

            if prev_update_id is not None and prev_update_id != self.last_update_id:
                self._flag_desync(f"sequence break pu={prev_update_id} last={self.last_update_id}")
                return False

            if first_update_id and first_update_id > self.last_update_id + 1:
                self._flag_desync(
                    f"gap expected {self.last_update_id + 1} got {first_update_id}"
                )
                return False

        bids = update.get('bids') or update.get('b') or []
        asks = update.get('asks') or update.get('a') or []

        self._overwrite_range(self.bids, bids, is_bid=True)
        self._overwrite_range(self.asks, asks, is_bid=False)

        if update_id is not None:
            self.last_update_id = int(update_id)
        if update.get('E'):
            self.last_event_time = float(update['E']) / 1000.0
        else:
            self.last_event_time = time.time()
        return True

    def _apply_buffered_updates(self):
        if not self.update_buffer or self.last_update_id is None:
            self.update_buffer.clear()
            return

        pending = []
        for update in self.update_buffer:
            update_id = update.get('u') or update.get('lastUpdateId')
            first_id = update.get('U') or update.get('firstUpdateId')

            if update_id is None or first_id is None:
                continue

            if first_id <= self.last_update_id + 1 and update_id >= self.last_update_id + 1:
                self.process_update(update)
            else:
                pending.append(update)

        self.update_buffer = pending

    # Invariants ---------------------------------------------------------
    def validate(self) -> bool:
        now = time.time()
        if now - self.last_validation < 1.0:
            return not self.desync_detected

        self.last_validation = now

        if not self.bids or not self.asks:
            return self._flag_desync('missing book sides')

        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()

        if not best_bid or not best_ask:
            return self._flag_desync('empty best levels')

        if best_bid[0] >= best_ask[0]:
            return self._flag_desync(
                f"crossed book bid={best_bid[0]} ask={best_ask[0]}"
            )

        for book in (self.bids, self.asks):
            for price, qty in book.items():
                if qty < 0 or math.isnan(qty) or math.isinf(qty):
                    return self._flag_desync('invalid qty')
                if not self._is_tick_aligned(price):
                    return self._flag_desync('tick misalignment')

        self.desync_detected = False
        return True

    def is_stale(self, monotonic_now: Optional[float] = None) -> bool:
        if not self.last_event_time:
            return True
        now = monotonic_now or time.time()
        return (now - self.last_event_time) > self.stale_tolerance

    def should_compare_snapshot(self) -> bool:
        return (time.time() - self.last_compare) >= self.resync_interval

    def record_snapshot_compare(self, ok: bool):
        self.last_compare = time.time()
        if not ok:
            logger.warning("Order book snapshot deviation detected")

    def compare_with_snapshot(self, snapshot: Dict) -> bool:
        bids = snapshot.get('bids') or snapshot.get('b') or []
        asks = snapshot.get('asks') or snapshot.get('a') or []
        depth = self.compare_depth

        def _max_deviation(local: List[Tuple[float, float]], remote: List[List[float]]):
            max_px = 0.0
            max_qty = 0.0
            for idx in range(min(depth, len(local), len(remote))):
                lp, lq = local[idx]
                rp = float(remote[idx][0])
                rq = float(remote[idx][1])
                px_dev = abs(lp - rp) / max(rp, self.tick_size)
                qty_dev = abs(lq - rq) / max(rq, 1e-9)
                max_px = max(max_px, px_dev)
                max_qty = max(max_qty, qty_dev)
            return max_px, max_qty

        local_top = self.get_top_levels(depth)
        bid_dev = _max_deviation(local_top['bids'], bids)
        ask_dev = _max_deviation(local_top['asks'], asks)
        px_dev = max(bid_dev[0], ask_dev[0])
        qty_dev = max(bid_dev[1], ask_dev[1])

        within = px_dev <= self.deviation_threshold and qty_dev <= 0.5
        self.record_snapshot_compare(within)
        return within

    # Query helpers ------------------------------------------------------
    def get_best_bid(self) -> Optional[Tuple[float, float]]:
        if not self.bids:
            return None
        price = self.bids.peekitem(-1)[0]
        return price, self.bids[price]

    def get_best_ask(self) -> Optional[Tuple[float, float]]:
        if not self.asks:
            return None
        price = self.asks.peekitem(0)[0]
        return price, self.asks[price]

    def get_mid_price(self) -> Optional[float]:
        bid = self.get_best_bid()
        ask = self.get_best_ask()
        if bid and ask:
            return (bid[0] + ask[0]) / 2.0
        return None

    def get_top_levels(self, depth: int = 10) -> Dict:
        depth = max(1, min(depth, self.max_depth))
        bids = list(self.bids.items())[-depth:]
        asks = list(self.asks.items())[:depth]
        bids = list(reversed(bids))
        best_bid = bids[0][0] if bids else None
        best_ask = asks[0][0] if asks else None
        timestamp_ms = time.time() * 1000
        return {
            'symbol': self.symbol,
            'bids': bids,
            'asks': asks,
            'timestamp': timestamp_ms,
            'best_bid': best_bid,
            'best_ask': best_ask,
        }

    def compute_obi(self, depth: int = 10) -> Dict:
        levels = self.get_top_levels(depth)
        bid_qty = sum(qty for _, qty in levels['bids'])
        ask_qty = sum(qty for _, qty in levels['asks'])
        total = bid_qty + ask_qty
        obi = (bid_qty - ask_qty) / total if total > 0 else 0.0
        best_bid = levels.get('best_bid')
        best_ask = levels.get('best_ask')
        spread = None
        mid_price = None
        if best_bid is not None and best_ask is not None:
            spread = max(best_ask - best_bid, 0.0)
            mid_price = (best_bid + best_ask) / 2.0
        return {
            'symbol': self.symbol,
            'timestamp': levels['timestamp'],
            'obi': obi,
            'bid_qty': bid_qty,
            'ask_qty': ask_qty,
            'depth': min(len(levels['bids']), len(levels['asks'])),
            'best_bid': best_bid,
            'best_ask': best_ask,
            'mid_price': mid_price,
            'spread': spread,
            'total_depth': total,
        }

    def to_dict(self) -> Dict:
        depth = min(50, self.max_depth)
        return {
            'symbol': self.symbol,
            'bids': [[price, qty] for price, qty in list(self.bids.items())[-depth:]],
            'asks': [[price, qty] for price, qty in list(self.asks.items())[:depth]],
            'last_update_id': self.last_update_id,
            'timestamp': time.time() * 1000,
        }

    # Internal helpers ---------------------------------------------------
    def _write_level(self, book: SortedDict, price: float, qty: float):
        price = self._quantize(price)
        if qty <= 0:
            book.pop(price, None)
            return
        book[price] = qty

    def _overwrite_range(self, book: SortedDict, levels: List[List[float]], is_bid: bool):
        if not levels:
            return
        prices = [self._quantize(float(level[0])) for level in levels]
        lo = min(prices)
        hi = max(prices)
        self._purge_range(book, lo, hi)
        for price, level in zip(prices, levels):
            qty = float(level[1])
            self._write_level(book, price, qty)
        self._cap_depth(book, is_bid)

    def _purge_range(self, book: SortedDict, lo: float, hi: float):
        keys = list(book.irange(lo, hi))
        for key in keys:
            book.pop(key, None)

    def _cap_depth(self, book: SortedDict, is_bid: bool):
        while len(book) > self.max_depth:
            if is_bid:
                book.popitem(index=0)
            else:
                book.popitem(index=-1)

    def _quantize(self, price: float) -> float:
        ticks = round(price / self.tick_size)
        return round(ticks * self.tick_size, 10)

    def _is_tick_aligned(self, price: float) -> bool:
        if self.tick_size <= 0:
            return True
        ticks = price / self.tick_size
        return abs(ticks - round(ticks)) < 1e-9

    def _flag_desync(self, reason: str) -> bool:
        logger.error("Order book desync: %s", reason)
        self.desync_detected = True
        return False
