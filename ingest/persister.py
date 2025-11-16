import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List

import asyncpg

from config import config


logger = logging.getLogger(__name__)


@dataclass
class BufferDefinition:
    name: str
    sql: str
    row_builder: Callable[[Dict[str, Any]], Any]


class PersistenceBuffer:
    def __init__(self, definition: BufferDefinition):
        self.definition = definition
        self.records: List[Dict[str, Any]] = []

    def append(self, payload: Dict[str, Any]) -> None:
        self.records.append(payload)

    def build_rows(self) -> List[Any]:
        return [self.definition.row_builder(entry) for entry in self.records]

    def clear(self) -> None:
        self.records.clear()


class DataPersister:
    def __init__(self):
        self.pool = None
        self._trade_warning_emitted = False

        self.batch_size = 500
        self.flush_interval = 5
        self.max_buffer_size = 5000
        self.running = False
        self._auto_task = None
        self.buffers = self._build_buffer_registry()
        self.book_buffer = self.buffers['book'].records
        self.mark_buffer = self.buffers['mark'].records
        self.oi_buffer = self.buffers['oi'].records
        self.indicator_buffer = self.buffers['indicator'].records
        self.signal_buffer = self.buffers['signal'].records
        self.obi_buffer = self.buffers['obi'].records
        
    async def initialize(self):
        db_config = config.database
        self.pool = await asyncpg.create_pool(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password'],
            min_size=5,
            max_size=20
        )
        
    async def close(self):
        if self.pool:
            await self.flush_all()
            await self.pool.close()
            
    async def insert_trade(self, trade: Dict[str, Any]):
        if not self._trade_warning_emitted:
            logger.warning(
                "Trade persistence disabled here; datapython owns inserts into core.binance_trades"
            )
            self._trade_warning_emitted = True
        return
            
    async def insert_book_snapshot(self, snapshot: Dict[str, Any]):
        await self._append('book', snapshot)

    async def insert_mark_price(self, mark: Dict[str, Any]):
        await self._append('mark', mark)

    async def insert_open_interest(self, oi: Dict[str, Any]):
        await self._append('oi', oi)

    async def insert_indicator_state(self, indicator: Dict[str, Any]):
        await self._append('indicator', indicator)

    async def insert_signal(self, signal: Dict[str, Any]):
        await self._append('signal', signal)

    async def insert_obi_snapshot(self, obi_snapshot: Dict[str, Any]):
        await self._append('obi', obi_snapshot)

    def _build_buffer_registry(self) -> Dict[str, PersistenceBuffer]:
        return {
            'book': PersistenceBuffer(BufferDefinition(
                'book',
                '''INSERT INTO book_snapshots (symbol, ts, bids_json, asks_json, last_update_id)
                   VALUES ($1, $2, $3, $4, $5)
                   ON CONFLICT (symbol, ts) DO NOTHING''',
                lambda b: (
                    b['symbol'],
                    datetime.fromtimestamp(b['timestamp'] / 1000),
                    json.dumps(b['bids']),
                    json.dumps(b['asks']),
                    int(b.get('last_update_id', 0) or 0),
                ),
            )),
            'mark': PersistenceBuffer(BufferDefinition(
                'mark',
                '''INSERT INTO mark_price (symbol, ts, mark_price, funding_rate)
                   VALUES ($1, $2, $3, $4)
                   ON CONFLICT (symbol, ts) DO NOTHING''',
                lambda m: (
                    m['symbol'],
                    datetime.fromtimestamp(m['timestamp'] / 1000),
                    m['mark_price'],
                    m.get('funding_rate'),
                ),
            )),
            'oi': PersistenceBuffer(BufferDefinition(
                'oi',
                '''INSERT INTO open_interest (symbol, ts, oi_value)
                   VALUES ($1, $2, $3)
                   ON CONFLICT (symbol, ts) DO NOTHING''',
                lambda o: (
                    o['symbol'],
                    datetime.fromtimestamp(o['timestamp'] / 1000),
                    o['openInterest'],
                ),
            )),
            'obi': PersistenceBuffer(BufferDefinition(
                'obi',
                '''INSERT INTO obi_regime_stats
                       (symbol, ts, obi, obi_z, bid_qty, ask_qty, depth_levels, total_depth, best_bid, best_ask, mid_price, spread)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                       ON CONFLICT (symbol, ts) DO UPDATE SET
                           obi = EXCLUDED.obi,
                           obi_z = EXCLUDED.obi_z,
                           bid_qty = EXCLUDED.bid_qty,
                           ask_qty = EXCLUDED.ask_qty,
                           depth_levels = EXCLUDED.depth_levels,
                           total_depth = EXCLUDED.total_depth,
                           best_bid = EXCLUDED.best_bid,
                           best_ask = EXCLUDED.best_ask,
                           mid_price = EXCLUDED.mid_price,
                           spread = EXCLUDED.spread''',
                lambda snap: (
                    snap['symbol'],
                    datetime.fromtimestamp(snap['timestamp'] / 1000),
                    snap.get('obi'),
                    snap.get('obi_z'),
                    snap.get('bid_qty'),
                    snap.get('ask_qty'),
                    snap.get('depth'),
                    snap.get('total_depth'),
                    snap.get('best_bid'),
                    snap.get('best_ask'),
                    snap.get('mid_price'),
                    snap.get('spread'),
                ),
            )),
            'indicator': PersistenceBuffer(BufferDefinition(
                'indicator',
                '''INSERT INTO indicator_state
                   (symbol, ts, rsi, macd, macd_signal, macd_hist, obv, ad_line, realized_vol, cvd, obi, obi_z, obi_bid_qty, obi_ask_qty, obi_depth_levels)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                   ON CONFLICT (symbol, ts) DO NOTHING''',
                lambda i: (
                    i['symbol'],
                    datetime.fromtimestamp(i['timestamp'] / 1000),
                    i.get('rsi'),
                    i.get('macd'),
                    i.get('macd_signal'),
                    i.get('macd_hist'),
                    i.get('obv'),
                    i.get('ad_line'),
                    i.get('realized_vol'),
                    i.get('cvd'),
                    i.get('obi'),
                    i.get('obi_z'),
                    i.get('obi_bid_qty'),
                    i.get('obi_ask_qty'),
                    i.get('obi_depth_levels'),
                ),
            )),
            'signal': PersistenceBuffer(BufferDefinition(
                'signal',
                '''INSERT INTO signals
                   (signal_id, symbol, ts, side, level_price, zone_low, zone_high,
                    score, state, divergences, entry_price, stop_price,
                    target_price, exit_price, pnl, order_id, filled_qty, avg_fill_price, feature_vector)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
                   ON CONFLICT (signal_id) DO UPDATE SET
                   state = EXCLUDED.state,
                   entry_price = EXCLUDED.entry_price,
                   stop_price = EXCLUDED.stop_price,
                   target_price = EXCLUDED.target_price,
                   exit_price = EXCLUDED.exit_price,
                   pnl = EXCLUDED.pnl,
                   order_id = EXCLUDED.order_id,
                   filled_qty = EXCLUDED.filled_qty,
                   avg_fill_price = EXCLUDED.avg_fill_price,
                   feature_vector = EXCLUDED.feature_vector''',
                lambda s: (
                    s['signal_id'],
                    s['symbol'],
                    datetime.fromtimestamp(s['timestamp'] / 1000),
                    s['side'],
                    s['level_price'],
                    s['zone_low'],
                    s['zone_high'],
                    s['score'],
                    s['state'],
                    json.dumps(s.get('divergences', {})),
                    s.get('entry_price'),
                    s.get('stop_price'),
                    s.get('target_price'),
                    s.get('exit_price'),
                    s.get('pnl'),
                    s.get('order_id'),
                    s.get('filled_qty'),
                    s.get('avg_fill_price'),
                    json.dumps(s.get('feature_vector', {})),
                ),
            )),
        }

    async def _append(self, name: str, payload: Dict[str, Any]) -> None:
        buffer = self.buffers[name]
        buffer.append(payload)
        self._enforce_bounds(name)
        if len(buffer.records) >= self.batch_size:
            await self._flush_buffer(name)

    def _enforce_bounds(self, name: str):
        buf = self.buffers[name].records
        if len(buf) > self.max_buffer_size:
            drop_n = max(int(self.max_buffer_size * 0.2), 1)
            del buf[:drop_n]
            try:
                from api.metrics import metrics
                metrics.mark_microstructure(False, 'backpressure_drop')
            except Exception:
                pass

    async def _flush_buffer(self, name: str):
        buffer = self.buffers[name]
        if not buffer.records:
            return
        rows = buffer.build_rows()
        async with self.pool.acquire() as conn:
            try:
                await conn.executemany(buffer.definition.sql, rows)
            except Exception as exc:
                logger.error("Flush failed for %s: %s", name, exc)
            finally:
                buffer.clear()

    async def flush_books(self):
        await self._flush_buffer('book')

    async def flush_marks(self):
        await self._flush_buffer('mark')

    async def flush_oi(self):
        await self._flush_buffer('oi')

    async def flush_obi_snapshots(self):
        await self._flush_buffer('obi')

    async def flush_indicators(self):
        await self._flush_buffer('indicator')

    async def flush_signals(self):
        await self._flush_buffer('signal')

    async def flush_all(self):
        for name in self.buffers.keys():
            await self._flush_buffer(name)
        
    async def auto_flush_loop(self):
        self.running = True
        try:
            while self.running:
                try:
                    await asyncio.sleep(self.flush_interval)
                except asyncio.CancelledError:
                    break
                await self.flush_all()
        finally:
            await self.flush_all()
            
    async def start(self):
        await self.initialize()
        if self._auto_task is None:
            self._auto_task = asyncio.create_task(self.auto_flush_loop())

    async def stop(self):
        self.running = False
        if self._auto_task is not None:
            self._auto_task.cancel()
            await asyncio.gather(self._auto_task, return_exceptions=True)
            self._auto_task = None
        await self.close()
