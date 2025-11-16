import asyncio
import asyncpg
import json
import logging
from typing import Dict, Any
from datetime import datetime
from config import config


logger = logging.getLogger(__name__)


class DataPersister:
    def __init__(self):
        self.pool = None
        self.book_buffer = []
        self.mark_buffer = []
        self.oi_buffer = []
        self.indicator_buffer = []
        self.signal_buffer = []
        self.obi_buffer = []
        self._trade_warning_emitted = False
        
        self.batch_size = 500
        self.flush_interval = 5
        self.max_buffer_size = 5000
        self.running = False
        self._auto_task = None
        
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
        self.book_buffer.append(snapshot)
        self._enforce_bounds('book')
        if len(self.book_buffer) >= self.batch_size:
            await self.flush_books()
            
    async def insert_mark_price(self, mark: Dict[str, Any]):
        self.mark_buffer.append(mark)
        self._enforce_bounds('mark')
        if len(self.mark_buffer) >= self.batch_size:
            await self.flush_marks()
            
    async def insert_open_interest(self, oi: Dict[str, Any]):
        self.oi_buffer.append(oi)
        self._enforce_bounds('oi')
        if len(self.oi_buffer) >= self.batch_size:
            await self.flush_oi()
            
    async def insert_indicator_state(self, indicator: Dict[str, Any]):
        self.indicator_buffer.append(indicator)
        self._enforce_bounds('indicator')
        if len(self.indicator_buffer) >= self.batch_size:
            await self.flush_indicators()

    async def insert_signal(self, signal: Dict[str, Any]):
        self.signal_buffer.append(signal)
        self._enforce_bounds('signal')
        if len(self.signal_buffer) >= self.batch_size:
            await self.flush_signals()

    async def insert_obi_snapshot(self, obi_snapshot: Dict[str, Any]):
        self.obi_buffer.append(obi_snapshot)
        self._enforce_bounds('obi')
        if len(self.obi_buffer) >= self.batch_size:
            await self.flush_obi_snapshots()

    def _enforce_bounds(self, which: str):
        buf = None
        if which == 'book':
            buf = self.book_buffer
        elif which == 'mark':
            buf = self.mark_buffer
        elif which == 'oi':
            buf = self.oi_buffer
        elif which == 'indicator':
            buf = self.indicator_buffer
        elif which == 'signal':
            buf = self.signal_buffer
        elif which == 'obi':
            buf = self.obi_buffer
        if buf is None:
            return
        if len(buf) > self.max_buffer_size:
            # Drop oldest 20% to relieve pressure
            drop_n = max(int(self.max_buffer_size * 0.2), 1)
            del buf[:drop_n]
            try:
                # Mark degradation explicitly
                from api.metrics import metrics
                metrics.mark_microstructure(False, 'backpressure_drop')
            except Exception:
                pass
            
    async def flush_books(self):
        if not self.book_buffer:
            return
            
        async with self.pool.acquire() as conn:
            await conn.executemany(
                '''INSERT INTO book_snapshots (symbol, ts, bids_json, asks_json, last_update_id)
                   VALUES ($1, $2, $3, $4, $5)
                   ON CONFLICT (symbol, ts) DO NOTHING''',
                [
                    (
                        b['symbol'],
                        datetime.fromtimestamp(b['timestamp'] / 1000),
                        json.dumps(b['bids']),
                        json.dumps(b['asks']),
                        int(b.get('last_update_id', 0)) if b.get('last_update_id') else 0
                    )
                    for b in self.book_buffer
                ]
            )
        self.book_buffer.clear()
        
    async def flush_marks(self):
        if not self.mark_buffer:
            return
            
        async with self.pool.acquire() as conn:
            await conn.executemany(
                '''INSERT INTO mark_price (symbol, ts, mark_price, funding_rate)
                   VALUES ($1, $2, $3, $4)
                   ON CONFLICT (symbol, ts) DO NOTHING''',
                [
                    (
                        m['symbol'],
                        datetime.fromtimestamp(m['timestamp'] / 1000),
                        m['mark_price'],
                        m.get('funding_rate')
                    )
                    for m in self.mark_buffer
                ]
            )
        self.mark_buffer.clear()
        
    async def flush_oi(self):
        if not self.oi_buffer:
            return
            
        async with self.pool.acquire() as conn:
            await conn.executemany(
                '''INSERT INTO open_interest (symbol, ts, oi_value)
                   VALUES ($1, $2, $3)
                   ON CONFLICT (symbol, ts) DO NOTHING''',
                [
                    (
                        o['symbol'],
                        datetime.fromtimestamp(o['timestamp'] / 1000),
                        o['openInterest']
                    )
                    for o in self.oi_buffer
                ]
            )
        self.oi_buffer.clear()

    async def flush_obi_snapshots(self):
        if not self.obi_buffer:
            return

        async with self.pool.acquire() as conn:
            try:
                await conn.executemany(
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
                    [
                        (
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
                            snap.get('spread')
                        )
                        for snap in self.obi_buffer
                    ]
                )
            except Exception as e:
                logger.error("OBI snapshot flush failed: %s", e)
        self.obi_buffer.clear()
        
    async def flush_indicators(self):
        if not self.indicator_buffer:
            return
            
        async with self.pool.acquire() as conn:
            await conn.executemany(
                '''INSERT INTO indicator_state 
                   (symbol, ts, rsi, macd, macd_signal, macd_hist, obv, ad_line, realized_vol, cvd, obi, obi_z, obi_bid_qty, obi_ask_qty, obi_depth_levels)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                   ON CONFLICT (symbol, ts) DO NOTHING''',
                [
                    (
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
                        i.get('obi_depth_levels')
                    )
                    for i in self.indicator_buffer
                ]
            )
        self.indicator_buffer.clear()
        
    async def flush_signals(self):
        if not self.signal_buffer:
            return
            
        async with self.pool.acquire() as conn:
            await conn.executemany(
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
                [
                    (
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
                        json.dumps(s.get('feature_vector', {}))
                    )
                    for s in self.signal_buffer
                ]
            )
        self.signal_buffer.clear()
        
    async def flush_all(self):
        await self.flush_books()
        await self.flush_marks()
        await self.flush_oi()
        await self.flush_obi_snapshots()
        await self.flush_indicators()
        await self.flush_signals()
        
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
