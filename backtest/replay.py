import asyncio
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from datetime import datetime

import asyncpg


@dataclass
class ReplayEvent:
    ts_ms: int
    type: str  # 'trade' | 'orderbook' | 'mark' | 'oi'
    payload: Dict[str, Any]


class ReplaySimulator:
    def __init__(self, trading_system):
        self.trading_system = trading_system
        self._pool: Optional[asyncpg.pool.Pool] = None
        self._events: List[ReplayEvent] = []

    async def _ensure_db(self):
        if self._pool is not None:
            return
        db = self.trading_system.persister
        await db.initialize()
        self._pool = db.pool

    async def load_from_db(self, symbol: str, start_iso: str, end_iso: str):
        await self._ensure_db()
        start_dt = datetime.fromisoformat(start_iso)
        end_dt = datetime.fromisoformat(end_iso)
        # Trades from datapython core.binance_trades
        dp_dsn = os.getenv(
            "DATAPYTHON_DATABASE_DSN",
            "postgresql://postgres:postgres@localhost:5432/market_data",
        )
        dp_conn = await asyncpg.connect(dsn=dp_dsn)
        try:
            trades_rows = await dp_conn.fetch(
                """
                SELECT
                    EXTRACT(EPOCH FROM trade_time)*1000 AS ts_ms,
                    price,
                    quantity AS qty,
                    CASE WHEN is_buyer_maker THEN 'sell' ELSE 'buy' END AS aggressor_side,
                    trade_id
                FROM core.binance_trades
                WHERE symbol = $1 AND trade_time >= $2 AND trade_time <= $3
                ORDER BY trade_time
                """,
                symbol,
                start_dt,
                end_dt,
            )
        finally:
            await dp_conn.close()

        # Book / mark / OI from local pivotdivergence schema
        async with self._pool.acquire() as conn:
            book_rows = await conn.fetch(
                """
                SELECT EXTRACT(EPOCH FROM ts)*1000 AS ts_ms, bids_json, asks_json, NULL::bigint AS last_update_id
                FROM book_snapshots
                WHERE symbol = $1 AND ts >= $2 AND ts <= $3
                ORDER BY ts
                """,
                symbol,
                start_dt,
                end_dt,
            )
            mark_rows = await conn.fetch(
                """
                SELECT EXTRACT(EPOCH FROM ts)*1000 AS ts_ms, mark_price, funding_rate
                FROM mark_price
                WHERE symbol = $1 AND ts >= $2 AND ts <= $3
                ORDER BY ts
                """,
                symbol,
                start_dt,
                end_dt,
            )
            oi_rows = await conn.fetch(
                """
                SELECT EXTRACT(EPOCH FROM ts)*1000 AS ts_ms, oi_value
                FROM open_interest
                WHERE symbol = $1 AND ts >= $2 AND ts <= $3
                ORDER BY ts
                """,
                symbol,
                start_dt,
                end_dt,
            )

        events: List[ReplayEvent] = []
        for r in trades_rows:
            events.append(
                ReplayEvent(
                    ts_ms=int(r["ts_ms"]),
                    type="trade",
                    payload={
                        "symbol": symbol,
                        "timestamp": int(r["ts_ms"]),
                        "price": float(r["price"]),
                        "amount": float(r["qty"]),
                        "side": r["aggressor_side"] or "unknown",
                        "id": int(r["trade_id"]) if r["trade_id"] else None,
                    },
                )
            )
        for r in book_rows:
            events.append(
                ReplayEvent(
                    ts_ms=int(r["ts_ms"]),
                    type="orderbook",
                    payload={
                        "bids": r["bids_json"],
                        "asks": r["asks_json"],
                        "timestamp": int(r["ts_ms"]),
                    },
                )
            )
        for r in mark_rows:
            events.append(
                ReplayEvent(
                    ts_ms=int(r["ts_ms"]),
                    type="mark",
                    payload={
                        "timestamp": int(r["ts_ms"]),
                        "info": {
                            "markPrice": float(r["mark_price"]) if r["mark_price"] is not None else None,
                            "fundingRate": float(r["funding_rate"]) if r["funding_rate"] is not None else None,
                        },
                    },
                )
            )
        for r in oi_rows:
            events.append(
                ReplayEvent(
                    ts_ms=int(r["ts_ms"]),
                    type="oi",
                    payload={
                        "symbol": symbol,
                        "timestamp": int(r["ts_ms"]),
                        "openInterest": float(r["oi_value"]) if r["oi_value"] is not None else 0.0,
                    },
                )
            )

        events.sort(key=lambda e: e.ts_ms)
        self._events = events

    def load_from_list(self, events: List[Dict[str, Any]]):
        parsed: List[ReplayEvent] = []
        for ev in events:
            parsed.append(ReplayEvent(ts_ms=int(ev["ts_ms"]), type=ev["type"], payload=ev["payload"]))
        parsed.sort(key=lambda e: e.ts_ms)
        self._events = parsed

    async def replay(self, realtime: bool = False):
        if not self._events:
            return
        await self.trading_system.initialize()
        await self.trading_system.persister.start()
        self.trading_system.running = True

        t0 = self._events[0].ts_ms
        loop0 = asyncio.get_event_loop().time() * 1000.0

        for ev in self._events:
            if realtime:
                delta_ms = max(0, ev.ts_ms - t0)
                now_ms = (asyncio.get_event_loop().time() * 1000.0) - loop0
                sleep_ms = delta_ms - now_ms
                if sleep_ms > 0:
                    await asyncio.sleep(sleep_ms / 1000.0)

            if ev.type == "trade":
                await self.trading_system.market_events.handle_trade(ev.payload)
            elif ev.type == "orderbook":
                await self.trading_system.market_events.handle_orderbook(ev.payload)
            elif ev.type == "mark":
                await self.trading_system.market_events.handle_ticker(ev.payload)
            elif ev.type == "oi":
                await self.trading_system.market_events.handle_oi(ev.payload)

        await self.trading_system.persister.stop()
        self.trading_system.running = False
