import asyncio
import json
import logging
import random
import time
from typing import Callable, Dict, Optional

import websockets

from config import config
from .binance_rest import BinanceRESTClient, BinanceAPIError
from monitoring.async_utils import run_tasks_with_cleanup


logger = logging.getLogger(__name__)


class WebSocketClient:
    def __init__(self, symbol: str = None):
        self.symbol = symbol or config.exchange["symbol"]
        self.reconnect_backoff = config.websocket["reconnect_backoff"]
        self.max_reconnects = config.websocket["max_reconnects_per_minute"]
        self.latency_threshold = config.websocket["latency_alert_threshold_ms"]
        self.latency_duration = config.websocket["latency_alert_duration_s"]
        self.ping_interval = config.websocket.get("ping_interval_s", 10)
        self.rotation_hours = config.websocket.get("rotation_hours", 23)
        self.stream_timeout = config.websocket.get("stream_stale_s", 5)
        self.liveness_grace = config.websocket.get("liveness_grace_s", 2)

        self.handlers: Dict[str, Callable] = {}
        self.running = False
        self.reconnect_count = 0
        self.last_reconnect_window = time.time()
        self.gap_start_ts: Optional[float] = None

        self.latency_violations = []
        # Exchange clock offset calibration (local_ms - exchange_ms)
        self.clock_offset_ms: float = 0.0
        self.last_clock_sync: float = 0.0
        self.clock_initialized: bool = False
        # Rate-limit sustained latency alerts
        self.last_latency_alert_ts: float = 0.0
        self.connection_started = time.time()
        mono_now = time.monotonic()
        self.stream_last_seen = {
            "trade": mono_now,
            "orderbook": mono_now,
            "ticker": mono_now,
            "account": mono_now,
        }
        self._ping_task: Optional[asyncio.Task] = None

        self._rest = BinanceRESTClient()
        self._listen_key: Optional[str] = None
        self._listen_key_last_refresh: float = 0.0

    async def initialize(self):
        # In paper/testnet mode, skip exchange clock calibration entirely.
        if not config.exchange.get("testnet", False):
            await self._calibrate_clock()
        # User data stream only needed for live trading; skip in paper mode.
        if not config.exchange.get("testnet", False):
            await self._ensure_listen_key()
        self.connection_started = time.time()

    async def _calibrate_clock(self):
        # Disable remote clock calibration; rely on local arrival timing.
        self.clock_offset_ms = 0.0
        self.clock_initialized = False
        self.last_clock_sync = time.time()

    async def _ensure_listen_key(self):
        if not self._rest.api_key:
            self._listen_key = None
            return
        now = time.time()
        # Refresh at most every 30 minutes
        if self._listen_key and (now - self._listen_key_last_refresh) < 30 * 60:
            return
        try:
            if not self._listen_key:
                data = await self._rest.post("/fapi/v1/listenKey")
                if isinstance(data, dict):
                    self._listen_key = data.get("listenKey")
                    self._listen_key_last_refresh = now
                    logger.info("Obtained listenKey for user data stream")
            else:
                await self._rest.put("/fapi/v1/listenKey", params={"listenKey": self._listen_key})
                self._listen_key_last_refresh = now
        except BinanceAPIError as e:
            logger.error("listenKey error: %s", e)
            if e.status == 401 or e.code in (-2014, -2015):
                self._listen_key = None
                self._rest.api_key = None
        except Exception as e:
            logger.error("listenKey error: %s", e)
            self._listen_key = None

    def register_handler(self, stream_type: str, handler: Callable):
        self.handlers[stream_type] = handler

    async def _handle_reconnect(self, backoff_index: int = 0):
        if backoff_index >= len(self.reconnect_backoff):
            backoff_index = len(self.reconnect_backoff) - 1

        now = time.time()
        if now - self.last_reconnect_window > 60:
            self.reconnect_count = 0
            self.last_reconnect_window = now

        self.reconnect_count += 1

        if self.reconnect_count > self.max_reconnects:
            logger.warning(
                "%s reconnects in 60s; entering degraded reconnect mode",
                self.reconnect_count,
            )
            if "kill_switch" in self.handlers and not config.exchange.get("testnet", False):
                await self.handlers["kill_switch"]("reconnect_limit")
            self.reconnect_count = 0
            self.last_reconnect_window = now
            delay = self.reconnect_backoff[-1] + random.uniform(0, 0.5)
            logger.info("Reconnecting in %.1fs (extended backoff)", delay)
            await asyncio.sleep(delay)
            return True

        delay = self.reconnect_backoff[backoff_index] + random.uniform(0, 0.5)
        logger.info("Reconnecting in %.1fs (attempt %s)", delay, self.reconnect_count)
        await asyncio.sleep(delay)

        return True

    def _rotation_due(self) -> bool:
        return (time.time() - self.connection_started) >= self.rotation_hours * 3600

    async def _check_latency(self, exchange_ts: int) -> bool:
        # Disable drift-based latency alerts; always accept events.
        return True

    async def subscribe_trades(self):
        backoff_index = 0
        stream = f"{self.symbol.lower()}@aggTrade"
        url = f"wss://fstream.binance.com/ws/{stream}"

        while self.running:
            try:
                async with websockets.connect(url, ping_interval=None) as ws:
                    while self.running:
                        if config.exchange.get("testnet", False):
                            raw = await ws.recv()
                        else:
                            timeout = self.stream_timeout + self.liveness_grace
                            try:
                                raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                            except asyncio.TimeoutError:
                                logger.warning("Trade stream stale; reconnecting")
                                self.gap_start_ts = time.time()
                                raise

                        data = json.loads(raw)
                        self.stream_last_seen["trade"] = time.monotonic()

                        if self.gap_start_ts is not None:
                            gap_duration = time.time() - self.gap_start_ts
                            logger.info(
                                "Trade stream reconnected after %.1fs gap",
                                gap_duration,
                            )
                            if gap_duration > 5 and "gap_detected" in self.handlers:
                                await self.handlers["gap_detected"](gap_duration)
                            self.gap_start_ts = None
                            backoff_index = 0

                        event_ts = data.get("T") or data.get("E")
                        if event_ts and not await self._check_latency(int(event_ts)):
                            continue

                        trade = {
                            "symbol": data.get("s"),
                            "price": float(data.get("p", 0.0)),
                            "amount": float(data.get("q", 0.0)),
                            "timestamp": int(data.get("T") or data.get("E") or 0),
                            "id": data.get("a"),
                            "info": data,
                        }

                        if "trade" in self.handlers:
                            await self.handlers["trade"](trade)

                        if self._rotation_due():
                            raise ConnectionError("rotation_due")

            except asyncio.CancelledError:
                break
            except asyncio.TimeoutError:
                if not await self._handle_reconnect(backoff_index):
                    break
                backoff_index = min(backoff_index + 1, len(self.reconnect_backoff) - 1)
                await self.initialize()
            except Exception as e:
                logger.error("Trade stream error: %s", e)
                self.gap_start_ts = time.time()
                if not await self._handle_reconnect(backoff_index):
                    break
                backoff_index = min(backoff_index + 1, len(self.reconnect_backoff) - 1)
                await self.initialize()
            else:
                backoff_index = 0

    async def subscribe_orderbook(self, limit: int = 20):
        backoff_index = 0
        depth = max(5, min(limit, 20))
        stream = f"{self.symbol.lower()}@depth{depth}@100ms"
        url = f"wss://fstream.binance.com/ws/{stream}"

        while self.running:
            try:
                async with websockets.connect(url, ping_interval=None) as ws:
                    while self.running:
                        if config.exchange.get("testnet", False):
                            raw = await ws.recv()
                        else:
                            timeout = self.stream_timeout + self.liveness_grace
                            try:
                                raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                            except asyncio.TimeoutError:
                                logger.warning("Orderbook stream stale; reconnecting")
                                self.gap_start_ts = time.time()
                                raise

                        data = json.loads(raw)
                        self.stream_last_seen["orderbook"] = time.monotonic()

                        if self.gap_start_ts is not None:
                            self.gap_start_ts = None
                            backoff_index = 0

                        event_ts = data.get("E") or data.get("T")
                        if event_ts and not await self._check_latency(int(event_ts)):
                            continue

                        orderbook = {
                            "bids": data.get("b") or [],
                            "asks": data.get("a") or [],
                            "lastUpdateId": data.get("u"),
                            "timestamp": data.get("E"),
                        }
                        if "orderbook" in self.handlers:
                            await self.handlers["orderbook"](orderbook)

                        if self._rotation_due():
                            raise ConnectionError("rotation_due")

            except asyncio.CancelledError:
                break
            except asyncio.TimeoutError:
                if not await self._handle_reconnect(backoff_index):
                    break
                backoff_index = min(backoff_index + 1, len(self.reconnect_backoff) - 1)
                await self.initialize()
            except Exception as e:
                logger.error("Orderbook stream error: %s", e)
                self.gap_start_ts = time.time()
                if not await self._handle_reconnect(backoff_index):
                    break
                backoff_index = min(backoff_index + 1, len(self.reconnect_backoff) - 1)
                await self.initialize()
            else:
                backoff_index = 0

    async def subscribe_ticker(self):
        backoff_index = 0
        stream = f"{self.symbol.lower()}@markPrice@1s"
        url = f"wss://fstream.binance.com/ws/{stream}"

        while self.running:
            try:
                async with websockets.connect(url, ping_interval=None) as ws:
                    while self.running:
                        if config.exchange.get("testnet", False):
                            raw = await ws.recv()
                        else:
                            timeout = self.stream_timeout + self.liveness_grace
                            try:
                                raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                            except asyncio.TimeoutError:
                                logger.warning("Ticker stream stale; reconnecting")
                                self.gap_start_ts = time.time()
                                raise

                        data = json.loads(raw)
                        self.stream_last_seen["ticker"] = time.monotonic()

                        if self.gap_start_ts is not None:
                            self.gap_start_ts = None
                            backoff_index = 0

                        event_ts = data.get("E") or data.get("T")
                        if event_ts and not await self._check_latency(int(event_ts)):
                            continue

                        ticker = {
                            "symbol": data.get("s"),
                            "timestamp": int(data.get("E") or 0),
                            "info": {
                                "markPrice": data.get("p"),
                                "fundingRate": data.get("r"),
                            },
                        }

                        if "ticker" in self.handlers:
                            await self.handlers["ticker"](ticker)

                        if self._rotation_due():
                            raise ConnectionError("rotation_due")

            except asyncio.CancelledError:
                break
            except asyncio.TimeoutError:
                if not await self._handle_reconnect(backoff_index):
                    break
                backoff_index = min(backoff_index + 1, len(self.reconnect_backoff) - 1)
                await self.initialize()
            except Exception as e:
                logger.error("Ticker stream error: %s", e)
                self.gap_start_ts = time.time()
                if not await self._handle_reconnect(backoff_index):
                    break
                backoff_index = min(backoff_index + 1, len(self.reconnect_backoff) - 1)
                await self.initialize()
            else:
                backoff_index = 0

    async def subscribe_account(self):
        backoff_index = 0

        if not self._listen_key:
            logger.info("No listenKey; skipping user data stream")
            return

        url = f"wss://fstream.binance.com/ws/{self._listen_key}"

        while self.running:
            try:
                async with websockets.connect(url, ping_interval=None) as ws:
                    while self.running:
                        timeout = max(self.stream_timeout * 2, 10)
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                        except asyncio.TimeoutError:
                            logger.warning("Account stream stale; reconnecting")
                            self.gap_start_ts = time.time()
                            raise

                        data = json.loads(raw)
                        self.stream_last_seen["account"] = time.monotonic()

                        event = data.get("data") if isinstance(data, dict) and "data" in data else data
                        if not isinstance(event, dict):
                            continue

                        event_ts = event.get("E") or event.get("T")
                        if event_ts and not await self._check_latency(int(event_ts)):
                            continue

                        if event.get("e") != "ORDER_TRADE_UPDATE":
                            continue

                        o = event.get("o") or {}
                        try:
                            avg_price_raw = o.get("ap")
                            avg_price: Optional[float]
                            if avg_price_raw in (None, "", "0", "0.0"):
                                avg_price = None
                            else:
                                avg_price = float(avg_price_raw)

                            order = {
                                "id": o.get("i"),
                                "orderId": o.get("i"),
                                "symbol": o.get("s"),
                                "status": o.get("X"),
                                "side": (o.get("S") or "").lower(),
                                "price": float(o.get("p", 0.0)),
                                "average": avg_price,
                                "amount": float(o.get("q", 0.0)),
                                "origQty": o.get("q"),
                                "executedQty": float(o.get("z", 0.0)),
                                "filled": float(o.get("z", 0.0)),
                                "info": event,
                            }
                        except Exception:
                            continue

                        if "user_order" in self.handlers:
                            await self.handlers["user_order"](order)

                        if self._rotation_due():
                            raise ConnectionError("rotation_due")

            except asyncio.CancelledError:
                break
            except asyncio.TimeoutError:
                if not await self._handle_reconnect(backoff_index):
                    if "kill_switch" in self.handlers:
                        await self.handlers["kill_switch"]("user_stream_lost")
                    break
                backoff_index = min(backoff_index + 1, len(self.reconnect_backoff) - 1)
                await self._ensure_listen_key()
                await self.initialize()
            except Exception as e:
                logger.error("Account stream error: %s", e)
                self.gap_start_ts = time.time()
                if not await self._handle_reconnect(backoff_index):
                    if "kill_switch" in self.handlers:
                        await self.handlers["kill_switch"]("user_stream_lost")
                    break
                backoff_index = min(backoff_index + 1, len(self.reconnect_backoff) - 1)
                await self._ensure_listen_key()
                await self.initialize()
            else:
                backoff_index = 0

    async def start(self):
        self.running = True
        await self.initialize()

        tasks = [
            asyncio.create_task(self.subscribe_trades()),
            asyncio.create_task(self.subscribe_orderbook()),
            asyncio.create_task(self.subscribe_ticker()),
            asyncio.create_task(self.subscribe_account()),
        ]
        self._ping_task = asyncio.create_task(self._ping_loop())
        tasks.append(self._ping_task)

        async def _cleanup():
            if self._ping_task:
                self._ping_task.cancel()
                await asyncio.gather(self._ping_task, return_exceptions=True)
                self._ping_task = None
            await self._rest.close()

        await run_tasks_with_cleanup(tasks, cleanup=_cleanup)

    async def stop(self):
        self.running = False
        await self._rest.close()

    async def fetch_orderbook_snapshot(self, depth: int = 200) -> Dict:
        params = {"symbol": self.symbol, "limit": depth}
        snapshot = await self._rest.get("/fapi/v1/depth", params=params)
        if isinstance(snapshot, dict):
            return snapshot
        raise RuntimeError("Invalid depth snapshot response")

    async def _ping_loop(self):
        while self.running:
            try:
                await asyncio.sleep(self.ping_interval)
                if not self.running:
                    continue
                # In live mode, ensure listenKey stays fresh; no-op in paper mode.
                if not config.exchange.get("testnet", False):
                    await self._ensure_listen_key()
            except asyncio.CancelledError:
                break
