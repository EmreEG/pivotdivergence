import asyncio
import logging
from datetime import datetime

from config import config
from .binance_rest import BinanceRESTClient


logger = logging.getLogger(__name__)


class RESTPoller:
    def __init__(self, symbol: str = None):
        self.symbol = symbol or config.exchange["symbol"]
        self.running = False
        self.oi_handler = None
        self.fail_count = 0
        self.max_fails = 3
        self._rest = BinanceRESTClient()

    async def initialize(self):
        return

    def register_oi_handler(self, handler):
        self.oi_handler = handler

    async def poll_open_interest(self, interval_s: int = 300):
        try:
            while self.running:
                try:
                    raw = await self._rest.get("/fapi/v1/openInterest", params={"symbol": self.symbol})
                    ts = None
                    oi_val = None
                    if isinstance(raw, dict):
                        ts = raw.get("time")
                        oi_val = raw.get("openInterest")
                    timestamp = int(ts) if ts is not None else int(datetime.utcnow().timestamp() * 1000)
                    try:
                        oi_value = float(oi_val) if oi_val is not None else 0.0
                    except Exception:
                        oi_value = 0.0

                    if self.oi_handler:
                        await self.oi_handler(
                            {
                                "symbol": self.symbol,
                                "timestamp": timestamp,
                                "openInterest": oi_value,
                            }
                        )

                    self.fail_count = 0

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    self.fail_count += 1
                    logger.warning(
                        "Open interest REST fetch failed (%s/%s): %s",
                        self.fail_count,
                        self.max_fails,
                        e,
                    )
                    if self.fail_count >= self.max_fails:
                        # In paper/testnet mode, keep retrying and logging but never trigger kill-switch.
                        if not config.exchange.get("testnet", False):
                            logger.error(
                                "Open interest fetch failed %s times; triggering kill-switch handler",
                                self.fail_count,
                            )
                            if self.oi_handler:
                                await self.oi_handler(None)

                try:
                    await asyncio.sleep(interval_s)
                except asyncio.CancelledError:
                    break
        finally:
            self.running = False

    async def start(self):
        self.running = True
        await self.initialize()
        try:
            await self.poll_open_interest()
        finally:
            await self._rest.close()

    async def stop(self):
        self.running = False
        await self._rest.close()
