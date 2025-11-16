import asyncio
import logging
from typing import Dict, Callable

from ingest.websocket_client import WebSocketClient
from ingest.rest_poller import RESTPoller

logger = logging.getLogger(__name__)

class MarketDataManager:
    def __init__(self, symbol: str, ws_client: WebSocketClient, rest_poller: RESTPoller):
        self.symbol = symbol
        self.ws_client = ws_client
        self.rest_poller = rest_poller

        self._trade_handler: Callable[[Dict], asyncio.Task] = None
        self._orderbook_handler: Callable[[Dict], asyncio.Task] = None
        self._ticker_handler: Callable[[Dict], asyncio.Task] = None
        self._oi_handler: Callable[[Dict], asyncio.Task] = None
        self._gap_handler: Callable[[float], asyncio.Task] = None
        self._latency_handler: Callable[[float], asyncio.Task] = None
        self._drop_handler: Callable[[str], asyncio.Task] = None
        self._kill_switch_handler: Callable[[str], asyncio.Task] = None
        self._user_order_handler: Callable[[Dict], asyncio.Task] = None

        self._register_ws_handlers()
        self._register_rest_handlers()

    def _register_ws_handlers(self):
        handlers = [
            ('trade', self._handle_trade),
            ('orderbook', self._handle_orderbook),
            ('ticker', self._handle_ticker),
            ('gap_detected', self._handle_gap),
            ('latency_alert', self._handle_latency_event),
            ('dropped_event', self._handle_drop),
            ('kill_switch', self._handle_kill_switch),
            ('user_order', self._handle_user_order)
        ]
        for event_name, handler in handlers:
            self.ws_client.register_handler(event_name, handler)

    def _register_rest_handlers(self):
        self.rest_poller.register_oi_handler(self._handle_oi)

    def register_handlers(
        self,
        trade_handler: Callable[[Dict], asyncio.Task],
        orderbook_handler: Callable[[Dict], asyncio.Task],
        ticker_handler: Callable[[Dict], asyncio.Task],
        oi_handler: Callable[[Dict], asyncio.Task],
        gap_handler: Callable[[float], asyncio.Task],
        latency_handler: Callable[[float], asyncio.Task],
        drop_handler: Callable[[str], asyncio.Task],
        kill_switch_handler: Callable[[str], asyncio.Task],
        user_order_handler: Callable[[Dict], asyncio.Task],
    ):
        self._trade_handler = trade_handler
        self._orderbook_handler = orderbook_handler
        self._ticker_handler = ticker_handler
        self._oi_handler = oi_handler
        self._gap_handler = gap_handler
        self._latency_handler = latency_handler
        self._drop_handler = drop_handler
        self._kill_switch_handler = kill_switch_handler
        self._user_order_handler = user_order_handler

    async def _handle_trade(self, trade: Dict):
        if self._trade_handler:
            await self._trade_handler(trade)

    async def _handle_orderbook(self, orderbook: Dict):
        if self._orderbook_handler:
            await self._orderbook_handler(orderbook)

    async def _handle_ticker(self, ticker: Dict):
        if self._ticker_handler:
            await self._ticker_handler(ticker)

    async def _handle_oi(self, oi_data: Dict):
        if self._oi_handler:
            await self._oi_handler(oi_data)

    async def _handle_gap(self, gap_duration: float):
        if self._gap_handler:
            await self._gap_handler(gap_duration)

    async def _handle_latency_event(self, drift_ms: float):
        if self._latency_handler:
            await self._latency_handler(drift_ms)

    async def _handle_drop(self, reason: str):
        if self._drop_handler:
            await self._drop_handler(reason)

    async def _handle_kill_switch(self, reason: str):
        if self._kill_switch_handler:
            await self._kill_switch_handler(reason)

    async def _handle_user_order(self, order: Dict):
        if self._user_order_handler:
            await self._user_order_handler(order)

    async def start(self):
        await self.ws_client.start()
        await self.rest_poller.start()

    async def stop(self):
        await self.ws_client.stop()
        await self.rest_poller.stop()
