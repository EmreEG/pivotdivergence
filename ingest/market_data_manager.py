import asyncio
import logging
from typing import Awaitable, Callable, Dict, Mapping, Optional

from ingest.websocket_client import WebSocketClient
from ingest.rest_poller import RESTPoller

logger = logging.getLogger(__name__)

Handler = Callable[[Dict], Awaitable[None]]


class MarketDataManager:
    """Route websocket and REST payloads to registered async handlers."""

    _WS_EVENT_MAP = {
        'trade': 'trade',
        'orderbook': 'orderbook',
        'ticker': 'ticker',
        'gap_detected': 'gap',
        'latency_alert': 'latency',
        'dropped_event': 'drop',
        'kill_switch': 'kill_switch',
        'user_order': 'user_order',
    }

    _ALIASES = {
        'trade_handler': 'trade',
        'orderbook_handler': 'orderbook',
        'ticker_handler': 'ticker',
        'oi_handler': 'oi',
        'gap_handler': 'gap',
        'latency_handler': 'latency',
        'drop_handler': 'drop',
        'kill_switch_handler': 'kill_switch',
        'user_order_handler': 'user_order',
    }

    def __init__(self, symbol: str, ws_client: WebSocketClient, rest_poller: RESTPoller):
        self.symbol = symbol
        self.ws_client = ws_client
        self.rest_poller = rest_poller
        self._handlers: Dict[str, Handler] = {}

        self._register_ws_handlers()
        self._register_rest_handlers()

    def _register_ws_handlers(self) -> None:
        for ws_event, logical_name in self._WS_EVENT_MAP.items():
            self.ws_client.register_handler(ws_event, self._build_dispatcher(logical_name))

    def _register_rest_handlers(self) -> None:
        self.rest_poller.register_oi_handler(self._build_dispatcher('oi'))

    def register_handlers(self, **handlers: Handler) -> None:
        """Register async callbacks per logical event name."""
        normalized = self._normalize_handlers(handlers)
        for name, handler in normalized.items():
            if handler is None:
                continue
            if not asyncio.iscoroutinefunction(handler) and not asyncio.iscoroutinefunction(getattr(handler, '__call__', None)):
                # Bound async methods report as regular functions; defer to runtime check
                pass
            self._handlers[name] = handler

    def _normalize_handlers(self, handlers: Mapping[str, Handler]) -> Dict[str, Optional[Handler]]:
        normalized: Dict[str, Optional[Handler]] = {}
        for key, handler in handlers.items():
            logical = self._ALIASES.get(key, key)
            normalized[logical] = handler
        return normalized

    def _build_dispatcher(self, logical_name: str) -> Handler:
        async def _dispatch(payload: Dict):
            handler = self._handlers.get(logical_name)
            if not handler:
                return
            try:
                await handler(payload)
            except Exception:
                logger.exception("Market data handler %s failed", logical_name)

        return _dispatch

    async def start(self):
        await self.ws_client.start()
        await self.rest_poller.start()

    async def stop(self):
        await self.ws_client.stop()
        await self.rest_poller.stop()
