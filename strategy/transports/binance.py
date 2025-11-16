import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ingest.binance_rest import BinanceAPIError, BinanceRESTClient

from strategy.execution_types import OrderTicket


__all__ = ["BinanceTransport", "SymbolInfo", "BinanceAPIError"]


@dataclass
class SymbolInfo:
    symbol: str
    price_precision: Optional[int]
    quantity_precision: Optional[int]
    contract_size: Optional[float]
    price_tick: Optional[float]
    amount_step: Optional[float]
    raw: Dict[str, Any]


class BinanceTransport:
    """Thin adapter around Binance REST with typed responses and retries."""

    def __init__(self) -> None:
        self._rest: Optional[BinanceRESTClient] = None
        self._lock = asyncio.Lock()

    def _client(self) -> BinanceRESTClient:
        if self._rest is None:
            self._rest = BinanceRESTClient()
        return self._rest

    async def fetch_symbol_info(self, symbol: str) -> Optional[SymbolInfo]:
        rest = self._client()
        data = await rest.get("/fapi/v1/exchangeInfo", params={"symbol": symbol})
        if not isinstance(data, dict):
            return None
        symbols = data.get("symbols") or []
        if not symbols:
            return None
        payload = symbols[0]
        return self._parse_symbol_info(payload)

    async def place_limit_order(
        self,
        symbol: str,
        side: str,
        price: float,
        qty: float,
        tif: str,
    ) -> Optional[OrderTicket]:
        rest = self._client()
        params = {
            "symbol": symbol,
            "side": side.upper(),
            "type": "LIMIT",
            "timeInForce": tif,
            "quantity": str(qty),
            "price": str(price),
            "newOrderRespType": "RESULT",
        }
        data = await rest.post("/fapi/v1/order", params=params, signed=True)
        return self._parse_order_ack(data)

    async def place_stop_market_order(
        self,
        symbol: str,
        side: str,
        stop_price: float,
        qty: float,
    ) -> Optional[OrderTicket]:
        rest = self._client()
        params = {
            "symbol": symbol,
            "side": side.upper(),
            "type": "STOP_MARKET",
            "stopPrice": str(stop_price),
            "quantity": str(qty),
            "newOrderRespType": "RESULT",
        }
        data = await rest.post("/fapi/v1/order", params=params, signed=True)
        return self._parse_order_ack(data)

    async def cancel_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        client_order_id: Optional[str] = None,
    ) -> None:
        if order_id is None and not client_order_id:
            raise ValueError("Either order_id or client_order_id must be provided")
        rest = self._client()
        params: Dict[str, Any] = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = order_id
        if client_order_id:
            params["origClientOrderId"] = client_order_id
        await rest.delete("/fapi/v1/order", params=params, signed=True)

    async def fetch_open_orders(self, symbol: str) -> List[OrderTicket]:
        rest = self._client()
        payload = await rest.get(
            "/fapi/v1/openOrders",
            params={"symbol": symbol},
            signed=True,
        )
        if not isinstance(payload, list):
            return []
        orders: List[OrderTicket] = []
        for item in payload:
            ticket = self._parse_order_ack(item)
            if ticket:
                orders.append(ticket)
        return orders

    async def cancel_all_orders(self, symbol: str) -> None:
        rest = self._client()
        await rest.delete("/fapi/v1/allOpenOrders", params={"symbol": symbol}, signed=True)

    async def fetch_position_qty(self, symbol: str) -> Optional[float]:
        rest = self._client()
        data = await rest.get(
            "/fapi/v2/positionRisk",
            params={"symbol": symbol},
            signed=True,
        )
        if not isinstance(data, list):
            return None
        for pos in data:
            if pos.get("symbol") != symbol:
                continue
            amt = pos.get("positionAmt")
            if amt is None:
                continue
            try:
                return float(amt)
            except Exception:
                continue
        return None

    async def fetch_account_equity(self) -> Optional[float]:
        rest = self._client()
        data = await rest.get("/fapi/v2/account", signed=True)
        if not isinstance(data, dict):
            return None
        total_wallet = data.get("totalWalletBalance")
        if total_wallet is not None:
            try:
                return float(total_wallet)
            except Exception:
                pass
        assets = data.get("assets") or []
        for asset in assets:
            if asset.get("asset") != "USDT":
                continue
            bal = asset.get("walletBalance") or asset.get("marginBalance")
            if bal is None:
                continue
            try:
                return float(bal)
            except Exception:
                continue
        return None

    async def close(self) -> None:
        async with self._lock:
            if self._rest:
                try:
                    await self._rest.close()
                finally:
                    self._rest = None

    def _parse_symbol_info(self, payload: Dict[str, Any]) -> SymbolInfo:
        price_precision = self._as_int(payload.get("pricePrecision"))
        qty_precision = self._as_int(payload.get("quantityPrecision") or payload.get("baseAssetPrecision"))
        contract_size = self._as_float(payload.get("contractSize"))
        price_tick = None
        amount_step = None
        for filt in payload.get("filters", []):
            ftype = filt.get("filterType")
            if ftype == "PRICE_FILTER" and price_tick is None:
                price_tick = self._as_float(filt.get("tickSize"))
            elif ftype == "LOT_SIZE" and amount_step is None:
                amount_step = self._as_float(filt.get("stepSize"))
        return SymbolInfo(
            symbol=payload.get("symbol"),
            price_precision=price_precision,
            quantity_precision=qty_precision,
            contract_size=contract_size,
            price_tick=price_tick,
            amount_step=amount_step,
            raw=payload,
        )

    def _parse_order_ack(self, payload: Any) -> Optional[OrderTicket]:
        if not isinstance(payload, dict):
            return None
        price = self._as_float(payload.get("price"))
        stop_price = self._as_float(payload.get("stopPrice"))
        qty_val = payload.get("origQty") or payload.get("cumQty") or payload.get("quantity")
        quantity = self._as_float(qty_val) or 0.0
        return OrderTicket(
            symbol=payload.get("symbol", ""),
            side=(payload.get("side") or "").upper(),
            type=payload.get("type") or "LIMIT",
            quantity=quantity,
            status=payload.get("status"),
            price=price,
            stop_price=stop_price,
            client_order_id=payload.get("clientOrderId"),
            exchange_order_id=self._as_int(payload.get("orderId")),
            raw=payload,
        )

    @staticmethod
    def _as_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _as_int(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
