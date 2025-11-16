import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from config import config
from ingest.binance_rest import BinanceRESTClient, BinanceAPIError
from api.metrics import metrics


logger = logging.getLogger(__name__)


@dataclass
class MarketMetadataCache:
    default_tick_size: float
    default_contract_multiplier: float
    data: Dict[str, Any] = field(default_factory=dict)

    def update(self, info: Dict[str, Any]) -> None:
        precision: Dict[str, int] = {}
        price_precision = info.get("pricePrecision")
        qty_precision = info.get("quantityPrecision") or info.get("baseAssetPrecision")
        if price_precision is not None:
            try:
                precision["price"] = int(price_precision)
            except Exception:
                pass
        if qty_precision is not None:
            try:
                precision["amount"] = int(qty_precision)
            except Exception:
                pass

        limits = {"price": {}, "amount": {}}
        for filt in info.get("filters", []):
            ftype = filt.get("filterType")
            if ftype == "PRICE_FILTER":
                tick = filt.get("tickSize")
                if tick is not None:
                    try:
                        limits["price"]["min"] = float(tick)
                    except Exception:
                        pass
            elif ftype == "LOT_SIZE":
                step = filt.get("stepSize")
                if step is not None:
                    try:
                        limits["amount"]["min"] = float(step)
                    except Exception:
                        pass

        meta: Dict[str, Any] = {"precision": precision, "limits": limits}
        contract_size = info.get("contractSize")
        if contract_size is not None:
            try:
                meta["contractSize"] = float(contract_size)
            except Exception:
                pass
        self.data = meta

    def _precision_tick(self, key: str) -> float:
        precision = self.data.get("precision", {}).get(key)
        if precision is None:
            return 0.0
        try:
            return round(10 ** -precision, 10)
        except Exception:
            return 0.0

    def get_tick_size(self) -> float:
        tick = self.data.get("limits", {}).get("price", {}).get("min")
        if tick:
            try:
                return float(tick)
            except Exception:
                pass
        precision_tick = self._precision_tick("price")
        if precision_tick:
            return precision_tick
        return self.default_tick_size

    def get_contract_multiplier(self) -> float:
        contract_size = self.data.get("contractSize")
        if contract_size:
            try:
                return float(contract_size)
            except Exception:
                pass
        return self.default_contract_multiplier

    def get_amount_step(self) -> float:
        step = self.data.get("limits", {}).get("amount", {}).get("min")
        if step:
            try:
                return float(step)
            except Exception:
                pass
        precision_tick = self._precision_tick("amount")
        if precision_tick:
            return precision_tick
        return 0.0


class PaperPortfolio:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self._equity = 1000.0
        self._orders: Dict[str, Dict[str, Any]] = {}
        self._positions: Dict[str, Dict[str, Any]] = {}

    @property
    def equity(self) -> float:
        return self._equity

    def update_equity(self, pnl: float) -> None:
        self._equity += pnl

    def create_order(self, order_type: str, side: str, qty: float, **details) -> Optional[Dict]:
        if qty <= 0:
            return None
        oid = f"paper-{uuid.uuid4().hex[:8]}"
        order = {
            "id": oid,
            "symbol": self.symbol,
            "type": order_type,
            "side": side.upper(),
            "status": "open",
            **details,
        }
        direction = 'long' if side.lower() == 'buy' else 'short'
        self._orders[oid] = order
        self._positions[oid] = {"qty": qty, "side": direction}
        return order

    def remove_position(self, order_id: str) -> None:
        self._positions.pop(order_id, None)
        self._orders.pop(order_id, None)

    def cancel_order(self, order_id: str) -> bool:
        removed = order_id in self._orders
        self.remove_position(order_id)
        return removed

    def cancel_all(self) -> int:
        count = len(self._orders)
        self._orders.clear()
        self._positions.clear()
        return count

    def net_position(self) -> float:
        qty = 0.0
        for pos in self._positions.values():
            side = pos.get("side")
            amount = float(pos.get("qty") or 0)
            qty += amount if side == "long" else -amount
        return qty

    def get_position_qty(self, order_id: str) -> float:
        if order_id in self._positions:
            return float(self._positions[order_id].get("qty") or 0.0)
        return 0.0


class BinanceTransport:
    def __init__(self):
        self._rest: Optional[BinanceRESTClient] = None

    def _client(self) -> BinanceRESTClient:
        if self._rest is None:
            self._rest = BinanceRESTClient()
        return self._rest

    async def fetch_exchange_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        rest = self._client()
        data = await rest.get("/fapi/v1/exchangeInfo", params={"symbol": symbol})
        if not isinstance(data, dict):
            return None
        symbols = data.get("symbols") or []
        if not symbols:
            return None
        return symbols[0]

    async def place_limit_order(self, symbol: str, side: str, price: float, qty: float, tif: str) -> Optional[Dict]:
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
        return await rest.post("/fapi/v1/order", params=params, signed=True)

    async def place_stop_market_order(self, symbol: str, side: str, stop_price: float, qty: float) -> Optional[Dict]:
        rest = self._client()
        params = {
            "symbol": symbol,
            "side": side.upper(),
            "type": "STOP_MARKET",
            "stopPrice": str(stop_price),
            "quantity": str(qty),
            "newOrderRespType": "RESULT",
        }
        return await rest.post("/fapi/v1/order", params=params, signed=True)

    async def cancel_order(self, symbol: str, order_id: str) -> None:
        rest = self._client()
        await rest.delete(
            "/fapi/v1/order",
            params={"symbol": symbol, "orderId": order_id},
            signed=True,
        )

    async def fetch_open_orders(self, symbol: str) -> List[Dict[str, Any]]:
        rest = self._client()
        orders = await rest.get(
            "/fapi/v1/openOrders",
            params={"symbol": symbol},
            signed=True,
        )
        return orders if isinstance(orders, list) else []

    async def cancel_all_orders(self, symbol: str) -> None:
        rest = self._client()
        await rest.delete(
            "/fapi/v1/allOpenOrders",
            params={"symbol": symbol},
            signed=True,
        )

    async def fetch_position_qty(self, symbol: str) -> Optional[float]:
        rest = self._client()
        data = await rest.get(
            "/fapi/v2/positionRisk",
            params={"symbol": symbol},
            signed=True,
        )
        positions = data if isinstance(data, list) else []
        for pos in positions:
            if pos.get("symbol") != symbol:
                continue
            amt = pos.get("positionAmt")
            if amt is not None:
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

    async def close(self):
        if self._rest:
            try:
                await self._rest.close()
            except Exception:
                pass


class ExecutionManager:
    """Manage live and paper execution against Binance futures."""

    def __init__(self):
        self.symbol = config.exchange["symbol"]
        self.post_only = config.execution["post_only"]
        self.retry_delay = config.execution["retry_delay_s"]
        self.retry_tick_offset = config.execution["retry_tick_offset"]
        # Paper-trading mode when testnet is enabled or on init failure
        self.paper_mode = bool(config.exchange.get("testnet", False))
        self.transport = BinanceTransport()
        self.paper = PaperPortfolio(self.symbol)
        self.metadata = MarketMetadataCache(
            default_tick_size=float(config.exchange.get("tick_size", 0.1)),
            default_contract_multiplier=float(config.exchange.get("contract_multiplier", 1.0)),
        )

    async def initialize(self):
        if self.paper_mode:
            return

        try:
            await self._load_market_meta()
        except Exception as e:
            logger.error("Init failed; switching to paper mode: %s", e)
            self.paper_mode = True
            self.metadata.data = {}

    async def _load_market_meta(self):
        info = await self.transport.fetch_exchange_info(self.symbol)
        if info:
            self.metadata.update(info)

    async def place_limit_order(self, side: str, price: float, qty: float) -> Optional[Dict]:
        if qty <= 0:
            return None
        if self.paper_mode:
            return self.paper.create_order(
                "limit",
                side,
                qty,
                price=price,
                amount=qty,
            )

        try:
            tif = "GTX" if self.post_only else "GTC"
            data = await self.transport.place_limit_order(self.symbol, side, price, qty, tif)
            if isinstance(data, dict):
                return data
            return None
        except BinanceAPIError as e:
            if e.code == -2021 and self.post_only:
                await asyncio.sleep(self.retry_delay)
                tick_size = self.get_tick_size()
                adjusted_price = (
                    price - self.retry_tick_offset * tick_size
                    if side.lower() == "buy"
                    else price + self.retry_tick_offset * tick_size
                )
                return await self.place_limit_order(side, adjusted_price, qty)
            logger.error("Order placement error: %s", e)
            return None
        except Exception as e:
            logger.exception("Unexpected error while placing limit order: %s", e)
            return None

    async def place_stop_market_order(self, side: str, stop_price: float, qty: float) -> Optional[Dict]:
        if qty <= 0:
            return None
        if self.paper_mode:
            return self.paper.create_order(
                "stop_market",
                side,
                qty,
                stopPrice=stop_price,
                amount=qty,
            )

        try:
            data = await self.transport.place_stop_market_order(
                self.symbol,
                side,
                stop_price,
                qty,
            )
            if isinstance(data, dict):
                return data
            return None
        except Exception as e:
            logger.error("Stop-market order error: %s", e)
            return None

    async def cancel_order(self, order_id: str) -> bool:
        if self.paper_mode:
            return self.paper.cancel_order(order_id)
        try:
            await self.transport.cancel_order(self.symbol, order_id)
            return True
        except Exception as e:
            logger.error("Cancel order error for %s: %s", order_id, e)
            return False

    async def cancel_all_open_orders(self) -> int:
        cancelled = 0
        if self.paper_mode:
            cancelled = self.paper.cancel_all()
            logger.info("Paper cancel-all completed: %s orders", cancelled)
            return cancelled

        try:
            orders = await self.transport.fetch_open_orders(self.symbol)
            for order in orders:
                try:
                    oid = order.get("orderId") or order.get("clientOrderId")
                    if not oid:
                        continue
                    await self.transport.cancel_order(self.symbol, oid)
                    cancelled += 1
                except Exception as ce:
                    logger.error("Cancel failed for %s: %s", order.get("orderId"), ce)
            logger.info("Cancel-all completed: %s orders", cancelled)
            return cancelled
        except Exception as e:
            try:
                await self.transport.cancel_all_orders(self.symbol)
                logger.warning("allOpenOrders invoked for symbol %s", self.symbol)
                return -1
            except Exception as e2:
                logger.error(
                    "Cancel-all error: %s; fallback failed: %s",
                    e,
                    e2,
                )
                return cancelled

    async def fetch_position_qty(self) -> Optional[float]:
        if self.paper_mode:
            return self.paper.net_position()
        try:
            return await self.transport.fetch_position_qty(self.symbol)
        except Exception as e:
            logger.error("Fetch position failed: %s", e)
            return None

    async def get_account_equity(self) -> Optional[float]:
        if self.paper_mode:
            return self.paper.equity
        try:
            return await self.transport.fetch_account_equity()
        except Exception as e:
            logger.error("Balance fetch error: %s", e)
            return None

    async def close(self):
        await self.transport.close()

    def get_tick_size(self) -> float:
        return self.metadata.get_tick_size()

    def get_contract_multiplier(self) -> float:
        return self.metadata.get_contract_multiplier()

    def get_amount_step(self) -> float:
        return self.metadata.get_amount_step()

    def get_paper_position_qty(self, order_id: str) -> float:
        return self.paper.get_position_qty(order_id)

    def update_paper_equity(self, pnl: float):
        if self.paper_mode:
            self.paper.update_equity(pnl)
            logger.info(
                "Paper equity updated to %.2f (PnL: %.2f)",
                self.paper.equity,
                pnl,
            )
            metrics.record_pnl(pnl)
            metrics.update_equity(self.paper.equity)

    def remove_paper_position(self, order_id: str):
        if self.paper_mode:
            self.paper.remove_position(order_id)
