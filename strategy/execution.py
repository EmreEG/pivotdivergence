import asyncio
import logging
import uuid
from typing import Dict, Optional

from config import config
from ingest.binance_rest import BinanceRESTClient, BinanceAPIError
from api.metrics import metrics


logger = logging.getLogger(__name__)


class ExecutionManager:
    """Manage live and paper execution against Binance futures."""
    def __init__(self):
        self.symbol = config.exchange["symbol"]
        self.post_only = config.execution["post_only"]
        self.retry_delay = config.execution["retry_delay_s"]
        self.retry_tick_offset = config.execution["retry_tick_offset"]
        # Paper-trading mode when testnet is enabled or on init failure
        self.paper_mode = bool(config.exchange.get("testnet", False))
        self._paper_equity = 1000.0
        self._paper_orders: Dict[str, Dict] = {}
        self._paper_positions: Dict[str, Dict] = {}
        self._market_meta: Dict = {}
        self._rest: Optional[BinanceRESTClient] = None

    def _ensure_rest_client(self) -> BinanceRESTClient:
        if self._rest is None:
            self._rest = BinanceRESTClient()
        return self._rest

    def _create_paper_order(self, order_type: str, side: str, qty: float, **details) -> Optional[Dict]:
        if qty <= 0:
            return None
        oid = f"paper-{uuid.uuid4().hex[:8]}"
        order: Dict = {
            "id": oid,
            "symbol": self.symbol,
            "type": order_type,
            "side": side,
            "status": "open",
            **details
        }
        self._paper_orders[oid] = order
        self._paper_positions[oid] = {"qty": qty, "side": side}
        return order

    async def initialize(self):
        if self.paper_mode:
            self._market_meta = {}
            return

        try:
            self._ensure_rest_client()
            await self._load_market_meta()
        except Exception as e:
            logger.error("Init failed; switching to paper mode: %s", e)
            self.paper_mode = True
            self._market_meta = {}
            self._rest = None

    async def _load_market_meta(self):
        rest = self._ensure_rest_client()
        data = await rest.get("/fapi/v1/exchangeInfo", params={"symbol": self.symbol})
        if not isinstance(data, dict):
            return
        symbols = data.get("symbols") or []
        if not symbols:
            return
        info = symbols[0]

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
        for f in info.get("filters", []):
            ftype = f.get("filterType")
            if ftype == "PRICE_FILTER":
                tick = f.get("tickSize")
                if tick is not None:
                    try:
                        limits["price"]["min"] = float(tick)
                    except Exception:
                        pass
            elif ftype == "LOT_SIZE":
                step = f.get("stepSize")
                if step is not None:
                    try:
                        limits["amount"]["min"] = float(step)
                    except Exception:
                        pass

        meta: Dict = {"precision": precision, "limits": limits}
        contract_size = info.get("contractSize")
        if contract_size is not None:
            try:
                meta["contractSize"] = float(contract_size)
            except Exception:
                pass
        self._market_meta = meta

    async def place_limit_order(self, side: str, price: float, qty: float) -> Optional[Dict]:
        if qty <= 0:
            return None
        if self.paper_mode:
            return self._create_paper_order(
                "limit",
                side,
                qty,
                price=price,
                amount=qty
            )

        try:
            rest = self._ensure_rest_client()
            tif = "GTX" if self.post_only else "GTC"
            params = {
                "symbol": self.symbol,
                "side": side.upper(),
                "type": "LIMIT",
                "timeInForce": tif,
                "quantity": str(qty),
                "price": str(price),
                "newOrderRespType": "RESULT",
            }
            data = await rest.post("/fapi/v1/order", params=params, signed=True)
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
            return self._create_paper_order(
                "stop_market",
                side,
                qty,
                stopPrice=stop_price,
                amount=qty
            )

        try:
            rest = self._ensure_rest_client()
            params = {
                "symbol": self.symbol,
                "side": side.upper(),
                "type": "STOP_MARKET",
                "stopPrice": str(stop_price),
                "quantity": str(qty),
                "newOrderRespType": "RESULT",
            }
            data = await rest.post("/fapi/v1/order", params=params, signed=True)
            if isinstance(data, dict):
                return data
            return None
        except Exception as e:
            logger.error("Stop-market order error: %s", e)
            return None

    async def cancel_order(self, order_id: str) -> bool:
        if self.paper_mode:
            removed = self._paper_orders.pop(order_id, None) is not None
            if removed:
                self._paper_positions.pop(order_id, None)
            return removed
        try:
            rest = self._ensure_rest_client()
            await rest.delete(
                "/fapi/v1/order",
                params={"symbol": self.symbol, "orderId": order_id},
                signed=True,
            )
            return True
        except Exception as e:
            logger.error("Cancel order error for %s: %s", order_id, e)
            return False

    async def cancel_all_open_orders(self) -> int:
        cancelled = 0
        if self.paper_mode:
            ids = list(self._paper_orders.keys())
            for oid in ids:
                removed = self._paper_orders.pop(oid, None) is not None
                if removed:
                    self._paper_positions.pop(oid, None)
                    cancelled += 1
            logger.info("Paper cancel-all completed: %s orders", cancelled)
            return cancelled

        try:
            rest = self._ensure_rest_client()
            orders = await rest.get(
                "/fapi/v1/openOrders",
                params={"symbol": self.symbol},
                signed=True,
            )
            if isinstance(orders, list):
                for o in orders:
                    try:
                        oid = o.get("orderId") or o.get("clientOrderId")
                        if oid:
                            await rest.delete(
                                "/fapi/v1/order",
                                params={"symbol": self.symbol, "orderId": oid},
                                signed=True,
                            )
                            cancelled += 1
                    except Exception as ce:
                        logger.error(
                            "Cancel failed for %s: %s",
                            o.get("orderId"),
                            ce,
                        )
            logger.info("Cancel-all completed: %s orders", cancelled)
            return cancelled
        except Exception as e:
            try:
                rest = self._ensure_rest_client()
                await rest.delete(
                    "/fapi/v1/allOpenOrders",
                    params={"symbol": self.symbol},
                    signed=True,
                )
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
            qty = 0.0
            for pos in self._paper_positions.values():
                side = pos.get("side")
                amount = float(pos.get("qty") or 0)
                qty += amount if side == "long" else -amount
            return qty
        try:
            rest = self._ensure_rest_client()
            data = await rest.get(
                "/fapi/v2/positionRisk",
                params={"symbol": self.symbol},
                signed=True,
            )
            positions = data if isinstance(data, list) else []
            qty = 0.0
            for p in positions:
                if p.get("symbol") != self.symbol:
                    continue
                amt = p.get("positionAmt")
                if amt is not None:
                    qty = float(amt)
                    break
            return qty
        except Exception as e:
            logger.error("Fetch position failed: %s", e)
            return None

    async def get_account_equity(self) -> Optional[float]:
        if self.paper_mode:
            return self._paper_equity
        try:
            rest = self._ensure_rest_client()
            data = await rest.get("/fapi/v2/account", signed=True)
            if not isinstance(data, dict):
                return None
            total_wallet = data.get("totalWalletBalance")
            try:
                if total_wallet is not None:
                    return float(total_wallet)
            except Exception:
                pass
            assets = data.get("assets") or []
            for a in assets:
                if a.get("asset") == "USDT":
                    bal = a.get("walletBalance") or a.get("marginBalance")
                    if bal is not None:
                        try:
                            return float(bal)
                        except Exception:
                            continue
            return None
        except Exception as e:
            logger.error("Balance fetch error: %s", e)
            return None

    async def close(self):
        if self._rest:
            try:
                await self._rest.close()
            except Exception:
                pass

    def get_tick_size(self) -> float:
        market = self._market_meta or {}
        limits = market.get("limits", {}).get("price", {})
        tick = limits.get("min")
        if tick:
            return float(tick)
        precision = market.get("precision", {}).get("price")
        if precision is not None:
            try:
                return round(10 ** -precision, 10)
            except Exception:
                pass
        return float(config.exchange.get("tick_size", 0.1))

    def get_contract_multiplier(self) -> float:
        market = self._market_meta or {}
        contract_size = market.get("contractSize")
        if contract_size:
            return float(contract_size)
        return float(config.exchange.get("contract_multiplier", 1.0))

    def get_amount_step(self) -> float:
        market = self._market_meta or {}
        limits = market.get("limits", {}).get("amount", {})
        step = limits.get("min")
        if step:
            try:
                return float(step)
            except Exception:
                pass
        precision = market.get("precision", {}).get("amount")
        if precision is not None:
            try:
                return float(10 ** -precision)
            except Exception:
                pass
        return 0.0

    def get_paper_position_qty(self, order_id: str) -> float:
        if order_id in self._paper_positions:
            return self._paper_positions[order_id]["qty"]
        return 0.0

    def update_paper_equity(self, pnl: float):
        if self.paper_mode:
            self._paper_equity += pnl
            logger.info(
                "Paper equity updated to %.2f (PnL: %.2f)",
                self._paper_equity,
                pnl,
            )
            metrics.record_pnl(pnl)
            metrics.update_equity(self._paper_equity)

    def remove_paper_position(self, order_id: str):
        if self.paper_mode:
            self._paper_positions.pop(order_id, None)
