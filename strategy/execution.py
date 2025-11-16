import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from config import config
from strategy.execution_types import OrderTicket
from strategy.simulators.paper import PaperTradingSimulator
from strategy.transports.binance import BinanceAPIError, BinanceTransport, SymbolInfo


logger = logging.getLogger(__name__)


@dataclass
class MarketMetadataCache:
    default_tick_size: float
    default_contract_multiplier: float
    data: Dict[str, Any] = field(default_factory=dict)

    def update(self, info: SymbolInfo) -> None:
        precision: Dict[str, int] = {}
        if info.price_precision is not None:
            precision["price"] = info.price_precision
        if info.quantity_precision is not None:
            precision["amount"] = info.quantity_precision

        limits = {"price": {}, "amount": {}}
        if info.price_tick is not None:
            limits["price"]["min"] = info.price_tick
        if info.amount_step is not None:
            limits["amount"]["min"] = info.amount_step

        meta: Dict[str, Any] = {"precision": precision, "limits": limits}
        if info.contract_size is not None:
            meta["contractSize"] = info.contract_size
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



class ExecutionManager:
    """Manage live and paper execution against Binance futures."""

    def __init__(self):
        self.symbol = config.exchange["symbol"]
        self.post_only = config.execution["post_only"]
        self.retry_delay = config.execution["retry_delay_s"]
        self.retry_tick_offset = config.execution["retry_tick_offset"]
        self.max_post_only_adjustments = int(config.execution.get("max_post_only_adjustments", 3))
        self.paper_mode = bool(config.exchange.get("testnet", False))
        self.transport = BinanceTransport()
        self.simulator = PaperTradingSimulator(self.symbol)
        self.metadata = MarketMetadataCache(
            default_tick_size=float(config.exchange.get("tick_size", 0.1)),
            default_contract_multiplier=float(config.exchange.get("contract_multiplier", 1.0)),
        )

    async def initialize(self):
        if self.paper_mode:
            return

        try:
            await self._load_market_meta()
        except Exception as exc:
            logger.error("Init failed; switching to paper mode: %s", exc)
            self.paper_mode = True
            self.metadata.data = {}

    async def _load_market_meta(self):
        info = await self.transport.fetch_symbol_info(self.symbol)
        if info:
            self.metadata.update(info)

    async def place_limit_order(self, side: str, price: float, qty: float) -> Optional[OrderTicket]:
        if qty <= 0:
            return None
        if self.paper_mode:
            return self.simulator.create_order(
                "limit",
                side,
                qty,
                price=price,
                amount=qty,
            )

        tif = "GTX" if self.post_only else "GTC"
        attempt_price = price
        adjustments = 0
        while True:
            try:
                return await self.transport.place_limit_order(
                    self.symbol,
                    side,
                    attempt_price,
                    qty,
                    tif,
                )
            except BinanceAPIError as exc:
                if self._should_adjust_post_only(exc, adjustments):
                    adjustments += 1
                    attempt_price = self._adjust_price(side, attempt_price)
                    await asyncio.sleep(self.retry_delay)
                    continue
                self._log_transport_error("limit order", exc)
                return None
            except Exception as exc:
                self._log_transport_error("limit order", exc)
                return None

    async def place_stop_market_order(self, side: str, stop_price: float, qty: float) -> Optional[OrderTicket]:
        if qty <= 0:
            return None
        if self.paper_mode:
            return self.simulator.create_order(
                "stop_market",
                side,
                qty,
                stopPrice=stop_price,
                amount=qty,
            )

        try:
            return await self.transport.place_stop_market_order(
                self.symbol,
                side,
                stop_price,
                qty,
            )
        except Exception as exc:
            self._log_transport_error("stop-market order", exc)
            return None

    async def cancel_order(self, order_id: str) -> bool:
        if self.paper_mode:
            return bool(self.simulator.cancel(order_id))
        numeric_id = self._parse_order_identifier(order_id)
        try:
            await self.transport.cancel_order(
                self.symbol,
                order_id=numeric_id,
                client_order_id=None if numeric_id is not None else order_id,
            )
            return True
        except Exception as exc:
            self._log_transport_error(f"cancel order {order_id}", exc)
            return False

    async def cancel_all_open_orders(self) -> int:
        cancelled = 0
        if self.paper_mode:
            cancelled = int(self.simulator.cancel())
            logger.info("Paper cancel-all completed: %s orders", cancelled)
            return cancelled

        try:
            orders = await self.transport.fetch_open_orders(self.symbol)
            for order in orders:
                try:
                    await self.transport.cancel_order(
                        self.symbol,
                        order_id=order.exchange_order_id,
                        client_order_id=None if order.exchange_order_id is not None else order.client_order_id,
                    )
                    cancelled += 1
                except Exception as exc:
                    self._log_transport_error(f"cancel order {order.id}", exc)
            logger.info("Cancel-all completed: %s orders", cancelled)
            return cancelled
        except Exception as exc:
            try:
                await self.transport.cancel_all_orders(self.symbol)
                logger.warning("allOpenOrders invoked for symbol %s", self.symbol)
                return -1
            except Exception as fallback_exc:
                logger.error(
                    "Cancel-all error: %s; fallback failed: %s",
                    exc,
                    fallback_exc,
                )
                return cancelled

    async def fetch_position_qty(self) -> Optional[float]:
        if self.paper_mode:
            return self._paper_net_position()
        try:
            return await self.transport.fetch_position_qty(self.symbol)
        except Exception as exc:
            self._log_transport_error("fetch position", exc)
            return None

    async def get_account_equity(self) -> Optional[float]:
        if self.paper_mode:
            return self.simulator.equity
        try:
            return await self.transport.fetch_account_equity()
        except Exception as exc:
            self._log_transport_error("fetch equity", exc)
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
        return self.simulator.position_qty(order_id)

    def update_paper_equity(self, pnl: float):
        if self.paper_mode:
            self.simulator.record_pnl(pnl)
            logger.info(
                "Paper equity updated to %.2f (PnL: %.2f)",
                self.simulator.equity,
                pnl,
            )

    def remove_paper_position(self, order_id: str):
        if self.paper_mode:
            self.simulator.remove_position(order_id)

    def _should_adjust_post_only(self, error: BinanceAPIError, adjustments: int) -> bool:
        if error.code != -2021 or not self.post_only:
            return False
        return adjustments < self.max_post_only_adjustments

    def _adjust_price(self, side: str, current_price: float) -> float:
        tick_size = self.get_tick_size()
        offset = self.retry_tick_offset * tick_size
        if side.lower() == "buy":
            return current_price - offset
        return current_price + offset

    def _paper_net_position(self) -> float:
        qty = 0.0
        for pos in self.simulator.positions.values():
            qty += pos.qty if pos.side == "long" else -pos.qty
        return qty

    def _log_transport_error(self, action: str, error: Exception) -> None:
        if isinstance(error, BinanceAPIError):
            logger.error(
                "Binance %s failed (code=%s, msg=%s)",
                action,
                error.code,
                error.msg,
            )
        else:
            logger.error("%s failed: %s", action, error)

    @staticmethod
    def _parse_order_identifier(order_id: str) -> Optional[int]:
        try:
            return int(order_id)
        except (TypeError, ValueError):
            return None
