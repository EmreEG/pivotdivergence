import uuid
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Dict, Mapping, Optional, Union

from api.metrics import metrics

from strategy.execution_types import OrderTicket


@dataclass
class PaperPosition:
    order_id: str
    qty: float
    side: str


class PaperTradingSimulator:
    """Paper-trading simulator responsible for order bookkeeping and metrics."""

    def __init__(self, symbol: str, initial_equity: float = 1000.0) -> None:
        self.symbol = symbol
        self._equity = initial_equity
        self._orders: Dict[str, OrderTicket] = {}
        self._positions: Dict[str, PaperPosition] = {}

    @property
    def equity(self) -> float:
        return self._equity

    @property
    def positions(self) -> Mapping[str, PaperPosition]:
        return MappingProxyType(self._positions)

    def create_order(self, order_type: str, side: str, qty: float, **details: Any) -> Optional[OrderTicket]:
        if qty <= 0:
            return None
        order_id = f"paper-{uuid.uuid4().hex[:8]}"
        price = self._coerce_float(details.get("price"))
        stop_price = self._coerce_float(details.get("stopPrice"))
        ticket = OrderTicket(
            symbol=self.symbol,
            side=side.upper(),
            type=order_type,
            quantity=qty,
            status="open",
            price=price,
            stop_price=stop_price,
            client_order_id=order_id,
            raw=details,
        )
        direction = "long" if side.lower() == "buy" else "short"
        self._orders[order_id] = ticket
        self._positions[order_id] = PaperPosition(order_id=order_id, qty=qty, side=direction)
        return ticket

    def cancel(self, order_id: Optional[str] = None) -> Union[int, bool]:
        if order_id is None:
            cancelled = len(self._orders)
            self._orders.clear()
            self._positions.clear()
            return cancelled
        removed = order_id in self._orders
        if removed:
            self._orders.pop(order_id, None)
            self._positions.pop(order_id, None)
        return removed

    def position_qty(self, order_id: str) -> float:
        pos = self._positions.get(order_id)
        if not pos:
            return 0.0
        return pos.qty

    def record_pnl(self, pnl: float) -> None:
        self._equity += pnl
        metrics.record_pnl(pnl)
        metrics.update_equity(self._equity)

    def remove_position(self, order_id: str) -> None:
        self._positions.pop(order_id, None)
        self._orders.pop(order_id, None)

    def net_position(self) -> float:
        qty = 0.0
        for pos in self._positions.values():
            qty += pos.qty if pos.side == "long" else -pos.qty
        return qty

    @staticmethod
    def _coerce_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
