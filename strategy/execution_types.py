from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class OrderTicket:
    """Normalized view of an order acknowledgement across live and paper flows."""

    symbol: str
    side: str
    type: str
    quantity: float
    status: Optional[str] = None
    price: Optional[float] = None
    stop_price: Optional[float] = None
    client_order_id: Optional[str] = None
    exchange_order_id: Optional[int] = None
    raw: Dict[str, Any] = field(default_factory=dict)

    @property
    def id(self) -> str:
        if self.client_order_id:
            return self.client_order_id
        if self.exchange_order_id is not None:
            return str(self.exchange_order_id)
        fallback = self.raw.get("id")
        if fallback is not None:
            return str(fallback)
        return "order"

    def as_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "id": self.id,
            "symbol": self.symbol,
            "side": self.side,
            "type": self.type,
            "status": self.status,
            "quantity": self.quantity,
            "price": self.price,
            "stop_price": self.stop_price,
            "client_order_id": self.client_order_id,
            "exchange_order_id": self.exchange_order_id,
        }
        if self.raw:
            data["raw"] = self.raw
        return data
