import logging
from typing import Dict, List

from analytics.services import ProfileService, OrderFlowService, SwingService


logger = logging.getLogger(__name__)


class AnalyticsEngine:
    """Thin facade around specialized analytics services."""

    def __init__(self, config: Dict, symbol: str, tick_size: float):
        self.config = config
        self.symbol = symbol
        self.tick_size = tick_size

        self.profile_service = ProfileService(config, symbol, tick_size)
        self.order_flow_service = OrderFlowService(config)
        self.swing_service = SwingService()

    def on_trade(self, trade: Dict):
        self.profile_service.handle_trade(trade)
        self.order_flow_service.handle_trade(trade)

    def on_orderbook(self, orderbook: Dict, book_manager):
        return self.order_flow_service.handle_orderbook(orderbook, book_manager)

    def on_oi(self, oi_data: Dict):
        self.order_flow_service.handle_oi(oi_data)

    def on_bar(self, current_price: float, timestamp: float):
        # Placeholder for future bar-level analytics.
        logger.debug("on_bar event price=%s ts=%s", current_price, timestamp)

    def get_candidate_levels(self) -> List[Dict]:
        return self.profile_service.get_candidate_levels()

    def get_indicator_snapshot(self) -> Dict:
        return self.order_flow_service.get_indicator_snapshot(self.symbol)
