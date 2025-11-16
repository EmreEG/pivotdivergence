import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ingest.websocket_client import WebSocketClient
from ingest.rest_poller import RESTPoller
from ingest.book_manager import OrderBookManager
from ingest.persister import DataPersister
from strategy.signal_manager import SignalManager, SignalState, Signal
from strategy.execution import ExecutionManager
from risk.position_sizer import RiskManager
from api.metrics import metrics, start_metrics_server
from api.alerts import alert_webhook
from config import config
from monitoring.signal_auditor import SignalAuditor
from monitoring.logging_utils import setup_logging
from monitoring.async_utils import run_tasks_with_cleanup
from monitoring.microstructure import MicrostructureHealthMonitor
from orchestration.persistence import PersistenceCoordinator
from orchestration.services import (
    MarketEventService,
    AnalyticsBarService,
    SignalLifecycleService,
    ExecutionRiskSupervisor,
)


logger = logging.getLogger(__name__)

class TradingSystem:
    """Orchestrate ingest, analytics, signal lifecycle, and execution."""
    def __init__(self, config_obj: Optional[Dict] = None):
        self.config = config_obj or config
        self.exchange_cfg = self.config.exchange
        self.orderbook_cfg = self.config.orderbook or {}
        self.profile_cfg = self.config.profile or {}
        self.levels_cfg = self.config.levels or {}
        self.microstructure_cfg = self.config.microstructure or {}
        self.indicator_cfg = self.config.indicators or {}
        self.divergence_cfg = self.config.divergence or {}
        self.monitoring_cfg = self.config.monitoring or {}
        self.breakthrough_cfg = self.config.breakthrough or {}
        self.execution_cfg = self.config.execution or {}

        self.symbol = self.exchange_cfg['symbol']
        self.tick_size = float(self.exchange_cfg.get('tick_size', 0.1))
        self.contract_multiplier = float(self.exchange_cfg.get('contract_multiplier', 1.0))
        self._signal_overlap_pct = self.levels_cfg.get('signal_overlap_pct', 0.001)
        self.cvd_snapshot_path = Path('logs') / 'cvd_snapshot.json'
        self.avwap_snapshot_path = Path('logs') / 'avwap_snapshot.json'
        self.reported_position = 0.0
        self.last_funding_rate: Optional[float] = None
        self.last_funding_anchor_ts: Optional[float] = None
        self.last_depth_stats = {'obi': None, 'bid_qty': 0.0, 'ask_qty': 0.0, 'depth': 0}
        self._last_market_event_ts = time.monotonic()
        self._cvd_bar_abs = {}
        self._cvd_bar_signed = {}
        self.current_price = None
        self.current_funding = None
        self.running = False
        self.current_volatility = 0.01
        self.current_levels = []
        self._profile_shapes: Dict[str, str] = {}

        from ingest.market_data_manager import MarketDataManager
        self.market_data_manager = MarketDataManager(
            self.symbol,
            WebSocketClient(self.symbol),
            RESTPoller(self.symbol)
        )
        self.ws_client = self.market_data_manager.ws_client

        self.book_manager = OrderBookManager(
            self.symbol,
            self.tick_size,
            self.orderbook_cfg.get('max_depth')
        )
        self.persister = DataPersister()

        from analytics.analytics_engine import AnalyticsEngine
        self.analytics_engine = AnalyticsEngine(
            self.config,
            self.symbol,
            self.tick_size
        )
        self.profile_service = self.analytics_engine.profile_service
        self.order_flow = self.analytics_engine.order_flow_service
        self.swing_service = self.analytics_engine.swing_service

        from strategy.signal_processor import SignalProcessor
        self.signal_processor = SignalProcessor(self.config)

        self.signal_manager = SignalManager(self.config)
        self.execution_manager = ExecutionManager()
        self.risk_manager = RiskManager()
        audit_log_path = self.monitoring_cfg.get('signal_audit_log', 'logs/signal_audit.jsonl')
        ack_budget_ms = self.monitoring_cfg.get('signal_ack_budget_ms', 1000)
        self.signal_auditor = SignalAuditor(audit_log_path, ack_budget_ms=ack_budget_ms)

        self.persistence_coordinator = PersistenceCoordinator(self)
        self.microstructure_monitor = MicrostructureHealthMonitor(
            self.microstructure_cfg,
            self.monitoring_cfg,
            self.execution_manager,
            self.order_flow.obi_calc,
            self.persister,
        )
        self.market_events = MarketEventService(
            self,
            self.microstructure_monitor,
            self.persistence_coordinator,
        )
        self.analytics_bar_service = AnalyticsBarService(self)
        self.signal_service = SignalLifecycleService(self, self.analytics_bar_service)
        self.execution_supervisor = ExecutionRiskSupervisor(self)

        self.persistence_coordinator.load_avwap_snapshot()
        self.persistence_coordinator.load_cvd_snapshot()



    def _sync_tick_size(self):
        self.tick_size = self.execution_manager.get_tick_size()
        self.contract_multiplier = self.execution_manager.get_contract_multiplier()
        self.book_manager.tick_size = self.tick_size
        self.profile_service.set_tick_size(self.tick_size)

    def _qprice(self, price: float) -> float:
        tick = self.tick_size or self.execution_manager.get_tick_size()
        if not tick:
            return price
        try:
            steps = round(price / tick)
            return round(steps * tick, 8)
        except Exception:
            return price


    async def initialize(self):
        await self.execution_manager.initialize()
        self._sync_tick_size()
        self.market_data_manager.register_handlers(
            trade_handler=self.market_events.handle_trade,
            orderbook_handler=self.market_events.handle_orderbook,
            ticker_handler=self.market_events.handle_ticker,
            oi_handler=self.market_events.handle_oi,
            gap_handler=self.market_events.handle_gap,
            latency_handler=self.market_events.handle_latency_event,
            drop_handler=self.market_events.handle_drop,
            kill_switch_handler=self.execution_supervisor.handle_kill_switch,
            user_order_handler=self.execution_supervisor.handle_user_order,
        )

    async def start(self):
        self.running = True
        await self.initialize()

        start_metrics_server(self.monitoring_cfg['prometheus_port'])

        # Seed account equity metric at startup
        try:
            equity = await self.execution_manager.get_account_equity()
            if equity is not None:
                metrics.update_equity(equity)
        except Exception:
            pass

        await self.analytics_bar_service.start()

        tasks = [
            asyncio.create_task(self.market_data_manager.start()),
            asyncio.create_task(self.persister.start()),
            asyncio.create_task(self.signal_service.process_levels_and_signals()),
        ]
        await self.execution_supervisor.start_audits()

        async def _cleanup():
            await self.stop()

        await run_tasks_with_cleanup(tasks, cleanup=_cleanup)
        
    async def stop(self):
        self.running = False
        await self.analytics_bar_service.stop()
        await self.market_data_manager.stop()
        await self.persister.stop()
        await self.execution_supervisor.stop_audits()
        await self.execution_manager.close()

async def main():
    system = TradingSystem(config)
    try:
        await system.start()
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("System shutting down on interrupt")
        await system.stop()

if __name__ == "__main__":
    setup_logging()
    asyncio.run(main())
