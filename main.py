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
from analytics.profile import VolumeProfile
from analytics.extrema import ExtremaDetector
from analytics.naked_poc import NakedPOCTracker
from analytics.avwap import AVWAPManager
from analytics.orderflow import CVDCalculator, OBICalculator, OIAnalyzer
from analytics.indicators import IndicatorCalculator
from analytics.swings import ZigZagSwing
from analytics.profile_shape import ProfileShapeDetector
from analytics.footprint import FootprintManager
from strategy.level_selector import LevelSelector
from strategy.divergence import DivergenceDetector
from strategy.signal_manager import SignalManager, SignalState, Signal
from strategy.execution import ExecutionManager
from risk.position_sizer import RiskManager
from api.metrics import metrics, start_metrics_server
from api.alerts import alert_webhook
from config import config
from monitoring.signal_auditor import SignalAuditor
from monitoring.logging_utils import setup_logging
from monitoring.async_utils import run_tasks_with_cleanup
from orchestration.services import MarketEventService, SignalLifecycleService, ExecutionController


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
        self.last_cvd_snapshot = 0.0
        self.last_avwap_snapshot = 0.0
        self.microstructure_contaminated_until = 0.0
        self.unsafe_mode = False
        self.reported_position = 0.0
        self.last_funding_rate: Optional[float] = None
        self.last_funding_anchor_ts: Optional[float] = None
        self.last_depth_stats = {'obi': None, 'bid_qty': 0.0, 'ask_qty': 0.0, 'depth': 0}
        self._last_market_event_ts = time.monotonic()
        self._cvd_bar_abs = {}
        self._cvd_bar_signed = {}

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
        self.swing_detector = self.analytics_engine.swing_detector

        self.level_registry = self.analytics_engine.level_registry
        self.avwap_manager = self.analytics_engine.avwap_manager
        self.profiles: Dict[str, VolumeProfile] = self.analytics_engine.profiles
        self.indicators = self.analytics_engine.indicators
        self.cvd_calc = self.analytics_engine.cvd_calc

        from strategy.signal_processor import SignalProcessor
        self.signal_processor = SignalProcessor(self.config)

        self.signal_manager = SignalManager(self.config)
        self.execution_manager = ExecutionManager()
        self.risk_manager = RiskManager()
        audit_log_path = self.monitoring_cfg.get('signal_audit_log', 'logs/signal_audit.jsonl')
        ack_budget_ms = self.monitoring_cfg.get('signal_ack_budget_ms', 1000)
        self.signal_auditor = SignalAuditor(audit_log_path, ack_budget_ms=ack_budget_ms)

        self.market_events = MarketEventService(self)
        self.signal_service = SignalLifecycleService(self)
        self.execution_service = ExecutionController(self)

        self.market_events.load_avwap_snapshot()
        self.market_events.load_cvd_snapshot()

        self.current_price = None
        self.current_funding = None
        self.running = False
        self.current_volatility = 0.01
        self.current_levels = []
        self._profile_shapes: Dict[str, str] = {}



    def _sync_tick_size(self):
        self.tick_size = self.execution_manager.get_tick_size()
        self.contract_multiplier = self.execution_manager.get_contract_multiplier()
        self.book_manager.tick_size = self.tick_size
        self.level_registry.set_tick_size(self.tick_size)
        for profile in self.profiles.values():
            profile.set_tick_size(self.tick_size)
        self.avwap_manager.set_tick_size(self.tick_size)

    def _qprice(self, price: float) -> float:
        tick = self.tick_size or self.execution_manager.get_tick_size()
        if not tick:
            return price
        try:
            steps = round(price / tick)
            return round(steps * tick, 8)
        except Exception:
            return price



    def _arm_hold_timer(self, signal: Signal) -> None:
        self.execution_service.arm_hold_timer(signal)

    async def _finalize_signal_exit(self, signal: Signal, exit_price: float, reason: str) -> None:
        await self.execution_service.finalize_signal_exit(signal, exit_price, reason)
        
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
            kill_switch_handler=self.execution_service.handle_kill_switch,
            user_order_handler=self.execution_service.handle_user_order,
        )
        
    async def handle_trade(self, trade: Dict):
        await self.market_events.handle_trade(trade)
        
    async def handle_orderbook(self, orderbook: Dict):
        await self.market_events.handle_orderbook(orderbook)
        
    def _zone_half_width_pct(self) -> float:
        return self.signal_service.zone_half_width_pct()

    def _has_conflicting_signal(self, price: float) -> bool:
        return self.signal_service.has_conflicting_signal(price)

    def _build_feature_context(self, level: Dict) -> Dict:
        return self.signal_service.build_feature_context(level)

    def _build_current_levels(self, long_levels: List[Dict], short_levels: List[Dict]) -> List[Dict]:
        return self.signal_service.build_current_levels(long_levels, short_levels)

    def _create_signals_for_levels(self, levels: List[Dict], side: str, zone_width_pct: float) -> None:
        self.signal_service.create_signals_for_levels(levels, side, zone_width_pct)

    def _should_skip_level_processing(self) -> bool:
        return self.signal_service.should_skip_level_processing()

    async def handle_ticker(self, ticker: Dict):
        await self.market_events.handle_ticker(ticker)

    async def handle_oi(self, oi_data: Dict):
        await self.market_events.handle_oi(oi_data)

    async def handle_gap(self, gap_duration: float):
        await self.market_events.handle_gap(gap_duration)

    async def handle_latency_event(self, drift_ms: float):
        await self.market_events.handle_latency_event(drift_ms)

    async def handle_drop(self, reason: str):
        await self.market_events.handle_drop(reason)

    async def handle_user_order(self, order: Dict):
        await self.execution_service.handle_user_order(order)

    async def _handle_signal_filled(self, signal: Signal, entry_px: float, filled_qty: float,
                                    total_qty: float, order_id: str, ack_status: str):
        await self.execution_service.handle_signal_filled(signal, entry_px, filled_qty, total_qty, order_id, ack_status)

    async def _handle_signal_cancelled(self, signal: Signal, total_qty: float, ack_status: str):
        await self.execution_service.handle_signal_cancelled(signal, total_qty, ack_status)

    async def _handle_signal_partial(self, signal: Signal, entry_px: float, filled_qty: float,
                                     total_qty: float, order_id: str, ack_status: str):
        await self.execution_service.handle_signal_partial(signal, entry_px, filled_qty, total_qty, order_id, ack_status)

    async def _handle_signal_update(self, signal: Signal, entry_px: float, filled_qty: float,
                                    total_qty: float, ack_status: str):
        await self.execution_service.handle_signal_update(signal, entry_px, filled_qty, total_qty, ack_status)

    async def _persist_and_audit_signal(self, signal: Signal, event_status: str,
                                        ack_status: str, filled_qty: float, total_qty: float,
                                        entry_px: float):
        await self.execution_service.persist_and_audit_signal(signal, event_status, ack_status, filled_qty, total_qty, entry_px)
        
    async def handle_kill_switch(self, reason: str):
        await self.execution_service.handle_kill_switch(reason)

    def _mark_microstructure_contaminated(self, source: str):
        self.market_events.mark_microstructure_contaminated(source)

    def _activate_unsafe_mode(self, reason: str):
        self.market_events.activate_unsafe_mode(reason)

    async def _resync_orderbook(self, reason: str):
        await self.market_events.resync_orderbook(reason)

    def _load_avwap_snapshot(self):
        self.market_events.load_avwap_snapshot()

    def _persist_avwap_snapshot(self):
        self.market_events.persist_avwap_snapshot()

    def _check_unsafe_mode(self) -> bool:
        return self.execution_service.check_unsafe_mode()

    def _check_backpressure(self) -> bool:
        return self.market_events.check_backpressure()

    def _load_cvd_snapshot(self):
        self.market_events.load_cvd_snapshot()

    def _persist_cvd_snapshot(self):
        self.market_events.persist_cvd_snapshot()
        
    async def process_bar(self):
        await self.signal_service.process_bar()

    async def process_levels_and_signals(self):
        await self.signal_service.process_levels_and_signals()
            
    async def execute_signal(self, signal):
        await self.execution_service.execute_signal(signal)

    def _build_feature_vector(self, signal, mode: str) -> Dict:
        return self.execution_service.build_feature_vector(signal, mode)

    def _current_total_exposure_usd(self) -> float:
        return self.execution_service.current_total_exposure_usd()

    def _available_notional_headroom(self, equity: float) -> float:
        return self.execution_service.available_notional_headroom(equity)

    def _compute_price_and_amount(self, notional_usd: float, raw_price: float) -> Tuple[float, float]:
        return self.execution_service.compute_price_and_amount(notional_usd, raw_price)

    def _mark_market_event(self):
        self.market_events.mark_market_event()

    def _record_signal_latency(self, path: str, start_ts: float):
        self.execution_service.record_signal_latency(path, start_ts)

    def _record_order_ack_latency(self, signal: Signal, status: str) -> float:
        return self.execution_service.record_order_ack_latency(signal, status)

    async def _run_signal_audits(self):
        await self.execution_service._run_signal_audits()

    def _is_trading_blocked(self) -> bool:
        return self.execution_service.is_trading_blocked()



    async def _check_breakthrough_opportunities(self, candidate_levels: List[Dict]):
        await self.execution_service.check_breakthrough_opportunities(candidate_levels)

    async def _execute_breakthrough(self, signal, entry_price: float):
        await self.execution_service.execute_breakthrough(signal, entry_price)

    def _prepare_order_details(
        self,
        signal,
        equity: float,
        atr: float,
        entry_price: float,
        qty_usd: float,
        target_rr: float,
        notional_cap_msg: str,
        headroom_exhausted_msg: str,
        zero_size_msg: str,
    ):
        return self.execution_service.prepare_order_details(
            signal,
            equity,
            atr,
            entry_price,
            qty_usd,
            target_rr,
            notional_cap_msg,
            headroom_exhausted_msg,
            zero_size_msg,
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

        tasks = [
            asyncio.create_task(self.market_data_manager.start()),
            asyncio.create_task(self.persister.start()),
            asyncio.create_task(self.process_bar()),
            asyncio.create_task(self.process_levels_and_signals()),
        ]
        await self.execution_service.start_audits()

        async def _cleanup():
            await self.stop()

        await run_tasks_with_cleanup(tasks, cleanup=_cleanup)
        
    async def stop(self):
        self.running = False
        await self.market_data_manager.stop()
        await self.persister.stop()
        await self.execution_service.stop_audits()
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
