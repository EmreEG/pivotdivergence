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
import numpy as np


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
        self._signal_audit_task: Optional[asyncio.Task] = None

        self._load_avwap_snapshot()
        self._load_cvd_snapshot()

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


    
    def _arm_hold_timer(self, signal: Signal) -> None:
        hold_seconds = self.risk_manager.get_max_hold_seconds()
        if hold_seconds > 0:
            signal.hold_expires_at = time.time() + hold_seconds
        else:
            signal.hold_expires_at = None

    async def _finalize_signal_exit(self, signal: Signal, exit_price: float, reason: str) -> None:
        pnl_usd = 0.0
        qty = 0.0
        if signal.order_id:
            qty = self.execution_manager.get_paper_position_qty(signal.order_id)
        if qty <= 0 and signal.filled_qty:
            qty = signal.filled_qty
        if signal.entry_price is not None and qty > 0:
            if signal.side == 'long':
                pnl_usd = (exit_price - signal.entry_price) * qty
            else:
                pnl_usd = (signal.entry_price - exit_price) * qty
            self.execution_manager.update_paper_equity(pnl_usd)
            if signal.order_id:
                self.execution_manager.remove_paper_position(signal.order_id)

        self.signal_manager.mark_closed(signal.signal_id, exit_price, pnl_usd, reason=reason)
        metrics.record_signal_closed(reason)
        logger.info(
            "Exit %s %s @ %.2f, PnL=%.2f",
            signal.signal_id,
            reason.upper(),
            exit_price,
            pnl_usd,
        )
        await self.persister.insert_signal(signal.to_dict())
        
    async def initialize(self):
        await self.execution_manager.initialize()
        self._sync_tick_size()
        self.market_data_manager.register_handlers(
            trade_handler=self.handle_trade,
            orderbook_handler=self.handle_orderbook,
            ticker_handler=self.handle_ticker,
            oi_handler=self.handle_oi,
            gap_handler=self.handle_gap,
            latency_handler=self.handle_latency_event,
            drop_handler=self.handle_drop,
            kill_switch_handler=self.handle_kill_switch,
            user_order_handler=self.handle_user_order,
        )
        
    async def handle_trade(self, trade: Dict):
        price = float(trade['price'])
        timestamp = trade['timestamp'] / 1000
        latency_s = max(0.0, time.time() - timestamp)
        self._mark_market_event()
        
        self.current_price = price
        self.analytics_engine.on_trade(trade)
        
        cvd_value = self.analytics_engine.cvd_calc.get_cvd()
        stable_cvd = self.analytics_engine.cvd_calc.is_stable()
        
        metrics.record_trade(latency_s)
        metrics.update_price(price)
        if stable_cvd:
            metrics.update_cvd(cvd_value)
        else:
            self._mark_microstructure_contaminated('cvd_gap')
        self._persist_cvd_snapshot()
        
        # datapython owns historical trade persistence; this hook is intentionally inert
        await self.persister.insert_trade(trade)
        self._check_backpressure()
        
    async def handle_orderbook(self, orderbook: Dict):
        payload = orderbook.get('info') or orderbook
        is_snapshot = (
            'lastUpdateId' in orderbook or
            'nonce' in orderbook or
            (not self.book_manager.initialized and payload.get('lastUpdateId'))
        )

        if is_snapshot:
            snapshot = {
                'bids': orderbook.get('bids') or payload.get('b') or [],
                'asks': orderbook.get('asks') or payload.get('a') or [],
                'lastUpdateId': orderbook.get('lastUpdateId') or payload.get('lastUpdateId') or payload.get('nonce')
            }
            self.book_manager.process_snapshot(snapshot)
        else:
            # Treat partial-depth updates as read-only snapshots unless Binance diff fields present
            has_diff_fields = any(k in payload for k in ('U', 'u', 'pu'))
            if not has_diff_fields:
                snap = {
                    'bids': orderbook.get('bids') or payload.get('b') or [],
                    'asks': orderbook.get('asks') or payload.get('a') or [],
                    'lastUpdateId': payload.get('lastUpdateId') or payload.get('nonce') or orderbook.get('nonce')
                }
                self.book_manager.process_snapshot(snap)
            else:
                update = {
                    'bids': payload.get('bids') or payload.get('b') or orderbook.get('bids', []),
                    'asks': payload.get('asks') or payload.get('a') or orderbook.get('asks', []),
                    'u': payload.get('u') or orderbook.get('lastUpdateId'),
                    'U': payload.get('U') or payload.get('firstUpdateId'),
                    'pu': payload.get('pu'),
                    'E': payload.get('E') or orderbook.get('timestamp')
                }
                if not self.book_manager.process_update(update):
                    await self._resync_orderbook('sequence_break')
                    return

        self._mark_market_event()

        if not self.book_manager.validate():
            await self._resync_orderbook('invariant_violation')
            return

        if self.book_manager.is_stale(time.monotonic()):
            self._mark_microstructure_contaminated('stale_book')

        obi_stats = self.analytics_engine.on_orderbook(orderbook, self.book_manager)
        self.last_depth_stats = dict(obi_stats)
        obi_z = obi_stats.get('obi_z')

        metrics.record_orderbook_update(obi_stats)
        if obi_z is not None:
            metrics.update_obi_z(obi_z)
            metrics.mark_microstructure(True, 'healthy')
        else:
            metrics.mark_microstructure(False, 'obi_disabled')

        await self.persister.insert_obi_snapshot({
            'symbol': self.symbol,
            **obi_stats
        })
        await self.persister.insert_book_snapshot(self.book_manager.to_dict())
        self._check_backpressure()
        
    def _zone_half_width_pct(self) -> float:
        base_pct = self.levels_cfg.get('zone_half_width_pct', 0.0015)
        if self.current_volatility > 0.02:
            return base_pct * (1 + self.current_volatility / 0.01)
        return base_pct

    def _has_conflicting_signal(self, price: float) -> bool:
        current = self.current_price or 0.0
        if current <= 0:
            return False
        threshold = self._signal_overlap_pct
        return any(abs(signal.level_price - price) / current < threshold
                   for signal in self.signal_manager.active_signals.values())

    def _build_feature_context(self, level: Dict) -> Dict:
        return {
            'type': level.get('type'),
            'score': level.get('score'),
            'prominence': level.get('prominence'),
            'window': level.get('window')
        }

    def _build_current_levels(self, long_levels: List[Dict], short_levels: List[Dict]) -> List[Dict]:
        try:
            return [
                {**lvl, 'side': 'long'} for lvl in long_levels
            ] + [
                {**lvl, 'side': 'short'} for lvl in short_levels
            ]
        except Exception:
            return []

    def _create_signals_for_levels(self, levels: List[Dict], side: str, zone_width_pct: float) -> None:
        if self.current_price is None:
            return
        for level in levels:
            price = level['price']
            if self._has_conflicting_signal(price):
                continue
            effective_pct = zone_width_pct
            shelf_width = level.get('shelf_width')
            if shelf_width and shelf_width > 0 and self.current_price and self.current_price > 0:
                shelf_mult = self.levels_cfg.get('zone_half_width_shelf_multiplier', 0.5)
                shelf_pct = (shelf_width / self.current_price) * max(shelf_mult, 0.0)
                if shelf_pct > 0:
                    effective_pct = min(zone_width_pct, shelf_pct)
            zone_half_width = self.current_price * effective_pct
            zone_low = price - zone_half_width
            zone_high = price + zone_half_width
            signal = self.signal_manager.create_signal(
                level,
                side,
                zone_low,
                zone_high,
                self.current_price,
                self.symbol
            )
            feature_context = self._build_feature_context(level)
            logger.info(
                "Created %s signal at %.2f, context=%s",
                side.upper(),
                price,
                feature_context,
            )

    def _should_skip_level_processing(self) -> bool:
        if self.current_price is None:
            return True
        if self.execution_manager.paper_mode:
            return False
        return time.time() < self.microstructure_contaminated_until

    async def handle_ticker(self, ticker: Dict):
        mark_price = ticker.get('info', {}).get('markPrice')
        funding_rate = ticker.get('info', {}).get('fundingRate')
        
        if mark_price:
            self.current_price = float(mark_price)
            metrics.update_price(self.current_price)
        if funding_rate:
            self.current_funding = float(funding_rate)
            metrics.update_funding(self.current_funding)
            # Create funding AVWAP/event anchors on rate change
            if self.last_funding_rate is None or self.current_funding != self.last_funding_rate:
                ts = ticker['timestamp'] / 1000
                anchor_id = f"funding_{int(ts)}"
                self.avwap_manager.create_avwap(anchor_id, ts, 'funding')
                self._persist_avwap_snapshot()
                event_profile = VolumeProfile(self.symbol, 86400)
                event_profile.set_tick_size(self.tick_size)
                event_profile.anchor_ts = ts
                self.event_anchors[anchor_id] = event_profile
                self.last_funding_anchor_ts = ts
                self.last_funding_rate = self.current_funding
            
        await self.persister.insert_mark_price({
            'symbol': self.symbol,
            'timestamp': ticker['timestamp'],
            'mark_price': self.current_price,
            'funding_rate': self.current_funding
        })
        
    async def handle_oi(self, oi_data: Dict):
        if oi_data is None:
            if not self.execution_manager.paper_mode:
                logger.error("Open interest fetch failed; triggering kill switch")
                await self.handle_kill_switch('oi_fetch_failed')
            return
            
        self.analytics_engine.on_oi(oi_data)
        
        await self.persister.insert_open_interest(oi_data)

    async def handle_gap(self, gap_duration: float):
        self._mark_microstructure_contaminated('gap')
        metrics.record_reconnect()
        # Hard resync book after any gap
        try:
            await self._resync_orderbook('stream_gap')
        except Exception as e:
            logger.error("Order book resync on gap failed: %s", e)
        if gap_duration > 10:
            await alert_webhook.send_alert('stream_gap', f'Stream gap {gap_duration:.1f}s', 'warning')

    async def handle_latency_event(self, drift_ms: float):
        await alert_webhook.latency_alert(drift_ms)
        # In paper mode, do not trip global kill-switch on latency; keep running.
        if not self.execution_manager.paper_mode:
            if drift_ms > self.monitoring_cfg.get('latency_budget_ms', 50):
                await self.handle_kill_switch('latency_drift')

    async def handle_drop(self, reason: str):
        metrics.record_drop(reason)
        self._mark_microstructure_contaminated(f"message_loss_{reason}")

    async def handle_user_order(self, order: Dict):
        order_id = order.get('id') or order.get('orderId')
        if not order_id:
            return
        status = (order.get('status') or '').lower()
        avg_price = order.get('average') or order.get('price')
        filled_qty = float(order.get('filled') or order.get('executedQty') or 0)
        total_qty = float(order.get('amount') or order.get('origQty') or order.get('quantity') or 0)
        for signal in list(self.signal_manager.active_signals.values()):
            if signal.order_id != order_id:
                continue
            entry_px = float(avg_price or signal.entry_price or signal.level_price or self.current_price or 0)
            prev_filled = signal.filled_qty or 0.0
            incremental = max(filled_qty - prev_filled, 0.0)
            if incremental > 0:
                direction = 1 if signal.side == 'long' else -1
                self.reported_position += direction * incremental
                notional = abs(self.reported_position * (self.current_price or entry_px))
                cap = self.risk_manager.max_notional * (1 + self.risk_manager.position_epsilon_pct)
                if notional > cap:
                    await self.handle_kill_switch('position_mismatch')
                    return
                try:
                    exch_qty = await self.execution_manager.fetch_position_qty()
                    if exch_qty is not None:
                        eps = max(abs(exch_qty), 1e-9) * self.risk_manager.position_epsilon_pct
                        if abs(exch_qty - self.reported_position) > max(eps, 1e-6):
                            await self.handle_kill_switch('position_mismatch')
                            return
                except Exception as e:
                    logger.error("Position cross-check failed: %s", e)

            if status in ('closed', 'filled'):
                await self._handle_signal_filled(signal, entry_px, filled_qty, total_qty, order_id, status)
            elif status in ('canceled', 'cancelled', 'expired'):
                await self._handle_signal_cancelled(signal, total_qty, status)
            elif filled_qty > 0 and (total_qty == 0 or filled_qty < total_qty or status in ('partial', 'partially_filled', 'open')):
                await self._handle_signal_partial(signal, entry_px, filled_qty, total_qty, order_id, status)
            else:
                await self._handle_signal_update(signal, entry_px, filled_qty, total_qty, status or 'update')

    async def _handle_signal_filled(self, signal: Signal, entry_px: float, filled_qty: float,
                                    total_qty: float, order_id: str, ack_status: str):
        current_price = self.current_price or entry_px
        stop_price = signal.stop_price or self.risk_manager.calculate_stop_price(
            entry_px,
            signal.side,
            current_price * 0.01 if current_price else entry_px * 0.005,
            current_price * self.levels_cfg.get('zone_half_width_pct', 0.0015)
        )
        target_price = signal.target_price or self.risk_manager.calculate_target_price(
            entry_px,
            stop_price,
            signal.side,
            target_rr=2.0 if getattr(signal, 'is_breakthrough', False) else 1.5
        )
        self.signal_manager.mark_filled(
            signal.signal_id,
            entry_px,
            stop_price,
            target_price,
            order_id,
            filled_qty=filled_qty or total_qty,
            avg_fill_price=entry_px
        )
        self._arm_hold_timer(signal)
        metrics.record_order_filled()
        try:
            mid = self.book_manager.get_mid_price() or self.current_price
            if mid and mid > 0:
                slippage_bps = abs((entry_px - float(mid)) / float(mid)) * 10000.0
                metrics.record_slippage_bps(slippage_bps)
        except Exception:
            pass
        await self._persist_and_audit_signal(
            signal,
            'filled',
            ack_status or 'filled',
            filled_qty or total_qty,
            total_qty,
            entry_px
        )

    async def _handle_signal_cancelled(self, signal: Signal, total_qty: float, ack_status: str):
        signal.state = SignalState.CANCELLED
        signal.closed_at = time.time()
        metrics.record_order_cancelled()
        await self._persist_and_audit_signal(
            signal,
            'cancelled',
            ack_status or 'cancelled',
            signal.filled_qty or 0.0,
            total_qty,
            signal.entry_price or self.current_price or 0.0
        )

    async def _handle_signal_partial(self, signal: Signal, entry_px: float, filled_qty: float,
                                     total_qty: float, order_id: str, ack_status: str):
        self.signal_manager.mark_partial(
            signal.signal_id,
            entry_px,
            filled_qty,
            entry_px,
            order_id
        )
        await self._persist_and_audit_signal(
            signal,
            'partial',
            ack_status or 'partial',
            filled_qty,
            total_qty,
            entry_px
        )

    async def _handle_signal_update(self, signal: Signal, entry_px: float, filled_qty: float,
                                    total_qty: float, ack_status: str):
        await self._persist_and_audit_signal(
            signal,
            ack_status,
            ack_status,
            filled_qty,
            total_qty,
            entry_px
        )

    async def _persist_and_audit_signal(self, signal: Signal, event_status: str,
                                        ack_status: str, filled_qty: float, total_qty: float,
                                        entry_px: float):
        if signal is None:
            return
        payload = {
            'status': ack_status,
            'filled_qty': filled_qty,
            'total_qty': total_qty,
            'avg_price': entry_px
        }
        ack_latency = self._record_order_ack_latency(signal, ack_status)
        self.signal_auditor.record_execution(
            signal,
            event_status,
            payload,
            ack_latency_s=ack_latency or signal.last_ack_latency
        )
        await self.persister.insert_signal(signal.to_dict())
        
    async def handle_kill_switch(self, reason: str):
        logger.error("Kill switch triggered: %s", reason)
        metrics.record_kill_switch(reason)
        await alert_webhook.kill_switch_alert(reason)
        try:
            cancelled = await self.execution_manager.cancel_all_open_orders()
            logger.info("Kill switch cancelled %s open orders", cancelled)
        except Exception as e:
            logger.error("Kill switch cancel-all failed: %s", e)
        self._mark_microstructure_contaminated(f"kill_switch_{reason}")

    def _mark_microstructure_contaminated(self, source: str):
        hold = self.microstructure_cfg.get('contamination_hold_s', 60)
        if self.execution_manager.paper_mode:
            self.microstructure_contaminated_until = 0.0
        else:
            self.microstructure_contaminated_until = time.time() + hold
            self.obi_calc.mark_contaminated(hold)
        metrics.mark_microstructure(False, source)
        if not self.execution_manager.paper_mode and not self.unsafe_mode:
            self._activate_unsafe_mode(source)

    def _activate_unsafe_mode(self, reason: str):
        flag_path = self.monitoring_cfg.get('unsafe_flag_file')
        if not flag_path:
            return
        path = Path(flag_path)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                'reason': reason,
                'activated_at': int(time.time())
            }
            path.write_text(json.dumps(payload))
            self.unsafe_mode = True
            logger.warning("Unsafe mode activated automatically due to %s", reason)
        except Exception as e:
            logger.error("Failed to write unsafe-mode flag file: %s", e)

    async def _resync_orderbook(self, reason: str):
        logger.info("Order book resync triggered: %s", reason)
        try:
            snapshot = await self.ws_client.fetch_orderbook_snapshot(
                self.orderbook_cfg.get('max_depth', 200)
            )
            self.book_manager.process_snapshot(snapshot)
            self.cvd_calc.mark_resynced()
            self._mark_microstructure_contaminated(reason)
            metrics.record_orderbook_resync()
        except Exception as e:
            logger.error("Order book resync failed: %s", e)
            if not self.execution_manager.paper_mode:
                await self.handle_kill_switch('orderbook_resync_failed')
            else:
                self._mark_microstructure_contaminated('orderbook_resync_failed')

    def _load_avwap_snapshot(self):
        try:
            if self.avwap_snapshot_path.exists():
                data = json.loads(self.avwap_snapshot_path.read_text())
                self.avwap_manager.restore(data)
                logger.info("Restored AVWAP snapshot state")
        except Exception as e:
            logger.warning("AVWAP snapshot restore failed: %s", e)

    def _persist_avwap_snapshot(self):
        now = time.time()
        if now - self.last_avwap_snapshot < 30:
            return
        try:
            snapshot = self.avwap_manager.snapshot()
            self.avwap_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            self.avwap_snapshot_path.write_text(json.dumps(snapshot))
            self.last_avwap_snapshot = now
        except Exception as e:
            logger.error("AVWAP snapshot persist failed: %s", e)

    def _check_unsafe_mode(self) -> bool:
        if self.execution_manager.paper_mode:
            self.unsafe_mode = False
            return False
        flag_path = self.monitoring_cfg.get('unsafe_flag_file')
        if not flag_path:
            return False
        path = Path(flag_path)
        if path.exists():
            if not self.unsafe_mode:
                logger.warning("Unsafe mode enabled – blocking new orders")
            self.unsafe_mode = True
            return True
        self.unsafe_mode = False
        return False

    def _check_backpressure(self) -> bool:
        max_buffer = self.persister.batch_size * 10
        buffers = {
            'book': len(self.persister.book_buffer),
            'mark': len(self.persister.mark_buffer)
        }
        if any(count > max_buffer for count in buffers.values()):
            self._mark_microstructure_contaminated('backpressure')
            return True
        return False

    def _load_cvd_snapshot(self):
        try:
            if self.cvd_snapshot_path.exists():
                data = json.loads(self.cvd_snapshot_path.read_text())
                self.cvd_calc.restore_state(data)
                logger.info("Restored CVD snapshot state")
        except Exception as e:
            logger.warning("CVD snapshot restore failed: %s", e)

    def _persist_cvd_snapshot(self):
        now = time.time()
        if now - self.last_cvd_snapshot < 30:
            return
        try:
            snapshot = self.cvd_calc.snapshot_state()
            if snapshot and snapshot.get('stable', True):
                self.cvd_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
                self.cvd_snapshot_path.write_text(json.dumps(snapshot))
                self.last_cvd_snapshot = now
        except Exception as e:
            logger.error("CVD snapshot persist failed: %s", e)
        
    async def process_bar(self):
        while self.running:
            await asyncio.sleep(60)
            
            if self.current_price is None:
                continue
            
            self.analytics_engine.on_bar(self.current_price, time.time())
            
            rsi = self.analytics_engine.indicators.get_rsi()
            if rsi is not None:
                metrics.update_rsi(rsi)
            
            realized_vol = self.analytics_engine.indicators.get_realized_volatility()
            if realized_vol is not None:
                self.current_volatility = realized_vol
                metrics.update_realized_volatility(realized_vol)

            new_swing = self.analytics_engine.swing_detector.update(
                self.current_price,
                time.time(),
                rsi,
                self.analytics_engine.cvd_calc.get_cvd(),
                self.analytics_engine.obi_calc.get_obi_z_score(),
                macd_hist=self.analytics_engine.indicators.get_macd().get('histogram'),
                volume=self.analytics_engine.footprint_manager.get_current_bar().total_volume,
            )
            
            if new_swing:
                logger.info(
                    "New swing %s at %.2f",
                    new_swing["type"],
                    new_swing["price"],
                )
                
                anchor_id = f"swing_{new_swing['type']}_{int(new_swing['timestamp'])}"
                self.analytics_engine.avwap_manager.create_avwap(anchor_id, new_swing['timestamp'], f"swing_{new_swing['type']}")
                self._persist_avwap_snapshot()
                
                event_profile = VolumeProfile(self.symbol, 86400)
                event_profile.set_tick_size(self.tick_size)
                event_profile.anchor_ts = new_swing['timestamp']
                self.analytics_engine.event_anchors[anchor_id] = event_profile
                
            # Health metrics: stream lags and queue depths
            mono_now = time.monotonic()
            for stream, last_seen in self.market_data_manager.ws_client.stream_last_seen.items():
                lag = max(0.0, mono_now - float(last_seen))
                metrics.update_stream_lag(stream, lag)
            metrics.update_queue_depth('book', len(self.persister.book_buffer))
            metrics.update_queue_depth('mark', len(self.persister.mark_buffer))
            metrics.update_queue_depth('oi', len(self.persister.oi_buffer))
            metrics.update_queue_depth('indicator', len(self.persister.indicator_buffer))
            metrics.update_queue_depth('signal', len(self.persister.signal_buffer))

            indicator_snapshot = self.analytics_engine.get_indicator_snapshot()
            await self.persister.insert_indicator_state(indicator_snapshot)

    async def process_levels_and_signals(self):
        while self.running:
            await asyncio.sleep(10)
            
            if self._should_skip_level_processing():
                continue

            candidate_levels = self.analytics_engine.get_candidate_levels()
            
            now_ts = time.time()
            self.analytics_engine.level_registry.register_levels(candidate_levels, now_ts)
            candidate_levels = self.analytics_engine.level_registry.enrich_levels(candidate_levels, now_ts)
                    
            best_bid = self.book_manager.get_best_bid()
            best_ask = self.book_manager.get_best_ask()
            if best_bid and best_ask:
                self.analytics_engine.poc_tracker.check_touch(best_bid[0], best_ask[0], time.time())
                
            self.analytics_engine.poc_tracker.cleanup_old(max_age_hours=72)
            
            naked_pocs = self.analytics_engine.poc_tracker.get_naked_pocs()
            avwaps = self.analytics_engine.avwap_manager.get_all()
            swings = self.analytics_engine.swing_detector.get_last_n_swings(10)
            
            long_levels, short_levels = self.signal_processor.select_levels(
                candidate_levels,
                self.current_price,
                naked_pocs,
                avwaps,
                swings,
            )

            # expose selected levels to API
            self.current_levels = self._build_current_levels(long_levels, short_levels)

            # Check for breakthrough opportunities before processing regular signals
            await self._check_breakthrough_opportunities(candidate_levels)

            zone_width_pct = self._zone_half_width_pct()
            self._create_signals_for_levels(long_levels, 'long', zone_width_pct)
            self._create_signals_for_levels(short_levels, 'short', zone_width_pct)
                    
            obi_z = self.analytics_engine.obi_calc.get_obi_z_score()
            oi_slope = self.analytics_engine.oi_analyzer.get_slope()
            
            top_levels = self.book_manager.get_top_levels(self.microstructure_cfg.get('obi_depth_levels', 10))
            book_depth = len(top_levels['bids']) + len(top_levels['asks'])
            suppress_obi = (
                book_depth < self.microstructure_cfg.get('obi_disable_depth', 10) or
                ((not self.execution_manager.paper_mode) and time.time() < self.microstructure_contaminated_until) or
                self.analytics_engine.obi_calc.is_contaminated()
            )
            
            divergence_results = {}
            swings_high_pair = self.analytics_engine.swing_detector.get_last_two_highs()
            swings_low_pair = self.analytics_engine.swing_detector.get_last_two_lows()

            for signal_id, signal in self.signal_manager.active_signals.items():
                if signal.is_in_zone(self.current_price):
                    confirmations = self.signal_processor.get_divergence_confirmations(
                        signal,
                        swings_high_pair,
                        swings_low_pair,
                        obi_z,
                        oi_slope,
                        self.current_volatility,
                        suppress_obi,
                    )
                    divergence_results[signal_id] = confirmations
                    
            transitions = self.signal_manager.update_signals(self.current_price, divergence_results)
            
            for transition in transitions:
                if transition.action == 'place_order':
                    signal = transition.signal
                    if signal:
                        await self.execute_signal(signal)
                        
                elif transition.action == 'cancel_order':
                    snapshot = transition.signal
                    if snapshot and snapshot.order_id:
                        try:
                            ok = await self.execution_manager.cancel_order(snapshot.order_id)
                            if ok:
                                logger.info(
                                    "Cancelled signal %s order %s",
                                    transition.signal_id,
                                    snapshot.order_id,
                                )
                            else:
                                logger.warning(
                                    "Cancel request failed for signal %s order %s",
                                    transition.signal_id,
                                    snapshot.order_id,
                                )
                        except Exception as e:
                            logger.error(
                                "Cancel error for signal %s: %s",
                                transition.signal_id,
                                e,
                            )
                    # Persist cancelled state
                    if snapshot:
                        await self.persister.insert_signal(snapshot.to_dict())

            # Fill detection for ARMED orders (explicit zone-hit / threshold crossing)
            for sig in list(self.signal_manager.active_signals.values()):
                if sig.state.value == 'armed':
                    # Determine trigger condition based on order type
                    trigger_price = sig.level_price
                    filled = False
                    if getattr(sig, 'is_breakthrough', False):
                        if sig.side == 'long' and self.current_price is not None and self.current_price >= trigger_price:
                            filled = True
                        if sig.side == 'short' and self.current_price is not None and self.current_price <= trigger_price:
                            filled = True
                    else:
                        if sig.side == 'long' and self.current_price is not None and self.current_price <= trigger_price:
                            filled = True
                        if sig.side == 'short' and self.current_price is not None and self.current_price >= trigger_price:
                            filled = True

                    # Only auto-mark filled in paper mode; otherwise rely on user-data stream
                    if filled and self.execution_manager.paper_mode:
                        entry_px = trigger_price
                        # Ensure stop/target computed
                        atr = self.current_price * 0.01
                        stop_price = sig.stop_price or self.risk_manager.calculate_stop_price(
                            entry_px, sig.side, atr, self.current_price * self.levels_cfg['zone_half_width_pct']
                        )
                        target_price = sig.target_price or self.risk_manager.calculate_target_price(
                            entry_px,
                            stop_price,
                            sig.side,
                            target_rr=2.0 if getattr(sig, 'is_breakthrough', False) else 1.5
                        )
                        self.signal_manager.mark_filled(
                            sig.signal_id,
                            self._qprice(entry_px),
                            self._qprice(stop_price),
                            self._qprice(target_price),
                            sig.order_id or 'unknown'
                        )
                        self._arm_hold_timer(sig)
                        await self.persister.insert_signal(sig.to_dict())
                    
            for signal in list(self.signal_manager.active_signals.values()):
                if signal.state in (SignalState.FILLED, SignalState.PARTIAL) and signal.entry_price:
                    # Check stop/target exit
                    hit_stop = False
                    hit_target = False
                    
                    if signal.stop_price and self.current_price:
                        if signal.side == 'long' and self.current_price <= signal.stop_price:
                            hit_stop = True
                        elif signal.side == 'short' and self.current_price >= signal.stop_price:
                            hit_stop = True
                    
                    if signal.target_price and self.current_price:
                        if signal.side == 'long' and self.current_price >= signal.target_price:
                            hit_target = True
                        elif signal.side == 'short' and self.current_price <= signal.target_price:
                            hit_target = True
                    
                    if hit_stop or hit_target:
                        exit_price = signal.stop_price if hit_stop else signal.target_price
                        reason = 'stop' if hit_stop else 'target'
                        await self._finalize_signal_exit(signal, exit_price, reason)
                        continue

                    hold_seconds = self.risk_manager.get_max_hold_seconds()
                    if (
                        hold_seconds > 0
                        and signal.hold_expires_at
                        and time.time() >= signal.hold_expires_at
                    ):
                        exit_price = self.current_price or signal.entry_price or 0.0
                        await self._finalize_signal_exit(signal, exit_price, 'hold_timeout')
                        continue
                    
                    # Trailing stop update
                    avwap_id = f"entry_{signal.signal_id}"
                    if avwap_id not in self.analytics_engine.avwap_manager.avwaps:
                        self.analytics_engine.avwap_manager.create_avwap(avwap_id, signal.filled_at, 'entry')
                        self._persist_avwap_snapshot()
                        
                    avwap_obj = self.analytics_engine.avwap_manager.get_avwap(avwap_id)
                    if avwap_obj and avwap_obj.avwap and avwap_obj.sigma:
                        trail_stop = self.risk_manager.update_trail_stop(
                            signal.entry_price,
                            self.current_price,
                            avwap_obj.avwap,
                            avwap_obj.sigma,
                            signal.side
                        )
                        if trail_stop:
                            signal.stop_price = trail_stop
                            logger.info(
                                "Updated trailing stop for %s to %.2f",
                                signal.signal_id,
                                trail_stop,
                            )
                    
            state_counts: Dict[str, int] = {state.value: 0 for state in SignalState}
            for s in self.signal_manager.active_signals.values():
                state_counts[s.state.value] = state_counts.get(s.state.value, 0) + 1
            metrics.update_signals(state_counts)
            
    async def execute_signal(self, signal):
        if self._is_trading_blocked():
            logger.warning(
                "Unsafe mode active, skipping signal %s",
                signal.signal_id,
            )
            return
        # Check concurrent position limit
        current_filled = [s for s in self.signal_manager.active_signals.values() 
                         if s.state in (SignalState.FILLED, SignalState.PARTIAL) and s.side == signal.side]
        if not self.risk_manager.can_add_signal(len(current_filled)):
            logger.warning(
                "Max concurrent %s positions reached; skipping signal %s",
                signal.side,
                signal.signal_id,
            )
            return
        
        # Check funding window block
        funding_interval = self.config.backtest.get('funding_interval_s', 28800)
        no_position_min = self.config.risk.get('no_position_before_funding_min', 5)
        current_ts = time.time()
        time_to_next_funding = funding_interval - (current_ts % funding_interval)
        if time_to_next_funding < no_position_min * 60:
            logger.info(
                "Too close to funding window (%.1f min); skipping signal %s",
                time_to_next_funding / 60.0,
                signal.signal_id,
            )
            return
        
        equity = await self.execution_manager.get_account_equity()
        if equity is None:
            equity = 1000
        metrics.update_equity(equity)

        # Check drawdown limit
        if self.risk_manager.check_drawdown_limit(equity):
            await self.handle_kill_switch('max_drawdown_exceeded')
            return

        atr = self.current_price * 0.01

        qty_usd = self.risk_manager.calculate_position_size(
            equity,
            atr,
            self.tick_size,
            signal.side,
            self.current_price or signal.level_price,
            self.current_funding or 0.0,
        )

        prepared = self._prepare_order_details(
            signal=signal,
            equity=equity,
            atr=atr,
            entry_price=signal.level_price,
            qty_usd=qty_usd,
            target_rr=1.5,
            notional_cap_msg="Notional cap reached; skipping signal %s",
            headroom_exhausted_msg="Headroom exhausted for signal %s",
            zero_size_msg="Order size rounded to zero for signal %s",
        )
        if not prepared:
            return
        px, amount, stop_price, target_price = prepared
        
        order_side = 'buy' if signal.side == 'long' else 'sell'
        
        path_start = self._last_market_event_ts
        t0 = time.monotonic()
        order = await self.execution_manager.place_limit_order(order_side, px, amount)
        metrics.record_order_send_latency(time.monotonic() - t0)
        
        if order:
            order_id = order.get('id', 'unknown')
            logger.info(
                "Placed %s order %s @ %.2f, qty=%.4f",
                order_side,
                order_id,
                px,
                amount,
            )
            
            # Register order details without marking filled
            signal.order_id = order_id
            signal.stop_price = stop_price
            signal.target_price = target_price
            signal.execution_mode = 'limit'
            signal.feature_vector = self._build_feature_vector(signal, mode='limit')
            signal.path_start_monotonic = path_start
            
            metrics.record_order_placed('limit')
            self._record_signal_latency('limit', path_start)
            self.signal_auditor.record_plan(signal)
            
            await alert_webhook.signal_alert(
                signal.signal_id,
                signal.side,
                px,
                'order_placed'
            )
            
            await self.persister.insert_signal(signal.to_dict())
        else:
            logger.error(
                "Failed to place order for signal %s",
                signal.signal_id,
            )

    def _build_feature_vector(self, signal, mode: str) -> Dict:
        now = time.time()
        depth_stats = self.last_depth_stats or {}
        feature_vector = {
            'timestamp': now,
            'mode': mode,
            'level_price': signal.level_price,
            'current_price': self.current_price,
            'rsi': self.indicators.get_rsi(),
            'cvd': self.cvd_calc.get_cvd(),
            'obi': self.obi_calc.get_latest_obi(),
            'obi_z': self.obi_calc.get_obi_z_score(),
            'depth_bid_qty': depth_stats.get('bid_qty'),
            'depth_ask_qty': depth_stats.get('ask_qty'),
            'depth_levels': depth_stats.get('depth'),
            'total_depth': depth_stats.get('total_depth'),
            'best_bid': depth_stats.get('best_bid'),
            'best_ask': depth_stats.get('best_ask'),
            'mid_price': depth_stats.get('mid_price'),
            'spread': depth_stats.get('spread'),
            'oi_slope': self.oi_analyzer.get_slope(),
            'divergences': signal.divergences,
            'cvd_unstable': not self.cvd_calc.is_stable(),
            'microstructure_degraded': time.time() < self.microstructure_contaminated_until,
            'volatility': self.current_volatility,
            'requested_qty': signal.requested_qty,
            'requested_notional': signal.requested_notional,
            'naked_poc_count': len(self.poc_tracker.get_naked_pocs()),
            'avwap_count': len(self.avwap_manager.get_all()),
            'active_levels': [
                {
                    'price': lvl.get('price'),
                    'type': lvl.get('type'),
                    'score': lvl.get('score')
                }
                for lvl in self.current_levels[:5]
            ]
        }
        return feature_vector

    def _current_total_exposure_usd(self) -> float:
        price = self.current_price or 0.0
        exposure = abs(self.reported_position * price)
        for sig in self.signal_manager.active_signals.values():
            outstanding = max((sig.requested_qty or 0.0) - (sig.filled_qty or 0.0), 0.0)
            if outstanding <= 0:
                continue
            ref_price = sig.level_price or price
            exposure += abs(outstanding * (ref_price or 0.0))
        return exposure

    def _available_notional_headroom(self, equity: float) -> float:
        equity = equity or 0.0
        cap = min(self.risk_manager.max_notional, equity * self.risk_manager.notional_cap_pct)
        used = self._current_total_exposure_usd()
        return max(cap - used, 0.0)

    def _compute_price_and_amount(self, notional_usd: float, raw_price: float) -> Tuple[float, float]:
        """
        Quantize price to the current tick size and derive contract amount
        from a USD notional, applying exchange-specific contract metadata.
        """
        px = self._qprice(raw_price)
        contract_mult = self.execution_manager.get_contract_multiplier()
        amt_step = self.execution_manager.get_amount_step()
        denom = max(px * max(contract_mult, 1e-9), 1e-9)
        amount = max(notional_usd / denom, 0.0)
        if amt_step:
            amount = (amount // amt_step) * amt_step
        return px, amount

    def _mark_market_event(self):
        self._last_market_event_ts = time.monotonic()

    def _record_signal_latency(self, path: str, start_ts: float):
        if start_ts is None or start_ts <= 0:
            return
        latency = time.monotonic() - start_ts
        if latency >= 0:
            metrics.record_signal_path_latency(path, latency)

    def _record_order_ack_latency(self, signal: Signal, status: str) -> float:
        if signal is None:
            return 0.0
        if signal.ack_logged:
            return signal.last_ack_latency or 0.0
        start_ts = getattr(signal, 'path_start_monotonic', None)
        if not start_ts:
            return 0.0
        latency = time.monotonic() - start_ts
        if latency < 0:
            return 0.0
        path_label = f"{signal.execution_mode}_full_path"
        metrics.record_signal_path_latency(path_label, latency)
        signal.ack_logged = True
        signal.last_ack_latency = latency
        signal.path_start_monotonic = None
        return latency

    async def _run_signal_audits(self):
        interval = max(self.monitoring_cfg.get('signal_audit_interval_s', 60), 5)
        while self.running:
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            try:
                self.signal_auditor.audit_active_signals(self.signal_manager.active_signals)
            except Exception as exc:
                logger.error("Signal audit loop error: %s", exc)

    def _is_trading_blocked(self) -> bool:
        if not self.execution_manager.paper_mode:
            if self._check_unsafe_mode():
                return True
            if time.time() < self.microstructure_contaminated_until:
                return True
            if self.book_manager.is_stale():
                return True
        return False


    
    async def _check_breakthrough_opportunities(self, candidate_levels: List[Dict]):
        """Check for LVN gap breakthrough opportunities with strong OBI/CVD momentum"""
        if self.current_price is None:
            return
            
        # Get current order flow conditions
        obi_z = self.obi_calc.get_obi_z_score()
        self.cvd_calc.get_cvd()
        oi_slope = self.oi_analyzer.get_slope()
        
        # Find nearby LVNs ahead of price
        lvns_ahead = [
            level for level in candidate_levels if level["type"] == "LVN"
        ]
        
        for lvn in lvns_ahead:
            distance_pct = abs(lvn['price'] - self.current_price) / self.current_price
            
            # Check if LVN is within breakthrough range
            if distance_pct > self.breakthrough_cfg['min_gap_pct'] and distance_pct < 0.01:
                # Determine direction
                is_bullish = lvn['price'] > self.current_price
                
                # Check OBI threshold
                obi_check = (is_bullish and obi_z and obi_z > self.breakthrough_cfg['obi_z_threshold']) or \
                           (not is_bullish and obi_z and obi_z < -self.breakthrough_cfg['obi_z_threshold'])
                
                if not obi_check:
                    continue
                    
                # Check OI slope requirement
                if self.breakthrough_cfg['oi_slope_positive_required'] and oi_slope is not None:
                    if (is_bullish and oi_slope <= 0) or (not is_bullish and oi_slope >= 0):
                        continue
                        
                # Check if we haven't exceeded max breakthrough trades for this period
                recent_breakthroughs = [s for s in self.signal_manager.active_signals.values() 
                                       if hasattr(s, 'is_breakthrough') and s.is_breakthrough]
                if len(recent_breakthroughs) >= self.breakthrough_cfg.get('max_per_4h', 1):
                    continue
                    
                logger.info(
                    "Breakthrough detected at %.2f, OBI z=%.2f, OI slope=%s",
                    lvn["price"],
                    obi_z,
                    oi_slope,
                )
                
                # Create breakthrough stop-entry order
                side = 'long' if is_bullish else 'short'
                zone_half_width = self.current_price * self.levels_cfg['zone_half_width_pct']
                
                signal = self.signal_manager.create_signal(
                    lvn, side, 
                    lvn['price'] - zone_half_width,
                    lvn['price'] + zone_half_width,
                    self.current_price, 
                    self.symbol
                )
                signal.is_breakthrough = True
                
                # Execute as stop-market order
                await self._execute_breakthrough(signal, lvn['price'])
                
    async def _execute_breakthrough(self, signal, entry_price: float):
        """Execute breakthrough with stop-market order"""
        equity = await self.execution_manager.get_account_equity()
        if equity is None:
            equity = 1000

        atr = self.current_price * 0.01

        # Reduced size for breakthrough (higher risk)
        qty_usd = self.risk_manager.calculate_position_size(
            equity,
            atr,
            self.tick_size,
            signal.side,
            self.current_price or entry_price,
            self.current_funding or 0.0,
        ) * 0.5  # Half size for breakthrough

        prepared = self._prepare_order_details(
            signal=signal,
            equity=equity,
            atr=atr,
            entry_price=entry_price,
            qty_usd=qty_usd,
            target_rr=2.0,
            notional_cap_msg="Notional cap reached; skipping breakthrough %s",
            headroom_exhausted_msg="Headroom exhausted for breakthrough %s",
            zero_size_msg="Breakthrough order size rounded to zero for %s",
        )
        if not prepared:
            return
        px, amount, stop_price, target_price = prepared
        
        order_side = 'buy' if signal.side == 'long' else 'sell'
        
        # Place stop-market order
        path_start = self._last_market_event_ts
        t0 = time.monotonic()
        order = await self.execution_manager.place_stop_market_order(order_side, px, amount)
        metrics.record_order_send_latency(time.monotonic() - t0)
        
        if order:
            order_id = order.get('id', 'unknown')
            logger.info(
                "Placed %s stop-market %s @ %.2f, qty=%.4f",
                order_side,
                order_id,
                px,
                amount,
            )
            
            # Register order details without marking filled
            signal.order_id = order_id
            signal.stop_price = stop_price
            signal.target_price = target_price
            signal.execution_mode = 'breakthrough'
            signal.feature_vector = self._build_feature_vector(signal, mode='breakthrough')
            signal.path_start_monotonic = path_start
            
            metrics.record_order_placed('stop_market')
            self._record_signal_latency('stop_market', path_start)
            self.signal_auditor.record_plan(signal)
            
            await alert_webhook.signal_alert(
                signal.signal_id,
                signal.side,
                px,
                'breakthrough_order_placed'
            )
            await self.persister.insert_signal(signal.to_dict())

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
        headroom = self._available_notional_headroom(equity)
        if headroom <= 0:
            logger.warning(
                notional_cap_msg,
                signal.signal_id,
            )
            return None
        qty_usd = min(qty_usd, headroom)
        if qty_usd <= 0:
            logger.warning(
                headroom_exhausted_msg,
                signal.signal_id,
            )
            return None

        px, amount = self._compute_price_and_amount(qty_usd, entry_price)
        if amount <= 0:
            logger.warning(
                zero_size_msg,
                signal.signal_id,
            )
            return None

        signal.requested_qty = amount
        signal.requested_notional = qty_usd

        stop_price = self._qprice(
            self.risk_manager.calculate_stop_price(
                px,
                signal.side,
                atr,
                self.current_price * self.levels_cfg['zone_half_width_pct'],
            )
        )

        target_price = self._qprice(
            self.risk_manager.calculate_target_price(
                px,
                stop_price,
                signal.side,
                target_rr=target_rr,
            )
        )

        return px, amount, stop_price, target_price
                
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

        self._signal_audit_task = asyncio.create_task(self._run_signal_audits())
        tasks = [
            asyncio.create_task(self.market_data_manager.start()),
            asyncio.create_task(self.persister.start()),
            asyncio.create_task(self.process_bar()),
            asyncio.create_task(self.process_levels_and_signals()),
            self._signal_audit_task,
        ]

        async def _cleanup():
            await self.stop()

        await run_tasks_with_cleanup(tasks, cleanup=_cleanup)
        
    async def stop(self):
        self.running = False
        await self.market_data_manager.stop()
        await self.persister.stop()
        if self._signal_audit_task is not None:
            self._signal_audit_task.cancel()
            await asyncio.gather(self._signal_audit_task, return_exceptions=True)
            self._signal_audit_task = None
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
