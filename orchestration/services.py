import asyncio
import logging
import time
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING

from api.alerts import alert_webhook
from api.metrics import metrics
from strategy.execution_types import OrderTicket
from strategy.signal_manager import SignalState, Signal

if TYPE_CHECKING:
    from main import TradingSystem


logger = logging.getLogger(__name__)


class MarketEventService:
    def __init__(self, system: 'TradingSystem', microstructure_monitor, persistence_coordinator):
        self.system = system
        self.microstructure = microstructure_monitor
        self.persistence = persistence_coordinator

    def mark_market_event(self) -> None:
        self.system._last_market_event_ts = time.monotonic()

    async def resync_orderbook(self, reason: str):
        system = self.system
        logger.info("Order book resync triggered: %s", reason)
        try:
            snapshot = await system.ws_client.fetch_orderbook_snapshot(
                system.orderbook_cfg.get('max_depth', 200)
            )
            system.book_manager.process_snapshot(snapshot)
            system.order_flow.mark_cvd_resynced()
            self.microstructure.mark_contaminated(reason)
            metrics.record_orderbook_resync()
        except Exception as exc:
            logger.error("Order book resync failed: %s", exc)
            if not system.execution_manager.paper_mode:
                await system.execution_supervisor.handle_kill_switch('orderbook_resync_failed')
            else:
                self.microstructure.mark_contaminated('orderbook_resync_failed')

    async def handle_trade(self, trade: Dict):
        system = self.system
        price = float(trade['price'])
        timestamp = trade['timestamp'] / 1000
        latency_s = max(0.0, time.time() - timestamp)
        self.mark_market_event()

        system.current_price = price
        system.analytics_engine.on_trade(trade)

        cvd_value = system.order_flow.get_cvd()
        stable_cvd = system.order_flow.is_cvd_stable()

        metrics.record_trade(latency_s)
        metrics.update_price(price)
        if stable_cvd:
            metrics.update_cvd(cvd_value)
        else:
            self.microstructure.mark_contaminated('cvd_gap')

        await self.persistence.persist_trade(trade)
        self.microstructure.check_backpressure()

    async def handle_orderbook(self, orderbook: Dict):
        system = self.system
        payload = orderbook.get('info') or orderbook
        is_snapshot = (
            'lastUpdateId' in orderbook or
            'nonce' in orderbook or
            (not system.book_manager.initialized and payload.get('lastUpdateId'))
        )

        if is_snapshot:
            snapshot = {
                'bids': orderbook.get('bids') or payload.get('b') or [],
                'asks': orderbook.get('asks') or payload.get('a') or [],
                'lastUpdateId': orderbook.get('lastUpdateId') or payload.get('lastUpdateId') or payload.get('nonce')
            }
            system.book_manager.process_snapshot(snapshot)
        else:
            has_diff_fields = any(k in payload for k in ('U', 'u', 'pu'))
            if not has_diff_fields:
                snap = {
                    'bids': orderbook.get('bids') or payload.get('b') or [],
                    'asks': orderbook.get('asks') or payload.get('a') or [],
                    'lastUpdateId': payload.get('lastUpdateId') or payload.get('nonce') or orderbook.get('nonce')
                }
                system.book_manager.process_snapshot(snap)
            else:
                update = {
                    'bids': payload.get('bids') or payload.get('b') or orderbook.get('bids', []),
                    'asks': payload.get('asks') or payload.get('a') or orderbook.get('asks', []),
                    'u': payload.get('u') or orderbook.get('lastUpdateId'),
                    'U': payload.get('U') or payload.get('firstUpdateId'),
                    'pu': payload.get('pu'),
                    'E': payload.get('E') or orderbook.get('timestamp')
                }
                if not system.book_manager.process_update(update):
                    await self.resync_orderbook('sequence_break')
                    return

        self.mark_market_event()

        if not system.book_manager.validate():
            await self.resync_orderbook('invariant_violation')
            return

        if system.book_manager.is_stale(time.monotonic()):
            self.microstructure.mark_contaminated('stale_book')

        obi_stats = system.analytics_engine.on_orderbook(orderbook, system.book_manager)
        system.last_depth_stats = dict(obi_stats)
        obi_z = obi_stats.get('obi_z')

        metrics.record_orderbook_update(obi_stats)
        if obi_z is not None:
            metrics.update_obi_z(obi_z)
            self.microstructure.mark_healthy()
        else:
            metrics.mark_microstructure(False, 'obi_disabled')

        await self.persistence.persist_orderbook(
            obi_stats,
            system.book_manager.to_dict()
        )
        self.microstructure.check_backpressure()

    async def handle_ticker(self, ticker: Dict):
        system = self.system
        mark_price = ticker.get('info', {}).get('markPrice')
        funding_rate = ticker.get('info', {}).get('fundingRate')

        if mark_price:
            system.current_price = float(mark_price)
            metrics.update_price(system.current_price)
        if funding_rate:
            system.current_funding = float(funding_rate)
            metrics.update_funding(system.current_funding)
            if system.last_funding_rate is None or system.current_funding != system.last_funding_rate:
                ts = ticker['timestamp'] / 1000
                anchor_id = f"funding_{int(ts)}"
                system.profile_service.create_avwap(anchor_id, ts, 'funding')
                self.persistence.persist_avwap_snapshot()
                system.profile_service.create_event_anchor(anchor_id, ts)
                system.last_funding_anchor_ts = ts
                system.last_funding_rate = system.current_funding

        await self.persistence.persist_mark_price({
            'symbol': system.symbol,
            'timestamp': ticker['timestamp'],
            'mark_price': system.current_price,
            'funding_rate': system.current_funding
        })

    async def handle_oi(self, oi_data: Dict):
        system = self.system
        if oi_data is None:
            if not system.execution_manager.paper_mode:
                logger.error("Open interest fetch failed; triggering kill switch")
                await system.execution_supervisor.handle_kill_switch('oi_fetch_failed')
            return

        system.analytics_engine.on_oi(oi_data)
        await self.persistence.persist_open_interest(oi_data)

    async def handle_gap(self, gap_duration: float):
        self.microstructure.mark_contaminated('gap')
        metrics.record_reconnect()
        try:
            await self.resync_orderbook('stream_gap')
        except Exception as exc:
            logger.error("Order book resync on gap failed: %s", exc)
        if gap_duration > 10:
            await alert_webhook.send_alert('stream_gap', f'Stream gap {gap_duration:.1f}s', 'warning')

    async def handle_latency_event(self, drift_ms: float):
        system = self.system
        await alert_webhook.latency_alert(drift_ms)
        if not system.execution_manager.paper_mode:
            if drift_ms > system.monitoring_cfg.get('latency_budget_ms', 50):
                await system.execution_supervisor.handle_kill_switch('latency_drift')

    async def handle_drop(self, reason: str):
        metrics.record_drop(reason)
        self.microstructure.mark_contaminated(f"message_loss_{reason}")


class AnalyticsBarService:
    def __init__(self, system: 'TradingSystem'):
        self.system = system
        self._run_task: Optional[asyncio.Task] = None
        self._latest_rsi: Optional[float] = None
        self._current_volatility: float = system.current_volatility
        self._last_swing: Optional[Dict] = None

    async def start(self):
        if self._run_task is None:
            self._run_task = asyncio.create_task(self._run())

    async def stop(self):
        if self._run_task is not None:
            self._run_task.cancel()
            await asyncio.gather(self._run_task, return_exceptions=True)
            self._run_task = None

    async def _run(self):
        system = self.system
        while system.running:
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break

            if system.current_price is None:
                continue

            now = time.time()
            system.analytics_engine.on_bar(system.current_price, now)

            indicators = system.order_flow.indicators
            rsi = indicators.get_rsi()
            self._latest_rsi = rsi
            if rsi is not None:
                metrics.update_rsi(rsi)

            realized_vol = indicators.get_realized_volatility()
            if realized_vol is not None:
                self._current_volatility = realized_vol
                system.current_volatility = realized_vol
                metrics.update_realized_volatility(realized_vol)

            macd_vals = indicators.get_macd()
            macd_hist = macd_vals.get('histogram') if macd_vals else None
            footprint_bar = system.order_flow.get_current_footprint_bar()
            volume = footprint_bar.total_volume if footprint_bar else 0.0

            new_swing = system.swing_service.update(
                system.current_price,
                now,
                rsi,
                system.order_flow.get_cvd(),
                system.order_flow.get_obi_z(),
                macd_hist=macd_hist,
                volume=volume,
            )

            if new_swing:
                self._last_swing = new_swing
                logger.info(
                    "New swing %s at %.2f",
                    new_swing["type"],
                    new_swing["price"],
                )

                anchor_id = f"swing_{new_swing['type']}_{int(new_swing['timestamp'])}"
                system.profile_service.create_avwap(anchor_id, new_swing['timestamp'], f"swing_{new_swing['type']}")
                system.persistence_coordinator.persist_avwap_snapshot()
                system.profile_service.create_event_anchor(anchor_id, new_swing['timestamp'])

            mono_now = time.monotonic()
            for stream, last_seen in system.market_data_manager.ws_client.stream_last_seen.items():
                lag = max(0.0, mono_now - float(last_seen))
                metrics.update_stream_lag(stream, lag)
            metrics.update_queue_depth('book', len(system.persister.book_buffer))
            metrics.update_queue_depth('mark', len(system.persister.mark_buffer))
            metrics.update_queue_depth('oi', len(system.persister.oi_buffer))
            metrics.update_queue_depth('indicator', len(system.persister.indicator_buffer))
            metrics.update_queue_depth('signal', len(system.persister.signal_buffer))

            indicator_snapshot = system.analytics_engine.get_indicator_snapshot()
            await system.persistence_coordinator.persist_indicator_state(indicator_snapshot)

    def get_current_rsi(self) -> Optional[float]:
        return self._latest_rsi

    def get_current_volatility(self) -> float:
        if self._current_volatility is not None:
            return self._current_volatility
        return self.system.current_volatility

    def get_last_swing(self) -> Optional[Dict]:
        return self._last_swing

    def get_recent_swings(self, count: int = 10) -> List[Dict]:
        return self.system.swing_service.get_last_n_swings(count)


class SignalLifecycleService:
    def __init__(self, system: 'TradingSystem', bar_service: AnalyticsBarService):
        self.system = system
        self.bar_service = bar_service

    def zone_half_width_pct(self) -> float:
        system = self.system
        base_pct = system.levels_cfg.get('zone_half_width_pct', 0.0015)
        current_vol = self.bar_service.get_current_volatility()
        if current_vol > 0.02:
            return base_pct * (1 + current_vol / 0.01)
        return base_pct

    def has_conflicting_signal(self, price: float) -> bool:
        system = self.system
        current = system.current_price or 0.0
        if current <= 0:
            return False
        threshold = system._signal_overlap_pct
        return any(
            abs(signal.level_price - price) / current < threshold
            for signal in system.signal_manager.active_signals.values()
        )

    def build_feature_context(self, level: Dict) -> Dict:
        return {
            'type': level.get('type'),
            'score': level.get('score'),
            'prominence': level.get('prominence'),
            'window': level.get('window')
        }

    def build_current_levels(self, long_levels: List[Dict], short_levels: List[Dict]) -> List[Dict]:
        try:
            return [
                {**lvl, 'side': 'long'} for lvl in long_levels
            ] + [
                {**lvl, 'side': 'short'} for lvl in short_levels
            ]
        except Exception:
            return []

    def create_signals_for_levels(self, levels: List[Dict], side: str, zone_width_pct: float) -> None:
        system = self.system
        if system.current_price is None:
            return
        for level in levels:
            price = level['price']
            if self.has_conflicting_signal(price):
                continue
            effective_pct = zone_width_pct
            shelf_width = level.get('shelf_width')
            if shelf_width and shelf_width > 0 and system.current_price and system.current_price > 0:
                shelf_mult = system.levels_cfg.get('zone_half_width_shelf_multiplier', 0.5)
                shelf_pct = (shelf_width / system.current_price) * max(shelf_mult, 0.0)
                if shelf_pct > 0:
                    effective_pct = min(zone_width_pct, shelf_pct)
            zone_half_width = system.current_price * effective_pct
            zone_low = price - zone_half_width
            zone_high = price + zone_half_width
            signal = system.signal_manager.create_signal(
                level,
                side,
                zone_low,
                zone_high,
                system.current_price,
                system.symbol
            )
            feature_context = self.build_feature_context(level)
            logger.info(
                "Created %s signal at %.2f, context=%s",
                side.upper(),
                price,
                feature_context,
            )

    def should_skip_level_processing(self) -> bool:
        system = self.system
        if system.current_price is None:
            return True
        if system.execution_manager.paper_mode:
            return False
        return system.microstructure_monitor.is_contaminated()

    async def process_levels_and_signals(self):
        system = self.system
        while system.running:
            await asyncio.sleep(10)

            if self.should_skip_level_processing():
                continue

            candidate_levels = system.analytics_engine.get_candidate_levels()

            now_ts = time.time()
            system.profile_service.register_levels(candidate_levels, now_ts)
            candidate_levels = system.profile_service.enrich_levels(candidate_levels, now_ts)

            best_bid = system.book_manager.get_best_bid()
            best_ask = system.book_manager.get_best_ask()
            if best_bid and best_ask:
                system.profile_service.note_poc_touch(best_bid[0], best_ask[0], time.time())

            system.profile_service.cleanup_pocs(max_age_hours=72)

            naked_pocs = system.profile_service.get_naked_pocs()
            avwaps = system.profile_service.get_avwaps()
            swings = self.bar_service.get_recent_swings(10)

            long_levels, short_levels = system.signal_processor.select_levels(
                candidate_levels,
                system.current_price,
                naked_pocs,
                avwaps,
                swings,
            )

            system.current_levels = self.build_current_levels(long_levels, short_levels)

            await system.execution_supervisor.check_breakthrough_opportunities(candidate_levels)

            zone_width_pct = self.zone_half_width_pct()
            self.create_signals_for_levels(long_levels, 'long', zone_width_pct)
            self.create_signals_for_levels(short_levels, 'short', zone_width_pct)

            obi_z = system.order_flow.get_obi_z()
            oi_slope = system.order_flow.get_oi_slope()

            top_levels = system.book_manager.get_top_levels(system.microstructure_cfg.get('obi_depth_levels', 10))
            book_depth = len(top_levels['bids']) + len(top_levels['asks'])
            suppress_obi = (
                book_depth < system.microstructure_cfg.get('obi_disable_depth', 10) or
                ((not system.execution_manager.paper_mode) and system.microstructure_monitor.is_contaminated()) or
                system.order_flow.is_obi_contaminated()
            )

            divergence_results = {}
            swings_high_pair = system.swing_service.get_last_two_highs()
            swings_low_pair = system.swing_service.get_last_two_lows()

            for signal_id, signal in system.signal_manager.active_signals.items():
                if signal.is_in_zone(system.current_price):
                    confirmations = system.signal_processor.get_divergence_confirmations(
                        signal,
                        swings_high_pair,
                        swings_low_pair,
                        obi_z,
                        oi_slope,
                        self.bar_service.get_current_volatility(),
                        suppress_obi,
                    )
                    divergence_results[signal_id] = confirmations

            transitions = system.signal_manager.update_signals(system.current_price, divergence_results)

            for transition in transitions:
                if transition.action == 'place_order':
                    signal = transition.signal
                    if signal:
                        await system.execution_supervisor.execute_signal(signal)

                elif transition.action == 'cancel_order':
                    snapshot = transition.signal
                    if snapshot and snapshot.order_id:
                        try:
                            ok = await system.execution_manager.cancel_order(snapshot.order_id)
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
                        except Exception as exc:
                            logger.error(
                                "Cancel error for signal %s: %s",
                                transition.signal_id,
                                exc,
                            )
                    if snapshot:
                        await system.persistence_coordinator.persist_signal(snapshot.to_dict())

            for sig in list(system.signal_manager.active_signals.values()):
                if sig.state.value == 'armed':
                    trigger_price = sig.level_price
                    filled = False
                    if getattr(sig, 'is_breakthrough', False):
                        if sig.side == 'long' and system.current_price is not None and system.current_price >= trigger_price:
                            filled = True
                        if sig.side == 'short' and system.current_price is not None and system.current_price <= trigger_price:
                            filled = True
                    else:
                        if sig.side == 'long' and system.current_price is not None and system.current_price <= trigger_price:
                            filled = True
                        if sig.side == 'short' and system.current_price is not None and system.current_price >= trigger_price:
                            filled = True

                    if filled and system.execution_manager.paper_mode:
                        entry_px = trigger_price
                        atr = system.current_price * 0.01
                        stop_price = sig.stop_price or system.risk_manager.calculate_stop_price(
                            entry_px, sig.side, atr, system.current_price * system.levels_cfg['zone_half_width_pct']
                        )
                        target_price = sig.target_price or system.risk_manager.calculate_target_price(
                            entry_px,
                            stop_price,
                            sig.side,
                            target_rr=2.0 if getattr(sig, 'is_breakthrough', False) else 1.5,
                        )
                        system.signal_manager.mark_filled(
                            sig.signal_id,
                            system._qprice(entry_px),
                            system._qprice(stop_price),
                            system._qprice(target_price),
                            sig.order_id or 'unknown'
                        )
                        system.execution_supervisor.arm_hold_timer(sig)
                        await system.persistence_coordinator.persist_signal(sig.to_dict())

            for signal in list(system.signal_manager.active_signals.values()):
                if signal.state in (SignalState.FILLED, SignalState.PARTIAL) and signal.entry_price:
                    hit_stop = False
                    hit_target = False

                    if signal.stop_price and system.current_price:
                        if signal.side == 'long' and system.current_price <= signal.stop_price:
                            hit_stop = True
                        elif signal.side == 'short' and system.current_price >= signal.stop_price:
                            hit_stop = True

                    if signal.target_price and system.current_price:
                        if signal.side == 'long' and system.current_price >= signal.target_price:
                            hit_target = True
                        elif signal.side == 'short' and system.current_price <= signal.target_price:
                            hit_target = True

                    if hit_stop or hit_target:
                        exit_price = signal.stop_price if hit_stop else signal.target_price
                        reason = 'stop' if hit_stop else 'target'
                        await system.execution_supervisor.finalize_signal_exit(signal, exit_price, reason)
                        continue

                    hold_seconds = system.risk_manager.get_max_hold_seconds()
                    if (
                        hold_seconds > 0
                        and signal.hold_expires_at
                        and time.time() >= signal.hold_expires_at
                    ):
                        exit_price = system.current_price or signal.entry_price or 0.0
                        await system.execution_supervisor.finalize_signal_exit(signal, exit_price, 'hold_timeout')
                        continue

                    avwap_id = f"entry_{signal.signal_id}"
                    avwap_obj = system.profile_service.get_avwap(avwap_id)
                    if not avwap_obj:
                        system.profile_service.create_avwap(avwap_id, signal.filled_at, 'entry')
                        system.persistence_coordinator.persist_avwap_snapshot()
                        avwap_obj = system.profile_service.get_avwap(avwap_id)
                    if avwap_obj and avwap_obj.avwap and avwap_obj.sigma:
                        trail_stop = system.risk_manager.update_trail_stop(
                            signal.entry_price,
                            system.current_price,
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
            for sig in system.signal_manager.active_signals.values():
                state_counts[sig.state.value] = state_counts.get(sig.state.value, 0) + 1
            metrics.update_signals(state_counts)


class ExecutionRiskSupervisor:
    def __init__(self, system: 'TradingSystem'):
        self.system = system
        self._signal_audit_task: Optional[asyncio.Task] = None

    def arm_hold_timer(self, signal: Signal) -> None:
        system = self.system
        hold_seconds = system.risk_manager.get_max_hold_seconds()
        if hold_seconds > 0:
            signal.hold_expires_at = time.time() + hold_seconds
        else:
            signal.hold_expires_at = None

    async def finalize_signal_exit(self, signal: Signal, exit_price: float, reason: str) -> None:
        system = self.system
        pnl_usd = 0.0
        qty = 0.0
        if signal.order_id:
            qty = system.execution_manager.get_paper_position_qty(signal.order_id)
        if qty <= 0 and signal.filled_qty:
            qty = signal.filled_qty
        if signal.entry_price is not None and qty > 0:
            if signal.side == 'long':
                pnl_usd = (exit_price - signal.entry_price) * qty
            else:
                pnl_usd = (signal.entry_price - exit_price) * qty
            system.execution_manager.update_paper_equity(pnl_usd)
            if signal.order_id:
                system.execution_manager.remove_paper_position(signal.order_id)

        system.signal_manager.mark_closed(signal.signal_id, exit_price, pnl_usd, reason=reason)
        metrics.record_signal_closed(reason)
        logger.info(
            "Exit %s %s @ %.2f, PnL=%.2f",
            signal.signal_id,
            reason.upper(),
            exit_price,
            pnl_usd,
        )
        await system.persistence_coordinator.persist_signal(signal.to_dict())

    async def handle_signal_filled(self, signal: Signal, entry_px: float, filled_qty: float,
                                    total_qty: float, order_id: str, ack_status: str):
        system = self.system
        current_price = system.current_price or entry_px
        stop_price = signal.stop_price or system.risk_manager.calculate_stop_price(
            entry_px,
            signal.side,
            current_price * 0.01 if current_price else entry_px * 0.005,
            current_price * system.levels_cfg.get('zone_half_width_pct', 0.0015)
        )
        target_price = signal.target_price or system.risk_manager.calculate_target_price(
            entry_px,
            stop_price,
            signal.side,
            target_rr=2.0 if getattr(signal, 'is_breakthrough', False) else 1.5
        )
        system.signal_manager.mark_filled(
            signal.signal_id,
            entry_px,
            stop_price,
            target_price,
            order_id,
            filled_qty=filled_qty or total_qty,
            avg_fill_price=entry_px
        )
        self.arm_hold_timer(signal)
        metrics.record_order_filled()
        try:
            mid = system.book_manager.get_mid_price() or system.current_price
            if mid and mid > 0:
                slippage_bps = abs((entry_px - float(mid)) / float(mid)) * 10000.0
                metrics.record_slippage_bps(slippage_bps)
        except Exception:
            pass
        await self.persist_and_audit_signal(
            signal,
            'filled',
            ack_status or 'filled',
            filled_qty or total_qty,
            total_qty,
            entry_px
        )

    async def handle_signal_cancelled(self, signal: Signal, total_qty: float, ack_status: str):
        signal.state = SignalState.CANCELLED
        signal.closed_at = time.time()
        metrics.record_order_cancelled()
        await self.persist_and_audit_signal(
            signal,
            'cancelled',
            ack_status or 'cancelled',
            signal.filled_qty or 0.0,
            total_qty,
            signal.entry_price or self.system.current_price or 0.0
        )

    async def handle_signal_partial(self, signal: Signal, entry_px: float, filled_qty: float,
                                     total_qty: float, order_id: str, ack_status: str):
        system = self.system
        system.signal_manager.mark_partial(
            signal.signal_id,
            entry_px,
            filled_qty,
            entry_px,
            order_id
        )
        await self.persist_and_audit_signal(
            signal,
            'partial',
            ack_status or 'partial',
            filled_qty,
            total_qty,
            entry_px
        )

    async def handle_signal_update(self, signal: Signal, entry_px: float, filled_qty: float,
                                    total_qty: float, ack_status: str):
        await self.persist_and_audit_signal(
            signal,
            ack_status,
            ack_status,
            filled_qty,
            total_qty,
            entry_px
        )

    async def persist_and_audit_signal(self, signal: Optional[Signal], event_status: str,
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
        ack_latency = self.record_order_ack_latency(signal, ack_status)
        self.system.signal_auditor.record_execution(
            signal,
            event_status,
            payload,
            ack_latency_s=ack_latency or signal.last_ack_latency
        )
        await self.system.persistence_coordinator.persist_signal(signal.to_dict())

    def record_signal_latency(self, path: str, start_ts: float):
        if start_ts is None or start_ts <= 0:
            return
        latency = time.monotonic() - start_ts
        if latency >= 0:
            metrics.record_signal_path_latency(path, latency)

    def record_order_ack_latency(self, signal: Signal, status: str) -> float:
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

    async def start_audits(self):
        if self._signal_audit_task is None:
            self._signal_audit_task = asyncio.create_task(self._run_signal_audits())

    async def stop_audits(self):
        if self._signal_audit_task is not None:
            self._signal_audit_task.cancel()
            await asyncio.gather(self._signal_audit_task, return_exceptions=True)
            self._signal_audit_task = None

    async def _run_signal_audits(self):
        system = self.system
        interval = max(system.monitoring_cfg.get('signal_audit_interval_s', 60), 5)
        while system.running:
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            try:
                system.signal_auditor.audit_active_signals(system.signal_manager.active_signals)
            except Exception as exc:
                logger.error("Signal audit loop error: %s", exc)

    def is_trading_blocked(self) -> bool:
        system = self.system
        if not system.execution_manager.paper_mode:
            monitor = system.microstructure_monitor
            if monitor.check_unsafe_flag():
                return True
            if monitor.is_contaminated():
                return True
            if system.book_manager.is_stale():
                return True
        return False

    async def execute_signal(self, signal: Signal):
        system = self.system
        if self.is_trading_blocked():
            logger.warning(
                "Unsafe mode active, skipping signal %s",
                signal.signal_id,
            )
            return
        current_filled = [s for s in system.signal_manager.active_signals.values()
                         if s.state in (SignalState.FILLED, SignalState.PARTIAL) and s.side == signal.side]
        if not system.risk_manager.can_add_signal(len(current_filled)):
            logger.warning(
                "Max concurrent %s positions reached; skipping signal %s",
                signal.side,
                signal.signal_id,
            )
            return

        funding_interval = system.config.backtest.get('funding_interval_s', 28800)
        no_position_min = system.config.risk.get('no_position_before_funding_min', 5)
        current_ts = time.time()
        time_to_next_funding = funding_interval - (current_ts % funding_interval)
        if time_to_next_funding < no_position_min * 60:
            logger.info(
                "Too close to funding window (%.1f min); skipping signal %s",
                time_to_next_funding / 60.0,
                signal.signal_id,
            )
            return

        equity = await system.execution_manager.get_account_equity()
        if equity is None:
            equity = 1000
        metrics.update_equity(equity)

        if system.risk_manager.check_drawdown_limit(equity):
            await self.handle_kill_switch('max_drawdown_exceeded')
            return

        atr = system.current_price * 0.01

        qty_usd = system.risk_manager.calculate_position_size(
            equity,
            atr,
            system.tick_size,
            signal.side,
            system.current_price or signal.level_price,
            system.current_funding or 0.0,
        )

        prepared = self.prepare_order_details(
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

        path_start = system._last_market_event_ts
        t0 = time.monotonic()
        order = await system.execution_manager.place_limit_order(order_side, px, amount)
        metrics.record_order_send_latency(time.monotonic() - t0)

        if order:
            order_id = order.id if isinstance(order, OrderTicket) else str(getattr(order, 'get', lambda *_: 'unknown')('id', 'unknown'))
            logger.info(
                "Placed %s order %s @ %.2f, qty=%.4f",
                order_side,
                order_id,
                px,
                amount,
            )

            signal.order_id = order_id
            signal.stop_price = stop_price
            signal.target_price = target_price
            signal.execution_mode = 'limit'
            signal.feature_vector = self.build_feature_vector(signal, mode='limit')
            signal.path_start_monotonic = path_start

            metrics.record_order_placed('limit')
            self.record_signal_latency('limit', path_start)
            system.signal_auditor.record_plan(signal)

            await alert_webhook.signal_alert(
                signal.signal_id,
                signal.side,
                px,
                'order_placed'
            )

            await system.persistence_coordinator.persist_signal(signal.to_dict())
        else:
            logger.error(
                "Failed to place order for signal %s",
                signal.signal_id,
            )

    def build_feature_vector(self, signal, mode: str) -> Dict:
        system = self.system
        now = time.time()
        depth_stats = system.last_depth_stats or {}
        order_flow = system.order_flow
        profile_service = system.profile_service
        feature_vector = {
            'timestamp': now,
            'mode': mode,
            'level_price': signal.level_price,
            'current_price': system.current_price,
            'rsi': order_flow.indicators.get_rsi(),
            'cvd': order_flow.get_cvd(),
            'obi': order_flow.obi_calc.get_latest_obi(),
            'obi_z': order_flow.get_obi_z(),
            'depth_bid_qty': depth_stats.get('bid_qty'),
            'depth_ask_qty': depth_stats.get('ask_qty'),
            'depth_levels': depth_stats.get('depth'),
            'total_depth': depth_stats.get('total_depth'),
            'best_bid': depth_stats.get('best_bid'),
            'best_ask': depth_stats.get('best_ask'),
            'mid_price': depth_stats.get('mid_price'),
            'spread': depth_stats.get('spread'),
            'oi_slope': order_flow.get_oi_slope(),
            'divergences': signal.divergences,
            'cvd_unstable': not order_flow.is_cvd_stable(),
            'microstructure_degraded': system.microstructure_monitor.is_contaminated(),
            'volatility': self.bar_service.get_current_volatility(),
            'requested_qty': signal.requested_qty,
            'requested_notional': signal.requested_notional,
            'naked_poc_count': len(profile_service.get_naked_pocs()),
            'avwap_count': len(profile_service.get_avwaps()),
            'active_levels': [
                {
                    'price': lvl.get('price'),
                    'type': lvl.get('type'),
                    'score': lvl.get('score')
                }
                for lvl in system.current_levels[:5]
            ]
        }
        return feature_vector

    def current_total_exposure_usd(self) -> float:
        system = self.system
        price = system.current_price or 0.0
        exposure = abs(system.reported_position * price)
        for sig in system.signal_manager.active_signals.values():
            outstanding = max((sig.requested_qty or 0.0) - (sig.filled_qty or 0.0), 0.0)
            if outstanding <= 0:
                continue
            ref_price = sig.level_price or price
            exposure += abs(outstanding * (ref_price or 0.0))
        return exposure

    def available_notional_headroom(self, equity: float) -> float:
        system = self.system
        equity = equity or 0.0
        cap = min(system.risk_manager.max_notional, equity * system.risk_manager.notional_cap_pct)
        used = self.current_total_exposure_usd()
        return max(cap - used, 0.0)

    def compute_price_and_amount(self, notional_usd: float, raw_price: float) -> Tuple[float, float]:
        system = self.system
        px = system._qprice(raw_price)
        contract_mult = system.execution_manager.get_contract_multiplier()
        amt_step = system.execution_manager.get_amount_step()
        denom = max(px * max(contract_mult, 1e-9), 1e-9)
        amount = max(notional_usd / denom, 0.0)
        if amt_step:
            amount = (amount // amt_step) * amt_step
        return px, amount

    def prepare_order_details(
        self,
        signal: Signal,
        equity: float,
        atr: float,
        entry_price: float,
        qty_usd: float,
        target_rr: float,
        notional_cap_msg: str,
        headroom_exhausted_msg: str,
        zero_size_msg: str,
    ):
        headroom = self.available_notional_headroom(equity)
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

        px, amount = self.compute_price_and_amount(qty_usd, entry_price)
        if amount <= 0:
            logger.warning(
                zero_size_msg,
                signal.signal_id,
            )
            return None

        signal.requested_qty = amount
        signal.requested_notional = qty_usd

        system = self.system
        stop_price = system._qprice(
            system.risk_manager.calculate_stop_price(
                px,
                signal.side,
                atr,
                system.current_price * system.levels_cfg['zone_half_width_pct'],
            )
        )

        target_price = system._qprice(
            system.risk_manager.calculate_target_price(
                px,
                stop_price,
                signal.side,
                target_rr=target_rr,
            )
        )

        return px, amount, stop_price, target_price

    async def handle_kill_switch(self, reason: str):
        system = self.system
        logger.error("Kill switch triggered: %s", reason)
        metrics.record_kill_switch(reason)
        await alert_webhook.kill_switch_alert(reason)
        try:
            cancelled = await system.execution_manager.cancel_all_open_orders()
            logger.info("Kill switch cancelled %s open orders", cancelled)
        except Exception as exc:
            logger.error("Kill switch cancel-all failed: %s", exc)
        system.microstructure_monitor.mark_contaminated(f"kill_switch_{reason}")

    async def handle_user_order(self, order: Dict):
        system = self.system
        order_id = order.get('id') or order.get('orderId')
        if not order_id:
            return
        status = (order.get('status') or '').lower()
        avg_price = order.get('average') or order.get('price')
        filled_qty = float(order.get('filled') or order.get('executedQty') or 0)
        total_qty = float(order.get('amount') or order.get('origQty') or order.get('quantity') or 0)
        for signal in list(system.signal_manager.active_signals.values()):
            if signal.order_id != order_id:
                continue
            entry_px = float(avg_price or signal.entry_price or signal.level_price or system.current_price or 0)
            prev_filled = signal.filled_qty or 0.0
            incremental = max(filled_qty - prev_filled, 0.0)
            if incremental > 0:
                direction = 1 if signal.side == 'long' else -1
                system.reported_position += direction * incremental
                notional = abs(system.reported_position * (system.current_price or entry_px))
                cap = system.risk_manager.max_notional * (1 + system.risk_manager.position_epsilon_pct)
                if notional > cap:
                    await self.handle_kill_switch('position_mismatch')
                    return
                try:
                    exch_qty = await system.execution_manager.fetch_position_qty()
                    if exch_qty is not None:
                        eps = max(abs(exch_qty), 1e-9) * system.risk_manager.position_epsilon_pct
                        if abs(exch_qty - system.reported_position) > max(eps, 1e-6):
                            await self.handle_kill_switch('position_mismatch')
                            return
                except Exception as exc:
                    logger.error("Position cross-check failed: %s", exc)

            if status in ('closed', 'filled'):
                await self.handle_signal_filled(signal, entry_px, filled_qty, total_qty, order_id, status)
            elif status in ('canceled', 'cancelled', 'expired'):
                await self.handle_signal_cancelled(signal, total_qty, status)
            elif filled_qty > 0 and (total_qty == 0 or filled_qty < total_qty or status in ('partial', 'partially_filled', 'open')):
                await self.handle_signal_partial(signal, entry_px, filled_qty, total_qty, order_id, status)
            else:
                await self.handle_signal_update(signal, entry_px, filled_qty, total_qty, status or 'update')

    async def check_breakthrough_opportunities(self, candidate_levels: List[Dict]):
        system = self.system
        if system.current_price is None:
            return

        obi_z = system.order_flow.get_obi_z()
        system.order_flow.get_cvd()
        oi_slope = system.order_flow.get_oi_slope()

        lvns_ahead = [
            level for level in candidate_levels if level["type"] == "LVN"
        ]

        for lvn in lvns_ahead:
            distance_pct = abs(lvn['price'] - system.current_price) / system.current_price

            if distance_pct > system.breakthrough_cfg['min_gap_pct'] and distance_pct < 0.01:
                is_bullish = lvn['price'] > system.current_price

                obi_check = (is_bullish and obi_z and obi_z > system.breakthrough_cfg['obi_z_threshold']) or \
                           (not is_bullish and obi_z and obi_z < -system.breakthrough_cfg['obi_z_threshold'])

                if not obi_check:
                    continue

                if system.breakthrough_cfg['oi_slope_positive_required'] and oi_slope is not None:
                    if (is_bullish and oi_slope <= 0) or (not is_bullish and oi_slope >= 0):
                        continue

                recent_breakthroughs = [s for s in system.signal_manager.active_signals.values()
                                       if hasattr(s, 'is_breakthrough') and s.is_breakthrough]
                if len(recent_breakthroughs) >= system.breakthrough_cfg.get('max_per_4h', 1):
                    continue

                logger.info(
                    "Breakthrough detected at %.2f, OBI z=%.2f, OI slope=%s",
                    lvn["price"],
                    obi_z,
                    oi_slope,
                )

                side = 'long' if is_bullish else 'short'
                zone_half_width = system.current_price * system.levels_cfg['zone_half_width_pct']

                signal = system.signal_manager.create_signal(
                    lvn, side,
                    lvn['price'] - zone_half_width,
                    lvn['price'] + zone_half_width,
                    system.current_price,
                    system.symbol
                )
                signal.is_breakthrough = True

                await self.execute_breakthrough(signal, lvn['price'])

    async def execute_breakthrough(self, signal, entry_price: float):
        system = self.system
        equity = await system.execution_manager.get_account_equity()
        if equity is None:
            equity = 1000

        atr = system.current_price * 0.01

        qty_usd = system.risk_manager.calculate_position_size(
            equity,
            atr,
            system.tick_size,
            signal.side,
            system.current_price or entry_price,
            system.current_funding or 0.0,
        ) * 0.5

        prepared = self.prepare_order_details(
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

        path_start = system._last_market_event_ts
        t0 = time.monotonic()
        order = await system.execution_manager.place_stop_market_order(order_side, px, amount)
        metrics.record_order_send_latency(time.monotonic() - t0)

        if order:
            order_id = order.id if isinstance(order, OrderTicket) else str(getattr(order, 'get', lambda *_: 'unknown')('id', 'unknown'))
            logger.info(
                "Placed %s stop-market %s @ %.2f, qty=%.4f",
                order_side,
                order_id,
                px,
                amount,
            )

            signal.order_id = order_id
            signal.stop_price = stop_price
            signal.target_price = target_price
            signal.execution_mode = 'breakthrough'
            signal.feature_vector = self.build_feature_vector(signal, mode='breakthrough')
            signal.path_start_monotonic = path_start

            metrics.record_order_placed('stop_market')
            self.record_signal_latency('stop_market', path_start)
            system.signal_auditor.record_plan(signal)

            await alert_webhook.signal_alert(
                signal.signal_id,
                signal.side,
                px,
                'breakthrough_order_placed'
            )
            await system.persistence_coordinator.persist_signal(signal.to_dict())
