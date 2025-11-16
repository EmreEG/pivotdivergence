from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional
import time
import uuid



class SignalState(Enum):
    INACTIVE = "inactive"
    MONITORED = "monitored"
    ARMED = "armed"
    PARTIAL = "partial"
    FILLED = "filled"
    CANCELLED = "cancelled"
    CLOSED = "closed"

@dataclass
class Transition:
    signal_id: str
    from_state: str
    to_state: str
    action: str
    signal: Optional[Signal] = None

@dataclass
class Signal:
    symbol: str
    side: str
    level_price: float
    zone_low: float
    zone_high: float
    score: float
    signal_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    state: SignalState = SignalState.INACTIVE
    created_at: float = field(default_factory=time.time)
    monitored_at: Optional[float] = None
    armed_at: Optional[float] = None
    filled_at: Optional[float] = None
    closed_at: Optional[float] = None
    divergences: Dict = field(default_factory=dict)
    entry_price: Optional[float] = None
    stop_price: Optional[float] = None
    target_price: Optional[float] = None
    exit_price: Optional[float] = None
    pnl: Optional[float] = None
    filled_qty: float = 0.0
    avg_fill_price: Optional[float] = None
    feature_vector: Dict = field(default_factory=dict)
    requested_qty: Optional[float] = None
    requested_notional: Optional[float] = None
    partial_started_at: Optional[float] = None
    execution_mode: str = 'limit'
    path_start_monotonic: Optional[float] = None
    ack_logged: bool = False
    last_ack_latency: Optional[float] = None
    order_id: Optional[str] = None
    hold_expires_at: Optional[float] = None
    close_reason: Optional[str] = None

    def distance_to_level(self, current_price: float) -> float:
        return abs(current_price - self.level_price) / current_price
        
    def is_in_zone(self, current_price: float) -> bool:
        return self.zone_low <= current_price <= self.zone_high
        
    def beyond_stop(self, current_price: float) -> bool:
        if self.stop_price is None:
            return False
        if self.side == 'long':
            return current_price < self.stop_price
        return current_price > self.stop_price
            
    def to_dict(self) -> Dict:
        return {
            'signal_id': self.signal_id,
            'symbol': self.symbol,
            'timestamp': int(self.created_at * 1000),
            'side': self.side,
            'level_price': self.level_price,
            'zone_low': self.zone_low,
            'zone_high': self.zone_high,
            'score': self.score,
            'state': self.state.value,
            'divergences': self.divergences,
            'entry_price': self.entry_price,
            'stop_price': self.stop_price,
            'target_price': self.target_price,
            'exit_price': self.exit_price,
            'pnl': self.pnl,
            'order_id': self.order_id,
            'filled_qty': self.filled_qty,
            'avg_fill_price': self.avg_fill_price,
            'feature_vector': self.feature_vector,
            'requested_qty': self.requested_qty,
            'requested_notional': self.requested_notional,
            'execution_mode': self.execution_mode,
            'hold_expires_at': self.hold_expires_at,
            'close_reason': self.close_reason
        }

from .signal_states import InactiveState, MonitoredState, ArmedState, PartialState

class SignalManager:
    def __init__(self, config: Dict):
        self.active_signals: Dict[str, Signal] = {}
        self.monitor_distance_pct = config['levels']['monitor_distance_pct']
        self.confirmation_count = config['divergence']['confirmation_count']
        self.timeout_s = config['divergence']['timeout_s']
        execution_cfg = config.get('execution', {})
        self.partial_fill_timeout_s = execution_cfg.get('partial_fill_timeout_s', 60)
        self.partial_fill_completion_pct = execution_cfg.get('partial_fill_completion_pct', 0.9)
        
        self.state_map = {
            SignalState.INACTIVE: InactiveState,
            SignalState.MONITORED: MonitoredState,
            SignalState.ARMED: ArmedState,
            SignalState.PARTIAL: PartialState,
        }

    def create_signal(self, level: Dict, side: str, zone_low: float, 
                     zone_high: float, current_price: float, symbol: str) -> Signal:
        signal = Signal(
            symbol=symbol,
            side=side,
            level_price=level['price'],
            zone_low=zone_low,
            zone_high=zone_high,
            score=level.get('score', 0)
        )
        self.active_signals[signal.signal_id] = signal
        return signal
        
    def update_signals(self, current_price: float, divergence_results: Dict) -> List[Transition]:
        transitions: List[Transition] = []
        to_remove: List[str] = []

        for signal_id, signal in list(self.active_signals.items()):
            if signal.state in self.state_map:
                state_processor = self.state_map[signal.state](signal, self)
                transitions.extend(state_processor.process(current_price, divergence_results, to_remove))

        for signal_id in to_remove:
            self.active_signals.pop(signal_id, None)

        return transitions

    def _emit_transition(self, transitions: List[Transition], signal_id: str, from_state: str,
                         to_state: str, action: str, signal: Optional[Signal] = None) -> None:
        transitions.append(
            Transition(
                signal_id=signal_id,
                from_state=from_state,
                to_state=to_state,
                action=action,
                signal=signal
            )
        )



    def _cancel_signal(
        self,
        signal_id: str,
        signal: Signal,
        from_state: str,
        reason: str,
        transitions: List[Transition],
        to_remove: List[str],
        include_signal: bool = False,
    ) -> None:
        signal.state = SignalState.CANCELLED
        to_remove.append(signal_id)
        self._emit_transition(
            transitions,
            signal_id,
            from_state,
            'cancelled',
            reason,
            signal=signal if include_signal else None,
        )
        
    def mark_partial(self, signal_id: str, entry_price: float, filled_qty: float,
                     avg_fill_price: float, order_id: str):
        if signal_id in self.active_signals:
            signal = self.active_signals[signal_id]
            prev_qty = signal.filled_qty or 0.0
            cumulative = max(filled_qty, prev_qty)
            incremental = max(cumulative - prev_qty, 0.0)
            if cumulative <= 0:
                return
            signal.state = SignalState.PARTIAL
            now = time.time()
            signal.filled_at = now
            if signal.partial_started_at is None:
                signal.partial_started_at = now
            signal.entry_price = entry_price
            if avg_fill_price is not None:
                if prev_qty > 0 and signal.avg_fill_price is not None:
                    blended = ((signal.avg_fill_price * prev_qty) + (avg_fill_price * incremental)) / max(cumulative, 1e-9)
                    signal.avg_fill_price = blended
                else:
                    signal.avg_fill_price = avg_fill_price
            signal.filled_qty = cumulative
            signal.order_id = order_id

    def mark_filled(self, signal_id: str, entry_price: float, stop_price: float, 
                   target_price: float, order_id: str, filled_qty: float = None,
                   avg_fill_price: float = None):
        if signal_id in self.active_signals:
            signal = self.active_signals[signal_id]
            signal.state = SignalState.FILLED
            signal.filled_at = time.time()
            signal.partial_started_at = None
            signal.entry_price = entry_price
            signal.stop_price = stop_price
            signal.target_price = target_price
            signal.order_id = order_id
            signal.close_reason = None
            signal.hold_expires_at = None
            if filled_qty is not None:
                signal.filled_qty = filled_qty
            if avg_fill_price is not None:
                signal.avg_fill_price = avg_fill_price
            
    def mark_closed(self, signal_id: str, exit_price: float, pnl: float, reason: Optional[str] = None):
        if signal_id in self.active_signals:
            signal = self.active_signals[signal_id]
            signal.state = SignalState.CLOSED
            signal.closed_at = time.time()
            signal.exit_price = exit_price
            signal.pnl = pnl
            signal.close_reason = reason
            del self.active_signals[signal_id]
            
    def get_active_signals(self) -> List[Dict]:
        return [signal.to_dict() for signal in self.active_signals.values()]
