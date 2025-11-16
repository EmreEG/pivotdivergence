from __future__ import annotations
import time
from typing import TYPE_CHECKING, List
from abc import ABC, abstractmethod

if TYPE_CHECKING:
    from .signal_manager import Signal, SignalManager, Transition, SignalState

class SignalStateProcessor(ABC):
    def __init__(self, signal: Signal, manager: SignalManager):
        self.signal = signal
        self.manager = manager

    @abstractmethod
    def process(self, current_price: float, divergence_results: dict, to_remove: List[str]) -> List[Transition]:
        pass

class InactiveState(SignalStateProcessor):
    def process(self, current_price: float, divergence_results: dict, to_remove: List[str]) -> List[Transition]:
        transitions = []
        distance = self.signal.distance_to_level(current_price)
        if distance <= self.manager.monitor_distance_pct:
            self.signal.state = SignalState.MONITORED
            self.signal.monitored_at = time.time()
            self.manager._emit_transition(transitions, self.signal.signal_id, 'inactive', 'monitored', 'start_monitoring')
        return transitions

class MonitoredState(SignalStateProcessor):
    def process(self, current_price: float, divergence_results: dict, to_remove: List[str]) -> List[Transition]:
        transitions = []
        confirmations = divergence_results.get(self.signal.signal_id, {})
        if self.signal.is_in_zone(current_price):
            if confirmations.get('count', 0) >= self.manager.confirmation_count:
                self.signal.state = SignalState.ARMED
                self.signal.armed_at = time.time()
                self.signal.divergences = confirmations
                self.manager._emit_transition(
                    transitions, self.signal.signal_id, 'monitored', 'armed', 'place_order', signal=self.signal
                )

        if self.signal.monitored_at and time.time() - self.signal.monitored_at > self.manager.timeout_s:
            self.manager._cancel_signal(
                self.signal.signal_id, self.signal, 'monitored', 'timeout', transitions, to_remove
            )
            return transitions

        if self.signal.beyond_stop(current_price):
            self.manager._cancel_signal(
                self.signal.signal_id, self.signal, 'monitored', 'stop_pierced', transitions, to_remove
            )
        return transitions

class ArmedState(SignalStateProcessor):
    def process(self, current_price: float, divergence_results: dict, to_remove: List[str]) -> List[Transition]:
        transitions = []
        if self.signal.armed_at and time.time() - self.signal.armed_at > self.manager.timeout_s:
            self.manager._cancel_signal(
                self.signal.signal_id, self.signal, 'armed', 'cancel_order', transitions, to_remove, include_signal=True
            )
            return transitions

        if self.signal.beyond_stop(current_price):
            self.manager._cancel_signal(
                self.signal.signal_id, self.signal, 'armed', 'cancel_order', transitions, to_remove, include_signal=True
            )
        return transitions

class PartialState(SignalStateProcessor):
    def process(self, current_price: float, divergence_results: dict, to_remove: List[str]) -> List[Transition]:
        transitions = []
        now = time.time()
        if self.signal.partial_started_at and now - self.signal.partial_started_at > self.manager.partial_fill_timeout_s:
            self.manager._cancel_signal(
                self.signal.signal_id, self.signal, 'partial', 'partial_timeout', transitions, to_remove
            )
            return transitions

        if self.signal.beyond_stop(current_price):
            self.manager._cancel_signal(
                self.signal.signal_id, self.signal, 'partial', 'partial_stop', transitions, to_remove
            )
        return transitions
