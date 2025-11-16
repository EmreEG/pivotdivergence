import json
import logging
import time
from pathlib import Path
from typing import Dict, Optional

from strategy.signal_manager import Signal


logger = logging.getLogger(__name__)


class SignalAuditor:
    """Logs full signal feature vectors and audits execution lag."""

    def __init__(self, log_path: str, ack_budget_ms: int = 1000):
        self.log_path = Path(log_path or 'logs/signal_audit.jsonl')
        self.ack_budget_s = max(ack_budget_ms / 1000.0, 0.0)
        self.pending: Dict[str, Dict] = {}

    def record_plan(self, signal: Signal):
        self.pending[signal.signal_id] = self._build_snapshot(signal)

    def record_execution(
        self,
        signal: Signal,
        event_type: str,
        event_payload: Dict,
        ack_latency_s: Optional[float] = None,
    ):
        entry = self.pending.setdefault(signal.signal_id, self._build_snapshot(signal))
        if ack_latency_s:
            entry['ack_latency'] = ack_latency_s
        payload = {
            'timestamp': time.time(),
            'signal_id': signal.signal_id,
            'order_id': signal.order_id,
            'state': signal.state.value,
            'event': event_type,
            'feature_vector': entry.get('feature_vector'),
            'requested_qty': entry.get('requested_qty'),
            'filled_qty': signal.filled_qty,
            'avg_fill_price': signal.avg_fill_price,
            'ack_latency_s': entry.get('ack_latency'),
            'execution_mode': entry.get('execution_mode'),
            'side': signal.side,
            'details': event_payload,
        }
        self._write_entry(payload)
        if event_type in ('filled', 'cancelled', 'expired'):
            self.pending.pop(signal.signal_id, None)

    def audit_active_signals(self, active_signals: Dict[str, Signal]):
        now = time.time()
        for signal_id, entry in list(self.pending.items()):
            if entry.get('ack_latency') is not None:
                continue
            elapsed = now - entry['planned_at']
            if elapsed <= self.ack_budget_s:
                continue
            signal = active_signals.get(signal_id)
            if not signal:
                continue
            details = {
                'reason': 'ack_timeout',
                'elapsed_s': elapsed,
                'order_id': entry.get('order_id'),
            }
            self.record_execution(signal, 'audit_violation', details, ack_latency_s=elapsed)

    def _write_entry(self, payload: Dict):
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with self.log_path.open('a', encoding='utf-8') as handle:
                handle.write(json.dumps(payload, default=str) + '\n')
        except Exception as exc:
            logger.error("Failed to persist audit log: %s", exc)

    def _build_snapshot(self, signal: Signal) -> Dict:
        return {
            'planned_at': time.time(),
            'order_id': signal.order_id,
            'side': signal.side,
            'feature_vector': signal.feature_vector,
            'requested_qty': signal.requested_qty,
            'execution_mode': signal.execution_mode,
            'ack_latency': None,
        }
