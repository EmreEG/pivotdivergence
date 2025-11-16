from pathlib import Path
import sys

sys.path.insert(0, '.')

from monitoring.signal_auditor import SignalAuditor
from strategy.signal_manager import SignalManager


def test_signal_auditor_logs_timeout(tmp_path):
    manager = SignalManager()
    signal = manager.create_signal({'price': 10.0, 'score': 1.0, 'window': 'test'}, 'long', 9.5, 10.5, 10.2, 'TEST')
    signal.order_id = 'OID-1'
    signal.feature_vector = {'mode': 'limit'}
    auditor = SignalAuditor(tmp_path / 'audit.jsonl', ack_budget_ms=1)
    auditor.record_plan(signal)

    # Force timeout condition
    auditor.pending[signal.signal_id]['planned_at'] -= 10
    auditor.audit_active_signals({signal.signal_id: signal})

    log_path = Path(tmp_path / 'audit.jsonl')
    assert log_path.exists()
    contents = log_path.read_text()
    assert 'ack_timeout' in contents
