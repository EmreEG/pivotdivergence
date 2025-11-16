import sys

sys.path.insert(0, '.')

from strategy.signal_manager import SignalManager, SignalState


def test_signal_manager_handles_partial_timeout():
    manager = SignalManager()
    level = {
        'price': 100.0,
        'score': 1.0,
        'window': 'test'
    }
    signal = manager.create_signal(level, 'long', 99.5, 100.5, 100.2, 'TEST')
    signal.requested_qty = 2.0
    manager.mark_partial(signal.signal_id, 100.0, 1.0, 100.0, 'OID-1')
    assert signal.state == SignalState.PARTIAL
    assert signal.partial_started_at is not None

    # Force timeout
    signal.partial_started_at -= (manager.partial_fill_timeout_s + 1)
    transitions = manager.update_signals(100.0, {})
    assert any(t['action'] == 'partial_timeout' for t in transitions)


def test_signal_manager_partial_preserves_average_price():
    manager = SignalManager()
    level = {'price': 50.0, 'score': 1.0, 'window': 'test'}
    signal = manager.create_signal(level, 'short', 49.0, 51.0, 50.5, 'TEST')
    signal.requested_qty = 4.0
    manager.mark_partial(signal.signal_id, 50.5, 1.0, 50.5, 'OID-2')
    manager.mark_partial(signal.signal_id, 50.5, 3.0, 50.0, 'OID-2')
    expected_avg = (50.5 + 50.0 + 50.0) / 3
    assert abs(signal.avg_fill_price - expected_avg) < 1e-6
    assert signal.state == SignalState.PARTIAL
