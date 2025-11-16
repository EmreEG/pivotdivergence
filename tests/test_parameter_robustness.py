import asyncio
import sys

sys.path.insert(0, '.')

from backtest.robustness import ParameterRobustnessHarness


class _FakeEngine:
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date

    async def run_backtest(self, symbol, params):
        threshold = params.get('obi_z_threshold', 2.0)
        sharpe = max(0.0, 2.0 - abs(threshold - 2.0))
        return {
            'sharpe_ratio': sharpe,
            'max_drawdown': 0.05,
            'total_return': 0.1,
        }


def test_parameter_robustness_harness_runs_trials():
    harness = ParameterRobustnessHarness('2024-01-01', '2024-02-01', engine_cls=_FakeEngine)
    ranges = {
        'obi_z_threshold': {'min': 1.0, 'max': 3.0},
        'divergence_timeout_s': {'base': 120, 'jitter_pct': 0.5},
    }
    summary = asyncio.run(harness.run_trials('TEST', parameter_ranges=ranges, trials=8))
    assert summary['trials'] == 8
    assert summary['worst_sharpe'] >= 0.0
    assert 'results' in summary and len(summary['results']) == 8
    assert summary['failures'] == 0
