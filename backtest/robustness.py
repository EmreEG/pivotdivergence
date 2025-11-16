import random
from dataclasses import dataclass
from statistics import mean, pstdev
from typing import Dict, List, Optional

from backtest.engine import BacktestEngine
from config import config


@dataclass
class RobustnessResult:
    params: Dict[str, float]
    metrics: Dict[str, float]


class ParameterRobustnessHarness:
    """Randomizes key parameters and evaluates performance stability."""

    def __init__(self, start_date: str, end_date: str, engine_cls=BacktestEngine):
        self.start_date = start_date
        self.end_date = end_date
        self.engine_cls = engine_cls
        self.random = random.Random()
        backtest_cfg = config.backtest or {}
        self.default_trials = backtest_cfg.get('robustness_trials', 25)
        self.robustness_floor = backtest_cfg.get('robustness_min_sharpe', 0.5)

    async def run_trials(
        self,
        symbol: str,
        parameter_ranges: Optional[Dict[str, Dict[str, float]]] = None,
        trials: Optional[int] = None,
    ) -> Dict:
        ranges = parameter_ranges or default_parameter_ranges()
        attempts = trials or self.default_trials
        results: List[RobustnessResult] = []
        for _ in range(attempts):
            params = self._sample_params(ranges)
            engine = self.engine_cls(self.start_date, self.end_date)
            metrics = await engine.run_backtest(symbol, params)
            results.append(RobustnessResult(params=params, metrics=metrics or {}))
        return self._summarize(results)

    def _sample_params(self, ranges: Dict[str, Dict[str, float]]) -> Dict[str, float]:
        sampled = {}
        for key, spec in ranges.items():
            if 'min' in spec and 'max' in spec:
                sampled[key] = self.random.uniform(spec['min'], spec['max'])
            else:
                base = spec.get('base', 0.0)
                jitter = spec.get('jitter_pct', 0.1)
                delta = base * jitter
                sampled[key] = base + self.random.uniform(-delta, delta)
        return sampled

    def _summarize(self, results: List[RobustnessResult]) -> Dict:
        if not results:
            return {'trials': 0, 'robust': False}
        sharpes = [r.metrics.get('sharpe_ratio', 0.0) for r in results]
        worst = min(sharpes) if sharpes else 0.0
        avg = mean(sharpes) if sharpes else 0.0
        std = pstdev(sharpes) if len(sharpes) > 1 else 0.0
        failures = sum(1 for s in sharpes if s < self.robustness_floor)
        return {
            'trials': len(results),
            'worst_sharpe': worst,
            'avg_sharpe': avg,
            'std_sharpe': std,
            'failures': failures,
            'robust': failures == 0,
            'results': [r.__dict__ for r in results],
        }


def default_parameter_ranges() -> Dict[str, Dict[str, float]]:
    profile_cfg = config.profile or {}
    divergence = config.divergence or {}
    microstructure = config.microstructure or {}
    return {
        'lvn_prominence': {'base': profile_cfg.get('lvn_prominence_multiplier', 1.5), 'jitter_pct': 0.5},
        'hvn_prominence': {'base': profile_cfg.get('hvn_prominence_multiplier', 2.0), 'jitter_pct': 0.5},
        'divergence_timeout_s': {'base': divergence.get('timeout_s', 120), 'jitter_pct': 0.5},
        'obi_z_threshold': {'min': 1.0, 'max': divergence.get('obi_z_threshold', 2.5) * 2},
        'micro_noise_pct': {'base': divergence.get('micro_noise_pct', 0.0005), 'jitter_pct': 1.0},
        'obi_depth_levels': {'base': microstructure.get('obi_depth_levels', 10), 'jitter_pct': 0.5},
    }
