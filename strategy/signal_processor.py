import logging
from typing import Dict, List
from strategy.level_selector import LevelSelector
from strategy.divergence import DivergenceDetector

logger = logging.getLogger(__name__)

class SignalProcessor:
    def __init__(self, config: Dict):
        self.config = config
        self.divergence_cfg = self.config.get('divergence', {})
        
        self.level_selector = LevelSelector()
        self.divergence_detector = DivergenceDetector(
            disable_rsi=self.divergence_cfg.get('disable_rsi_divergence', False),
            disable_cvd=self.divergence_cfg.get('disable_cvd_divergence', False),
            disable_obi=self.divergence_cfg.get('disable_obi_divergence', False)
        )

    def select_levels(
        self,
        candidate_levels: List[Dict],
        current_price: float,
        naked_pocs: List[Dict],
        avwaps: List[Dict],
        swings: List[Dict],
    ) -> (List[Dict], List[Dict]):
        long_levels = self.level_selector.select_levels(
            candidate_levels,
            current_price,
            naked_pocs,
            avwaps,
            swings,
            side='long'
        )
        
        short_levels = self.level_selector.select_levels(
            candidate_levels,
            current_price,
            naked_pocs,
            avwaps,
            swings,
            side='short'
        )
        return long_levels, short_levels

    def get_divergence_confirmations(
        self,
        signal,
        swings_high_pair,
        swings_low_pair,
        obi_z,
        oi_slope,
        current_volatility,
        suppress_obi,
    ):
        bearish = signal.side == 'short'
        effective_obi_z = None if suppress_obi else obi_z
        return self.divergence_detector.count_confirmations(
            swings_high_pair if bearish else None,
            swings_low_pair if not bearish else None,
            effective_obi_z,
            oi_slope,
            bearish,
            current_volatility,
        )
