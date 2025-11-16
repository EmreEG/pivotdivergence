import logging
import time
from typing import Dict, List, Optional
import numpy as np
from analytics.profile import VolumeProfile
from analytics.extrema import ExtremaDetector
from analytics.naked_poc import NakedPOCTracker
from analytics.avwap import AVWAPManager
from analytics.orderflow import CVDCalculator, OBICalculator, OIAnalyzer
from analytics.indicators import IndicatorCalculator
from analytics.swings import ZigZagSwing
from analytics.profile_shape import ProfileShapeDetector
from analytics.footprint import FootprintManager
from analytics.level_registry import LevelRegistry

logger = logging.getLogger(__name__)

class AnalyticsEngine:
    def __init__(self, config: Dict, symbol: str, tick_size: float):
        self.config = config
        self.symbol = symbol
        self.tick_size = tick_size
        self.profile_cfg = self.config.get('profile', {})
        self.microstructure_cfg = self.config.get('microstructure', {})
        self.indicator_cfg = self.config.get('indicators', {})
        self.divergence_cfg = self.config.get('divergence', {})
        self.levels_cfg = self.config.get('levels', {})

        self.profiles = self._build_profiles()
        self.event_anchors = {}

        self._init_analytics()

    def _build_profiles(self) -> Dict[str, VolumeProfile]:
        profiles = {}
        windows_cfg = self.profile_cfg.get('windows', {})
        for window in windows_cfg.get('rolling', []):
            profile = VolumeProfile(self.symbol, window, self.config)
            profile.set_tick_size(self.tick_size)
            profiles[f'rolling_{window}'] = profile

        for session_range in windows_cfg.get('session_utc', []):
            if len(session_range) != 2:
                continue
            session_start, session_end = session_range
            session_duration = session_end - session_start
            session_name = f'session_{session_start//3600}h_{session_end//3600}h'
            profile = VolumeProfile(self.symbol, max(1, session_duration), self.config)
            profile.set_tick_size(self.tick_size)
            profiles[session_name] = profile

        return profiles

    def _init_analytics(self):
        self.extrema_detector = ExtremaDetector()
        self.poc_tracker = NakedPOCTracker()
        self.avwap_manager = AVWAPManager(self.tick_size)
        self.cvd_calc = CVDCalculator()
        obi_depth_min = self.microstructure_cfg.get('obi_disable_depth', 10)
        obi_lookback = self.indicator_cfg.get('obi_lookback', 10)
        self.obi_calc = OBICalculator(lookback=obi_lookback, min_depth=obi_depth_min)
        indicator_cfg = self.indicator_cfg
        self.oi_analyzer = OIAnalyzer(
            slope_period_s=self.divergence_cfg.get('oi_slope_period_s', 300)
        )
        self.indicators = IndicatorCalculator(
            rsi_period=indicator_cfg.get('rsi_period', 14),
            macd_fast=indicator_cfg.get('macd_fast', 12),
            macd_slow=indicator_cfg.get('macd_slow', 26),
            macd_signal=indicator_cfg.get('macd_signal', 9),
            realized_vol_window=indicator_cfg.get('realized_vol_window', 60)
        )
        self.swing_detector = ZigZagSwing()
        self.shape_detector = ProfileShapeDetector()
        self.footprint_manager = FootprintManager(bar_duration=60)
        touch_pct = self.levels_cfg.get('monitor_distance_pct', 0.0015)
        self.level_registry = LevelRegistry(self.tick_size, epsilon_pct=touch_pct)

    def on_trade(self, trade: Dict):
        price = float(trade['price'])
        qty = float(trade['amount'])
        timestamp = trade['timestamp'] / 1000

        self.level_registry.note_trade(price, timestamp)
        
        for profile in self.profiles.values():
            profile.add_trade(price, qty, timestamp)
            
        for anchor_id, anchor_profile in self.event_anchors.items():
            if hasattr(anchor_profile, 'anchor_ts') and timestamp >= anchor_profile.anchor_ts:
                anchor_profile.add_trade(price, qty, timestamp)
            
        self.avwap_manager.add_trade_to_all(price, qty, timestamp)
        
        self.cvd_calc.process_trade(trade)
        self.footprint_manager.add_trade(trade)

    def on_orderbook(self, orderbook: Dict, book_manager):
        obi_depth = self.microstructure_cfg.get('obi_depth_levels', 10)
        obi_stats = book_manager.compute_obi(obi_depth)
        obi_z = self.obi_calc.update_from_depth(obi_stats)
        obi_stats['obi_z'] = obi_z
        return obi_stats

    def on_oi(self, oi_data: Dict):
        self.oi_analyzer.add_oi(
            oi_data['openInterest'],
            oi_data['timestamp'] / 1000
        )

    def get_candidate_levels(self) -> List[Dict]:
        candidate_levels = []
        all_profiles = {**self.profiles, **self.event_anchors}
        
        for profile_id, profile in all_profiles.items():
            prices, volumes = profile.get_histogram_array()
            if len(prices) == 0:
                continue
                
            extrema = self.extrema_detector.detect_extrema(prices, volumes)
            
            profile_data = profile.to_dict()
            poc_idx = None
            val_idx = None
            vah_idx = None
            
            if profile_data['poc'] is not None:
                poc_price = profile_data['poc']
                poc_idx = np.argmin(np.abs(prices - poc_price)) if len(prices) > 0 else None
                
            if profile_data['val'] is not None and profile_data['vah'] is not None:
                val_price = profile_data['val']
                vah_price = profile_data['vah']
                val_idx = np.argmin(np.abs(prices - val_price)) if len(prices) > 0 else None
                vah_idx = np.argmin(np.abs(prices - vah_price)) if len(prices) > 0 else None
                
            shape = 'unknown'
            if poc_idx is not None and val_idx is not None and vah_idx is not None:
                shape = self.shape_detector.detect_shape(prices, volumes, poc_idx, val_idx, vah_idx)

            shape_bias = self.shape_detector.get_shape_bias(shape)

            for lvn in extrema['lvns']:
                candidate_levels.append(
                    self._build_level(
                        price=lvn['price'],
                        level_type='LVN',
                        prominence=lvn['prominence'],
                        profile_id=profile_id,
                        shape_bias=shape_bias,
                        extra={'gradient': lvn.get('gradient', 0)},
                    )
                )

            for hvn in extrema['hvns']:
                candidate_levels.append(
                    self._build_level(
                        price=hvn['price'],
                        level_type='HVN',
                        prominence=hvn['prominence'],
                        profile_id=profile_id,
                        shape_bias=shape_bias,
                    )
                )

            for shelf in extrema.get('shelves', []):
                width = max(
                    0.0,
                    float(shelf.get('price_high', 0.0)) - float(shelf.get('price_low', 0.0)),
                )
                center = (
                    float(shelf.get('price_low', 0.0) + shelf.get('price_high', 0.0)) / 2.0
                    if width > 0
                    else shelf.get('price_low', 0.0)
                )
                candidate_levels.append(
                    self._build_level(
                        price=center,
                        level_type='SHELF',
                        prominence=shelf.get('avg_volume', 0.0),
                        profile_id=profile_id,
                        shape_bias=shape_bias,
                        extra={'shelf_width': width},
                    )
                )

            if profile_data['val'] is not None:
                candidate_levels.append(
                    self._build_level(
                        price=profile_data['val'],
                        level_type='VAL',
                        prominence=0,
                        profile_id=profile_id,
                        shape_bias=shape_bias,
                    )
                )
            if profile_data['vah'] is not None:
                candidate_levels.append(
                    self._build_level(
                        price=profile_data['vah'],
                        level_type='VAH',
                        prominence=0,
                        profile_id=profile_id,
                        shape_bias=shape_bias,
                    )
                )
            if profile_data['poc'] is not None:
                poc_id = f"{profile_id}_poc_{int(time.time())}"
                self.poc_tracker.register_poc(
                    poc_id,
                    self._qprice(profile_data['poc']),
                    time.time(),
                    profile_id
                )
        return candidate_levels



    def _qprice(self, price: float) -> float:
        if price is None:
            return None
        if self.tick_size and self.tick_size > 0:
            ticks = round(price / self.tick_size)
            return round(ticks * self.tick_size, 10)
        return price

    def _build_level(
        self,
        price: float,
        level_type: str,
        prominence: float,
        profile_id: str,
        shape_bias: float,
        extra: Optional[Dict] = None,
    ) -> Dict:
        level: Dict = {
            'price': self._qprice(price),
            'type': level_type,
            'prominence': prominence,
            'window': profile_id,
            'hours_untouched': 0,
            'touch_count': 0,
            'shape_bias': shape_bias,
        }
        if extra:
            level.update(extra)
        return level

    def on_bar(self, current_price: float, timestamp: float):
        # This method would be called periodically by the main loop
        pass

    def get_indicator_snapshot(self) -> Dict:
        macd_vals = self.indicators.get_macd()
        return {
            'symbol': self.symbol,
            'timestamp': int(time.time() * 1000),
            'rsi': self.indicators.get_rsi(),
            'macd': macd_vals.get('macd') if macd_vals else None,
            'macd_signal': macd_vals.get('signal') if macd_vals else None,
            'macd_hist': macd_vals.get('histogram') if macd_vals else None,
            'obv': self.indicators.get_obv(),
            'ad_line': self.indicators.get_ad_line(),
            'realized_vol': self.indicators.get_realized_volatility(),
            'cvd': self.cvd_calc.get_cvd(),
            'obi': self.obi_calc.get_latest_obi(),
            'obi_z': self.obi_calc.get_obi_z_score(),
        }
