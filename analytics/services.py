import time
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


class ProfileService:
    def __init__(self, config: Dict, symbol: str, tick_size: float):
        self.config = config
        self.symbol = symbol
        self.tick_size = tick_size
        self.profile_cfg = config.get('profile', {})
        self.levels_cfg = config.get('levels', {})

        self.extrema_detector = ExtremaDetector()
        self.shape_detector = ProfileShapeDetector()
        self.poc_tracker = NakedPOCTracker()
        self.avwap_manager = AVWAPManager(tick_size)
        touch_pct = self.levels_cfg.get('monitor_distance_pct', 0.0015)
        self.level_registry = LevelRegistry(tick_size, epsilon_pct=touch_pct)

        self.profiles = self._build_profiles()
        self.event_anchors: Dict[str, VolumeProfile] = {}

    def _build_profiles(self) -> Dict[str, VolumeProfile]:
        profiles: Dict[str, VolumeProfile] = {}
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

    def set_tick_size(self, tick_size: float):
        self.tick_size = tick_size
        self.level_registry.set_tick_size(tick_size)
        self.avwap_manager.set_tick_size(tick_size)
        for profile in self.profiles.values():
            profile.set_tick_size(tick_size)
        for anchor in self.event_anchors.values():
            anchor.set_tick_size(tick_size)

    def handle_trade(self, trade: Dict):
        price = float(trade['price'])
        qty = float(trade['amount'])
        timestamp = trade['timestamp'] / 1000

        self.level_registry.note_trade(price, timestamp)

        for profile in self.profiles.values():
            profile.add_trade(price, qty, timestamp)

        for anchor_profile in self.event_anchors.values():
            anchor_ts = getattr(anchor_profile, 'anchor_ts', 0)
            if timestamp >= anchor_ts:
                anchor_profile.add_trade(price, qty, timestamp)

        self.avwap_manager.add_trade_to_all(price, qty, timestamp)

    def register_event_anchor(self, anchor_id: str, anchor_profile: VolumeProfile):
        anchor_profile.set_tick_size(self.tick_size)
        self.event_anchors[anchor_id] = anchor_profile

    def create_event_anchor(self, anchor_id: str, anchor_ts: float, duration: int = 86400):
        profile = VolumeProfile(self.symbol, duration, self.config)
        profile.set_tick_size(self.tick_size)
        profile.anchor_ts = anchor_ts
        self.register_event_anchor(anchor_id, profile)
        return profile

    def get_candidate_levels(self) -> List[Dict]:
        candidate_levels: List[Dict] = []
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

    def register_levels(self, levels: List[Dict], timestamp: float):
        self.level_registry.register_levels(levels, timestamp)

    def enrich_levels(self, levels: List[Dict], timestamp: float) -> List[Dict]:
        return self.level_registry.enrich_levels(levels, timestamp)

    def note_poc_touch(self, bid_price: float, ask_price: float, timestamp: float):
        self.poc_tracker.check_touch(bid_price, ask_price, timestamp)

    def cleanup_pocs(self, max_age_hours: int = 72):
        self.poc_tracker.cleanup_old(max_age_hours=max_age_hours)

    def get_naked_pocs(self) -> List[Dict]:
        return self.poc_tracker.get_naked_pocs()

    def get_avwaps(self) -> Dict[str, Dict]:
        return self.avwap_manager.get_all()

    def create_avwap(self, anchor_id: str, anchor_ts: float, label: str):
        self.avwap_manager.create_avwap(anchor_id, anchor_ts, label)

    def ensure_avwap(self, anchor_id: str, anchor_ts: float, label: str):
        if anchor_id not in self.avwap_manager.avwaps:
            self.create_avwap(anchor_id, anchor_ts, label)

    def get_avwap(self, anchor_id: str):
        return self.avwap_manager.get_avwap(anchor_id)

    def snapshot_avwaps(self) -> Dict:
        return self.avwap_manager.snapshot()

    def restore_avwaps(self, data: Dict):
        self.avwap_manager.restore(data)

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

    def _qprice(self, price: float) -> Optional[float]:
        if price is None:
            return None
        if self.tick_size and self.tick_size > 0:
            ticks = round(price / self.tick_size)
            return round(ticks * self.tick_size, 10)
        return price


class OrderFlowService:
    def __init__(self, config: Dict):
        self.config = config
        microstructure_cfg = config.get('microstructure', {})
        indicator_cfg = config.get('indicators', {})
        divergence_cfg = config.get('divergence', {})

        obi_depth_min = microstructure_cfg.get('obi_disable_depth', 10)
        obi_lookback = indicator_cfg.get('obi_lookback', 10)

        self.cvd_calc = CVDCalculator()
        self.obi_calc = OBICalculator(lookback=obi_lookback, min_depth=obi_depth_min)
        self.oi_analyzer = OIAnalyzer(
            slope_period_s=divergence_cfg.get('oi_slope_period_s', 300)
        )
        self.indicators = IndicatorCalculator(
            rsi_period=indicator_cfg.get('rsi_period', 14),
            macd_fast=indicator_cfg.get('macd_fast', 12),
            macd_slow=indicator_cfg.get('macd_slow', 26),
            macd_signal=indicator_cfg.get('macd_signal', 9),
            realized_vol_window=indicator_cfg.get('realized_vol_window', 60)
        )
        self.footprint_manager = FootprintManager(bar_duration=60)
        self.microstructure_cfg = microstructure_cfg

    def handle_trade(self, trade: Dict):
        self.cvd_calc.process_trade(trade)
        self.footprint_manager.add_trade(trade)

    def handle_orderbook(self, orderbook: Dict, book_manager):
        obi_depth = self.microstructure_cfg.get('obi_depth_levels', 10)
        obi_stats = book_manager.compute_obi(obi_depth)
        obi_z = self.obi_calc.update_from_depth(obi_stats)
        obi_stats['obi_z'] = obi_z
        return obi_stats

    def handle_oi(self, oi_data: Dict):
        self.oi_analyzer.add_oi(
            oi_data['openInterest'],
            oi_data['timestamp'] / 1000
        )

    def get_indicator_snapshot(self, symbol: str) -> Dict:
        macd_vals = self.indicators.get_macd()
        return {
            'symbol': symbol,
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

    def get_cvd(self) -> float:
        return self.cvd_calc.get_cvd()

    def is_cvd_stable(self) -> bool:
        return self.cvd_calc.is_stable()

    def mark_cvd_resynced(self):
        self.cvd_calc.mark_resynced()

    def get_obi_z(self) -> Optional[float]:
        return self.obi_calc.get_obi_z_score()

    def is_obi_contaminated(self) -> bool:
        return self.obi_calc.is_contaminated()

    def get_oi_slope(self) -> float:
        return self.oi_analyzer.get_slope()

    def get_current_oi(self) -> Optional[float]:
        return self.oi_analyzer.get_current_oi()

    def get_current_footprint_bar(self):
        return self.footprint_manager.get_current_bar()

    def detect_absorption(self, price: float, lookback_bars: int = 5):
        return self.footprint_manager.detect_absorption_at_level(price, lookback_bars=lookback_bars)

    def get_recent_footprint_bars(self, count: int = 5):
        return self.footprint_manager.get_last_n_bars(count)

    def snapshot_cvd_state(self) -> Optional[Dict]:
        return self.cvd_calc.snapshot_state()

    def restore_cvd_state(self, data: Dict):
        self.cvd_calc.restore_state(data)


class SwingService:
    def __init__(self):
        self.detector = ZigZagSwing()

    def update(self, *args, **kwargs):
        return self.detector.update(*args, **kwargs)

    def get_last_n_swings(self, count: int = 10):
        return self.detector.get_last_n_swings(count)

    def get_last_two_highs(self):
        return self.detector.get_last_two_highs()

    def get_last_two_lows(self):
        return self.detector.get_last_two_lows()

    def to_dict(self) -> Dict:
        return self.detector.to_dict()
