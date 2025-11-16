import logging
import math
import numpy as np
from typing import Dict, Tuple, Optional
from collections import deque



logger = logging.getLogger(__name__)


from .histogram import Histogram

class VolumeProfile:
    def __init__(self, symbol: str, window_seconds: int, config: Dict, bin_width_pct: float = None):
        self.symbol = symbol
        self.window_seconds = window_seconds
        self.config = config
        self.profile_cfg = self.config.get('profile', {})
        self.exchange_cfg = self.config.get('exchange', {})
        self.bin_width_pct = bin_width_pct or self.profile_cfg['bin_width_pct']
        self.tick_size = 0.1
        self.contract_multiplier = float(self.exchange_cfg.get('contract_multiplier', 1.0))
        
        self.histogram = Histogram(self.tick_size, self.bin_width_pct)
        self.trades = deque()
        
        self.price_min = None
        self.price_max = None
        
        self.poc = None
        self.vah = None
        self.val = None
        self.total_volume = 0
        self.shape_type = None
        
        self.flash_crash_freeze_until = 0
        self.last_price = None
        self.last_price_time = None
        
    def set_tick_size(self, tick_size: float):
        if tick_size and tick_size > 0:
            self.tick_size = tick_size
            self.histogram.tick_size = tick_size
        
    def add_trade(self, price: float, qty: float, timestamp: float):
        ts = float(timestamp)
        if ts < self.flash_crash_freeze_until:
            return
        
        if self.last_price is not None and self.last_price_time is not None:
            elapsed = ts - float(self.last_price_time)
            if 0 <= elapsed <= self.profile_cfg['flash_crash_duration_s']:
                if self.last_price and self.last_price > 0 and price and price > 0:
                    denom = max(self.last_price, 1e-9)
                    price_change_pct = abs(price - self.last_price) / denom
                    if price_change_pct > self.profile_cfg['flash_crash_pct']:
                        logger.warning(
                            "Flash crash detected: %.2f%% in %.1fs",
                            price_change_pct * 100,
                            elapsed,
                        )
                        self.flash_crash_freeze_until = ts + self.profile_cfg['flash_crash_freeze_s']
                        return
        
        self.last_price = price
        self.last_price_time = ts
        
        volume = qty * self.contract_multiplier
        self.trades.append((timestamp, price, volume))
        self.histogram.add(price, volume)
        
        cutoff_time = timestamp - self.window_seconds
        while self.trades and self.trades[0][0] < cutoff_time:
            old_ts, old_price, old_volume = self.trades.popleft()
            self.histogram.remove(old_price, old_volume)
            
        self._compute_features()
        
    def _compute_features(self):
        if not self.histogram.histogram:
            return
            
        smoothed = self._smooth_histogram()
        
        self.poc = max(smoothed.items(), key=lambda x: x[1])[0] if smoothed else None
        
        if self.poc is not None:
            self._compute_value_area(smoothed)
            self._detect_shape(smoothed)
            
    def _smooth_histogram(self) -> Dict[int, float]:
        if not self.histogram.histogram:
            return {}
            
        k = self.profile_cfg['smooth_window']
        bins = sorted(self.histogram.histogram.keys())
        
        smoothed = {}
        for i, bin_idx in enumerate(bins):
            window_start = max(0, i - k // 2)
            window_end = min(len(bins), i + k // 2 + 1)
            window_bins = bins[window_start:window_end]
            
            window_sum = sum(self.histogram.histogram.get(b, 0) for b in window_bins)
            smoothed[bin_idx] = window_sum / len(window_bins)
            
        return smoothed
        
    def _compute_value_area(self, smoothed: Dict[int, float]):
        if not smoothed or self.poc is None:
            return
            
        target_volume = self.histogram.total_volume * 0.70
        accumulated = smoothed.get(self.poc, 0)
        
        bins = sorted(smoothed.keys())
        poc_idx = bins.index(self.poc)
        
        lower = poc_idx
        upper = poc_idx
        
        while accumulated < target_volume and (lower > 0 or upper < len(bins) - 1):
            lower_vol = smoothed.get(bins[lower - 1], 0) if lower > 0 else 0
            upper_vol = smoothed.get(bins[upper + 1], 0) if upper < len(bins) - 1 else 0
            
            if lower_vol > upper_vol and lower > 0:
                lower -= 1
                accumulated += smoothed.get(bins[lower], 0)
            elif upper < len(bins) - 1:
                upper += 1
                accumulated += smoothed.get(bins[upper], 0)
            else:
                break
                
        self.val = bins[lower]
        self.vah = bins[upper]
        
    def _detect_shape(self, smoothed: Dict[int, float]):
        """Detect profile shape: D (distribution), P (profile), or b (balanced)"""
        if not smoothed or self.poc is None or self.val is None or self.vah is None:
            self.shape_type = None
            return
            
        bins = sorted(smoothed.keys())
        if len(bins) < 5:
            self.shape_type = 'b'  # Default to balanced if not enough data
            return
            
        poc_idx = bins.index(self.poc)
        val_idx = bins.index(self.val)
        vah_idx = bins.index(self.vah)
        
        # POC position relative to range
        poc_position = poc_idx / len(bins) if len(bins) > 0 else 0.5
        
        # Value area width
        va_width = vah_idx - val_idx
        range_width = len(bins)
        va_ratio = va_width / range_width if range_width > 0 else 0
        
        # D-shape: POC at extremes (top or bottom), distribution/trend day
        if poc_position < 0.3 or poc_position > 0.7:
            self.shape_type = 'D'
        # P-shape: POC in middle, relatively narrow value area (mean reversion)
        elif 0.35 <= poc_position <= 0.65 and va_ratio < 0.5:
            self.shape_type = 'P'
        # b-shape: balanced, wider value area
        else:
            self.shape_type = 'b'
        
    def get_poc_price(self) -> Optional[float]:
        if self.poc is None or self.histogram.bin_base_price is None:
            return None
        price = self.histogram.bin_base_price + self.poc * self.histogram.bin_width
        return self.histogram._quantize_price(price)
        
    def get_va_prices(self) -> Tuple[Optional[float], Optional[float]]:
        if self.val is None or self.vah is None or self.histogram.bin_base_price is None:
            return None, None
        val_price = self.histogram.bin_base_price + self.val * self.histogram.bin_width
        vah_price = self.histogram.bin_base_price + self.vah * self.histogram.bin_width
        return (
            self.histogram._quantize_price(val_price),
            self.histogram._quantize_price(vah_price)
        )
        
    def get_histogram_array(self) -> Tuple[np.ndarray, np.ndarray]:
        if not self.histogram.histogram or self.histogram.bin_base_price is None:
            return np.array([]), np.array([])
            
        bins = sorted(self.histogram.histogram.keys())
        prices = np.array([self.histogram.bin_base_price + b * self.histogram.bin_width for b in bins])
        volumes = np.array([self.histogram.histogram[b] for b in bins])
        
        return prices, volumes
        
    def to_dict(self) -> Dict:
        poc_price = self.get_poc_price()
        val_price, vah_price = self.get_va_prices()

        def _aligned(px: Optional[float]) -> bool:
            return (
                px is None or self.tick_size <= 0 or
                abs((px / self.tick_size) - round(px / self.tick_size)) < 1e-9
            )
        if not _aligned(poc_price) or not _aligned(val_price) or not _aligned(vah_price):
            raise AssertionError('Profile levels not aligned to tick size')
        
        return {
            'symbol': self.symbol,
            'window_seconds': self.window_seconds,
            'poc': poc_price,
            'vah': vah_price,
            'val': val_price,
            'total_volume': self.histogram.total_volume,
            'trade_count': len(self.trades),
            'price_min': self.price_min,
            'price_max': self.price_max,
            'shape_type': self.shape_type,
            'bin_base_price': self.histogram.bin_base_price,
            'bin_width': self.histogram.bin_width
        }
