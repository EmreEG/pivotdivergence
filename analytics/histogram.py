import math
from typing import Dict, Optional

class Histogram:
    def __init__(self, tick_size: float, bin_width_pct: float):
        self.tick_size = tick_size
        self.bin_width_pct = bin_width_pct
        self.bin_base_price: Optional[float] = None
        self.bin_width: Optional[float] = None
        self.histogram: Dict[int, float] = {}
        self.total_volume = 0.0

    def _quantize_price(self, price: float) -> float:
        if self.tick_size <= 0:
            return price
        ticks = math.floor(price / self.tick_size)
        return ticks * self.tick_size

    def _compute_bin_width(self, current_price: float) -> float:
        dynamic_width = current_price * self.bin_width_pct
        return max(self.tick_size, dynamic_width)

    def _get_bin_index(self, price: float) -> int:
        if self.bin_base_price is None or self.bin_width is None or self.bin_width == 0:
            raise RuntimeError('bin mapping not initialized')
        return int(round((price - self.bin_base_price) / self.bin_width))

    def _ensure_bin_mapping(self, price: float):
        quantized = self._quantize_price(price)
        if self.bin_base_price is None:
            self.bin_base_price = quantized
        if self.bin_width is None:
            self.bin_width = self._compute_bin_width(max(quantized, self.tick_size))

    def add(self, price: float, volume: float):
        self._ensure_bin_mapping(price)
        bin_idx = self._get_bin_index(price)
        self.histogram[bin_idx] = self.histogram.get(bin_idx, 0) + volume
        self.total_volume += volume

    def remove(self, price: float, volume: float):
        if not self.histogram:
            return
        bin_idx = self._get_bin_index(price)
        if bin_idx in self.histogram:
            self.histogram[bin_idx] -= volume
            if self.histogram[bin_idx] <= 0:
                del self.histogram[bin_idx]
            self.total_volume -= volume
