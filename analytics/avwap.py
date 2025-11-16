import logging
import numpy as np
from typing import Dict, Optional, Tuple
from collections import deque


logger = logging.getLogger(__name__)

class AVWAP:
    def __init__(self, anchor_timestamp: float, anchor_type: str = 'manual', tick_size: float = 0.1):
        self.anchor_timestamp = anchor_timestamp
        self.anchor_type = anchor_type
        self.tick_size = tick_size
        
        self.sum_pv = 0.0
        self.sum_v = 0.0
        self.avwap = None
        
        self.price_diffs = deque(maxlen=1000)
        self.sigma = None
        
    def add_trade(self, price: float, volume: float, timestamp: float):
        if timestamp < self.anchor_timestamp:
            return
            
        price = self._quantize(price)
        self.sum_pv += price * volume
        self.sum_v += volume
        
        if self.sum_v > 0:
            self.avwap = self.sum_pv / self.sum_v
            self.price_diffs.append(price - self.avwap)
            
            if len(self.price_diffs) > 10:
                self.sigma = np.std(self.price_diffs)
                
    def get_avwap(self) -> Optional[float]:
        if self.avwap is None:
            return None
        # Normalize AVWAP to tick size for storage/IO
        return self._quantize(self.avwap)
        
    def get_bands(self, num_sigma: float = 1.0) -> Tuple[Optional[float], Optional[float]]:
        if self.avwap is None or self.sigma is None:
            return None, None

        upper = self.avwap + num_sigma * self.sigma
        lower = self.avwap - num_sigma * self.sigma

        # Normalize bands to tick size for storage/IO
        return self._quantize(lower), self._quantize(upper)
        
    def to_dict(self) -> Dict:
        lower, upper = self.get_bands()
        avwap_value = self.get_avwap()
        
        return {
            'anchor_timestamp': self.anchor_timestamp,
            'anchor_type': self.anchor_type,
            'avwap': avwap_value,
            'sigma': self.sigma,
            'lower_band': lower,
            'upper_band': upper,
            'trade_count': self.sum_v,
            'tick_size': self.tick_size
        }
    
    def snapshot(self) -> Dict:
        return {
            'anchor_timestamp': self.anchor_timestamp,
            'anchor_type': self.anchor_type,
            'sum_pv': self.sum_pv,
            'sum_v': self.sum_v,
            'sigma': self.sigma,
            'tick_size': self.tick_size
        }
    
    def restore(self, snapshot: Dict):
        self.sum_pv = snapshot.get('sum_pv', 0.0)
        self.sum_v = snapshot.get('sum_v', 0.0)
        self.sigma = snapshot.get('sigma')
        self.tick_size = snapshot.get('tick_size', self.tick_size)
        self.avwap = self.sum_pv / self.sum_v if self.sum_v else None
    
    def _quantize(self, price: float) -> float:
        if self.tick_size <= 0:
            return price
        ticks = round(price / self.tick_size)
        return ticks * self.tick_size

class AVWAPManager:
    def __init__(self, tick_size: float = 0.1):
        self.avwaps: Dict[str, AVWAP] = {}
        self.tick_size = tick_size
        
    def set_tick_size(self, tick_size: float):
        if tick_size and tick_size > 0:
            self.tick_size = tick_size
            for avwap in self.avwaps.values():
                avwap.tick_size = tick_size

    def create_avwap(self, avwap_id: str, anchor_timestamp: float, anchor_type: str = 'manual') -> AVWAP:
        if avwap_id in self.avwaps:
            return self.avwaps[avwap_id]
        avwap = AVWAP(anchor_timestamp, anchor_type, self.tick_size)
        self.avwaps[avwap_id] = avwap
        logger.info(
            "Anchored AVWAP %s at %s (%s)",
            avwap_id,
            anchor_timestamp,
            anchor_type,
        )
        return avwap

    def add_trade_to_all(self, price: float, volume: float, timestamp: float):
        for avwap in self.avwaps.values():
            avwap.add_trade(price, volume, timestamp)
        
    def get_avwap(self, avwap_id: str) -> Optional[AVWAP]:
        return self.avwaps.get(avwap_id)
        
    def remove_avwap(self, avwap_id: str):
        if avwap_id in self.avwaps:
            del self.avwaps[avwap_id]
            
    def get_all(self) -> Dict[str, Dict]:
        return {
            avwap_id: avwap.to_dict()
            for avwap_id, avwap in self.avwaps.items()
        }

    def snapshot(self) -> Dict[str, Dict]:
        return {
            avwap_id: avwap.snapshot()
            for avwap_id, avwap in self.avwaps.items()
        }

    def restore(self, snapshot: Dict[str, Dict]):
        for avwap_id, state in snapshot.items():
            anchor_ts = state.get('anchor_timestamp')
            if anchor_ts is None:
                continue
            avwap = AVWAP(anchor_ts, state.get('anchor_type', 'manual'), state.get('tick_size', self.tick_size))
            avwap.restore(state)
            self.avwaps[avwap_id] = avwap
