import numpy as np
from typing import List, Dict
from scipy.signal import find_peaks
from config import config

class ExtremaDetector:
    def __init__(self):
        self.lvn_prominence_mult = config.profile['lvn_prominence_multiplier']
        self.hvn_prominence_mult = config.profile['hvn_prominence_multiplier']
        self.shelf_gradient_mult = config.profile['shelf_gradient_multiplier']
        
    def detect_extrema(self, prices: np.ndarray, volumes: np.ndarray) -> Dict:
        if len(prices) == 0 or len(volumes) == 0:
            return {
                'hvns': [],
                'lvns': [],
                'shelves': []
            }
            
        deltas = np.diff(volumes)
        med_delta = np.median(np.abs(deltas)) if len(deltas) > 0 else 1.0
        
        if med_delta == 0:
            med_delta = 1.0
            
        theta_l = self.lvn_prominence_mult * med_delta
        theta_h = self.hvn_prominence_mult * med_delta
        gamma = self.shelf_gradient_mult * med_delta
        
        hvns = self._detect_hvns(prices, volumes, theta_h)
        lvns = self._detect_lvns(prices, volumes, theta_l, gamma, deltas)
        shelves = self._detect_shelves(prices, volumes, deltas, med_delta)
        
        return {
            'hvns': hvns,
            'lvns': lvns,
            'shelves': shelves
        }
        
    def _detect_hvns(self, prices: np.ndarray, volumes: np.ndarray, theta_h: float) -> List[Dict]:
        peaks, properties = find_peaks(volumes, prominence=theta_h)
        
        hvns = []
        for idx, prom in zip(peaks, properties['prominences']):
            hvns.append({
                'price': float(prices[idx]),
                'volume': float(volumes[idx]),
                'prominence': float(prom),
                'type': 'HVN'
            })
            
        return hvns
        
    def _detect_lvns(self, prices: np.ndarray, volumes: np.ndarray, 
                      theta_l: float, gamma: float, deltas: np.ndarray) -> List[Dict]:
        inverted_volumes = -volumes
        peaks, properties = find_peaks(inverted_volumes, prominence=theta_l)
        
        lvns = []
        for idx, prom in zip(peaks, properties['prominences']):
            if idx == 0 or idx >= len(deltas):
                continue
                
            left_gradient = abs(deltas[idx - 1]) if idx > 0 else 0
            right_gradient = abs(deltas[idx]) if idx < len(deltas) else 0
            max_gradient = max(left_gradient, right_gradient)
            
            if max_gradient >= gamma:
                lvns.append({
                    'price': float(prices[idx]),
                    'volume': float(volumes[idx]),
                    'prominence': float(prom),
                    'gradient': float(max_gradient),
                    'type': 'LVN'
                })
                
        return lvns
        
    def _detect_shelves(self, prices: np.ndarray, volumes: np.ndarray, 
                        deltas: np.ndarray, med_delta: float) -> List[Dict]:
        shelves = []
        
        threshold = 0.3 * med_delta
        spike_threshold = 1.5 * med_delta
        
        in_shelf = False
        shelf_bins = []
        
        for i in range(len(deltas)):
            if abs(deltas[i]) < threshold:
                if not in_shelf:
                    if i > 0 and abs(deltas[i - 1]) > spike_threshold:
                        in_shelf = True
                        shelf_bins = [i]
                else:
                    shelf_bins.append(i)
            else:
                if in_shelf and abs(deltas[i]) > spike_threshold:
                    if len(shelf_bins) >= 3:
                        shelf_prices = prices[shelf_bins]
                        shelf_volumes = volumes[shelf_bins]
                        shelves.append({
                            'price_low': float(shelf_prices[0]),
                            'price_high': float(shelf_prices[-1]),
                            'avg_volume': float(np.mean(shelf_volumes)),
                            'width': len(shelf_bins),
                            'type': 'SHELF'
                        })
                    in_shelf = False
                    shelf_bins = []
                    
        if in_shelf and len(shelf_bins) >= 3:
            shelf_prices = prices[shelf_bins]
            shelf_volumes = volumes[shelf_bins]
            shelves.append({
                'price_low': float(shelf_prices[0]),
                'price_high': float(shelf_prices[-1]),
                'avg_volume': float(np.mean(shelf_volumes)),
                'width': len(shelf_bins),
                'type': 'SHELF'
            })
            
        return shelves
