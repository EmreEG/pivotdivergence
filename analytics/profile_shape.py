from typing import Literal
import numpy as np

Shape = Literal['D', 'P', 'b', 'unknown']

class ProfileShapeDetector:
    def __init__(self):
        self.d_threshold = 0.15
        self.p_threshold = 0.25
        
    def detect_shape(self, prices: np.ndarray, volumes: np.ndarray, 
                     poc_idx: int, val_idx: int, vah_idx: int) -> str:
        if len(volumes) < 3:
            return 'unknown'
            
        total_volume = np.sum(volumes)
        if total_volume == 0:
            return 'unknown'
            
        lower_third_end = len(volumes) // 3
        upper_third_start = 2 * len(volumes) // 3
        
        lower_vol = np.sum(volumes[:lower_third_end])
        upper_vol = np.sum(volumes[upper_third_start:])
        
        lower_frac = lower_vol / total_volume
        upper_frac = upper_vol / total_volume
        
        if abs(lower_frac - upper_frac) < self.d_threshold:
            return 'D'
        elif upper_frac > lower_frac + self.p_threshold:
            return 'P'
        elif lower_frac > upper_frac + self.p_threshold:
            return 'b'
        else:
            return 'D'
            
    def get_shape_bias(self, shape: str) -> float:
        if shape == 'D':
            return 0.0
        elif shape == 'P':
            return 0.5
        elif shape == 'b':
            return -0.5
        else:
            return 0.0
