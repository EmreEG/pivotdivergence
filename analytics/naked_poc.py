from typing import Dict, List, Optional
import time

class NakedPOCTracker:
    def __init__(self):
        self.pocs: Dict[str, Dict] = {}
        self.touch_epsilon_pct = 0.001
        
    def register_poc(self, poc_id: str, price: float, timestamp: float, window_type: str):
        self.pocs[poc_id] = {
            'price': price,
            'timestamp': timestamp,
            'window_type': window_type,
            'touched': False,
            'touch_timestamp': None,
            'touch_count': 0,
            'hours_untouched': 0
        }
        
    def check_touch(self, best_bid: Optional[float], best_ask: Optional[float], 
                    current_time: float):
        if best_bid is None or best_ask is None:
            return
            
        for poc_id, poc in self.pocs.items():
            if poc['touched']:
                continue
                
            poc_price = poc['price']
            epsilon = poc_price * self.touch_epsilon_pct
            
            if best_bid >= poc_price - epsilon and best_ask <= poc_price + epsilon:
                poc['touched'] = True
                poc['touch_timestamp'] = current_time
                poc['touch_count'] = poc.get('touch_count', 0) + 1
                
            poc['hours_untouched'] = (current_time - poc['timestamp']) / 3600
                
    def get_naked_pocs(self) -> List[Dict]:
        return [
            {
                'id': poc_id,
                'price': poc['price'],
                'timestamp': poc['timestamp'],
                'window_type': poc['window_type'],
                'hours_untouched': poc['hours_untouched']
            }
            for poc_id, poc in self.pocs.items()
            if not poc['touched']
        ]
        
    def cleanup_old(self, max_age_hours: float = 72):
        current_time = time.time()
        to_remove = []
        
        for poc_id, poc in self.pocs.items():
            age_hours = (current_time - poc['timestamp']) / 3600
            if age_hours > max_age_hours:
                to_remove.append(poc_id)
                
        for poc_id in to_remove:
            del self.pocs[poc_id]
            
    def reset_on_regime_change(self, window_type: str):
        to_remove = [
            poc_id for poc_id, poc in self.pocs.items()
            if poc['window_type'] == window_type
        ]
        for poc_id in to_remove:
            del self.pocs[poc_id]
