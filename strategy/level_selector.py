from typing import List, Dict, Optional
from config import config

class LevelSelector:
    def __init__(self, weights_override: Optional[Dict[str, float]] = None):
        self.weights = dict(config.scoring['weights'])
        if weights_override:
            self.weights.update(weights_override)
        self.confluence_radius_pct = config.scoring['confluence_radius_pct']
        self.dedup_spacing_pct = config.scoring['dedup_spacing_pct']
        self.top_k = config.scoring['top_k_per_side']
        self.min_distance_pct = config.scoring['min_order_distance_pct']
        
    def score_level(self, level: Dict, current_price: float, 
                    naked_pocs: List, avwaps: Dict, swings: List) -> float:
        price = float(level['price']) if level.get('price') is not None else 0.0
        # Guard against zero/invalid prices during startup
        if price <= 0:
            return 0.0
        if current_price is None or current_price <= 0:
            return 0.0
        
        lvn_score = 0
        if level.get('type') == 'LVN':
            prominence = level.get('prominence', 0)
            max_prom = prominence
            lvn_score = self.weights['lvn_prominence'] * (prominence / max_prom if max_prom > 0 else 0)
            
        confluence_score = self._compute_confluence(price, current_price, level, avwaps, swings)
        
        naked_poc_score = 0
        for poc in naked_pocs:
            if poc.get('price') is None:
                continue
            if abs(poc['price'] - price) / max(price, 1e-9) < self.confluence_radius_pct:
                naked_poc_score = self.weights['naked_poc']
                break
                
        distance_pct = abs(price - current_price) / max(current_price, 1e-9)
        distance_score = self.weights['distance'] * (1 - min(distance_pct, 1.0))
        
        hours_untouched = level.get('hours_untouched', 0)
        touch_count = level.get('touch_count', 0)
        
        age_score = self.weights['untouched_age'] * (1 if hours_untouched > 0 else 0) * min(hours_untouched / 24, 1.0)
        
        if hours_untouched > 72 or touch_count >= 3:
            age_score *= 0.5
        
        shape_bias = level.get('shape_bias', 0)
        shape_score = self.weights['shape_bias'] * shape_bias
        
        total_score = (lvn_score + confluence_score + naked_poc_score + 
                      distance_score + age_score + shape_score)
        
        return total_score
        
    def _compute_confluence(self, price: float, current_price: float, 
                           level: Dict, avwaps: Dict, swings: List) -> float:
        confluence_count = 0
        ref = price if price > 0 else (current_price if current_price and current_price > 0 else 0)
        if ref <= 0:
            return 0.0
        threshold = ref * self.confluence_radius_pct
        
        for avwap_data in avwaps.values():
            avwap = avwap_data.get('avwap')
            lower = avwap_data.get('lower_band')
            upper = avwap_data.get('upper_band')
            
            if avwap and abs(price - avwap) < threshold:
                confluence_count += 1
            elif lower and abs(price - lower) < threshold:
                confluence_count += 1
            elif upper and abs(price - upper) < threshold:
                confluence_count += 1
                
        for swing in swings[-10:]:
            if swing.get('price') is None:
                continue
            if abs(price - swing['price']) < threshold:
                confluence_count += 1
                
        if price > 0 and (price % 1000 < threshold or (1000 - price % 1000) < threshold):
            confluence_count += 1
            
        return self.weights['confluence'] * confluence_count
        
    def select_levels(self, candidates: List[Dict], current_price: float,
                     naked_pocs: List, avwaps: Dict, swings: List,
                     side: str) -> List[Dict]:
        scored_levels = []
        
        for level in candidates:
            lvl_price = float(level.get('price', 0) or 0)
            if current_price is None or current_price <= 0 or lvl_price <= 0:
                continue
            if side == 'long' and lvl_price >= current_price:
                continue
            if side == 'short' and lvl_price <= current_price:
                continue
            
            score = self.score_level(level, current_price, naked_pocs, avwaps, swings)
            scored_levels.append({
                **level,
                'score': score
            })
            
        scored_levels.sort(key=lambda x: x['score'], reverse=True)
        
        deduped = []
        for level in scored_levels:
            if not deduped:
                deduped.append(level)
                continue
                
            too_close = False
            for existing in deduped:
                if abs(level['price'] - existing['price']) / max(current_price, 1e-9) < self.dedup_spacing_pct:
                    too_close = True
                    break
                    
            if not too_close:
                deduped.append(level)
                
            if len(deduped) >= self.top_k:
                break
                
        filtered = [
            level for level in deduped
            if abs(level['price'] - current_price) / max(current_price, 1e-9) >= self.min_distance_pct
        ]
        
        return filtered
