from typing import Optional
import logging
from config import config


logger = logging.getLogger(__name__)


class RiskManager:
    def __init__(self):
        self.position_size_pct = config.risk['position_size_pct']
        self.max_notional = config.risk['max_notional_usd']
        self.max_concurrent_per_side = config.risk['max_concurrent_per_side']
        self.stop_atr_mult = config.risk['stop_atr_multiplier']
        self.stop_zone_mult = config.risk['stop_zone_multiplier']
        self.atr_period = config.risk['atr_period']
        self.funding_threshold = config.risk['funding_threshold_pct']
        self.funding_reduction = config.risk['funding_size_reduction']
        self.max_drawdown = config.risk['max_drawdown_pct']
        self.position_epsilon_pct = config.risk.get('position_epsilon_pct', 0.0)
        self.notional_cap_pct = config.risk.get('notional_cap_pct', 1.0)
        self.max_hold_minutes = config.risk.get('max_hold_minutes', 0)
        
        self.session_pnl = 0.0
        self.session_start_equity = 0.0
        
    def calculate_position_size(self, equity: float, atr: float, 
                               tick_size: float, side: str,
                               current_price: float, funding_rate: float = 0.0) -> float:
        base_qty_usd = equity * self.position_size_pct
        
        if abs(funding_rate) > self.funding_threshold:
            if (side == 'long' and funding_rate > 0) or (side == 'short' and funding_rate < 0):
                base_qty_usd *= self.funding_reduction
                
        notional_cap = min(self.max_notional, equity * self.notional_cap_pct)
        qty_usd = min(base_qty_usd, notional_cap)
        
        stop_distance = atr * self.stop_atr_mult
        if stop_distance > 0 and current_price > 0:
            risk_amount = equity * self.position_size_pct
            max_qty_by_risk = risk_amount / (stop_distance / current_price)
            qty_usd = min(qty_usd, max_qty_by_risk)
            
        return qty_usd
        
    def calculate_stop_price(self, entry_price: float, side: str, 
                            atr: float, zone_width: float) -> float:
        atr_stop = atr * self.stop_atr_mult
        zone_stop = zone_width * self.stop_zone_mult
        
        stop_distance = max(atr_stop, zone_stop)
        
        if side == 'long':
            return entry_price - stop_distance
        else:
            return entry_price + stop_distance
            
    def calculate_target_price(self, entry_price: float, stop_price: float, 
                              side: str, target_rr: float = None) -> float:
        target_rr = target_rr or config.risk['target_min_rr']
        
        risk = abs(entry_price - stop_price)
        reward = risk * target_rr
        
        if side == 'long':
            return entry_price + reward
        else:
            return entry_price - reward
            
    def update_trail_stop(self, entry_price: float, current_price: float, 
                         avwap: float, sigma: float, side: str) -> Optional[float]:
        trail_distance = sigma * config.risk['trail_avwap_sigma']
        
        if side == 'long':
            trail_stop = avwap - trail_distance
            if trail_stop > entry_price:
                return trail_stop
        else:
            trail_stop = avwap + trail_distance
            if trail_stop < entry_price:
                return trail_stop
                
        return None
        
    def check_drawdown_limit(self, current_equity: float) -> bool:
        if self.session_start_equity == 0:
            self.session_start_equity = current_equity
            return False
            
        drawdown = (self.session_start_equity - current_equity) / self.session_start_equity
        
        if drawdown > self.max_drawdown:
            logger.error("Max drawdown exceeded: %.2f%%", drawdown * 100)
            return True
            
        return False
        
    def can_add_signal(self, current_count: int) -> bool:
        return current_count < self.max_concurrent_per_side

    def get_max_hold_seconds(self) -> float:
        if not self.max_hold_minutes:
            return 0.0
        return max(0.0, float(self.max_hold_minutes) * 60.0)
