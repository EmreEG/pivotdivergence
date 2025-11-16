from typing import Dict, List
from collections import defaultdict

class FootprintBar:
    def __init__(self, bar_start: float, bar_duration: int = 60):
        self.bar_start = bar_start
        self.bar_duration = bar_duration
        self.bar_end = bar_start + bar_duration
        
        self.price_levels = defaultdict(lambda: {'bid_vol': 0, 'ask_vol': 0, 'delta': 0})
        
        self.total_bid_vol = 0
        self.total_ask_vol = 0
        self.total_delta = 0
        self.total_volume = 0.0
        self.open_price = None
        self.high_price = None
        self.low_price = None
        self.close_price = None
        
    def add_trade(self, price: float, qty: float, is_buyer_maker: bool, timestamp: float):
        if timestamp < self.bar_start or timestamp >= self.bar_end:
            return False
            
        price_key = round(price, 2)
        self._update_ohlc(price, qty)
        
        if is_buyer_maker:
            self.price_levels[price_key]['ask_vol'] += qty
            self.price_levels[price_key]['delta'] -= qty
            self.total_ask_vol += qty
            self.total_delta -= qty
        else:
            self.price_levels[price_key]['bid_vol'] += qty
            self.price_levels[price_key]['delta'] += qty
            self.total_bid_vol += qty
            self.total_delta += qty
        self.total_volume += qty
            
        return True

    def _update_ohlc(self, price: float, qty: float):
        if self.open_price is None:
            self.open_price = price
            self.high_price = price
            self.low_price = price
        self.close_price = price
        if self.high_price is None or price > self.high_price:
            self.high_price = price
        if self.low_price is None or price < self.low_price:
            self.low_price = price
        
    def get_price_level_data(self, price: float) -> Dict:
        price_key = round(price, 2)
        return self.price_levels.get(price_key, {'bid_vol': 0, 'ask_vol': 0, 'delta': 0})
        
    def detect_absorption(self, price: float, volume_threshold: float = 100) -> bool:
        data = self.get_price_level_data(price)
        
        if data['bid_vol'] > volume_threshold and data['delta'] < 0:
            return True
        if data['ask_vol'] > volume_threshold and data['delta'] > 0:
            return True
            
        return False
        
    def get_max_absorption_levels(self, top_n: int = 3) -> List[Dict]:
        absorption_levels = []
        
        for price, data in self.price_levels.items():
            total_vol = data['bid_vol'] + data['ask_vol']
            if total_vol == 0:
                continue
                
            imbalance = abs(data['delta']) / total_vol
            
            if imbalance < 0.3:
                absorption_levels.append({
                    'price': price,
                    'bid_vol': data['bid_vol'],
                    'ask_vol': data['ask_vol'],
                    'delta': data['delta'],
                    'total_vol': total_vol,
                    'absorption_score': total_vol * (1 - imbalance)
                })
                
        absorption_levels.sort(key=lambda x: x['absorption_score'], reverse=True)
        
        return absorption_levels[:top_n]
        
    def to_dict(self) -> Dict:
        return {
            'bar_start': self.bar_start,
            'bar_end': self.bar_end,
            'total_bid_vol': self.total_bid_vol,
            'total_ask_vol': self.total_ask_vol,
            'total_delta': self.total_delta,
            'total_volume': self.total_volume,
            'open': self.open_price,
            'high': self.high_price,
            'low': self.low_price,
            'close': self.close_price,
            'price_levels': dict(self.price_levels),
            'num_price_levels': len(self.price_levels)
        }

class FootprintManager:
    def __init__(self, bar_duration: int = 60):
        self.bar_duration = bar_duration
        self.bars = {}
        self.current_bar_start = None
        
    def add_trade(self, trade: Dict):
        price = trade['price']
        qty = trade['amount']
        timestamp = trade['timestamp'] / 1000
        
        is_buyer_maker = trade.get('info', {}).get('m', None)
        if is_buyer_maker is None:
            is_buyer_maker = (trade.get('side') == 'sell')
            
        bar_start = int(timestamp // self.bar_duration) * self.bar_duration
        
        if bar_start not in self.bars:
            self.bars[bar_start] = FootprintBar(bar_start, self.bar_duration)
            
        self.bars[bar_start].add_trade(price, qty, is_buyer_maker, timestamp)
        self.current_bar_start = bar_start
        
        self._cleanup_old_bars(timestamp)
        
    def _cleanup_old_bars(self, current_time: float, keep_bars: int = 100):
        if len(self.bars) <= keep_bars:
            return
            
        bar_starts = sorted(self.bars.keys())
        cutoff_time = current_time - (keep_bars * self.bar_duration)
        
        to_remove = [bs for bs in bar_starts if bs < cutoff_time]
        for bs in to_remove:
            del self.bars[bs]
            
    def get_current_bar(self) -> FootprintBar:
        if self.current_bar_start is None:
            return None
        return self.bars.get(self.current_bar_start)
        
    def get_bar(self, bar_start: float) -> FootprintBar:
        return self.bars.get(bar_start)
        
    def get_last_n_bars(self, n: int) -> List[FootprintBar]:
        bar_starts = sorted(self.bars.keys(), reverse=True)
        return [self.bars[bs] for bs in bar_starts[:n]]
        
    def detect_absorption_at_level(self, price: float, lookback_bars: int = 5) -> bool:
        recent_bars = self.get_last_n_bars(lookback_bars)
        
        absorption_count = 0
        for bar in recent_bars:
            if bar.detect_absorption(price):
                absorption_count += 1
                
        return absorption_count >= 2
