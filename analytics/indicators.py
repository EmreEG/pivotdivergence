import numpy as np
import talib
from typing import Dict, Optional
from collections import deque


class IndicatorCalculator:
    def __init__(
        self,
        rsi_period: int = 14,
        macd_fast: int = 12,
        macd_slow: int = 26,
        macd_signal: int = 9,
        realized_vol_window: int = 60
    ):
        self.rsi_period = rsi_period
        self.macd_fast = macd_fast
        self.macd_slow = macd_slow
        self.macd_signal = macd_signal
        self.realized_vol_window = realized_vol_window
        
        self.price_history = deque(maxlen=500)
        self.volume_history = deque(maxlen=500)
        self.return_history = deque(maxlen=realized_vol_window)
        
        self.rsi = None
        self.macd = None
        self.macd_signal_line = None
        self.macd_hist = None
        self.obv = 0.0
        self.ad_line = 0.0
        self.realized_volatility = 0.0
        self.last_close = None
        
    def add_bar(self, close: float, volume: float, high: Optional[float] = None, low: Optional[float] = None):
        self.price_history.append(close)
        self.volume_history.append(volume)
        
        if len(self.price_history) >= self.rsi_period:
            self._compute_rsi()
            
        if len(self.price_history) >= self.macd_slow:
            self._compute_macd()
            
        self._compute_obv()
        self._compute_ad(high, low, close, volume)
        self._update_realized_volatility(close)
        
    def _compute_rsi(self):
        prices = np.array(self.price_history, dtype=float)
        rsi_values = talib.RSI(prices, timeperiod=self.rsi_period)
        self.rsi = float(rsi_values[-1]) if not np.isnan(rsi_values[-1]) else None
        
    def _compute_macd(self):
        prices = np.array(self.price_history, dtype=float)
        macd, signal, hist = talib.MACD(
            prices, 
            fastperiod=self.macd_fast,
            slowperiod=self.macd_slow,
            signalperiod=self.macd_signal
        )
        
        if not np.isnan(macd[-1]):
            self.macd = float(macd[-1])
            self.macd_signal_line = float(signal[-1])
            self.macd_hist = float(hist[-1])
            
    def _compute_obv(self):
        if len(self.price_history) < 2:
            return
            
        current_price = self.price_history[-1]
        prev_price = self.price_history[-2]
        current_volume = self.volume_history[-1]
        
        if current_price > prev_price:
            self.obv += current_volume
        elif current_price < prev_price:
            self.obv -= current_volume

    def _compute_ad(self, high: Optional[float], low: Optional[float], close: float, volume: float):
        if high is None or low is None:
            return
        price_range = high - low
        if price_range == 0:
            return
        mfm = ((close - low) - (high - close)) / price_range
        mfv = mfm * volume
        self.ad_line += mfv

    def _update_realized_volatility(self, close: float):
        if close is None or close <= 0:
            return
        if self.last_close is None:
            self.last_close = close
            return
        if self.last_close <= 0:
            self.last_close = close
            return
        ret = np.log(close / self.last_close)
        self.return_history.append(ret)
        self.last_close = close
        if len(self.return_history) >= 2:
            arr = np.array(self.return_history, dtype=float)
            self.realized_volatility = float(np.sqrt(np.mean(np.square(arr))))
        
    def get_rsi(self) -> Optional[float]:
        return self.rsi
        
    def get_macd(self) -> Dict[str, Optional[float]]:
        return {
            'macd': self.macd,
            'signal': self.macd_signal_line,
            'histogram': self.macd_hist
        }
        
    def get_obv(self) -> float:
        return self.obv

    def get_ad_line(self) -> float:
        return self.ad_line

    def get_realized_volatility(self) -> Optional[float]:
        return self.realized_volatility if self.realized_volatility else None
        
    def to_dict(self) -> Dict:
        return {
            'rsi': self.rsi,
            'macd': self.macd,
            'macd_signal': self.macd_signal_line,
            'macd_hist': self.macd_hist,
            'obv': self.obv,
            'ad_line': self.ad_line,
            'realized_volatility': self.get_realized_volatility()
        }
