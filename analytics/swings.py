from typing import Dict, List, Optional
from collections import deque
from config import config


class ZigZagSwing:
    def __init__(self, swing_pct: float = None, history_size: int = None):
        self.swing_pct = swing_pct or config.swing["zigzag_pct"]
        self.history_size = history_size or config.swing["history_size"]

        self.swings = deque(maxlen=self.history_size)

        self.last_swing_type = None
        self.last_swing_price = None
        self.last_swing_bar = None
        self.last_swing_timestamp = None

        self.current_extreme_price = None
        self.current_extreme_bar = None
        self.current_extreme_timestamp = None
        self.current_extreme_meta = {
            "rsi": None,
            "cvd": None,
            "obi_z": None,
            "volume": None,
            "macd_hist": None,
        }

        self.bar_count = 0

    def update(
        self,
        price: float,
        timestamp: float,
        rsi: Optional[float] = None,
        cvd: Optional[float] = None,
        obi_z: Optional[float] = None,
        macd_hist: Optional[float] = None,
        volume: Optional[float] = None,
    ):
        self.bar_count += 1

        if self.last_swing_price is None:
            self.last_swing_price = price
            self.last_swing_bar = self.bar_count
            self.last_swing_timestamp = timestamp
            self.current_extreme_price = price
            self.current_extreme_bar = self.bar_count
            self.current_extreme_timestamp = timestamp
            self.current_extreme_meta = {
                "rsi": rsi,
                "cvd": cvd,
                "obi_z": obi_z,
                "macd_hist": macd_hist,
                "volume": volume,
            }
            return None

        if self.last_swing_type is None:
            if price > self.last_swing_price * (1 + self.swing_pct):
                self.last_swing_type = "low"
                self.current_extreme_price = price
                self.current_extreme_bar = self.bar_count
                self.current_extreme_timestamp = timestamp
            elif price < self.last_swing_price * (1 - self.swing_pct):
                self.last_swing_type = "high"
            self._set_current_extreme(price, self.bar_count, timestamp, rsi, cvd, obi_z, macd_hist, volume)
            return None

        new_swing = None

        if self.last_swing_type == "low":
            if price > self.current_extreme_price:
                self._set_current_extreme(price, self.bar_count, timestamp, rsi, cvd, obi_z, macd_hist, volume)
            elif price < self.current_extreme_price * (1 - self.swing_pct):
                new_swing = {
                    "type": "high",
                    "price": self.current_extreme_price,
                    "bar": self.current_extreme_bar,
                    "timestamp": self.current_extreme_timestamp,
                    "rsi": self.current_extreme_meta["rsi"],
                    "cvd": self.current_extreme_meta["cvd"],
                    "obi_z": self.current_extreme_meta["obi_z"],
                    "macd_hist": self.current_extreme_meta["macd_hist"],
                    "volume": self.current_extreme_meta["volume"],
                }
                self.swings.append(new_swing)

                self.last_swing_type = "high"
                self.last_swing_price = self.current_extreme_price
                self.last_swing_bar = self.current_extreme_bar
                self.last_swing_timestamp = self.current_extreme_timestamp
                self._set_current_extreme(price, self.bar_count, timestamp, rsi, cvd, obi_z, macd_hist, volume)

        elif self.last_swing_type == "high":
            if price < self.current_extreme_price:
                self._set_current_extreme(price, self.bar_count, timestamp, rsi, cvd, obi_z, macd_hist, volume)
            elif price > self.current_extreme_price * (1 + self.swing_pct):
                new_swing = {
                    "type": "low",
                    "price": self.current_extreme_price,
                    "bar": self.current_extreme_bar,
                    "timestamp": self.current_extreme_timestamp,
                    "rsi": self.current_extreme_meta["rsi"],
                    "cvd": self.current_extreme_meta["cvd"],
                    "obi_z": self.current_extreme_meta["obi_z"],
                    "macd_hist": self.current_extreme_meta["macd_hist"],
                    "volume": self.current_extreme_meta["volume"],
                }
                self.swings.append(new_swing)

                self.last_swing_type = "low"
                self.last_swing_price = self.current_extreme_price
                self.last_swing_bar = self.current_extreme_bar
                self.last_swing_timestamp = self.current_extreme_timestamp
                self._set_current_extreme(price, self.bar_count, timestamp, rsi, cvd, obi_z, macd_hist, volume)

        return new_swing

    def _set_current_extreme(
        self,
        price: float,
        bar: int,
        timestamp: float,
        rsi: Optional[float],
        cvd: Optional[float],
        obi_z: Optional[float],
        macd_hist: Optional[float],
        volume: Optional[float],
    ):
        self.current_extreme_price = price
        self.current_extreme_bar = bar
        self.current_extreme_timestamp = timestamp
        self.current_extreme_meta = {
            "rsi": rsi,
            "cvd": cvd,
            "obi_z": obi_z,
            "macd_hist": macd_hist,
            "volume": volume,
        }

    def get_last_n_swings(self, n: int, swing_type: Optional[str] = None) -> List[Dict]:
        if swing_type:
            filtered = [s for s in self.swings if s["type"] == swing_type]
            return list(filtered)[-n:] if filtered else []
        swing_list = list(self.swings)
        return swing_list[-n:] if swing_list else []

    def get_last_two_highs(self) -> Optional[tuple]:
        highs = self.get_last_n_swings(2, "high")
        if len(highs) < 2:
            return None
        return (highs[0], highs[1])

    def get_last_two_lows(self) -> Optional[tuple]:
        lows = self.get_last_n_swings(2, "low")
        if len(lows) < 2:
            return None
        return (lows[0], lows[1])

    def to_dict(self) -> Dict:
        return {
            "current_swing_type": self.last_swing_type,
            "current_swing_price": self.last_swing_price,
            "current_extreme_price": self.current_extreme_price,
            "swing_history": list(self.swings),
            "bar_count": self.bar_count,
        }
