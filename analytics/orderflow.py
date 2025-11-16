from collections import deque
from typing import Dict, Optional
import numpy as np
import time


class CVDCalculator:
    def __init__(self, volume_precision: int = 1_000_000, max_gap_ms: int = 1000):
        self.volume_precision = volume_precision
        self.max_gap_ms = max_gap_ms
        self.cvd: int = 0
        self.last_trade_id: Optional[int] = None
        self.last_event_ts: Optional[int] = None
        self.total_volume: int = 0
        self.unstable: bool = False
        self.unstable_reason: Optional[str] = None

    def process_trade(self, trade: Dict) -> float:
        qty = float(trade.get('amount', 0))
        scaled_qty = int(round(qty * self.volume_precision))
        if scaled_qty == 0:
            return self.get_cvd()

        trade_id = self._extract_trade_id(trade)
        event_ts = trade.get('timestamp')
        self._detect_gaps(trade_id, event_ts)

        direction = self._resolve_direction(trade)
        self.cvd += direction * scaled_qty
        self.total_volume += abs(scaled_qty)

        if trade_id is not None:
            self.last_trade_id = trade_id
        if event_ts is not None:
            self.last_event_ts = event_ts

        return self.get_cvd()

    def _extract_trade_id(self, trade: Dict) -> Optional[int]:
        trade_id = trade.get('id') or trade.get('trade_id')
        if trade_id is None:
            trade_id = trade.get('info', {}).get('a')
        if trade_id is None:
            return None
        try:
            return int(trade_id)
        except Exception:
            return None

    def _detect_gaps(self, trade_id: Optional[int], event_ts: Optional[int]):
        if self.last_trade_id is not None and trade_id is not None:
            if trade_id < self.last_trade_id:
                self._mark_gap('trade_replay')
        if self.last_event_ts is not None and event_ts is not None:
            # Treat only backward time jumps beyond max_gap_ms as structural gaps.
            # Forward gaps are handled via explicit stream gap detection on the WebSocket.
            if event_ts + self.max_gap_ms < self.last_event_ts:
                self._mark_gap('time_travel')

    def _resolve_direction(self, trade: Dict) -> int:
        maker_flag = trade.get('info', {}).get('m')
        if maker_flag is False:
            return 1
        if maker_flag is True:
            return -1
        side = (trade.get('side') or '').lower()
        return 1 if side == 'buy' else -1

    def resolve_direction(self, trade: Dict) -> int:
        """
        Public helper to obtain the signed CVD direction for a trade,
        ensuring any external consumers stay consistent with CVD logic.
        """
        return self._resolve_direction(trade)

    def _mark_gap(self, reason: str):
        self.unstable = True
        self.unstable_reason = reason

    def mark_resynced(self):
        self.unstable = False
        self.unstable_reason = None

    def is_stable(self) -> bool:
        return not self.unstable

    def get_cvd(self) -> float:
        return self.cvd / self.volume_precision

    def snapshot_state(self) -> Dict:
        return {
            'cvd': self.cvd,
            'last_trade_id': self.last_trade_id,
            'last_event_ts': self.last_event_ts,
            'volume_precision': self.volume_precision,
            'stable': self.is_stable(),
            'unstable_reason': self.unstable_reason
        }

    def restore_state(self, snapshot: Dict):
        self.cvd = int(snapshot.get('cvd', 0))
        self.last_trade_id = snapshot.get('last_trade_id')
        self.last_event_ts = snapshot.get('last_event_ts')
        self.unstable = not snapshot.get('stable', True)
        self.unstable_reason = snapshot.get('unstable_reason')


class OBICalculator:
    def __init__(self, lookback: int = 50, min_depth: int = 5):
        self.lookback = lookback
        self.min_depth = min_depth
        self.obi_history = deque(maxlen=lookback)
        self.depth_history = deque(maxlen=lookback)
        self.contaminated_until = 0.0

    def update_from_depth(self, stats: Dict) -> Optional[float]:
        obi = stats.get('obi')
        depth = stats.get('depth', 0)
        if obi is None:
            return None
        self.obi_history.append(obi)
        self.depth_history.append(depth)
        return self.get_obi_z_score()

    def get_obi_z_score(self) -> Optional[float]:
        if self.is_contaminated():
            return None
        if len(self.obi_history) < 2:
            return None
        if self.depth_history and self.depth_history[-1] < self.min_depth:
            return None

        arr = np.array(self.obi_history)
        mean = np.mean(arr)
        std = np.std(arr)
        if std == 0:
            return 0.0
        current = arr[-1]
        return (current - mean) / std
    
    def get_latest_obi(self) -> Optional[float]:
        if not self.obi_history:
            return None
        return self.obi_history[-1]

    def mark_contaminated(self, hold_s: float):
        self.contaminated_until = time.time() + hold_s

    def is_contaminated(self) -> bool:
        return time.time() < self.contaminated_until


class OIAnalyzer:
    def __init__(self, slope_period_s: int = 300):
        self.slope_period_s = slope_period_s
        self.oi_history = deque(maxlen=100)

    def add_oi(self, oi_value: float, timestamp: float):
        self.oi_history.append({
            'value': oi_value,
            'timestamp': timestamp
        })

    def get_slope(self) -> Optional[float]:
        if len(self.oi_history) < 2:
            return None

        current = self.oi_history[-1]

        target_time = current['timestamp'] - self.slope_period_s
        old_oi = None

        for oi_point in self.oi_history:
            if oi_point['timestamp'] <= target_time:
                old_oi = oi_point
            else:
                break

        if old_oi is None:
            old_oi = self.oi_history[0]

        time_diff = current['timestamp'] - old_oi['timestamp']
        if time_diff == 0:
            return 0.0

        slope = (current['value'] - old_oi['value']) / time_diff
        return slope

    def get_current_oi(self) -> Optional[float]:
        if len(self.oi_history) == 0:
            return None
        return self.oi_history[-1]['value']
