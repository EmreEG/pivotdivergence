import json
import logging
import time
from pathlib import Path
from typing import Dict

from api.metrics import metrics


logger = logging.getLogger(__name__)


class MicrostructureHealthMonitor:
    """Track market health, contamination windows, and persistence backpressure."""

    def __init__(self, microstructure_cfg: Dict, monitoring_cfg: Dict, execution_manager, obi_calc, persister):
        self.microstructure_cfg = microstructure_cfg or {}
        self.monitoring_cfg = monitoring_cfg or {}
        self.execution_manager = execution_manager
        self.obi_calc = obi_calc
        self.persister = persister
        self._contaminated_until = 0.0
        self._unsafe_mode = False

    @property
    def contaminated_until(self) -> float:
        return self._contaminated_until

    @property
    def unsafe_mode(self) -> bool:
        return self._unsafe_mode

    def clear_contamination(self) -> None:
        self._contaminated_until = 0.0

    def mark_contaminated(self, source: str) -> None:
        hold = self.microstructure_cfg.get('contamination_hold_s', 60)
        if self.execution_manager.paper_mode:
            self.clear_contamination()
        else:
            self._contaminated_until = time.time() + hold
            try:
                self.obi_calc.mark_contaminated(hold)
            except Exception:
                logger.exception("OBI contamination mark failed")
        metrics.mark_microstructure(False, source)
        if not self.execution_manager.paper_mode and not self._unsafe_mode:
            self.activate_unsafe_mode(source)

    def mark_healthy(self) -> None:
        metrics.mark_microstructure(True, 'healthy')

    def is_contaminated(self) -> bool:
        return time.time() < self._contaminated_until

    def activate_unsafe_mode(self, reason: str) -> None:
        flag_path = self.monitoring_cfg.get('unsafe_flag_file')
        if not flag_path:
            return
        path = Path(flag_path)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                'reason': reason,
                'activated_at': int(time.time())
            }
            path.write_text(json.dumps(payload))
            self._unsafe_mode = True
            logger.warning("Unsafe mode activated automatically due to %s", reason)
        except Exception as exc:
            logger.error("Failed to write unsafe-mode flag file: %s", exc)

    def check_unsafe_flag(self) -> bool:
        if self.execution_manager.paper_mode:
            self._unsafe_mode = False
            return False
        flag_path = self.monitoring_cfg.get('unsafe_flag_file')
        if not flag_path:
            self._unsafe_mode = False
            return False
        path = Path(flag_path)
        if path.exists():
            if not self._unsafe_mode:
                logger.warning("Unsafe mode enabled – blocking new orders")
            self._unsafe_mode = True
            return True
        self._unsafe_mode = False
        return False

    def check_backpressure(self) -> bool:
        if not self.microstructure_cfg:
            return False
        max_buffer = self.persister.batch_size * 10
        buffers = {
            'book': len(self.persister.book_buffer),
            'mark': len(self.persister.mark_buffer),
            'oi': len(self.persister.oi_buffer),
            'indicator': len(self.persister.indicator_buffer),
            'signal': len(self.persister.signal_buffer),
        }
        if any(count > max_buffer for count in buffers.values()):
            self.mark_contaminated('backpressure')
            return True
        return False
