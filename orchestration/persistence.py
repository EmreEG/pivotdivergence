import json
import json
import logging
import time
from typing import Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from main import TradingSystem


logger = logging.getLogger(__name__)


class PersistenceCoordinator:
    """High-level persistence interface covering snapshots and event storage."""

    def __init__(self, system: 'TradingSystem'):
        self.system = system
        self.persister = system.persister
        self.avwap_snapshot_path = system.avwap_snapshot_path
        self.cvd_snapshot_path = system.cvd_snapshot_path
        self.last_avwap_snapshot = 0.0
        self.last_cvd_snapshot = 0.0

    def load_avwap_snapshot(self):
        try:
            if self.avwap_snapshot_path.exists():
                data = json.loads(self.avwap_snapshot_path.read_text())
                self.system.profile_service.restore_avwaps(data)
                logger.info("Restored AVWAP snapshot state")
        except Exception as exc:
            logger.warning("AVWAP snapshot restore failed: %s", exc)

    def persist_avwap_snapshot(self):
        now = time.time()
        if now - self.last_avwap_snapshot < 30:
            return
        try:
            snapshot = self.system.profile_service.snapshot_avwaps()
            self.avwap_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            self.avwap_snapshot_path.write_text(json.dumps(snapshot))
            self.last_avwap_snapshot = now
        except Exception as exc:
            logger.error("AVWAP snapshot persist failed: %s", exc)

    def load_cvd_snapshot(self):
        try:
            if self.cvd_snapshot_path.exists():
                data = json.loads(self.cvd_snapshot_path.read_text())
                self.system.order_flow.restore_cvd_state(data)
                logger.info("Restored CVD snapshot state")
        except Exception as exc:
            logger.warning("CVD snapshot restore failed: %s", exc)

    def persist_cvd_snapshot(self):
        now = time.time()
        if now - self.last_cvd_snapshot < 30:
            return
        try:
            snapshot = self.system.order_flow.snapshot_cvd_state()
            if snapshot and snapshot.get('stable', True):
                self.cvd_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
                self.cvd_snapshot_path.write_text(json.dumps(snapshot))
                self.last_cvd_snapshot = now
        except Exception as exc:
            logger.error("CVD snapshot persist failed: %s", exc)

    async def persist_trade(self, trade: Dict):
        await self.persister.insert_trade(trade)
        self.persist_cvd_snapshot()

    async def persist_orderbook(self, obi_stats: Dict, book_snapshot: Dict):
        payload = {
            'symbol': self.system.symbol,
            **obi_stats
        }
        await self.persister.insert_obi_snapshot(payload)
        await self.persister.insert_book_snapshot(book_snapshot)

    async def persist_mark_price(self, payload: Dict):
        await self.persister.insert_mark_price(payload)

    async def persist_open_interest(self, payload: Dict):
        await self.persister.insert_open_interest(payload)

    async def persist_indicator_state(self, payload: Dict):
        await self.persister.insert_indicator_state(payload)

    async def persist_signal(self, payload: Dict):
        await self.persister.insert_signal(payload)
