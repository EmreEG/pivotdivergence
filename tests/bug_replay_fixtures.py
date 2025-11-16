from dataclasses import dataclass
from typing import Callable, Dict, List


@dataclass
class BugReplayFixture:
    fixture_id: str
    description: str
    events: List[Dict]
    assertion: Callable[[object, object], None]


def _obi_regime_fixture() -> BugReplayFixture:
    events = [
        {
            'ts_ms': 1_000,
            'type': 'orderbook',
            'payload': {
                'bids': [[100.0, 3.0], [99.5, 1.5]],
                'asks': [[100.5, 2.0], [101.0, 1.0]],
                'timestamp': 1_000,
                'lastUpdateId': 1,
            },
        },
        {
            'ts_ms': 2_000,
            'type': 'orderbook',
            'payload': {
                'bids': [[100.1, 2.5], [99.6, 1.0]],
                'asks': [[100.6, 2.2], [101.1, 1.3]],
                'timestamp': 2_000,
                'lastUpdateId': 2,
            },
        },
    ]

    def _assert(system, persister):
        assert persister.obi_buffer, 'OBI snapshots not persisted'
        latest = persister.obi_buffer[-1]
        assert latest.get('bid_qty') > 0
        assert latest.get('ask_qty') > 0
        assert latest.get('obi') is not None
        assert latest.get('mid_price') is not None

    return BugReplayFixture(
        fixture_id='obi_regime_storage',
        description='Regression for missing OBI depth persistence',
        events=events,
        assertion=_assert,
    )


BUG_FIXTURES: List[BugReplayFixture] = [
    _obi_regime_fixture(),
]
