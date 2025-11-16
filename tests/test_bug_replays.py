import asyncio
import sys

sys.path.insert(0, '.')

import pytest

from backtest.replay import ReplaySimulator
from main import TradingSystem
from tests.bug_replay_fixtures import BUG_FIXTURES
from tests.test_replay import DummyPersister


@pytest.mark.parametrize('fixture', BUG_FIXTURES)
def test_bug_replay_fixture_runs(fixture):
    async def _run():
        system = TradingSystem()
        dummy = DummyPersister()
        system.persister = dummy
        sim = ReplaySimulator(system)
        sim.load_from_list(fixture.events)
        await sim.replay(realtime=False)
        fixture.assertion(system, dummy)

    asyncio.run(_run())
