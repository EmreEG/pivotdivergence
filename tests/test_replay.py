#!/usr/bin/env python
import sys
import asyncio

sys.path.insert(0, '.')

from backtest.replay import ReplaySimulator
from main import TradingSystem


class DummyPersister:
    def __init__(self):
        self.trade_buffer = []
        self.book_buffer = []
        self.mark_buffer = []
        self.oi_buffer = []
        self.indicator_buffer = []
        self.signal_buffer = []
        self.batch_size = 1000
        self.obi_buffer = []

    async def initialize(self):
        return None

    async def start(self):
        return None

    async def stop(self):
        return None

    async def insert_trade(self, trade):
        self.trade_buffer.append(trade)

    async def insert_book_snapshot(self, snap):
        self.book_buffer.append(snap)

    async def insert_mark_price(self, mark):
        self.mark_buffer.append(mark)

    async def insert_open_interest(self, oi):
        self.oi_buffer.append(oi)

    async def insert_indicator_state(self, i):
        self.indicator_buffer.append(i)

    async def insert_signal(self, s):
        self.signal_buffer.append(s)

    async def insert_obi_snapshot(self, snap):
        self.obi_buffer.append(snap)


def test_replay_simulator_runs():
    async def _run():
        system = TradingSystem()
        # Replace real persister with dummy to avoid DB dependency
        system.persister = DummyPersister()
        sim = ReplaySimulator(system)
        sim.load_from_list([
            {
                'ts_ms': 1_000,
                'type': 'trade',
                'payload': {'symbol': 'TEST', 'timestamp': 1_000, 'price': 100.0, 'amount': 1.0, 'side': 'buy'}
            },
            {
                'ts_ms': 1_500,
                'type': 'mark',
                'payload': {'timestamp': 1_500, 'info': {'markPrice': 100.5, 'fundingRate': 0.0}}
            },
            {
                'ts_ms': 2_000,
                'type': 'orderbook',
                'payload': {'bids': [[100.0, 1.0]], 'asks': [[101.0, 1.0]], 'timestamp': 2_000}
            },
            {
                'ts_ms': 2_500,
                'type': 'oi',
                'payload': {'symbol': 'TEST', 'timestamp': 2_500, 'openInterest': 123.0}
            },
        ])
        await sim.replay(realtime=False)

        # Basic assertions: handlers executed without exceptions
        assert system.current_price is not None
        assert len(system.profiles) > 0

    asyncio.run(_run())
