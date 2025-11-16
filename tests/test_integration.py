#!/usr/bin/env python
"""
Integration test - runs full system for 30 seconds with real data
"""
import asyncio
import sys
import signal
sys.path.insert(0, '.')

from main import TradingSystem


async def _run_system():
    print("\n=== Integration Test: Full System ===\n")

    system = TradingSystem()

    def signal_handler(sig, frame):
        print("\n\nStopping system...")
        asyncio.create_task(system.stop())

    signal.signal(signal.SIGINT, signal_handler)

    try:
        print("Starting trading system...")
        print("Will run for 30 seconds to test all components\n")

        asyncio.create_task(system.start())

        await asyncio.sleep(30)

        print("\n\nTest duration complete. Stopping system...")
        system.running = False

        await asyncio.sleep(2)

        print("\n=== System Test Results ===\n")
        print(f"✓ WebSocket client: {'Connected' if system.ws_client else 'Failed'}")
        print(f"✓ Current price: ${system.current_price:.2f}" if system.current_price else "✗ No price data")
        print(f"✓ Profiles: {len(system.profiles)} windows")
        print(f"✓ Swing detector: {system.swing_detector.bar_count} bars processed")

        swings = system.swing_detector.get_last_n_swings(5)
        print(f"✓ Swings detected: {len(swings)}")

        indicators = system.indicators.to_dict()
        if indicators.get('rsi'):
            print(f"✓ RSI: {indicators['rsi']:.2f}")

        print(f"✓ CVD: {system.cvd_calc.get_cvd():.2f}")

        signals = system.signal_manager.get_active_signals()
        print(f"✓ Active signals: {len(signals)}")

        print("\n=== Integration Test Complete ✓ ===\n")

    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        await system.stop()


def test_system():
    asyncio.run(_run_system())


if __name__ == "__main__":
    asyncio.run(_run_system())
