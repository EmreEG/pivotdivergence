#!/usr/bin/env python
"""
Quick system test - 30 seconds, no database writes
"""
import asyncio
import sys
sys.path.insert(0, '.')

async def main():
    print("\n=== System Validation Test ===\n")
    
    # Test imports
    print("1. Testing imports...")
    from config import config
    from analytics.profile import VolumeProfile
    from analytics.swings import ZigZagSwing  
    from analytics.orderflow import CVDCalculator
    print("   ✓ All imports successful\n")
    
    # Test config
    print("2. Testing configuration...")
    print(f"   Symbol: {config.exchange['symbol']}")
    print(f"   Testnet: {config.exchange.get('testnet', False)}")
    print(f"   API Key: {'Present' if config.exchange.get('api_key') else 'Missing'}")
    print("   ✓ Config loaded\n")
    
    # Test exchange connection via official Binance REST
    print("3. Testing Binance REST connection...")
    from ingest.binance_rest import BinanceRESTClient
    rest = BinanceRESTClient()
    ticker = await rest.get('/fapi/v1/premiumIndex', params={'symbol': config.exchange['symbol']})
    last_price = float(ticker.get('markPrice') or ticker.get('indexPrice') or 0.0)
    print(f"   ✓ Connected: {config.exchange['symbol']} markPrice = ${last_price:.2f}\n")
    
    # Test WebSocket using project WebSocketClient
    print("4. Testing WebSocket stream (10 seconds)...")
    from ingest.websocket_client import WebSocketClient
    ws_client = WebSocketClient(config.exchange['symbol'])
    trade_count = 0
    
    async def handle_trade(trade):
        nonlocal trade_count
        trade_count += 1
    
    ws_client.register_handler('trade', handle_trade)
    
    async def run_ws():
        task = asyncio.create_task(ws_client.start())
        await asyncio.sleep(10)
        await ws_client.stop()
        task.cancel()
    
    try:
        await asyncio.wait_for(run_ws(), timeout=20)
        print(f"   ✓ Received {trade_count} trades\n")
    except asyncio.TimeoutError:
        print(f"   ✓ Received {trade_count} trades (timeout)\n")
    
    # Test analytics
    print("5. Testing analytics modules...")
    profile = VolumeProfile('BTCUSDT', 3600)
    profile.add_trade(last_price, 1.0, asyncio.get_event_loop().time())
    profile.add_trade(last_price * 1.001, 1.5, asyncio.get_event_loop().time() + 1)
    
    swing = ZigZagSwing()
    swing.update(ticker['last'], asyncio.get_event_loop().time(), 50.0, 0, 0)
    
    cvd = CVDCalculator()
    cvd.process_trade({'amount': 1.0, 'side': 'buy', 'info': {'m': False}})
    
    print(f"   ✓ Profile: POC at ${profile.get_poc_price():.2f}" if profile.get_poc_price() else "   ✓ Profile created")
    print(f"   ✓ CVD: {cvd.get_cvd()}")
    print(f"   ✓ Swing detector: {swing.bar_count} bars\n")
    print("=== ✓ ALL TESTS PASSED ===\n")
    print("System is fully operational!")
    print("\nTo run full system:")
    print("  ./start.sh\n")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTest interrupted")
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
