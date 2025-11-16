#!/usr/bin/env python
"""
Mock trading system for testing without exchange connection
"""
import asyncio
import sys
import time
import random
sys.path.insert(0, '.')

from analytics.profile import VolumeProfile
from analytics.extrema import ExtremaDetector
from analytics.swings import ZigZagSwing
from analytics.orderflow import CVDCalculator, OBICalculator
from analytics.indicators import IndicatorCalculator
from strategy.level_selector import LevelSelector
from strategy.divergence import DivergenceDetector
from risk.position_sizer import RiskManager

async def generate_mock_data():
    """Generate mock market data for testing"""
    print("\n=== Mock Trading System Test ===\n")
    
    profile = VolumeProfile('BTCUSDT', 3600)
    extrema_detector = ExtremaDetector()
    swing_detector = ZigZagSwing()
    cvd_calc = CVDCalculator()
    obi_calc = OBICalculator()
    indicators = IndicatorCalculator()
    LevelSelector()
    div_detector = DivergenceDetector()
    risk_manager = RiskManager()
    
    base_price = 50000
    current_time = time.time()
    
    print("Generating mock data...")
    
    for i in range(100):
        price_change = random.gauss(0, 0.002)
        price = base_price * (1 + price_change)
        qty = random.uniform(0.1, 2.0)
        
        profile.add_trade(price, qty, current_time + i)
        
        trade = {
            'amount': qty,
            'side': 'buy' if random.random() > 0.5 else 'sell',
            'info': {'m': random.random() > 0.5}
        }
        cvd_calc.process_trade(trade)

        if i % 10 == 0:
            high = price * (1 + random.uniform(0.0005, 0.001))
            low = price * (1 - random.uniform(0.0005, 0.001))
            indicators.add_bar(price, qty * price, high=high, low=low)

        rsi = indicators.get_rsi()
        cvd = cvd_calc.get_cvd()
        obi_z = obi_calc.get_obi_z_score()

        swing_detector.update(price, current_time + i, rsi, cvd, obi_z, volume=qty)

        base_price = price
        
    print("✓ Generated 100 trades")
    
    poc = profile.get_poc_price()
    val, vah = profile.get_va_prices()
    
    print("\nVolume Profile:")
    print(f"  POC: ${poc:.2f}" if poc else "  POC: None")
    print(f"  VAL: ${val:.2f}" if val else "  VAL: None")
    print(f"  VAH: ${vah:.2f}" if vah else "  VAH: None")
    
    prices, volumes = profile.get_histogram_array()
    
    if len(prices) > 0:
        extrema = extrema_detector.detect_extrema(prices, volumes)
        print("\nExtrema detected:")
        print(f"  HVNs: {len(extrema['hvns'])}")
        print(f"  LVNs: {len(extrema['lvns'])}")
        print(f"  Shelves: {len(extrema['shelves'])}")
        
        if extrema['lvns']:
            print("\nTop LVN levels:")
            for lvn in extrema['lvns'][:3]:
                print(f"  ${lvn['price']:.2f} (prominence: {lvn['prominence']:.2f})")
    
    swing_data = swing_detector.to_dict()
    print("\nSwing Detection:")
    print(f"  Current swing: {swing_data['current_swing_type']}")
    print(f"  Swing history: {len(swing_data['swing_history'])} swings")
    
    indicators_data = indicators.to_dict()
    print("\nIndicators:")
    print(f"  RSI: {indicators_data['rsi']:.2f}" if indicators_data['rsi'] else "  RSI: None")
    print(f"  MACD: {indicators_data['macd']:.2f}" if indicators_data['macd'] else "  MACD: None")
    print(f"  CVD: {cvd_calc.get_cvd():.2f}")
    
    highs = swing_detector.get_last_two_highs()
    if highs:
        print("\nTesting divergence detection...")
        div_result = div_detector.check_rsi_divergence(highs[0], highs[1], bearish=True)
        print(f"  RSI bearish divergence: {div_result}")
    
    if extrema['lvns']:
        print("\nTesting position sizing...")
        equity = 10000
        stop_dist = extrema['lvns'][0]['price'] * 0.005
        qty_usd = risk_manager.calculate_position_size(equity, stop_dist, 0.0001, 'long', base_price)
        print(f"  Position size for $10k equity: ${qty_usd:.2f}")
    
    print("\n=== Mock Test Complete ✓ ===\n")

if __name__ == "__main__":
    asyncio.run(generate_mock_data())
