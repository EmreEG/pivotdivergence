#!/usr/bin/env python
"""
Unit tests for core analytics modules
"""
import sys
sys.path.insert(0, '.')

from analytics.profile import VolumeProfile
from analytics.extrema import ExtremaDetector
from analytics.swings import ZigZagSwing
from analytics.orderflow import CVDCalculator, OBICalculator
from analytics.naked_poc import NakedPOCTracker
from ingest.book_manager import OrderBookManager
from strategy.divergence import DivergenceDetector
from risk.position_sizer import RiskManager
import numpy as np

def test_volume_profile():
    print("Testing VolumeProfile...")
    profile = VolumeProfile('BTCUSDT', 3600)
    
    profile.add_trade(50000, 1.0, 1000)
    profile.add_trade(50100, 2.0, 1001)
    profile.add_trade(50050, 1.5, 1002)
    profile.add_trade(50000, 1.0, 1003)
    
    poc = profile.get_poc_price()
    assert poc is not None
    
    val, vah = profile.get_va_prices()
    
    data = profile.to_dict()
    assert data['total_volume'] == 5.5

    # Tick alignment guarantees
    tick = 0.1
    def aligned(px):
        return px is None or abs((px / tick) - round(px / tick)) < 1e-9
    assert aligned(data['poc'])
    assert aligned(data['val'])
    assert aligned(data['vah'])
    
    print("  ✓ VolumeProfile OK")

def test_extrema_detector():
    print("Testing ExtremaDetector...")
    detector = ExtremaDetector()
    
    prices = np.array([50000, 50050, 50100, 50150, 50200])
    volumes = np.array([100, 50, 200, 30, 150])
    
    extrema = detector.detect_extrema(prices, volumes)
    
    assert 'hvns' in extrema
    assert 'lvns' in extrema
    assert 'shelves' in extrema
    
    print("  ✓ ExtremaDetector OK")

def test_zigzag_swing():
    print("Testing ZigZagSwing...")
    swing = ZigZagSwing(swing_pct=0.01)
    
    swing.update(50000, 1000, 50.0, 0, 0)
    swing.update(50600, 1001, 55.0, 100, 0.5)
    swing.update(50100, 1002, 45.0, -100, -0.5)
    
    swing.get_last_n_swings(2)
    
    print("  ✓ ZigZagSwing OK")

def test_cvd_calculator():
    print("Testing CVDCalculator...")
    cvd_calc = CVDCalculator()
    
    trade1 = {'amount': 1.0, 'side': 'buy'}
    trade2 = {'amount': 0.5, 'side': 'sell'}
    
    cvd_calc.process_trade(trade1)
    cvd_calc.process_trade(trade2)
    
    cvd = cvd_calc.get_cvd()
    assert cvd == 0.5
    
    print("  ✓ CVDCalculator OK")

def test_avwap_tick_alignment():
    print("Testing AVWAP tick alignment...")
    from analytics.avwap import AVWAP
    tick = 0.1
    av = AVWAP(anchor_timestamp=0, tick_size=tick)
    # Add trades off-tick and ensure outputs are normalized
    av.add_trade(50000.03, 1.0, 10)
    av.add_trade(50010.07, 1.0, 20)
    v = av.get_avwap()
    lower, upper = av.get_bands()
    def aligned(px):
        return px is None or abs((px / tick) - round(px / tick)) < 1e-9
    assert aligned(v)
    # Bands may be None early
    if lower is not None and upper is not None:
        assert aligned(lower) and aligned(upper)
    print("  ✓ AVWAP tick alignment OK")

def test_obi_calculator():
    print("Testing OBICalculator...")
    obi_calc = OBICalculator()
    
    obi_calc.update_from_depth({'obi': 0.1, 'depth': 10})
    obi_calc.update_from_depth({'obi': -0.2, 'depth': 10})
    obi_calc.update_from_depth({'obi': 0.3, 'depth': 10})
    
    z = obi_calc.get_obi_z_score()
    assert z is not None
    
    print("  ✓ OBICalculator OK")

def test_naked_poc_tracker():
    print("Testing NakedPOCTracker...")
    tracker = NakedPOCTracker()
    
    tracker.register_poc('poc1', 50000, 1000, 'daily')
    tracker.register_poc('poc2', 50500, 1001, 'session')
    
    tracker.check_touch(49999, 50001, 1002)
    
    naked_pocs = tracker.get_naked_pocs()
    assert len(naked_pocs) == 1
    
    print("  ✓ NakedPOCTracker OK")

def test_binance_m_flag_mapping():
    print("Testing Binance maker flag mapping in CVDCalculator...")
    cvd = CVDCalculator()
    # Binance aggTrade 'm' = True means SELL taker, i.e., negative sign
    trade_sell_taker = {'amount': 1.0, 'info': {'m': True}}
    # 'm' = False means BUY taker, i.e., positive sign
    trade_buy_taker = {'amount': 2.0, 'info': {'m': False}}
    cvd.process_trade(trade_sell_taker)
    cvd.process_trade(trade_buy_taker)
    # Expected CVD = -1 + 2 = 1
    assert abs(cvd.get_cvd() - 1.0) < 1e-9
    print("  ✓ Binance m-flag mapping OK")

def test_order_book_manager():
    print("Testing OrderBookManager...")
    book = OrderBookManager('BTCUSDT')
    
    snapshot = {
        'bids': [[50000, 1.5], [49990, 2.0]],
        'asks': [[50010, 1.0], [50020, 1.5]],
        'lastUpdateId': 1000
    }
    book.process_snapshot(snapshot)
    
    assert book.validate()
    
    mid = book.get_mid_price()
    assert mid == 50005.0
    
    obi_stats = book.compute_obi(depth=2)
    assert obi_stats['obi'] != 0
    
    print("  ✓ OrderBookManager OK")

def test_event_replay():
    print("Testing event replay...")
    book = OrderBookManager('TEST', tick_size=0.5, max_depth=5)
    snapshot = {
        'bids': [[9999.5, 2.0], [9999.0, 1.5]],
        'asks': [[10000.5, 1.0], [10001.0, 2.5]],
        'lastUpdateId': 10
    }
    book.process_snapshot(snapshot)
    assert book.validate()
    diff = {'bids': [[9999.5, 1.0]], 'asks': [[10000.5, 0.0]], 'U': 11, 'u': 11, 'pu': 10, 'E': 0}
    assert book.process_update(diff)
    replay_snapshot = {
        'bids': [[9999.5, 1.0], [9999.0, 1.5]],
        'asks': [[10001.0, 2.5]],
        'lastUpdateId': 11
    }
    assert book.compare_with_snapshot(replay_snapshot)
    cvd = CVDCalculator()
    cvd.process_trade({'amount': 0.5, 'price': 10000, 'timestamp': 1_000, 'info': {'m': False}})
    cvd.process_trade({'amount': 0.25, 'price': 10001, 'timestamp': 1_200, 'info': {'m': True}})
    assert abs(cvd.get_cvd() - 0.25) < 1e-6
    print("  ✓ Event replay OK")

def test_divergence_detector():
    print("Testing DivergenceDetector...")
    detector = DivergenceDetector()
    
    swing1 = {'price': 50000, 'rsi': 70, 'cvd': 1000, 'bar': 1, 'obi_z': 1.0}
    swing2 = {'price': 51000, 'rsi': 65, 'cvd': 900, 'bar': 10, 'obi_z': -2.5}
    
    rsi_div, _ = detector.check_rsi_divergence(swing1, swing2, bearish=True)
    assert rsi_div is not None
    assert rsi_div is True
    
    cvd_div, _ = detector.check_cvd_divergence(swing1, swing2, bearish=True)
    assert cvd_div is True
    
    obi_div, _ = detector.check_obi_divergence(swing2, bearish=True)
    assert obi_div is True

    swing3 = {'price': 50000, 'rsi': 70, 'cvd': 1000, 'bar': 1}
    swing4 = {'price': 51000, 'rsi': 65, 'cvd': 1000, 'bar': 10}

    slow_confirms = detector.count_confirmations(
        (swing3, swing4),
        None,
        0.0,
        0.0,
        bearish=True,
        volatility=0.005
    )
    assert slow_confirms['rsi'] is True
    assert slow_confirms['count'] >= 1

    fast_confirms = detector.count_confirmations(
        (swing3, swing4),
        None,
        0.0,
        0.0,
        bearish=True,
        volatility=0.05
    )
    assert fast_confirms['count'] == 0
    assert fast_confirms['rsi'] is False
    
    print("  ✓ DivergenceDetector OK")

def test_risk_manager():
    print("Testing RiskManager...")
    risk = RiskManager()
    
    qty = risk.calculate_position_size(10000, 100, 0.0001, 'long', 50000)
    assert qty > 0
    
    stop = risk.calculate_stop_price(50000, 'long', 150, 80)
    assert stop < 50000
    
    target = risk.calculate_target_price(50000, stop, 'long', 1.5)
    assert target > 50000
    
    print("  ✓ RiskManager OK")

if __name__ == "__main__":
    print("\n=== Running Unit Tests ===\n")
    
    try:
        test_volume_profile()
        test_extrema_detector()
        test_zigzag_swing()
        test_cvd_calculator()
        test_obi_calculator()
        test_naked_poc_tracker()
        test_order_book_manager()
        test_event_replay()
        test_divergence_detector()
        test_risk_manager()
        
        print("\n=== All Tests Passed ✓ ===\n")
        sys.exit(0)
        
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
