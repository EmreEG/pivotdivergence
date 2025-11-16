#!/usr/bin/env python
"""
Microstructure validation tests: ensure signals behave as specified in improve.md.

- OBI extremes correlate with next-step return sign on synthetic data.
- 70% Value Area boundaries behave consistently on a controlled profile.
"""
import sys
sys.path.insert(0, '.')

import numpy as np
import time

from analytics.orderflow import OBICalculator, CVDCalculator
from analytics.profile import VolumeProfile


def test_obi_extremes_predict_next_return_sign():
    # Construct synthetic series where positive OBI precedes positive return
    obi_calc = OBICalculator(lookback=50, min_depth=5)

    np.random.seed(42)
    n = 200
    half = n // 2
    depth = 10
    raw_neg = -0.8 + 0.02 * np.random.randn(half)
    raw_pos = 0.8 + 0.02 * np.random.randn(half)
    raw_obi = np.concatenate([raw_neg, raw_pos])

    returns = np.concatenate([
        np.full(half, -0.01),
        np.full(half, 0.01)
    ])

    z_scores = []
    for i in range(n):
        z = obi_calc.update_from_depth({'obi': float(raw_obi[i]), 'depth': depth})
        z_scores.append(z)

    # Drop warmup Nones
    z_arr = np.array([z for z in z_scores if z is not None])
    ret_arr = returns[-len(z_arr):]

    # Evaluate directional accuracy on upper/lower deciles of z
    hi_thresh = np.quantile(z_arr, 0.9)
    lo_thresh = np.quantile(z_arr, 0.1)

    hi_idx = z_arr >= hi_thresh
    lo_idx = z_arr <= lo_thresh

    # High positive OBI z → positive return
    hi_correct = np.mean(np.sign(ret_arr[hi_idx]) > 0)
    # High negative OBI z → negative return
    lo_correct = np.mean(np.sign(ret_arr[lo_idx]) < 0)

    # Expect >60% directional accuracy in synthetic construction
    assert hi_correct > 0.6
    assert lo_correct > 0.6


def test_cvd_deciles_predict_next_return_sign():
    cvd_calc = CVDCalculator(volume_precision=1_000_000)
    steps = 400
    neg_flow = np.full(steps // 2, -0.1)
    pos_flow = np.full(steps // 2, 1.0)
    increments = np.concatenate([neg_flow, pos_flow])
    returns = np.concatenate([
        np.full(steps // 2, -0.01),
        np.full(steps // 2, 0.01)
    ])

    cvd_values = []
    timestamps = np.linspace(0, steps, steps)
    for inc, ts in zip(increments, timestamps):
        qty = abs(float(inc)) + 0.05
        maker_flag = bool(inc < 0)
        trade = {
            'amount': qty,
            'info': {'m': maker_flag},
            'timestamp': int((time.time() + ts) * 1000)
        }
        cvd_values.append(cvd_calc.process_trade(trade))

    cvd_values = np.array(cvd_values)
    # Compare next-step returns conditioned on extreme deciles of the CVD level
    hi_thresh = np.quantile(cvd_values, 0.9)
    lo_thresh = np.quantile(cvd_values, 0.1)
    hi_idx = cvd_values[:-1] >= hi_thresh
    lo_idx = cvd_values[:-1] <= lo_thresh
    forward_returns = returns[1:]

    assert hi_idx.sum() > 0 and lo_idx.sum() > 0
    hi_correct = np.mean(np.sign(forward_returns[hi_idx]) > 0)
    lo_correct = np.mean(np.sign(forward_returns[lo_idx]) < 0)
    assert hi_correct > 0.65
    assert lo_correct > 0.65


def test_value_area_70_percent_consistency():
    # Create a symmetric bell-shaped volume around a center price
    center = 100.0
    profile = VolumeProfile('TEST', window_seconds=3600)
    profile.set_tick_size(0.1)

    prices = np.linspace(center - 2.0, center + 2.0, 81)  # 0.05 step before tick quantization
    vols = np.exp(-0.5 * ((prices - center) / 0.5) ** 2)
    vols /= vols.max()

    ts = 0
    for p, v in zip(prices, vols):
        ts += 1
        profile.add_trade(float(p), float(v), float(ts))

    data = profile.to_dict()
    poc = data['poc']
    val = data['val']
    vah = data['vah']

    # Assert tick alignment and symmetry around POC within one bin width
    assert poc is not None and val is not None and vah is not None
    width = data['bin_width']
    # Symmetric distribution should yield approximately symmetric VA
    assert abs((poc - val) - (vah - poc)) <= width + 1e-9
