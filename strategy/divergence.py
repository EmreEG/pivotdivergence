from typing import Optional, Dict
from config import config


class DivergenceDetector:
    def __init__(self, disable_rsi: bool = False, disable_cvd: bool = False, disable_obi: bool = False):
        self.sync_tolerance = config.divergence["sync_tolerance_bars"]
        self.rsi_delta = config.divergence["rsi_delta"]
        self.cvd_delta_pct = config.divergence["cvd_delta_pct"]
        self.obi_z_threshold = config.divergence["obi_z_threshold"]
        self.min_price_move_pct = config.divergence.get("min_price_move_pct", 0.0)
        self.min_indicator_move = config.divergence.get("min_indicator_move", 0.0)
        self.fast_vol_threshold = config.divergence.get("fast_volatility_threshold", 0.02)
        self.vol_delta_pct = config.divergence.get("vol_delta_pct", 0.2)
        self.macd_hist_delta_pct = config.divergence.get("macd_hist_delta_pct", 0.2)

        # Ablation flags
        self.disable_rsi = disable_rsi
        self.disable_cvd = disable_cvd
        self.disable_obi = disable_obi

    def check_rsi_divergence(self, swing1: Dict, swing2: Dict, bearish: bool):
        if swing1.get("rsi") is None or swing2.get("rsi") is None:
            return False, None

        price1, price2 = swing1["price"], swing2["price"]
        rsi1, rsi2 = swing1["rsi"], swing2["rsi"]
        price_move_pct = self._price_move_pct(price1, price2)
        context = {
            "p1": swing1,
            "p2": swing2,
            "price_move_pct": price_move_pct,
            "delta": rsi2 - rsi1,
        }
        if price_move_pct < self.min_price_move_pct or abs(rsi2 - rsi1) < self.min_indicator_move:
            return False, context

        abs(swing2["bar"] - swing1["bar"])

        if bearish:
            price_hh = price2 > price1
            rsi_lh = rsi2 <= rsi1 - self.rsi_delta
            return price_hh and rsi_lh, context
        else:
            price_ll = price2 < price1
            rsi_hl = rsi2 >= rsi1 + self.rsi_delta
            return price_ll and rsi_hl, context

    def check_cvd_divergence(self, swing1: Dict, swing2: Dict, bearish: bool):
        if swing1.get("cvd") is None or swing2.get("cvd") is None:
            return False, None

        price1, price2 = swing1["price"], swing2["price"]
        cvd1, cvd2 = swing1["cvd"], swing2["cvd"]
        price_move_pct = self._price_move_pct(price1, price2)
        context = {
            "p1": swing1,
            "p2": swing2,
            "price_move_pct": price_move_pct,
            "delta": cvd2 - cvd1,
        }
        if price_move_pct < self.min_price_move_pct:
            return False, context

        cvd_range = abs(cvd2 - cvd1)
        threshold = cvd_range * self.cvd_delta_pct if cvd_range > 0 else 0

        if bearish:
            price_hh = price2 > price1
            cvd_lh = cvd2 < cvd1 - threshold
            return price_hh and cvd_lh, context
        else:
            price_ll = price2 < price1
            cvd_hl = cvd2 > cvd1 + threshold
            return price_ll and cvd_hl, context

    def check_volume_divergence(self, swing1: Dict, swing2: Dict, bearish: bool):
        vol1 = swing1.get("volume")
        vol2 = swing2.get("volume")
        if vol1 is None or vol2 is None:
            return False, None

        try:
            v1 = float(vol1)
            v2 = float(vol2)
        except Exception:
            return False, None

        if v1 <= 0 or v2 <= 0:
            context = {
                "p1": swing1,
                "p2": swing2,
                "volume1": vol1,
                "volume2": vol2,
                "price_move_pct": self._price_move_pct(swing1["price"], swing2["price"]),
            }
            return False, context

        price_move_pct, context = self._build_price_context(
            swing1,
            swing2,
            extra={
                "volume1": v1,
                "volume2": v2,
            },
        )
        if price_move_pct < self.min_price_move_pct:
            return False, context

        # Divergence: price continues trend but volume contracts materially
        volume_down = v2 < v1 * (1.0 - max(self.vol_delta_pct, 0.0))
        if bearish:
            price_trend = price2 > price1  # higher high
        else:
            price_trend = price2 < price1  # lower low

        return price_trend and volume_down, context

    def check_macd_hist_divergence(self, swing1: Dict, swing2: Dict, bearish: bool):
        hist1 = swing1.get("macd_hist")
        hist2 = swing2.get("macd_hist")
        if hist1 is None or hist2 is None:
            return False, None

        try:
            h1 = float(hist1)
            h2 = float(hist2)
        except Exception:
            return False, None

        price_move_pct, context = self._build_price_context(
            swing1,
            swing2,
            extra={
                "hist1": h1,
                "hist2": h2,
                "delta": h2 - h1,
            },
        )
        if price_move_pct < self.min_price_move_pct:
            return False, context

        base = max(abs(h1), abs(h2))
        if base <= 0:
            return False, context

        # Divergence: price continues trend but MACD histogram amplitude contracts materially
        weaker = abs(h2) < abs(h1) * (1.0 - max(self.macd_hist_delta_pct, 0.0))
        if bearish:
            price_trend = price2 > price1  # higher high
        else:
            price_trend = price2 < price1  # lower low

        return price_trend and weaker, context

    def _build_price_context(
        self,
        swing1: Dict,
        swing2: Dict,
        extra: Optional[Dict] = None,
    ) -> Tuple[float, Dict]:
        from typing import cast

        price1, price2 = swing1["price"], swing2["price"]
        price_move_pct = self._price_move_pct(price1, price2)
        context: Dict = {
            "p1": swing1,
            "p2": swing2,
            "price_move_pct": price_move_pct,
        }
        if extra:
            context.update(cast(Dict, extra))
        return price_move_pct, context

    def check_obi_divergence(self, swing2: Dict, bearish: bool):
        obi_z = swing2.get("obi_z")
        if obi_z is None:
            return False, None

        if bearish:
            return obi_z < -self.obi_z_threshold, {"obi_z": obi_z}
        else:
            return obi_z > self.obi_z_threshold, {"obi_z": obi_z}

    def check_oi_filter(self, oi_slope: Optional[float], bearish: bool) -> bool:
        if oi_slope is None:
            return True

        if bearish:
            return oi_slope <= 0
        else:
            return oi_slope >= 0

    def count_confirmations(
        self,
        swings_high: Optional[tuple],
        swings_low: Optional[tuple],
        current_obi_z: Optional[float],
        oi_slope: Optional[float],
        bearish: bool,
        volatility: Optional[float] = None,
    ) -> Dict:
        confirmations = {
            "count": 0,
            "rsi": False,
            "cvd": False,
            "volume": False,
            "macd": False,
            "obi": False,
            "oi_filter": True,
            "context": {},
            "insufficient_swings": False,
        }
        insufficient_swings = False

        if bearish:
            if not swings_high or len(swings_high) < 2:
                insufficient_swings = True
            else:
                if not self.disable_rsi:
                    passed, ctx = self.check_rsi_divergence(swings_high[0], swings_high[1], bearish=True)
                    confirmations["context"]["rsi"] = ctx
                    if passed:
                        confirmations["rsi"] = True
                        confirmations["count"] += 1
                if not self.disable_cvd:
                    passed, ctx = self.check_cvd_divergence(swings_high[0], swings_high[1], bearish=True)
                    confirmations["context"]["cvd"] = ctx
                    if passed:
                        confirmations["cvd"] = True
                        confirmations["count"] += 1
                passed, ctx = self.check_volume_divergence(swings_high[0], swings_high[1], bearish=True)
                confirmations["context"]["volume"] = ctx
                if passed:
                    confirmations["volume"] = True
                    confirmations["count"] += 1
                passed, ctx = self.check_macd_hist_divergence(swings_high[0], swings_high[1], bearish=True)
                confirmations["context"]["macd"] = ctx
                if passed:
                    confirmations["macd"] = True
                    confirmations["count"] += 1
        else:
            if not swings_low or len(swings_low) < 2:
                insufficient_swings = True
            else:
                if not self.disable_rsi:
                    passed, ctx = self.check_rsi_divergence(swings_low[0], swings_low[1], bearish=False)
                    confirmations["context"]["rsi"] = ctx
                    if passed:
                        confirmations["rsi"] = True
                        confirmations["count"] += 1
                if not self.disable_cvd:
                    passed, ctx = self.check_cvd_divergence(swings_low[0], swings_low[1], bearish=False)
                    confirmations["context"]["cvd"] = ctx
                    if passed:
                        confirmations["cvd"] = True
                        confirmations["count"] += 1
                passed, ctx = self.check_volume_divergence(swings_low[0], swings_low[1], bearish=False)
                confirmations["context"]["volume"] = ctx
                if passed:
                    confirmations["volume"] = True
                    confirmations["count"] += 1
                passed, ctx = self.check_macd_hist_divergence(swings_low[0], swings_low[1], bearish=False)
                confirmations["context"]["macd"] = ctx
                if passed:
                    confirmations["macd"] = True
                    confirmations["count"] += 1

        confirmations["insufficient_swings"] = insufficient_swings
        if insufficient_swings:
            confirmations["context"].setdefault("reason", "insufficient_swings")

        if not self.disable_obi and current_obi_z is not None:
            if bearish and current_obi_z < -self.obi_z_threshold:
                confirmations["obi"] = True
                confirmations["count"] += 1
            elif not bearish and current_obi_z > self.obi_z_threshold:
                confirmations["obi"] = True
                confirmations["count"] += 1
            confirmations["context"]["obi"] = {"obi_z": current_obi_z}
        else:
            confirmations["context"]["obi"] = {"obi_z": current_obi_z}

        is_fast = volatility is not None and volatility >= self.fast_vol_threshold
        if is_fast:
            confirmations["context"]["fast_market"] = {
                "volatility": volatility,
                "threshold": self.fast_vol_threshold,
            }
            flow_confirms = 0
            if confirmations["cvd"]:
                flow_confirms += 1
            if confirmations["obi"]:
                flow_confirms += 1
            if confirmations["volume"]:
                flow_confirms += 1
            if flow_confirms == 0:
                if confirmations["rsi"]:
                    confirmations["rsi"] = False
                if confirmations["macd"]:
                    confirmations["macd"] = False
            confirmations["count"] = (
                int(bool(confirmations["rsi"]))
                + int(bool(confirmations["cvd"]))
                + int(bool(confirmations["volume"]))
                + int(bool(confirmations["macd"]))
                + int(bool(confirmations["obi"]))
            )

        oi_pass = self.check_oi_filter(oi_slope, bearish)
        confirmations["oi_filter"] = oi_pass

        if not oi_pass:
            confirmations["count"] = 0

        return confirmations

    def _price_move_pct(self, price1: float, price2: float) -> float:
        denom = max(abs(price1), 1e-9)
        return abs(price2 - price1) / denom
