Scope: design and implementation plan for a realtime system that pre-computes high‑probability pivot levels from volume profile, places resting orders ahead of price, and confirms entries with objective divergence at the level. Target market: BTCUSDT on Binance USDⓈ‑M futures. Extendable to other symbols.

Foundations and references
• Volume Profile components: Point of Control (POC), Value Area (VAH/VAL), High‑Volume Nodes (HVN), Low‑Volume Nodes (LVN). Value Area is typically built to contain ~70% of traded activity around the POC; HVNs mark acceptance, LVNs mark rejection/speed. ([TradingView][1])
• “Naked/virgin POC” (POC not yet revisited) is a durable attractor/magnet until tested. ([Vtrender][2])
• Anchored VWAP gives event‑anchored support/resistance and objective “where buyers/sellers are in control.” Use as dynamic confluence with profile. ([ChartSchool][3])
• Divergence signals: classical price–indicator divergence (RSI/MACD/OBV/A/D). Use as confirmation at pre‑computed levels. ([Investopedia][4])
• Order‑flow divergences: Cumulative Volume Delta (CVD) is bid‑ask volume imbalance accumulated over time; footprint/Numbers Bars expose per‑price bid/ask and absorption. ([NT][5])
• Order book imbalance (OBI) is predictive at short horizons. Compute from L2 book around best quotes. ([Wiley Online Library][6])

Exchange data and transport
• Trades: `<symbol>@aggTrade` (100 ms). Field `m` indicates buyer is maker; infer aggressor side for delta/CVD. ([Binance Developer Center][7])
• Order book: `<symbol>@depth<levels>` partial depth (5/10/20; 100–500–250 ms). Maintain a consistent local book per Binance spec. ([Binance Developer Center][8])
• Mark price + funding: `<symbol>@markPrice[@1s]`. Funding every 8h on Binance; use as regime filter. ([Binance Developer Center][9])
• Open interest: REST `GET /fapi/v1/openInterest`. Sample at 1–5 min for OI divergence. ([Binance Developer Center][10])

System architecture

1. Ingestion
   • WebSocket clients for aggTrade, depth, markPrice. REST poll for openInterest.
   • Normalize to microsecond UTC timestamps.
   • Persist to a time‑series store (e.g., columnar TSDB) partitioned by symbol and day.
   • Build a resilient local order book per Binance “How to manage a local order book correctly.” ([Binance Developer Center][11])

2. Core analytics services
   a) Volume‑at‑Price Engine
   • Maintain rolling histograms of traded volume by price for multiple windows and session types:
   – Intraday rolling windows: 2h, 8h, 24h.
   – Session windows: 00:00–24:00 UTC; funding sessions: [00:00–08:00), [08:00–16:00), [16:00–24:00).
   – Event‑anchored windows: anchor to last swing high/low, major liquidation spike, or funding timestamp.
   • Bin width = max(symbol tickSize, dynamic bin ≈ 0.02% of price).
   • Update per trade with lock‑free counters.

   b) Profile Feature Extraction
   • Compute POC, VAH, VAL per profile window using 70% rule starting at POC. ([help.cqg.com][12])
   • Smooth histogram (e.g., 5–9 bin moving average). Detect local maxima/minima: HVNs/LVNs with prominence ≥ θ.
   • Detect LVN “shelves” (steep negative Δvolume across adjacent bins).
   • Track “naked POC” levels until first touch. ([Vtrender][2])
   • Label profile shape (D/P/b) to infer mean‑reversion or continuation bias. ([NT][13])
   • Maintain multi‑TF level registry with versioning and touch history.

   c) Order‑Flow and Momentum Signals
   • Compute CVD in realtime from aggTrade aggressor side; maintain bar‑based and swing‑based CVD highs/lows. ([NT][5])
   • Compute OBI from L2: `(Σ bidQty_±k – Σ askQty_±k) / (Σ bid+ask)`, k = top 5–10 price levels. ([Wiley Online Library][6])
   • Classical indicators on mark price: RSI(14), MACD(12,26,9), OBV, A/D. ([Investopedia][14])
   • Regime features: funding rate sign/magnitude; OI slope; intraday realized volatility.

3. Level selection and scoring
   • Candidate set = union over windows of: {LVNs, VAH, VAL, POC, naked POC, steep HVN→LVN cliffs, Anchored VWAP bands}. ([ChartSchool][3])
   • Score each level L with:
   S(L) = w₁·LVN_prominence + w₂·multi‑profile_confluence + w₃·nakedPOC_flag + w₄·shape_bias + w₅·distance_normalized + w₆·time_since_last_touch + w₇·AVWAP_confluence.
   • De‑duplicate within δ = 0.05–0.10% of price; keep top‑K per side by S(L).

4. “Early” pivot planning
   • For each selected L, create a “zone” [L − z·ATR, L + z·ATR], z ≈ 0.10–0.20; thinner if LVN shelf is narrow.
   • Predefine hypothesis:
   – Rejection‑pivot: LVN edge or VA boundary. Expect first‑touch rejection. ([International Trading Institute (ITI)][15])
   – Magnet‑pivot: naked POC or HVN. Expect mean‑reversion into the node. ([Vtrender][2])
   – Break‑through: LVN gap with strong OBI/CVD thrust. ([Alchemy Markets][16])

5. Divergence confirmation at the level
   • When price enters zone for L, evaluate within a short horizon (e.g., last N bars on 1‑ or 3‑min):
   – RSI divergence: price HH vs RSI lower high (bearish) or price LL vs RSI higher low (bullish). ([Investopedia][14])
   – CVD divergence: price HH vs CVD lower high (seller absorption/exhaustion) or LL vs CVD higher low. ([NT][5])
   – OBI divergence: price HH with negative OBI or LL with positive OBI. ([Wiley Online Library][6])
   – OI divergence filter: new extremes in price without rising OI → likely squeeze/cover rather than new positioning. ([Binance Developer Center][10])
   – Optional footprint confirmation: large per‑price prints with limited progress = absorption. ([sierrachart.com][17])
   • Require ≥2 confirmations true within T seconds of zone touch to “arm” the order.

6. Signal and execution policy
   • Entry type
   – Rejection‑pivot short: place passive sell at LVN/VAH edge; arm when divergence confirms; cancel if no confirm within T.
   – Rejection‑pivot long: passive buy at LVN/VAL edge; same arming logic.
   – Magnet‑pivot: if approaching a naked POC/HVN from outside, fade first touch into node; target the node center. ([Vtrender][2])
   – Break‑through: if LVN gap and OBI>τ with rising CVD/OI, stop‑entry through LVN, first target next HVN. ([Alchemy Markets][16])
   • Risk
   – Initial stop = beyond zone by max(0.5·ATR, ½ shelf width).
   – Reduce size when funding is extreme against trade direction. ([Binance][18])
   – First target = nearest HVN/POC; trailer = anchored VWAP from the turn bar. ([ChartSchool][3])
   • Order types: post‑only limit for fades; stop‑market for break‑throughs.

Algorithms

A) Volume‑at‑Price histogram (per window W)
• For each trade (p,q): bin = ⌊(p − p₀)/Δp⌋; H[bin] += q.
• POC = argmax H.
• Build Value Area by expanding from POC until cumVol ≥ 70% of ΣH, per CQG/MPVA method. ([help.cqg.com][12])
• Smooth H with centered moving average; detect HVN/LVN via prominence thresholding.

B) LVN/HVN detection
• Prominence of a local extremum e = |H[e] − max(H[left_base], H[right_base])|.
• LVN if local minimum with prominence ≥ θₗ and neighbor gradient steepness ≥ γ.
• HVN if local maximum with prominence ≥ θₕ.
• Shelves: consecutive bins with |ΔH| small except at boundaries where |ΔH| spikes.

C) Naked POC tracker
• Maintain daily/session POC list with “touched” flag. “Touched” when best bid/ask crosses POC ± ε. ([Vtrender][2])

D) Anchored VWAP bands
• For anchor time tₐ, compute AVWAP(t) = Σ(pᵢ·vᵢ)/Σvᵢ over [tₐ, t]; optional bands at multiples of rolling σ of (p − AVWAP). ([ChartSchool][3])

E) CVD
• For each aggTrade: aggressor = buy if `m==false`, sell if `m==true`. CVD += (+q or −q). Maintain swing highs/lows of CVD in sync with price swings. ([Binance Developer Center][7])

F) Order Book Imbalance
• From local book snapshot, within k levels each side:
OBI = (Σ bidQty − Σ askQty) / (Σ bidQty + Σ askQty). Use rolling z‑score. ([Wiley Online Library][6])

G) Divergence checks (generic template)
• Let last two swing highs in price be P₁<P₂ in time with prices H₁,H₂. For indicator X with highs X₁,X₂:
– Bearish divergence if H₂>H₁ and X₂≤X₁−δ.
– Bullish divergence if L₂<L₁ and X₂≥X₁+δ for swing lows.
• Apply for RSI, CVD, and OBI; enforce synchrony within ±M bars. ([Investopedia][14])

Scoring and selection defaults
• Weights: w₁=3 (LVN prominence), w₂=2 (confluence count across windows/TFs), w₃=2 (naked POC), w₄=1 (shape bias), w₅=1 (distance), w₆=1 (untouched age), w₇=1 (AVWAP confluence).
• Confluence adds 1 per matching feature within 0.05%: VA edge, AVWAP, prior swing, round level.
• Keep top‑6 levels per side.

Execution flow

1. Pre‑market build (continuous for crypto)
   • Recompute composite, daily, and funding‑session profiles each minute.
   • Refresh candidate levels and zones.
   • Place passive orders for the top levels at distance ≥ 0.25% from current mid.

2. At‑level logic (state machine)
   State = {Inactive → Monitored → Armed → Filled | Cancelled}.
   • Inactive → Monitored when distance ≤ 0.15% of L.
   • Monitored → Armed when ≥2 divergences confirm within T=120 s.
   • Armed → Filled when order executed; else cancel if no confirm or if price pierces zone without confirm.
   • On fill: set stop, target HVN/POC, start AVWAP trailer.

3. Break‑through variant
   • If LVN gap ahead and OBI z‑score ≥ +2 with CVD rising and OI increasing over last 5 min, queue stop‑entry through LVN with pullback limit add. ([Alchemy Markets][16])

Backtest protocol

• Data: aggTrade, depth snapshots + diffs, markPrice, funding, open interest. 1‑second bars for indicators; tick‑level for profiles and CVD.
• Split: walk‑forward by month; train weights on months 1–3, test on month 4; roll.
• Metrics:
– Pivot prediction precision/recall (touch + reversal within X basis points and Y bars).
– Entry quality: MAE/MFE relative to zone width.
– Divergence confirmation lift: win‑rate delta with vs without confirmation.
– Slippage vs passive/taker.
• Ablations: remove each feature (LVN, naked POC, AVWAP, CVD, OBI, RSI) and re‑measure.

Performance and reliability

• Latency budget: <50 ms internal; snapshot‑safe order book per Binance spec to avoid drift. ([Binance Developer Center][11])
• Persistence: append‑only partitions per day; compact hourly.
• Determinism: seedable swing detection; store feature snapshots per signal for audit.
• Risk limits: max concurrent orders per side, per‑level notional cap, kill‑switch on WebSocket desync or OI/funding fetch failure.

Interfaces

• Level book API: returns ranked levels with zones, features, scores.
• Signal feed: emits {symbol, side, levelId, zone, reasons[], timestamps}.
• Execution API: places/cancels orders with post‑only for fades; stop‑market for breaks.
• Visualization:
– Volume profile overlay with highlighted LVNs/HVNs, POC/VAH/VAL.
– AVWAP bands and current distance.
– CVD/RSI/OBI panes with marked divergences.
– Footprint/Numbers Bars view at the level for audit. ([sierrachart.com][17])

Edge cases

• Fast markets: widen zones with volatility; downgrade reliance on RSI, upgrade CVD/OBI.
• Thin L2: if depth levels < threshold or churn too high, suppress OBI divergences.
• Multi‑touch degradation: decrease score after each touch; reset after 72 hours or after a profile regime change (P↔b↔D). ([NT][13])

Pseudocode fragments

Build profile and extract levels

```
for W in WINDOWS:
    H = zeros(num_bins(W))
    for trade in trades_in_window(W):
        idx = bin_index(trade.price, W.bin_size)
        H[idx] += trade.qty
    Hs = smooth(H, k=5)
    POC = argmax(Hs)
    (VAH, VAL) = build_value_area(Hs, start=POC, target=0.70)   # CQG method
    HVNs = local_maxima(Hs, prominence>=theta_h)
    LVNs = local_minima(Hs, prominence>=theta_l, slope>=gamma)
    shelves = detect_shelves(Hs)
    levels += tag_features({POC, VAH, VAL, HVNs, LVNs, shelves})
levels = dedup_and_score(levels)
```

CVD, OBI, divergences

```
on aggTrade(t):
    side = +1 if t.m == false else -1    # buyer maker? false => buyer taker
    CVD += side * t.qty
    update_swings(price, RSI, CVD, OBI)

divergence_at_level(L):
    cond = 0
    if price_HH(L.window) and RSI_lower_high(): cond += 1
    if price_HH(L.window) and CVD_lower_high(): cond += 1
    if price_HH(L.window) and OBI_z < -z_th:   cond += 1
    if price_HH(L.window) and OI_slope <= 0:   cond += 1
    return cond >= 2
```

Signal logic

```
for L in top_levels:
    if distance_to(L) <= monitor_thresh:
        if divergence_at_level(L):
            arm_order(L)
        else:
            keep_passive_but_unarmed(L)  # cancel if pierced without confirm
```

Default parameters

• WINDOWS: {2h, 8h, 24h rolling; daily; 3× funding sessions; last major swing anchor}.
• θₗ (LVN prominence)=1.5× median prominence; γ (slope)=1.0× median ΔH.
• monitor_thresh=0.15% of price; zone half‑width z=0.15% or ½ LVN shelf width.
• Divergence window M=5 bars on 1–3 min bars; confirmation T=120 s.
• OBI z‑score threshold |z|≥2.
• OI divergence: 5‑min ΔOI ≤ 0 for price HH (bearish) or ≥0 for price LL (bullish). ([Binance Developer Center][10])

Why this detects pivots “early”
• The system pre‑computes objective reaction zones from acceptance/rejection structure (HVN/LVN, VA edges, naked POCs) and event‑anchored flows (AVWAP). These are static before price arrives, allowing resting orders. ([International Trading Institute (ITI)][15])
• It requires local confirmation from divergences that statistically precede or accompany reversals: momentum (RSI), flow (CVD/OBI), and positioning (OI). ([Investopedia][14])

Tooling references for developers
• Volume‑profile basics and VA/POC math. ([TradingView][1])
• HVN/LVN roles, acceptance vs rejection. ([TradingView][1])
• Anchored VWAP definition and use. ([ChartSchool][3])
• CVD and footprint/Numbers Bars. ([NT][5])
• Binance streams and book management. ([Binance Developer Center][7])
• Divergence glossaries. ([Investopedia][4])

Deliverables checklist for coders

1. Ingest and local‑book services with replayable logs. ([Binance Developer Center][11])
2. Profile engine with windows, smoothing, extrema detection, and naked‑POC tracker. ([help.cqg.com][12])
3. AVWAP module with arbitrary anchors. ([ChartSchool][3])
4. Order‑flow module: CVD, OBI, OI, funding. ([NT][5])
5. Level scoring and selection API.
6. Divergence engine with swing synchronization across price/indicators. ([Investopedia][14])
7. Signal and execution orchestrator with state machine and risk limits.
8. Backtester with walk‑forward and ablations.
9. UI overlays: profile, levels, AVWAP, divergences, footprint at level. ([sierrachart.com][17])

This plan satisfies early level discovery via volume profile structure and delays commitment until divergence confirms at the pre‑defined zone.

[1]: https://www.tradingview.com/support/solutions/43000502040-volume-profile-indicators-basic-concepts/?utm_source=chatgpt.com "Volume profile indicators: basic concepts"
[2]: https://vtrender.com/posts/the-az-of-market-profile-order-flow-a-traders-glossary?utm_source=chatgpt.com "The A–Z of Market Profile & Order Flow: A Trader's Glossary"
[3]: https://chartschool.stockcharts.com/table-of-contents/technical-indicators-and-overlays/technical-overlays/anchored-vwap?utm_source=chatgpt.com "Anchored VWAP - ChartSchool - StockCharts.com"
[4]: https://www.investopedia.com/terms/d/divergence.asp?utm_source=chatgpt.com "What Is Divergence in Technical Analysis and Trading?"
[5]: https://ninjatrader.com/support/helpguides/nt8/order_flow_cumulative_delta.htm?utm_source=chatgpt.com "Order Flow Cumulative Delta"
[6]: https://onlinelibrary.wiley.com/doi/10.1155/2023/3996948?utm_source=chatgpt.com "Impact of High‐Frequency Trading with an Order Book ..."
[7]: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams "Aggregate Trade Streams | Binance Open Platform"
[8]: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams?utm_source=chatgpt.com "Partial Book Depth Streams | Binance Open Platform"
[9]: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Mark-Price-Stream?utm_source=chatgpt.com "Mark Price Stream | Binance Open Platform"
[10]: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Open-Interest?utm_source=chatgpt.com "Open Interest | Binance Open Platform"
[11]: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly "How To Manage A Local Order Book Correctly | Binance Open Platform"
[12]: https://help.cqg.com/cqgic/25/Documents/marketprofilevalueareasmpva.htm?utm_source=chatgpt.com "Market Profile Value Areas (MPVA)"
[13]: https://ninjatrader.com/futures/blogs/trade-futures-understanding-the-4-common-volume-profile-shapes/?utm_source=chatgpt.com "Understanding the 4 Common Volume Profile Shapes in ..."
[14]: https://www.investopedia.com/terms/r/rsi.asp?utm_source=chatgpt.com "Relative Strength Index (RSI): What It Is, How It Works, and Formula"
[15]: https://internationaltradinginstitute.com/blog/reading-the-volume-profile-from-acceptance-to-rejection/?utm_source=chatgpt.com "Trading with Volume Profile | Market Acceptance & Rejection"
[16]: https://alchemymarkets.com/education/indicators/volume-profile/?utm_source=chatgpt.com "Volume Profile Effective Trading Guide"
[17]: https://www.sierrachart.com/index.php?page=doc%2FNumbersBars.php&utm_source=chatgpt.com "Numbers Bars"
[18]: https://www.binance.com/en/blog/futures/421499824684903247?utm_source=chatgpt.com "What Is Futures Funding Rate And Why It Matters"
