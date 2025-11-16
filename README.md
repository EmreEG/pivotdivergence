# Pivot Divergence Trading System

Deterministic, event‑driven trading system for Binance USDⓈ‑M futures. Computes objective pivot levels from volume‑at‑price structure, confirms entries with flow/momentum divergences at the level, and executes with strict risk and kill‑switch controls. Default symbol: `BTCUSDT`.

—

## What Happens End‑to‑End

- Ingest trades, order book depth, mark price, and user orders via WebSocket; poll open interest via REST. (`ingest/websocket_client.py:1`, `ingest/rest_poller.py:1`)
- Maintain a snapshot‑correct local book with tick alignment and sequence checks; compute order‑book imbalance (OBI). (`ingest/book_manager.py:1`)
- Build rolling and session volume profiles; extract POC/VAH/VAL, HVN/LVN, shelves; tag profile shape (D/P/b). (`analytics/profile.py:1`, `analytics/extrema.py:1`, `analytics/profile_shape.py:1`)
- Track anchored VWAPs (AVWAP) for swings, funding changes, and liquidity absorption events; persist snapshots. (`analytics/avwap.py:1`, `main.py:1`)
- Maintain footprint bars (per‑price bid/ask volume) and validate CVD vs bar volume. (`analytics/footprint.py:1`, `main.py:400`)
- Compute RSI, MACD, OBV each minute; detect ZigZag swings and store indicator values at swing extremes. (`analytics/indicators.py:1`, `analytics/swings.py:1`)
- Generate candidate levels from LVN/HVN/VA edges/POC across windows and anchors; score with confluence and profile shape bias; deduplicate; publish top levels per side. (`strategy/level_selector.py:1`, `main.py:540`)
- Create signals with a price zone around each selected level; promote through states: Inactive → Monitored → Armed → Filled/Cancelled. (`strategy/signal_manager.py:1`, `main.py:639`)
- Confirm at‑level entries using divergences across RSI, CVD, OBI with an OI slope filter; require configured confirmation count within timeout. (`strategy/divergence.py:1`, `config/config.yaml: divergency`)
- Place post‑only limit orders for fades; place stop‑market orders for LVN breakthrough setups gated by OBI z‑score and OI slope. (`strategy/execution.py:1`, `main.py:993`)
- Size positions by equity percent with caps and funding bias; set stop via ATR/zone; set targets by RR; trail via AVWAP ±σ. (`risk/position_sizer.py:1`)
- Enforce kill‑switch on desyncs, stream loss, latency drift, backpressure, drawdown, and position mismatches; block trading while microstructure is contaminated. (`main.py:200`, `api/alerts.py:1`)
- Persist book snapshots, mark price, open interest, indicator state, OBI regime snapshots, and signals to TimescaleDB while datapython owns historical trades in `core.binance_trades`; expose Prometheus metrics; serve FastAPI and a Web UI. (`ingest/persister.py:1`, `api/metrics.py:1`, `api/fastapi_server.py:1`, `ui/`)

—

## Runtime Architecture

- Orchestrator: `main.py:1` defines `TradingSystem`. Tasks: WebSocket streams, REST poller, persister, 60‑s bar processor, 10‑s level/signal processor.
- Streams: `ingest/websocket_client.py` connects directly to Binance USDⓈ‑M futures WebSocket streams (aggTrade, depth, mark price, and user data); rotates connections every 23h; pings at 10s; recalibrates exchange clock via REST and checks sustained latency drift; raises kill‑switch after reconnect bursts or user‑stream loss.
- Order book: `ingest/book_manager.py` applies Binance `U/u/pu` sequencing; overwrites covered ranges; validates invariants; compares top‑N with REST snapshots every 30s; flags desync/stale conditions; computes OBI and depth stats.
- Persistence: `ingest/persister.py` batches inserts to TimescaleDB, flushes on interval/size; drops oldest 20% on buffer overflow and flags degradation; buffers: book, mark, open-interest, indicator state, OBI regime snapshots, and signals, while trades flow from datapython’s `core.binance_trades` via `DATAPYTHON_DATABASE_DSN`.
- Metrics: `api/metrics.py` exposes counters, gauges, histograms for stream health, indicators, orders, slippage, latency, queue depths, and kill-switch reasons.
  Prometheus listener auto-scans `monitoring.prometheus_port` + `prometheus_port_scan` and records the bound port in `logs/prometheus_port` to avoid clashes with other long-running modules.
- API/UI: `api/fastapi_server.py` serves JSON and a WebSocket; `ui/` renders levels, signals, indicators, AVWAP bands, and a footprint table.

—

## Data Ingestion and Health Controls

- Trades: aggTrade stream; maker flag `m` maps SELL taker → negative, BUY taker → positive. Gaps/replays mark CVD unstable. (`analytics/orderflow.py:1`)
- Depth: partial or diff snapshots; treat partials as read‑only unless diff fields present; diff updates require sequence continuity; periodic REST snapshot compare; invariant violations trigger resync and contamination. (`ingest/book_manager.py:1`, `main.py:160`)
- Ticker: mark price and funding; on funding change, anchor AVWAP and start an event profile. (`main.py:200`)
- OI: polled at 300s; three consecutive failures trip kill‑switch. (`ingest/rest_poller.py:1`)
- Latency: drift vs exchange clock; sustained violations emit alerts; stale stream triggers reconnect with backoff/jitter; per‑stream lag recorded. (`ingest/websocket_client.py:120`, `api/metrics.py:1`)
- Microstructure contamination: set hold window (default 60s) on CVD gaps, stale book, stream gaps, backpressure, snapshot deviations; divergence checks are suppressed while contaminated. (`main.py:130`)

—

## Trade Persistence Boundary

- Live trade ingestion and persistence run inside `/home/emre/PythonCode/datapython`, which writes to `core.binance_trades` via `python -m datapython.cli` entrypoints orchestrated by `./start.sh`.
- `pivotdivergence` does not define or touch `public.trades`; `DataPersister.insert_trade` stays inert so all historical reads come through `DATAPYTHON_DATABASE_DSN`.
- Schema and migration details for trades live in `../datapython/README.md`; update that project when trade storage changes are required.

—

## Volume Profile and Features

- Profiles: rolling windows `[7200, 28800, 86400]` and UTC sessions plus event‑anchored profiles; per‑window tick‑aligned bins with width `max(tick, price·bin_width_pct)`; flash‑crash freeze on >3% jump in 10s. (`config/config.yaml: profile`, `analytics/profile.py:1`)
- Features: POC/VAH/VAL via 70% rule starting at POC; HVN/LVN by prominence; “shelf” via small Δvolume run bounded by spikes; profile shape D/P/b and numeric bias. (`analytics/extrema.py:1`, `analytics/profile_shape.py:1`)
- Naked POC: register every computed POC; mark touched when best bid/ask straddles price; expire after 72h; use as scoring confluence. (`analytics/naked_poc.py:1`, `main.py:623`)

—

## Order‑Flow, Indicators, and Swings

- CVD: 64‑bit integer accumulation scaled by `volume_precision`; gaps/time‑travel mark unstable and block persistence snapshots until resynced; periodic snapshots to `logs/cvd_snapshot.json`. (`analytics/orderflow.py:1`, `main.py:360`)
- OBI: z‑score over a sliding window of top‑N depth; suppressed when depth < threshold or contaminated. (`analytics/orderflow.py:70`, `main.py:720`)
- OI: slope over 300s; used as sign filter in divergence and breakthrough logic. (`analytics/orderflow.py:120`)
- Indicators: RSI(14), MACD(12,26,9), OBV over 1‑minute bars. (`analytics/indicators.py:1`)
- Swings: ZigZag with 0.4% threshold; stores RSI/CVD/OBI at extremes; anchors AVWAP on new swings. (`analytics/swings.py:1`, `config/config.yaml: swing`)
- Footprint: per‑price bid/ask totals per 60‑s bar; detects absorption levels; anchors AVWAP on detected liquidity absorption. (`analytics/footprint.py:1`, `main.py:430`)

—

## Level Selection and Scoring

- Candidates: LVN/HVN, VA edges, POC across all profiles and event anchors.
- Scoring: weighted sum of LVN prominence, multi‑source confluence (AVWAP/bands, swings, round numbers), naked POC flag, distance normalization, untouched age, and profile shape bias. (`strategy/level_selector.py:1`, `config/config.yaml: scoring`)
- Deduplication: drop levels within `dedup_spacing_pct` of each other; enforce minimum distance from current price. Keep top‑K per side. (`strategy/level_selector.py:80`)
- Publication: `main.py` exposes `current_levels` for API/UI.

—

## Divergence and Signal Lifecycle

- Divergence checks (bearish at highs, bullish at lows):
  - RSI: price makes HH/LL while RSI makes LH/HL by ≥ `rsi_delta` with minimum indicator move and price move. (`strategy/divergence.py:8`)
  - CVD: price HH/LL while CVD makes LH/HL by `cvd_delta_pct` of swing range. (`strategy/divergence.py:28`)
  - OBI: current OBI z‑score crosses ±`obi_z_threshold` unless suppressed. (`strategy/divergence.py:48`)
  - OI slope filter: require non‑conflicting slope sign; failing this zeroes confirmation count. (`strategy/divergence.py:56`)
- Confirmation: require `confirmation_count` signals within `timeout_s` while price inside the level zone. (`config/config.yaml: divergence`)
- States: Inactive → Monitored when distance ≤ `monitor_distance_pct`; Monitored → Armed on confirmation; Armed → Filled by user stream (or auto in paper mode) or → Cancelled on timeout/stop pierce. (`strategy/signal_manager.py:1`, `main.py:720`)

—

## Execution and Risk

- Modes: paper mode when `exchange.testnet=true` or execution init fails; uses live Binance market data for analytics while routing orders via the official USDⓈ‑M REST API when live mode is enabled. (`strategy/execution.py:1`, `config/config.yaml: exchange`)
- Orders:
  - Fades: post‑only limit; on post‑only reject, retry once offset by `retry_tick_offset` after `retry_delay_s`. (`strategy/execution.py`)
  - Breakthroughs: stop‑market at LVN price with half size and higher RR target. (`main.py:1050`)
- Sizing: `risk/position_sizer.py` computes USD notional as equity×`position_size_pct` capped by `max_notional_usd` and `notional_cap_pct`; reduces on adverse funding; caps by ATR‑based risk; converts to contracts using tick and contract size; quantizes by amount step. (`strategy/execution.py:924`, `risk/position_sizer.py:1`)
- Stops/Targets: stop = max(ATR×mult, zone_width×mult) away; target by RR; trailing stop from AVWAP ±σ crossing beyond entry. (`risk/position_sizer.py:22`)
- Safeguards: block trading near funding window (`no_position_before_funding_min`); enforce max concurrent per side; drawdown kill‑switch; position reconciliation vs exchange; unsafe mode file `logs/UNSAFE_MODE` blocks new orders. (`main.py:973`)

—

## Breakthrough Variant (LVN Gap)

- Conditions: LVN ahead of price by `min_gap_pct`–1%; OBI z beyond ±threshold; optional positive OI slope; per‑4h cap on simultaneous attempts. (`config/config.yaml: breakthrough`, `main.py:993`)
- Action: create signal with zone; place stop‑market at LVN; half position size; RR=2.0 target.

—

## Persistence and Schema

- Database: PostgreSQL 18 + TimescaleDB 2.23+. `config/schema.sql` only provisions `book_snapshots`, `mark_price`, `open_interest`, `indicator_state`, `obi_regime_stats`, and `signals`; trade storage lives solely in datapython’s `core.binance_trades`. (`config/schema.sql:1`)
- Batching: `ingest/persister.py` uses `asyncpg` pooled connections; inserts with `executemany`; flushes every 5s or `batch_size` per buffer; drops oldest data on oversize with degradation metric.

—

## API and Web UI

- FastAPI (`api/fastapi_server.py:1`):
  - `GET /` service status.
  - `GET /health` liveness.
  - `GET /api/levels` selected levels.
  - `GET /api/signals` active signals.
  - `GET /api/profile/{window}` profile histogram + levels.
  - `GET /api/indicators` RSI/MACD/OBV + CVD/OBI/OI.
  - `GET /api/swings` swing state.
  - `GET /api/footprint/current` current bar footprint.
  - `GET /api/footprint/absorption/{price}` absorption details for a level.
  - `POST /api/kill_switch` manual kill‑switch.
  - `WS /ws` periodic broadcast of price, signals, indicators.
- UI (`ui/`): Vite + React; WebSocket to `/ws`; REST polling of `/api/*`; renders chart with price lines for levels, signals, AVWAP bands, and a sidebar with indicators and a footprint table.

—

## Metrics (Prometheus)

- Path: `:9090/metrics`.
- Key series: `trades_processed_total`, `orderbook_updates_total`, `current_price`, `current_funding_rate`, `active_signals_total{state=...}`, `cvd_current`, `obi_z_current`, `rsi_current`, `websocket_latency_seconds`, `processing_latency_seconds`, `order_send_latency_seconds`, `slippage_vs_mid_bps`, `orders_placed_total{type=...}`, `orders_filled_total`, `orders_cancelled_total`, `websocket_reconnects_total`, `kill_switch_triggers_total{reason=...}`, `orderbook_resyncs_total`, `microstructure_clean{reason=...}`, `degradation_events_total{reason=...}`, `dropped_events_total{reason=...}`, `stream_lag_seconds{stream=...}`, `queue_depth{buffer=...}`. (`api/metrics.py:1`)

—

## Safety and Kill‑Switch Matrix

- Triggers: order book invariant violation, snapshot deviation, sustained latency drift, repeated reconnects, user‑stream loss, OI poll failure, CVD gap/time‑travel, stale book, buffer backpressure, max drawdown, position mismatch, manual endpoint. (`main.py:120`, `ingest/websocket_client.py:180`, `ingest/rest_poller.py:50`)
- Effects: mark microstructure unhealthy; resync order book; cancel all open orders; block new orders until healthy; send alert webhook; persist state. (`main.py:108`, `api/alerts.py:1`)

—

## Backtesting and Replay

- Backtest: `backtest/engine.py` loads historical tables, computes profiles/extrema/swings/indicators, simulates reversals and trade accounting, and reports Sharpe, win‑rate, drawdown, PF, MAE/MFE, fees. (`backtest/engine.py:1`)
- Optimizer: `backtest/optimizer.py` walk‑forward grid search with optional ablations; summary across periods. (`backtest/optimizer.py:1`)
- Replay: `backtest/replay.py` replays DB events into live handlers for deterministic debugging and tests. (`tests/test_replay.py:1`)

—

## Configuration

- File: `config/config.yaml`. Environment variables via `.env` resolved in `config/__init__.py`.
- Critical keys (defaults shown):
  - `exchange`: `{ name: binance, testnet: true, symbol: BTCUSDT, tick_size: 0.1, contract_multiplier: 1 }`
  - `websocket`: backoff, reconnect limits, latency thresholds, rotation, stale timeouts.
  - `orderbook`: `max_depth: 500`, `resync_interval_s: 30`, `stale_tolerance_s: 3`, `compare_depth: 20`, `deviation_threshold_pct: 0.0005`.
  - `profile`: windows, `bin_width_pct: 0.0002`, `smooth_window: 5`, flash‑crash guards.
  - `swing`: `zigzag_pct: 0.004`, `history_size: 10`.
  - `scoring`: weights, `confluence_radius_pct: 0.0005`, `dedup_spacing_pct: 0.0008`, `top_k_per_side: 6`, `min_order_distance_pct: 0.0025`.
  - `divergence`: `rsi_delta: 3.0`, `cvd_delta_pct: 0.10`, `obi_z_threshold: 2.0`, `confirmation_count: 2`, `timeout_s: 120`, min moves.
  - `levels`: `monitor_distance_pct: 0.0015`, `zone_half_width_pct: 0.0015`.
  - `execution`: `post_only: true`, retry delay and tick offset.
  - `risk`: `position_size_pct: 0.02`, `max_notional_usd: 5000`, drawdown, trail `trail_avwap_sigma: 1.0`, funding bias, notional caps.
  - `risk.max_hold_minutes`: auto-close timer for paper/live positions; forces exit (hold-timeout reason) so Grafana metrics and audits reflect lifecycle even if targets/stops never hit.
  - `breakthrough`: LVN gap thresholds and rate‑limits.
  - `monitoring`: Prometheus port, webhook URL, latency budget, unsafe flag path.

—

## Install and Run

- Requirements: Python 3.10+, TimescaleDB 15+, TA‑Lib installed, Node 18+ for UI.
- Setup: `./setup.sh`.
- Database: `docker compose up -d timescaledb` or apply `config/schema.sql` manually.
- Start full stack (API + metrics + DB containers): `./start.sh`.
- Start orchestrator only: `./venv/bin/python main.py`.
- Start API only: `./venv/bin/python api/fastapi_server.py`.
- UI: `cd ui && npm install && npm run dev`.

—

## Tests

- Unit: `tests/test_core.py` covers profile math, extrema, swings, CVD/OBI, naked POC, order book invariants, divergence, risk, and tick alignment guarantees.
- Integration: `tests/test_integration.py` runs the full system for 30s with live data.
- Mock: `tests/test_mock.py` generates synthetic data and exercises modules.
- Replay: `tests/test_replay.py` replays recorded events through live handlers.
- Microstructure validation: `tests/test_microstructure_validation.py` checks OBI directional power and VA 70% symmetry on a controlled profile.

—

## File Map

- `main.py` — orchestrator, tasks, event handlers, signal/level loop, execution, kill‑switch.
- `ingest/` — WebSocket client, order book manager, REST poller, DB persister.
- `analytics/` — profile/AVWAP/extrema/footprint/indicators/orderflow/swings/shape.
- `strategy/` — level selection, divergences, signal manager, execution.
- `risk/` — position sizing, stops, trailing, drawdown.
- `api/` — FastAPI app, metrics, alerts.
- `backtest/` — engine, optimizer, replay.
- `config/` — YAML config, DB schema, Grafana/Prometheus configs.
- `ui/` — React client.

—

## Operational Notes

- Default mode uses live market data with paper execution. Set `exchange.testnet=false` and valid credentials to enable live order routing through Binance USDⓈ‑M REST endpoints; WebSocket and REST integrations use only official Binance APIs.
- Unsafe mode: create `logs/UNSAFE_MODE` to block new orders without stopping analytics.
- All published level prices and AVWAP values are tick‑aligned; misalignment raises assertions at emit time.
