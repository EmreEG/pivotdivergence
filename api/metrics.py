import errno
import logging
from pathlib import Path
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from typing import Dict, Optional

from config import config


logger = logging.getLogger(__name__)

_METRICS_SERVER_STARTED = False
_METRICS_PORT: Optional[int] = None


def _get_port_scan_limit() -> int:
    try:
        return int(config.monitoring.get('prometheus_port_scan', 0))
    except Exception:
        return 0


def _get_port_file() -> Optional[Path]:
    path_value = config.monitoring.get('metrics_port_file') if config.monitoring else None
    if not path_value:
        return None
    return Path(path_value)


def _write_port_file(port: int) -> None:
    port_file = _get_port_file()
    if not port_file:
        return
    try:
        port_file.parent.mkdir(parents=True, exist_ok=True)
        port_file.write_text(str(port))
    except Exception as exc:
        logger.warning("Failed to persist metrics port file %s: %s", port_file, exc)


class MetricsCollector:
    def __init__(self):
        self.trade_count = Counter('trades_processed_total', 'Total trades processed')
        self.trade_latency = Histogram('trade_path_latency_seconds', 'Latency between exchange event time and internal processing')
        self.orderbook_updates = Counter('orderbook_updates_total', 'Total orderbook updates')
        
        self.current_price = Gauge('current_price', 'Current market price')
        self.current_funding = Gauge('current_funding_rate', 'Current funding rate')
        
        self.signal_count = Gauge('active_signals_total', 'Total active signals', ['state'])
        
        self.cvd_value = Gauge('cvd_current', 'Current CVD value')
        self.obi_z_value = Gauge('obi_z_current', 'Current OBI z-score')
        self.rsi_value = Gauge('rsi_current', 'Current RSI value')
        self.rsi_normalized = Gauge('rsi_normalized', 'RSI normalized to -100..100')
        self.realized_volatility = Gauge('realized_volatility', 'Intraday realized volatility (std dev of log returns)')
        
        self.latency_ws = Histogram('websocket_latency_seconds', 'WebSocket message latency')
        self.latency_processing = Histogram('processing_latency_seconds', 'Internal processing latency')
        self.order_send_latency = Histogram('order_send_latency_seconds', 'Latency from order send to return/ACK')
        self.signal_path_latency = Histogram(
            'signal_path_latency_seconds',
            'Latency from latest market data event to order acknowledgement',
            ['path']
        )
        
        # Execution quality
        self.slippage_bps = Histogram('slippage_vs_mid_bps', 'Absolute slippage versus mid at fill, in basis points', buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10))
        
        self.book_depth_bids = Gauge('book_depth_bids', 'Order book bid depth')
        self.book_depth_asks = Gauge('book_depth_asks', 'Order book ask depth')
        
        self.profile_count = Gauge('volume_profile_levels', 'Volume profile levels detected', ['type'])
        
        self.orders_placed = Counter('orders_placed_total', 'Total orders placed', ['type'])
        self.orders_filled = Counter('orders_filled_total', 'Total orders filled')
        self.orders_cancelled = Counter('orders_cancelled_total', 'Total orders cancelled')
        self.signals_closed = Counter('signals_closed_total', 'Total closed signals', ['reason'])
        
        self.pnl_realized = Gauge('pnl_realized_total', 'Total realized PnL')
        self.equity = Gauge('account_equity', 'Current account equity')
        
        self.reconnect_count = Counter('websocket_reconnects_total', 'Total WebSocket reconnects')
        self.kill_switch_triggers = Counter('kill_switch_triggers_total', 'Total kill switch triggers', ['reason'])
        self.orderbook_resyncs = Counter('orderbook_resyncs_total', 'Total hard order book resyncs')
        self.microstructure_health = Gauge('microstructure_clean', 'Microstructure health flag', ['reason'])
        self.degradation_events = Counter('degradation_events_total', 'Total data-quality degradation events', ['reason'])
        self.dropped_events = Counter('dropped_events_total', 'Total dropped inbound events', ['reason'])
        
        # Additional health gauges
        self.stream_lag_seconds = Gauge('stream_lag_seconds', 'Seconds since last message seen', ['stream'])
        self.queue_depth = Gauge('queue_depth', 'Internal buffer depth', ['buffer'])
        
    def record_trade(self, latency_seconds: Optional[float] = None):
        self.trade_count.inc()
        if latency_seconds is not None:
            self.trade_latency.observe(latency_seconds)
        
    def record_orderbook_update(self, depth_stats: Optional[Dict] = None):
        self.orderbook_updates.inc()
        if depth_stats:
            self.book_depth_bids.set(depth_stats.get('bid_qty', 0))
            self.book_depth_asks.set(depth_stats.get('ask_qty', 0))
        
    def update_price(self, price: float):
        self.current_price.set(price)
        
    def update_funding(self, funding_rate: float):
        self.current_funding.set(funding_rate)
        
    def update_signals(self, signals_by_state: Dict[str, int]):
        for state, count in signals_by_state.items():
            self.signal_count.labels(state=state).set(count)
            
    def update_cvd(self, cvd: float):
        self.cvd_value.set(cvd)
        
    def update_obi_z(self, obi_z: float):
        self.obi_z_value.set(obi_z)
        
    def update_rsi(self, rsi: float):
        self.rsi_value.set(rsi)
        if rsi is not None:
            normalized = max(-100.0, min(100.0, (float(rsi) - 50.0) * 2.0))
            self.rsi_normalized.set(normalized)
        
    def update_realized_volatility(self, realized_vol: float):
        if realized_vol is not None:
            self.realized_volatility.set(realized_vol)
        
    def record_ws_latency(self, latency_seconds: float):
        self.latency_ws.observe(latency_seconds)
        
    def record_processing_latency(self, latency_seconds: float):
        self.latency_processing.observe(latency_seconds)

    def record_order_send_latency(self, latency_seconds: float):
        self.order_send_latency.observe(latency_seconds)

    def record_signal_path_latency(self, path: str, latency_seconds: float):
        if latency_seconds is not None:
            self.signal_path_latency.labels(path=path).observe(latency_seconds)

    def record_slippage_bps(self, bps: float):
        # Record absolute bps to allow percentile views downstream
        if bps is not None:
            self.slippage_bps.observe(abs(float(bps)))
        
    def update_book_depth(self, bids_depth: int, asks_depth: int):
        self.book_depth_bids.set(bids_depth)
        self.book_depth_asks.set(asks_depth)
        
    def update_profile_levels(self, level_type: str, count: int):
        self.profile_count.labels(type=level_type).set(count)
        
    def record_order_placed(self, order_type: str):
        self.orders_placed.labels(type=order_type).inc()
        
    def record_order_filled(self):
        self.orders_filled.inc()
        
    def record_order_cancelled(self):
        self.orders_cancelled.inc()
    
    def record_signal_closed(self, reason: str):
        self.signals_closed.labels(reason=reason).inc()
        
    def record_pnl(self, pnl: float):
        if pnl is None:
            return
        if pnl >= 0:
            self.pnl_realized.inc(pnl)
        else:
            self.pnl_realized.dec(abs(float(pnl)))
        
    def update_equity(self, equity: float):
        self.equity.set(equity)
        
    def record_reconnect(self):
        self.reconnect_count.inc()
        
    def record_kill_switch(self, reason: str):
        self.kill_switch_triggers.labels(reason=reason).inc()

    def record_orderbook_resync(self):
        self.orderbook_resyncs.inc()

    def mark_microstructure(self, healthy: bool, reason: str):
        value = 1 if healthy else 0
        self.microstructure_health.labels(reason=reason).set(value)
        if not healthy:
            self.degradation_events.labels(reason=reason).inc()

    def record_drop(self, reason: str):
        self.dropped_events.labels(reason=reason).inc()

    def update_stream_lag(self, stream: str, seconds: float):
        self.stream_lag_seconds.labels(stream=stream).set(seconds)

    def update_queue_depth(self, name: str, depth: int):
        self.queue_depth.labels(buffer=name).set(depth)

def start_metrics_server(port: int = 9090):
    global _METRICS_SERVER_STARTED, _METRICS_PORT
    if _METRICS_SERVER_STARTED:
        return
    port_scan_limit = max(0, _get_port_scan_limit())
    last_error: Optional[OSError] = None
    for offset in range(port_scan_limit + 1):
        candidate = port + offset
        try:
            start_http_server(candidate)
        except OSError as exc:
            last_error = exc
            if exc.errno == errno.EADDRINUSE:
                logger.warning(
                    "Prometheus metrics server port %s already in use; trying next candidate",
                    candidate,
                )
                continue
            raise
        _METRICS_SERVER_STARTED = True
        _METRICS_PORT = candidate
        _write_port_file(candidate)
        logger.info("Prometheus metrics server started on port %s", candidate)
        return
    if last_error and last_error.errno == errno.EADDRINUSE:
        raise RuntimeError(
            f"Unable to bind Prometheus metrics server on ports {port}-{port + port_scan_limit}"
        ) from last_error
    if last_error:
        raise last_error

metrics = MetricsCollector()
