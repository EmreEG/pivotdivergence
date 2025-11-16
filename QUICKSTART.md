# Pivot Divergence Trading System - Quick Start

## Prerequisites
- Python 3.10+
- Node.js 18+ (for UI)
- PostgreSQL 18+ with TimescaleDB 2.23+ extension
- Docker & Docker Compose (optional, for database)

## Installation

### 1. Quick Setup
```bash
./setup.sh
```

### 2. Database Setup

**Option A: Using Docker (Recommended)**
```bash
docker-compose up -d timescaledb
```

**Option B: Manual PostgreSQL**
```bash
createdb pivotdivergence
psql -U postgres -d pivotdivergence -f config/schema.sql
```

### 3. Configuration
Edit `.env` file with your credentials:
```bash
BINANCE_API_KEY=your_api_key_here
BINANCE_API_SECRET=your_api_secret_here
DB_PASSWORD=your_db_password
```

For testing, set `testnet: true` in `config/config.yaml`.

## Running

### Main Trading System
```bash
./start.sh
# or
./venv/bin/python main.py
```

### API Server
```bash
./venv/bin/python api/fastapi_server.py
```
Access at http://localhost:8080

### Web UI
```bash
cd ui
npm install
npm run dev
```
Access at http://localhost:3000

## Testing

### Unit Tests
```bash
./venv/bin/python tests/test_core.py
```

### Mock System Test
```bash
./venv/bin/python tests/test_mock.py
```

### Backtest
```bash
./venv/bin/python -m backtest.optimizer
```

## Monitoring

- **Prometheus**: http://localhost:9090/metrics
- **Grafana**: http://localhost:3001 (if using docker-compose)
- **API Docs**: http://localhost:8080/docs

## Project Structure
```
pivotdivergence/
├── config/          # Configuration and schema
├── ingest/          # Data ingestion (WebSocket, REST, persistence)
├── analytics/       # Volume profile, indicators, swings
├── strategy/        # Level selection, divergence, signals
├── risk/            # Position sizing, stops
├── backtest/        # Backtesting engine
├── api/             # FastAPI backend + metrics
├── ui/              # React frontend
└── tests/           # Unit and integration tests
```

## Key Features

✓ Realtime volume profile with POC/VAH/VAL  
✓ HVN/LVN detection with prominence  
✓ Naked POC tracking  
✓ Anchored VWAP  
✓ CVD, OBI z-score, OI slope  
✓ ZigZag swing detection  
✓ Multi-indicator divergence (RSI/CVD/OBI/OI)  
✓ Signal state machine  
✓ Risk management (2% sizing, ATR stops, AVWAP trailers)  
✓ Walk-forward backtesting  
✓ FastAPI + WebSocket backend  
✓ React + TradingView charts UI  
✓ Prometheus metrics  

## Configuration

All parameters in `config/config.yaml`:
- Profile windows and bin width
- Swing detection threshold (0.4%)
- Divergence confirmation count (2)
- Position size (2% equity)
- Stop multipliers
- Zone widths

## Safety Features

- Kill-switch on WebSocket desync
- Order book validation
- Max drawdown protection (10%)
- Funding rate filter
- Latency monitoring
- Alert webhooks

## Next Steps

1. ✅ Run unit tests
2. ✅ Run mock system test
3. Configure testnet credentials
4. Paper trade on testnet
5. Optimize parameters via backtest
6. Deploy to production

## Support

For issues or questions, check:
- README.md for detailed documentation
- config/config.yaml for all parameters
- tests/ for usage examples
