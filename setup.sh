#!/bin/bash
set -e

echo "=== Pivot Divergence System Setup ==="

if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python -m venv venv
fi

echo "Installing Python dependencies..."
./venv/bin/pip install -q -r requirements.txt

if [ ! -f ".env" ]; then
    echo "Creating .env file from template..."
    cp .env.example .env
    echo "⚠️  Please edit .env with your API credentials"
fi

echo ""
echo "✓ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Edit .env with your Binance API credentials"
echo "2. Set up TimescaleDB: psql -U postgres -d pivotdivergence -f config/schema.sql"
echo "3. Start system: ./venv/bin/python main.py"
echo "4. Start API: ./venv/bin/python api/fastapi_server.py"
echo "5. Start UI: cd ui && npm install && npm run dev"
