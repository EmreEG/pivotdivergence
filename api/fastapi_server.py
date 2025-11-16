import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware
from config import config
from monitoring.logging_utils import setup_logging


trading_system = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global trading_system
    from main import TradingSystem
    trading_system = TradingSystem()
    task = asyncio.create_task(trading_system.start())
    try:
        yield
    finally:
        if trading_system:
            await trading_system.stop()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


app = FastAPI(title="Pivot Divergence API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.api['cors_origins'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: Dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                pass

manager = ConnectionManager()

@app.get("/")
async def root():
    return {
        "service": "Pivot Divergence Trading System",
        "version": "1.0.0",
        "status": "running" if trading_system and trading_system.running else "stopped"
    }

@app.get("/favicon.ico")
async def favicon():
    return Response(content=b"", media_type="image/x-icon")

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "system_running": trading_system.running if trading_system else False
    }

@app.get("/api/levels")
async def get_levels():
    if not trading_system:
        return {"error": "Trading system not initialized"}
    out = []
    for lvl in trading_system.current_levels:
        out.append({
            "type": lvl.get('type', 'LEVEL'),
            "price": lvl.get('price'),
            "window": lvl.get('window'),
            "score": lvl.get('score'),
            "side": lvl.get('side'),
            "timestamp": datetime.utcnow().isoformat()
        })

    return {"levels": out, "count": len(out), "timestamp": datetime.utcnow().isoformat()}

@app.get("/api/signals")
async def get_signals():
    if not trading_system:
        return {"error": "Trading system not initialized"}
    
    signals = trading_system.signal_manager.get_active_signals()
    
    return {
        "signals": signals,
        "count": len(signals),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/api/profile/{window}")
async def get_profile(window: str):
    if not trading_system:
        return {"error": "Trading system not initialized"}
    
    profile = trading_system.profile_service.profiles.get(window)
    
    if not profile:
        return {"error": f"Profile window '{window}' not found"}
    
    prices, volumes = profile.get_histogram_array()
    
    histogram = []
    for i in range(len(prices)):
        histogram.append({
            "price": float(prices[i]),
            "volume": float(volumes[i])
        })
    
    profile_data = profile.to_dict()
    
    return {
        "window": window,
        "poc": profile_data['poc'],
        "vah": profile_data['vah'],
        "val": profile_data['val'],
        "histogram": histogram,
        "total_volume": profile_data['total_volume'],
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/api/indicators")
async def get_indicators():
    if not trading_system:
        return {"error": "Trading system not initialized"}
    
    indicators_data = trading_system.order_flow.indicators.to_dict()
    
    return {
        "rsi": indicators_data.get('rsi'),
        "macd": indicators_data.get('macd'),
        "macd_signal": indicators_data.get('macd_signal'),
        "macd_hist": indicators_data.get('macd_hist'),
        "obv": indicators_data.get('obv'),
        "ad_line": indicators_data.get('ad_line'),
        "realized_volatility": indicators_data.get('realized_volatility'),
        "cvd": trading_system.order_flow.get_cvd(),
        "obi_z": trading_system.order_flow.get_obi_z(),
        "oi": trading_system.order_flow.get_current_oi(),
        "oi_slope": trading_system.order_flow.get_oi_slope(),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/api/swings")
async def get_swings():
    if not trading_system:
        return {"error": "Trading system not initialized"}
    
    swing_data = trading_system.swing_service.to_dict()
    
    return {
        "current_swing_type": swing_data['current_swing_type'],
        "current_swing_price": swing_data['current_swing_price'],
        "current_extreme_price": swing_data['current_extreme_price'],
        "swing_history": swing_data['swing_history'],
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/api/avwap")
async def get_avwap():
    if not trading_system:
        return {"error": "Trading system not initialized"}
    
    avwaps = trading_system.profile_service.get_avwaps()
    
    return {
        "avwaps": avwaps,
        "count": len(avwaps),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/api/footprint/current")
async def get_current_footprint():
    if not trading_system:
        return {"error": "Trading system not initialized"}
    
    current_bar = trading_system.order_flow.get_current_footprint_bar()
    
    if not current_bar:
        return {"error": "No current footprint bar"}
    
    bar_data = current_bar.to_dict()
    
    price_levels = []
    for price, data in bar_data['price_levels'].items():
        price_levels.append({
            "price": price,
            "bid_vol": data['bid_vol'],
            "ask_vol": data['ask_vol'],
            "delta": data['delta']
        })
    
    price_levels.sort(key=lambda x: x['price'], reverse=True)
    
    return {
        "bar_start": bar_data['bar_start'],
        "bar_end": bar_data['bar_end'],
        "total_bid_vol": bar_data['total_bid_vol'],
        "total_ask_vol": bar_data['total_ask_vol'],
        "total_delta": bar_data['total_delta'],
        "price_levels": price_levels,
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/api/footprint/absorption/{level_price}")
async def get_absorption_at_level(level_price: float):
    if not trading_system:
        return {"error": "Trading system not initialized"}
    
    absorption_detected = trading_system.order_flow.detect_absorption(level_price, lookback_bars=5)

    recent_bars = trading_system.order_flow.get_recent_footprint_bars(5)
    
    bar_details = []
    for bar in recent_bars:
        data = bar.get_price_level_data(level_price)
        bar_details.append({
            "bar_start": bar.bar_start,
            "bid_vol": data['bid_vol'],
            "ask_vol": data['ask_vol'],
            "delta": data['delta']
        })
    
    return {
        "level_price": level_price,
        "absorption_detected": absorption_detected,
        "bar_details": bar_details,
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/api/footprint")
async def get_footprint():
    if not trading_system:
        return {"error": "Trading system not initialized"}
    levels = trading_system.book_manager.get_top_levels(depth=10)
    return {
        "bids": [[float(p), float(q)] for p, q in levels.get('bids', [])],
        "asks": [[float(p), float(q)] for p, q in levels.get('asks', [])],
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/api/kill_switch")
async def trigger_kill_switch():
    if trading_system:
        await trading_system.execution_supervisor.handle_kill_switch('manual')
        return {"status": "Kill switch triggered", "timestamp": datetime.utcnow().isoformat()}
    return {"error": "Trading system not initialized"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    
    try:
        while True:
            if trading_system and trading_system.running:
                indicator_payload = trading_system.order_flow.indicators.to_dict()
                indicator_payload.update({
                    "cvd": trading_system.order_flow.get_cvd(),
                    "obi_z": trading_system.order_flow.get_obi_z()
                })
                data = {
                    "type": "update",
                    "timestamp": datetime.utcnow().isoformat(),
                    "price": trading_system.current_price,
                    "signals": trading_system.signal_manager.get_active_signals(),
                    "indicators": indicator_payload
                }
                
                await websocket.send_json(data)
            
            await asyncio.sleep(1)
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)

if __name__ == "__main__":
    import uvicorn
    setup_logging()
    uvicorn.run(
        app,
        host=config.api['host'],
        port=config.api['port'],
        log_level="info"
    )
