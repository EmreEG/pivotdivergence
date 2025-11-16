import asyncio
import logging
import aiohttp
from typing import Dict
from config import config


logger = logging.getLogger(__name__)

class AlertWebhook:
    def __init__(self):
        url = config.monitoring.get('alert_webhook')
        # Treat empty or placeholder URLs as disabled
        if url and 'your-webhook-url' not in str(url):
            self.webhook_url = url
            self.enabled = True
        else:
            self.webhook_url = None
            self.enabled = False
        
    async def send_alert(self, alert_type: str, message: str, severity: str = 'warning', 
                        metadata: Dict = None):
        if not self.enabled:
            logger.warning(
                "[Alert] %s: %s - %s",
                severity.upper(),
                alert_type,
                message,
            )
            return
            
        payload = {
            'type': alert_type,
            'message': message,
            'severity': severity,
            'timestamp': asyncio.get_event_loop().time(),
            'metadata': metadata or {}
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.webhook_url,
                    json=payload,
                    headers={'Content-Type': 'application/json'},
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as response:
                    if response.status != 200:
                        logger.error(
                            "[Alert] Webhook failed with status %s",
                            response.status,
                        )
        except Exception as e:
            logger.error("[Alert] Webhook error: %s", e)
            
    async def kill_switch_alert(self, reason: str):
        await self.send_alert(
            'kill_switch',
            f'Kill switch triggered: {reason}',
            'critical',
            {'reason': reason}
        )
        
    async def desync_alert(self):
        await self.send_alert(
            'desync',
            'Order book desync detected',
            'critical'
        )
        
    async def latency_alert(self, drift_ms: float):
        await self.send_alert(
            'latency',
            f'Sustained latency drift: {drift_ms:.0f}ms',
            'warning',
            {'drift_ms': drift_ms}
        )
        
    async def drawdown_alert(self, drawdown_pct: float):
        await self.send_alert(
            'drawdown',
            f'Max drawdown exceeded: {drawdown_pct:.2%}',
            'critical',
            {'drawdown_pct': drawdown_pct}
        )
        
    async def signal_alert(self, signal_id: str, side: str, level_price: float, state: str):
        await self.send_alert(
            'signal',
            f'Signal {state}: {side} at {level_price}',
            'info',
            {
                'signal_id': signal_id,
                'side': side,
                'level_price': level_price,
                'state': state
            }
        )

alert_webhook = AlertWebhook()
