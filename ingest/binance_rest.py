import asyncio
import hmac
import hashlib
import json
import time
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import aiohttp

from config import config


class BinanceAPIError(Exception):
    def __init__(self, status: int, code: Optional[int], msg: Optional[str], body: str):
        self.status = status
        self.code = code
        self.msg = msg
        self.body = body
        text = f"Binance API error (status={status}, code={code}, msg={msg})"
        super().__init__(text)


class BinanceRESTClient:
    def __init__(self, base_url: Optional[str] = None):
        # USDⓈ‑M futures base URL
        self.base_url = (base_url or "https://fapi.binance.com").rstrip("/")
        self.api_key: Optional[str] = config.exchange.get("api_key")
        self.api_secret: Optional[str] = config.exchange.get("api_secret")
        self._session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()

    async def _get_session(self) -> aiohttp.ClientSession:
        async with self._lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession()
            return self._session

    async def close(self):
        async with self._lock:
            if self._session and not self._session.closed:
                await self._session.close()
                self._session = None

    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        signed: bool = False,
        send_in_body: bool = False,
    ) -> Any:
        session = await self._get_session()
        params = dict(params or {})
        headers: Dict[str, str] = {}

        if signed:
            if not self.api_key or not self.api_secret:
                raise RuntimeError("Binance API key/secret required for signed request")
            params.setdefault("timestamp", int(time.time() * 1000))
            params.setdefault("recvWindow", 5000)
            query = urlencode(params, doseq=True)
            signature = hmac.new(
                self.api_secret.encode("utf-8"),
                query.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
            params["signature"] = signature
            headers["X-MBX-APIKEY"] = self.api_key
        elif self.api_key:
            # User data stream listenKey and some account endpoints require API key only
            headers["X-MBX-APIKEY"] = self.api_key

        url = f"{self.base_url}{path}"
        request_params = params if not send_in_body else None
        data = urlencode(params, doseq=True) if send_in_body and params else None

        async with session.request(
            method.upper(),
            url,
            params=request_params,
            data=data,
            headers=headers,
            timeout=15,
        ) as resp:
            text = await resp.text()
            content_type = resp.headers.get("Content-Type", "")
            payload: Any
            if "application/json" in content_type:
                try:
                    payload = json.loads(text)
                except Exception:
                    payload = text
            else:
                payload = text

            if resp.status >= 400:
                code = None
                msg = None
                if isinstance(payload, dict):
                    code = payload.get("code")
                    msg = payload.get("msg")
                raise BinanceAPIError(resp.status, code, msg, text)

            return payload

    async def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        signed: bool = False,
    ) -> Any:
        return await self._request("GET", path, params=params, signed=signed)

    async def post(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        signed: bool = False,
    ) -> Any:
        # Binance REST accepts signed params in query string
        return await self._request("POST", path, params=params, signed=signed)

    async def delete(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        signed: bool = False,
    ) -> Any:
        return await self._request("DELETE", path, params=params, signed=signed)

    async def put(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        signed: bool = False,
    ) -> Any:
        return await self._request("PUT", path, params=params, signed=signed)

