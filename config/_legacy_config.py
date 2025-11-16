import os
import yaml
from pathlib import Path
from dotenv import load_dotenv
from typing import Any, Dict

load_dotenv()

class LegacyConfig:
    """Singleton loader for application configuration with environment expansion."""
    _instance = None
    _config: Dict[str, Any] = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_config()
        return cls._instance
    
    def _load_config(self):
        config_path = Path(__file__).parent / 'config.yaml'
        with open(config_path, 'r') as f:
            raw_config = yaml.safe_load(f)
        self._config = self._resolve_env_vars(raw_config)
    
    def _resolve_env_vars(self, config: Any) -> Any:
        if isinstance(config, dict):
            return {k: self._resolve_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._resolve_env_vars(item) for item in config]
        elif isinstance(config, str) and config.startswith('${') and config.endswith('}'):
            env_var = config[2:-1]
            return os.getenv(env_var, config)
        return config
    
    def get(self, *keys, default=None):
        value = self._config
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
                if value is None:
                    return default
            else:
                return default
        return value
    
    @property
    def exchange(self) -> Dict[str, Any]:
        return self._config.get('exchange', {})
    
    @property
    def database(self) -> Dict[str, Any]:
        return self._config.get('database', {})
    
    @property
    def websocket(self) -> Dict[str, Any]:
        return self._config.get('websocket', {})

    @property
    def orderbook(self) -> Dict[str, Any]:
        return self._config.get('orderbook', {})

    @property
    def profile(self) -> Dict[str, Any]:
        return self._config.get('profile', {})
    
    @property
    def swing(self) -> Dict[str, Any]:
        return self._config.get('swing', {})
    
    @property
    def scoring(self) -> Dict[str, Any]:
        return self._config.get('scoring', {})
    
    @property
    def divergence(self) -> Dict[str, Any]:
        return self._config.get('divergence', {})
    
    @property
    def levels(self) -> Dict[str, Any]:
        return self._config.get('levels', {})
    
    @property
    def execution(self) -> Dict[str, Any]:
        return self._config.get('execution', {})
    
    @property
    def risk(self) -> Dict[str, Any]:
        return self._config.get('risk', {})
    
    @property
    def breakthrough(self) -> Dict[str, Any]:
        return self._config.get('breakthrough', {})
    
    @property
    def backtest(self) -> Dict[str, Any]:
        return self._config.get('backtest', {})
    
    @property
    def indicators(self) -> Dict[str, Any]:
        return self._config.get('indicators', {})
    
    @property
    def monitoring(self) -> Dict[str, Any]:
        return self._config.get('monitoring', {})

    @property
    def microstructure(self) -> Dict[str, Any]:
        return self._config.get('microstructure', {})

    @property
    def api(self) -> Dict[str, Any]:
        return self._config.get('api', {})

legacy_config = LegacyConfig()
