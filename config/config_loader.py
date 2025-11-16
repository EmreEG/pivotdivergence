import yaml
from pathlib import Path
from typing import Dict, Any

class Config:
    def __init__(self, config_path: str = 'config/config.yaml'):
        self.config_path = Path(config_path)
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        try:
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            raise RuntimeError(f"Configuration file not found at {self.config_path}")
        except yaml.YAMLError as e:
            raise RuntimeError(f"Error parsing YAML configuration: {e}")

    def get(self, key: str, default: Any = None) -> Any:
        return self.config.get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self.config[key]

    def __getattr__(self, name: str) -> Any:
        try:
            return self.config[name]
        except KeyError as exc:
            raise AttributeError(f"Config key '{name}' not found") from exc

# Global config instance
config_loader = Config()
