import os
from pathlib import Path
from collections.abc import Mapping
from typing import Any, Dict

import yaml
from dotenv import load_dotenv

load_dotenv()


class SectionProxy(Mapping):
    def __init__(self, data: Dict[str, Any]):
        self._data = data or {}

    def __getitem__(self, key: str) -> Any:
        value = self._data[key]
        if isinstance(value, dict):
            return SectionProxy(value)
        return value

    def __getattr__(self, name: str) -> Any:
        if name in self.__dict__:
            return super().__getattribute__(name)
        value = self._data.get(name)
        if isinstance(value, dict):
            return SectionProxy(value)
        if value is None:
            raise AttributeError(f"Config key '{name}' not found")
        return value

    def __iter__(self):
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def get(self, key: str, default: Any = None) -> Any:
        value = self._data.get(key, default)
        if isinstance(value, dict):
            return SectionProxy(value)
        return value

    def to_dict(self) -> Dict[str, Any]:
        return self._data


class Config:
    def __init__(self, config_path: str = 'config/config.yaml'):
        self.config_path = Path(config_path)
        self._data = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        if not self.config_path.exists():
            raise RuntimeError(f"Configuration file not found at {self.config_path}")
        with self.config_path.open('r') as fh:
            try:
                raw = yaml.safe_load(fh) or {}
            except yaml.YAMLError as exc:
                raise RuntimeError(f"Error parsing YAML configuration: {exc}") from exc
        return self._resolve_env_vars(raw)

    def _resolve_env_vars(self, node: Any) -> Any:
        if isinstance(node, dict):
            return {key: self._resolve_env_vars(value) for key, value in node.items()}
        if isinstance(node, list):
            return [self._resolve_env_vars(item) for item in node]
        if isinstance(node, str) and node.startswith('${') and node.endswith('}'):
            env_key = node[2:-1]
            return os.getenv(env_key, node)
        return node

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def __getitem__(self, key: str) -> Any:
        value = self._data[key]
        if isinstance(value, dict):
            return SectionProxy(value)
        return value

    def __getattr__(self, name: str) -> Any:
        try:
            value = self._data[name]
        except KeyError as exc:
            raise AttributeError(f"Config key '{name}' not found") from exc
        if isinstance(value, dict):
            return SectionProxy(value)
        return value

    def reload(self) -> None:
        self._data = self._load_config()


config = Config()
