"""Helper utilities for accessing configuration sections regardless of the backing loader."""
from __future__ import annotations

from typing import Any, Dict


def get_config_section(source: Any, section: str) -> Dict:
    """Return a dictionary section from Config, LegacyConfig, or plain dict objects."""
    if source is None:
        return {}

    attr = getattr(source, section, None)
    if isinstance(attr, dict):
        return attr

    if isinstance(source, dict):
        candidate = source.get(section, {})
        if isinstance(candidate, dict):
            return candidate

    getter = getattr(source, 'get', None)
    if callable(getter):
        candidate = getter(section, {})
        if isinstance(candidate, dict):
            return candidate

    try:
        candidate = source[section]  # type: ignore[index]
        if isinstance(candidate, dict):
            return candidate
    except Exception:
        pass

    return {}
