import logging
from typing import Optional


def setup_logging(level: int = logging.INFO, log_format: Optional[str] = None) -> None:
    """
    Configure process-wide logging with a consistent format.

    Intended to be called once from the main entrypoint or service startup.
    Safe to call multiple times; subsequent calls are ignored if handlers exist.
    """
    if logging.getLogger().handlers:
        return

    fmt = log_format or "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(level=level, format=fmt)

