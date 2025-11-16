from .config_loader import config_loader as config
from ._legacy_config import legacy_config

# The legacy_config is deprecated and will be removed in a future version.
# Please use the new `config` object for accessing configuration.
__all__ = ['config', 'legacy_config']