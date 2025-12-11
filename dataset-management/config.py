"""
Configuration for dataset management tools.

Extends the shared BaseConfig with dataset-management specific settings.
"""

import sys
from pathlib import Path

# Set up import paths - local first, then parent for shared package
_this_dir = Path(__file__).parent
sys.path.insert(0, str(_this_dir))
sys.path.insert(1, str(_this_dir.parent))

from shared.config import BaseConfig, API_HOST, PAGE_SIZE, CACHE_DIR, OUTPUT_DIR


class DatasetManagementConfig(BaseConfig):
    """Configuration specific to dataset management tools."""
    pass


# Re-export for convenience
__all__ = [
    "BaseConfig",
    "DatasetManagementConfig",
    "API_HOST",
    "PAGE_SIZE",
    "CACHE_DIR",
    "OUTPUT_DIR",
]
