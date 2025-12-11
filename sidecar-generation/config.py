"""
Configuration for sidecar generation tools.

Extends the shared BaseConfig with sidecar-generation specific settings.
"""

import os
import sys
from pathlib import Path

# Add parent to path for package imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.config import BaseConfig, API_HOST, PAGE_SIZE, CACHE_DIR, OUTPUT_DIR


class SidecarGenerationConfig(BaseConfig):
    """Configuration specific to sidecar generation tools."""

    # Path to master CSV with EPS reference/ground data
    # Override with MASTER_CSV_PATH env var
    MASTER_CSV_PATH = os.getenv(
        "MASTER_CSV_PATH",
        str(Path(__file__).parent / "data" / "master.csv")
    )


# Convenience export
MASTER_CSV_PATH = SidecarGenerationConfig.MASTER_CSV_PATH

# Re-export for convenience
__all__ = [
    "BaseConfig",
    "SidecarGenerationConfig",
    "API_HOST",
    "PAGE_SIZE",
    "CACHE_DIR",
    "OUTPUT_DIR",
    "MASTER_CSV_PATH",
]
