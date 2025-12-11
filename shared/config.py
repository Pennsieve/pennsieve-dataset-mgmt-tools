"""
Base configuration for Pennsieve tools.

This module provides the base configuration that can be extended
by specific tool modules (sidecar-generation, dataset-management).

Override via environment variables where noted.
"""

import os


class BaseConfig:
    """Base configuration class with common settings."""

    # Pennsieve API base URL (override with PENNSIEVE_API_HOST env var)
    API_HOST = os.getenv("PENNSIEVE_API_HOST", "https://api.pennsieve.io")

    # Pagination settings
    PAGE_SIZE = 25

    # Cache directory for API responses (override with PENNSIEVE_CACHE_DIR env var)
    CACHE_DIR = os.getenv("PENNSIEVE_CACHE_DIR", "cache")

    # Output directory for generated files (override with PENNSIEVE_OUTPUT_DIR env var)
    OUTPUT_DIR = os.getenv("PENNSIEVE_OUTPUT_DIR", "output")

    @classmethod
    def ensure_directories(cls):
        """Create cache and output directories if they don't exist."""
        os.makedirs(cls.CACHE_DIR, exist_ok=True)
        os.makedirs(cls.OUTPUT_DIR, exist_ok=True)


# Create directories on import
BaseConfig.ensure_directories()

# Convenience exports for direct import
API_HOST = BaseConfig.API_HOST
PAGE_SIZE = BaseConfig.PAGE_SIZE
CACHE_DIR = BaseConfig.CACHE_DIR
OUTPUT_DIR = BaseConfig.OUTPUT_DIR
