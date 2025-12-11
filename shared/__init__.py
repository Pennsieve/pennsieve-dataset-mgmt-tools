"""
Shared utilities for Pennsieve tools.

This package provides common functionality used across
sidecar-generation and dataset-management modules.
"""

from .config import BaseConfig, API_HOST, PAGE_SIZE, CACHE_DIR, OUTPUT_DIR
from .auth import PennsieveAuth, authenticate, get_headers, get_token, get_auth
from .helpers import (
    get_all_datasets,
    find_dataset_by_name,
    get_dataset_packages,
    get_freq_duration,
    get_electrode_data,
    save_data,
    load_data,
    eps_to_penn_epi,
    penn_epi_to_eps,
    generate_new_name,
    sanitize_group_name,
    clean_channel_name,
    get_channel_info,
    multi_dataset_read_csv_to_dict,
    read_csv_to_dict,
    parse_electrode_txt,
)

__all__ = [
    # Config
    "BaseConfig",
    "API_HOST",
    "PAGE_SIZE",
    "CACHE_DIR",
    "OUTPUT_DIR",
    # Auth
    "PennsieveAuth",
    "authenticate",
    "get_headers",
    "get_token",
    "get_auth",
    # Helpers
    "get_all_datasets",
    "find_dataset_by_name",
    "get_dataset_packages",
    "get_freq_duration",
    "get_electrode_data",
    "save_data",
    "load_data",
    "eps_to_penn_epi",
    "penn_epi_to_eps",
    "generate_new_name",
    "sanitize_group_name",
    "clean_channel_name",
    "get_channel_info",
    "multi_dataset_read_csv_to_dict",
    "read_csv_to_dict",
    "parse_electrode_txt",
]
