"""
DatasetManager package for Pennsieve dataset operations.

This package provides a unified interface for managing Pennsieve datasets,
including metadata updates, collaborator management, and package operations.

Usage:
    from dataset_manager import DatasetManager

    manager = DatasetManager(auth, dry_run=True)
    manager.process_dataset("MyDataset", name="New Name")

CLI:
    python -m dataset_manager --help
"""

from .manager import DatasetManager
from .core import DatasetManagerCore
from .references import RELATIONSHIP_TYPES

__all__ = [
    "DatasetManager",
    "DatasetManagerCore",
    "RELATIONSHIP_TYPES",
]
