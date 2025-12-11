"""
Package operations for DatasetManager.

Handles package listing, deletion, renaming, and duplicate cleanup.
"""

import fnmatch
import logging
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

# Set up import paths
_this_dir = Path(__file__).parent
sys.path.insert(0, str(_this_dir.parent))
sys.path.insert(1, str(_this_dir.parent.parent))

from shared.helpers import load_data, save_data

from .core import DatasetManagerCore

logger = logging.getLogger(__name__)


class PackageOperationsMixin:
    """Mixin providing package management and duplicate cleanup operations."""

    def get_dataset_packages(
        self: DatasetManagerCore,
        dataset_id: str,
        force_reload: bool = False
    ) -> List[Dict]:
        """
        Get all packages for a dataset, with caching.

        Args:
            dataset_id: Dataset node ID
            force_reload: If True, bypass cache

        Returns:
            List of package objects
        """
        # Extract name from ID for cache key
        cache_key = f"packages_{dataset_id.replace(':', '_')}"
        packages = load_data(cache_key, force_reload=force_reload)

        if packages is not None:
            logger.debug(f"Using cached packages ({len(packages)} packages)")
            return packages

        logger.info(f"Fetching packages for dataset {dataset_id}...")
        encoded_id = quote(dataset_id, safe="")
        base_url = (
            f"{self.api_host}/datasets/{encoded_id}/packages"
            f"?pageSize=1000&includeSourceFiles=false"
        )

        all_packages = []
        cursor = None

        while True:
            url = f"{base_url}&cursor={cursor}" if cursor else base_url
            result = self._make_request("GET", url)

            if not result:
                break

            all_packages.extend(result.get('packages', []))
            cursor = result.get('cursor')
            if not cursor:
                break

        logger.info(f"Fetched {len(all_packages)} packages")
        save_data(all_packages, cache_key)
        return all_packages

    def get_package_path(self: DatasetManagerCore, package: Dict, all_packages: List[Dict]) -> str:
        """
        Reconstruct the path to a package by walking up parent IDs.

        Returns path like 'ieeg/subfolder' or '' for root-level packages.
        """
        pkg_lookup = {}
        for pkg in all_packages:
            content = pkg.get("content", {})
            pkg_id = content.get("id")
            if pkg_id:
                pkg_lookup[pkg_id] = pkg

        path_parts = []
        current = package

        while True:
            content = current.get("content", {})
            parent_id = content.get("parentId")

            if not parent_id or parent_id not in pkg_lookup:
                break

            parent = pkg_lookup[parent_id]
            parent_content = parent.get("content", {})
            parent_name = parent_content.get("name", "")

            if parent_name:
                path_parts.insert(0, parent_name)

            current = parent

        return "/".join(path_parts)

    def delete_package(self: DatasetManagerCore, package_id: str) -> bool:
        """
        Delete a package by ID.

        Args:
            package_id: Package node ID

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Deleting package: {package_id}")

        if self.dry_run:
            self._log_dry_run(f"Would delete package: {package_id}")
            return True

        url = f"{self.api_host}/data/delete"
        payload = {"things": [package_id]}

        result = self._make_request("POST", url, json=payload)
        if result is not None:
            logger.info(f"  Deleted package: {package_id}")
            return True

        logger.error(f"  Failed to delete package: {package_id}")
        return False

    def rename_package(self: DatasetManagerCore, package_id: str, new_name: str) -> bool:
        """
        Rename a package by ID.

        Args:
            package_id: Package node ID
            new_name: New name for the package

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Renaming package {package_id} -> '{new_name}'")

        if self.dry_run:
            self._log_dry_run(f"Would rename package {package_id} -> '{new_name}'")
            return True

        url = f"{self.api_host}/packages/{package_id}?updateStorage=false"
        payload = {"name": new_name}

        result = self._make_request("PUT", url, json=payload)
        if result is not None:
            logger.info(f"  Renamed package -> '{new_name}'")
            return True

        logger.error(f"  Failed to rename package: {package_id}")
        return False

    # =========================================================================
    # Delete by Pattern/Path
    # =========================================================================

    def delete_by_pattern(
        self: DatasetManagerCore,
        dataset_name: str,
        pattern: str,
        force_reload: bool = False
    ) -> Tuple[int, int]:
        """
        Delete files matching a glob pattern.

        Args:
            dataset_name: Name of the dataset
            pattern: Glob pattern to match (e.g., "*.tsv", "*_ieeg.json", "sub-*/*_channels.tsv")
            force_reload: If True, bypass package cache

        Returns:
            Tuple of (deleted_count, failed_count)
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Deleting by pattern in dataset: {dataset_name}")
        logger.info(f"Pattern: {pattern}")
        logger.info(f"{'='*60}")

        # Find dataset
        dataset = self.find_dataset_by_name(dataset_name)
        if not dataset:
            logger.error(f"Dataset not found: {dataset_name}")
            return (0, 0)

        dataset_id = self.get_dataset_id(dataset)
        packages = self.get_dataset_packages(dataset_id, force_reload=force_reload)
        logger.info(f"  Found {len(packages)} packages")

        # Find matching packages
        matches = []
        for pkg in packages:
            content = pkg.get("content", {})
            name = content.get("name", "")
            pkg_type = content.get("packageType", "")

            # Skip folders
            if pkg_type == "Collection":
                continue

            # Build full path for matching
            pkg_path = self.get_package_path(pkg, packages)
            full_path = f"{pkg_path}/{name}" if pkg_path else name

            if fnmatch.fnmatch(full_path, pattern) or fnmatch.fnmatch(name, pattern):
                matches.append((full_path, pkg))

        if not matches:
            logger.info(f"  No files match pattern: {pattern}")
            return (0, 0)

        logger.info(f"  Found {len(matches)} matching file(s):")
        for path, _ in matches:
            logger.info(f"    - {path}")

        # Delete matches
        deleted_count = 0
        failed_count = 0

        for path, pkg in matches:
            node_id = pkg.get("content", {}).get("nodeId")
            if self.delete_package(node_id):
                deleted_count += 1
            else:
                failed_count += 1

        logger.info(f"\n  Deleted: {deleted_count}, Failed: {failed_count}")
        return (deleted_count, failed_count)

    def delete_by_path(
        self: DatasetManagerCore,
        dataset_name: str,
        file_paths: List[str],
        force_reload: bool = False
    ) -> Tuple[int, int]:
        """
        Delete specific files by their paths.

        Args:
            dataset_name: Name of the dataset
            file_paths: List of file paths relative to dataset root
                        (e.g., ["ieeg/sub-001_channels.tsv", "README.txt"])
            force_reload: If True, bypass package cache

        Returns:
            Tuple of (deleted_count, not_found_count)
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Deleting by path in dataset: {dataset_name}")
        logger.info(f"{'='*60}")

        # Find dataset
        dataset = self.find_dataset_by_name(dataset_name)
        if not dataset:
            logger.error(f"Dataset not found: {dataset_name}")
            return (0, len(file_paths))

        dataset_id = self.get_dataset_id(dataset)
        packages = self.get_dataset_packages(dataset_id, force_reload=force_reload)

        # Build lookup by full path
        pkg_by_path: Dict[str, Dict] = {}
        for pkg in packages:
            content = pkg.get("content", {})
            name = content.get("name", "")
            pkg_path = self.get_package_path(pkg, packages)
            full_path = f"{pkg_path}/{name}" if pkg_path else name
            pkg_by_path[full_path] = pkg

        deleted_count = 0
        not_found_count = 0

        for file_path in file_paths:
            # Normalize path (remove leading slash if present)
            normalized_path = file_path.lstrip("/")

            logger.info(f"\n  Looking for: {normalized_path}")

            pkg = pkg_by_path.get(normalized_path)
            if not pkg:
                logger.warning(f"    NOT FOUND: {normalized_path}")
                not_found_count += 1
                continue

            node_id = pkg.get("content", {}).get("nodeId")
            if self.delete_package(node_id):
                deleted_count += 1
            else:
                not_found_count += 1

        logger.info(f"\n  Deleted: {deleted_count}, Not found/failed: {not_found_count}")
        return (deleted_count, not_found_count)

    # =========================================================================
    # Duplicate Cleanup
    # =========================================================================

    def _get_duplicate_name(self: DatasetManagerCore, filename: str) -> str:
        """
        Generate the (1) duplicate name for a file.

        Examples:
            'file.json' -> 'file (1).json'
        """
        p = Path(filename)
        stem = p.stem
        suffix = p.suffix
        return f"{stem} (1){suffix}"

    def _get_original_name(self: DatasetManagerCore, duplicate_name: str) -> str:
        """
        Get the original name from a (1) duplicate name.

        Examples:
            'file (1).json' -> 'file.json'
        """
        match = re.match(r'^(.+) \(1\)(\.[^.]+)$', duplicate_name)
        if match:
            return f"{match.group(1)}{match.group(2)}"
        return duplicate_name

    def cleanup_duplicates(
        self: DatasetManagerCore,
        dataset_name: str,
        file_paths: List[str],
        force_reload: bool = False
    ) -> Tuple[int, int]:
        """
        Clean up duplicate files in a dataset.

        When files are re-uploaded to Pennsieve, duplicates get a (1) suffix.
        This method finds these duplicates and:
        1. Deletes the original file
        2. Renames the (1) version to remove the suffix

        Only acts when BOTH files exist in the same folder.

        Args:
            dataset_name: Name of the dataset
            file_paths: List of file paths to check (relative to dataset root).
                        Use {dataset} as placeholder for dataset name.
            force_reload: If True, bypass package cache

        Returns:
            Tuple of (success_count, skip_count)
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Cleaning duplicates in dataset: {dataset_name}")
        logger.info(f"{'='*60}")

        # Find dataset
        dataset = self.find_dataset_by_name(dataset_name)
        if not dataset:
            logger.error(f"Dataset not found: {dataset_name}")
            return (0, len(file_paths))

        dataset_id = self.get_dataset_id(dataset)
        logger.info(f"  Dataset ID: {dataset_id}")

        # Get all packages
        packages = self.get_dataset_packages(dataset_id, force_reload=force_reload)
        logger.info(f"  Found {len(packages)} packages")

        # Build lookup by (path, name)
        pkg_by_location: Dict[Tuple[str, str], Dict] = {}
        for pkg in packages:
            content = pkg.get("content", {})
            name = content.get("name", "")
            pkg_path = self.get_package_path(pkg, packages)
            pkg_by_location[(pkg_path, name)] = pkg

        success_count = 0
        skip_count = 0

        for file_path_template in file_paths:
            # Replace {dataset} placeholder with actual dataset name
            file_path = file_path_template.replace("{dataset}", dataset_name)

            p = Path(file_path)
            parent_folder = str(p.parent) if p.parent != Path('.') else ''
            filename = p.name
            duplicate_name = self._get_duplicate_name(filename)

            logger.info(f"\n  Looking for: {file_path}")
            logger.info(f"    Original: {parent_folder}/{filename}" if parent_folder else f"    Original: {filename}")
            logger.info(f"    Duplicate: {parent_folder}/{duplicate_name}" if parent_folder else f"    Duplicate: {duplicate_name}")

            # Find both packages
            original_pkg = pkg_by_location.get((parent_folder, filename))
            duplicate_pkg = pkg_by_location.get((parent_folder, duplicate_name))

            if not original_pkg and not duplicate_pkg:
                logger.info(f"    SKIP: Neither file found")
                skip_count += 1
                continue

            if original_pkg and not duplicate_pkg:
                logger.info(f"    SKIP: Only original exists (no duplicate to replace with)")
                skip_count += 1
                continue

            if duplicate_pkg and not original_pkg:
                logger.info(f"    SKIP: Only duplicate exists (no original to delete)")
                skip_count += 1
                continue

            # Both exist - proceed with cleanup
            original_id = original_pkg.get("content", {}).get("nodeId")
            duplicate_id = duplicate_pkg.get("content", {}).get("nodeId")

            logger.info(f"    FOUND BOTH:")
            logger.info(f"      Original ID: {original_id}")
            logger.info(f"      Duplicate ID: {duplicate_id}")

            # Step 1: Delete original
            logger.info(f"    Step 1: Delete original")
            if not self.delete_package(original_id):
                logger.error(f"    ERROR: Failed to delete original, skipping rename")
                skip_count += 1
                continue

            # Step 2: Rename duplicate to original name
            logger.info(f"    Step 2: Rename duplicate")
            if not self.rename_package(duplicate_id, filename):
                logger.error(f"    ERROR: Failed to rename duplicate")
                skip_count += 1
                continue

            logger.info(f"    SUCCESS: Cleaned up {filename}")
            success_count += 1

        return (success_count, skip_count)
