"""
Metadata operations for DatasetManager.

Handles dataset name, subtitle, tags, license, readme, and banner updates.
"""

import logging
import os
from typing import List

import requests

from .core import DatasetManagerCore

logger = logging.getLogger(__name__)


class MetadataOperationsMixin:
    """Mixin providing metadata update operations."""

    def update_name(self: DatasetManagerCore, dataset_id: str, name: str) -> bool:
        """
        Update dataset name.

        Args:
            dataset_id: Dataset node ID
            name: New dataset name

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Updating name for dataset {dataset_id}")
        logger.info(f"  New name: {name}")

        if self.dry_run:
            self._log_dry_run(f"Would update name to: {name}")
            return True

        url = f"{self.api_host}/datasets/{dataset_id}"
        payload = {"name": name}

        result = self._make_request("PUT", url, json=payload)
        if result is not None:
            logger.info(f"  Successfully updated name to: {name}")
            return True

        logger.error("  Failed to update name")
        return False

    def update_subtitle(self: DatasetManagerCore, dataset_id: str, subtitle: str) -> bool:
        """
        Update dataset subtitle (description field).

        Args:
            dataset_id: Dataset node ID
            subtitle: New subtitle/description

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Updating subtitle for dataset {dataset_id}")
        logger.info(f"  New subtitle: {subtitle[:100]}{'...' if len(subtitle) > 100 else ''}")

        if self.dry_run:
            self._log_dry_run(f"Would update subtitle to: {subtitle[:50]}...")
            return True

        url = f"{self.api_host}/datasets/{dataset_id}"
        payload = {"description": subtitle}

        result = self._make_request("PUT", url, json=payload)
        if result is not None:
            logger.info("  Successfully updated subtitle")
            return True

        logger.error("  Failed to update subtitle")
        return False

    def update_tags(self: DatasetManagerCore, dataset_id: str, tags: List[str]) -> bool:
        """
        Update dataset tags.

        Args:
            dataset_id: Dataset node ID
            tags: List of tags to set

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Updating tags for dataset {dataset_id}")
        logger.info(f"  Tags: {tags}")

        if self.dry_run:
            self._log_dry_run(f"Would update tags to: {tags}")
            return True

        url = f"{self.api_host}/datasets/{dataset_id}"
        payload = {"tags": tags}

        result = self._make_request("PUT", url, json=payload)
        if result is not None:
            logger.info("  Successfully updated tags")
            return True

        logger.error("  Failed to update tags")
        return False

    def add_tag(self: DatasetManagerCore, dataset_id: str, tag: str) -> bool:
        """
        Add a single tag to dataset (preserving existing tags).

        Args:
            dataset_id: Dataset node ID
            tag: Tag to add

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Adding tag '{tag}' to dataset {dataset_id}")

        # First, get current tags
        url = f"{self.api_host}/datasets/{dataset_id}"
        result = self._make_request("GET", url)

        if result is None:
            logger.error("  Failed to fetch current dataset info")
            return False

        current_tags = result.get("content", {}).get("tags", [])
        if tag in current_tags:
            logger.info(f"  Tag '{tag}' already exists")
            return True

        new_tags = current_tags + [tag]
        return self.update_tags(dataset_id, new_tags)

    def remove_tag(self: DatasetManagerCore, dataset_id: str, tag: str) -> bool:
        """
        Remove a single tag from dataset.

        Args:
            dataset_id: Dataset node ID
            tag: Tag to remove

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Removing tag '{tag}' from dataset {dataset_id}")

        # First, get current tags
        url = f"{self.api_host}/datasets/{dataset_id}"
        result = self._make_request("GET", url)

        if result is None:
            logger.error("  Failed to fetch current dataset info")
            return False

        current_tags = result.get("content", {}).get("tags", [])
        if tag not in current_tags:
            logger.info(f"  Tag '{tag}' not found")
            return True

        new_tags = [t for t in current_tags if t != tag]
        return self.update_tags(dataset_id, new_tags)

    def update_license(self: DatasetManagerCore, dataset_id: str, license_name: str) -> bool:
        """
        Update dataset license.

        Args:
            dataset_id: Dataset node ID
            license_name: License name (e.g., "Creative Commons Attribution")

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Updating license for dataset {dataset_id}")
        logger.info(f"  License: {license_name}")

        if self.dry_run:
            self._log_dry_run(f"Would update license to: {license_name}")
            return True

        url = f"{self.api_host}/datasets/{dataset_id}"
        payload = {"license": license_name}

        result = self._make_request("PUT", url, json=payload)
        if result is not None:
            logger.info("  Successfully updated license")
            return True

        logger.error("  Failed to update license")
        return False

    def update_readme(self: DatasetManagerCore, dataset_id: str, readme_text: str) -> bool:
        """
        Update dataset readme.

        Args:
            dataset_id: Dataset node ID
            readme_text: New readme content

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Updating readme for dataset {dataset_id}")
        logger.info(f"  Readme: {readme_text[:100]}{'...' if len(readme_text) > 100 else ''}")

        if self.dry_run:
            self._log_dry_run(f"Would update readme to: {readme_text[:50]}...")
            return True

        url = f"{self.api_host}/datasets/{dataset_id}/readme"
        payload = {"readme": readme_text}

        result = self._make_request("PUT", url, json=payload)
        if result is not None:
            logger.info("  Successfully updated readme")
            return True

        logger.error("  Failed to update readme")
        return False

    def update_banner(self: DatasetManagerCore, dataset_id: str, image_path: str) -> bool:
        """
        Update dataset banner image.

        Args:
            dataset_id: Dataset node ID
            image_path: Path to banner image file (PNG, JPG, or GIF)

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Updating banner for dataset {dataset_id}")
        logger.info(f"  Image path: {image_path}")

        if not os.path.exists(image_path):
            logger.error(f"  Banner file not found: {image_path}")
            return False

        if self.dry_run:
            self._log_dry_run(f"Would upload banner from: {image_path}")
            return True

        url = f"{self.api_host}/datasets/{dataset_id}/banner"

        # Determine content type
        ext = os.path.splitext(image_path)[1].lower()
        content_types = {
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".gif": "image/gif"
        }
        content_type = content_types.get(ext, "image/png")

        try:
            with open(image_path, "rb") as img_file:
                files = {
                    "banner": (os.path.basename(image_path), img_file, content_type)
                }
                response = requests.put(
                    url,
                    headers={"Authorization": f"Bearer {self.auth.token}"},
                    files=files
                )
                response.raise_for_status()

            logger.info("  Successfully updated banner")
            return True

        except requests.RequestException as e:
            logger.error(f"  Failed to update banner: {e}")
            return False
