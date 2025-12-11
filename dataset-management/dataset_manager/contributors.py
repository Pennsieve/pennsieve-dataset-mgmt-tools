"""
Contributor operations for DatasetManager.

Handles adding, removing, and updating dataset contributors.
"""

import logging
from typing import List, Optional

import requests

from .core import DatasetManagerCore

logger = logging.getLogger(__name__)


class ContributorOperationsMixin:
    """Mixin providing contributor management operations."""

    def add_contributor(self: DatasetManagerCore, dataset_id: str, contributor_id: int) -> bool:
        """
        Add a contributor to a dataset.

        Args:
            dataset_id: Dataset node ID
            contributor_id: Contributor ID (integer)

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Adding contributor to dataset {dataset_id}")
        logger.info(f"  Contributor ID: {contributor_id}")

        if self.dry_run:
            self._log_dry_run(f"Would add contributor {contributor_id}")
            return True

        url = f"{self.api_host}/datasets/{dataset_id}/contributors"
        payload = {"contributorId": contributor_id}

        result = self._make_request("PUT", url, json=payload)
        if result is not None:
            logger.info(f"  Successfully added contributor {contributor_id}")
            return True

        logger.error(f"  Failed to add contributor {contributor_id}")
        return False

    def remove_contributor(self: DatasetManagerCore, dataset_id: str, contributor_id: int) -> bool:
        """
        Remove a contributor from a dataset.

        Args:
            dataset_id: Dataset node ID
            contributor_id: Contributor ID to remove

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Removing contributor from dataset {dataset_id}")
        logger.info(f"  Contributor ID: {contributor_id}")

        if self.dry_run:
            self._log_dry_run(f"Would remove contributor {contributor_id}")
            return True

        url = f"{self.api_host}/datasets/{dataset_id}/contributors/{contributor_id}"

        try:
            response = requests.delete(url, headers=self.auth.get_headers())

            if response.status_code == 404:
                logger.info(f"  Contributor {contributor_id} not found (already removed)")
                return True
            elif response.status_code in [200, 204]:
                logger.info(f"  Successfully removed contributor {contributor_id}")
                return True
            else:
                logger.error(f"  Unexpected status: {response.status_code}")
                return False

        except requests.RequestException as e:
            logger.error(f"  Failed to remove contributor: {e}")
            return False

    def update_contributors(
        self: DatasetManagerCore,
        dataset_id: str,
        contributor_ids: List[int],
        remove_ids: Optional[List[int]] = None
    ) -> bool:
        """
        Update dataset contributors (remove then add).

        Args:
            dataset_id: Dataset node ID
            contributor_ids: List of contributor IDs to add
            remove_ids: Optional list of contributor IDs to remove first

        Returns:
            True if all operations succeeded, False otherwise
        """
        logger.info(f"Updating contributors for dataset {dataset_id}")

        success = True

        # Remove contributors first
        if remove_ids:
            for cid in remove_ids:
                if not self.remove_contributor(dataset_id, cid):
                    success = False

        # Add contributors
        for cid in contributor_ids:
            if not self.add_contributor(dataset_id, cid):
                success = False

        return success
