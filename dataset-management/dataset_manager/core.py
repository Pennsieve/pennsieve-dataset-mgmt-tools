"""
Core functionality for DatasetManager.

Contains base class with authentication, HTTP requests, and dataset discovery.
"""

import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any

import requests

# Set up import paths
_this_dir = Path(__file__).parent
sys.path.insert(0, str(_this_dir.parent))
sys.path.insert(1, str(_this_dir.parent.parent))

from shared.config import API_HOST
from shared.auth import PennsieveAuth

logger = logging.getLogger(__name__)


class DatasetManagerCore:
    """
    Core functionality for Pennsieve dataset operations.

    Provides authentication, HTTP request handling, and dataset discovery.
    """

    def __init__(
        self,
        auth: PennsieveAuth,
        api_host: str = API_HOST,
        dry_run: bool = False
    ):
        self.auth = auth
        self.api_host = api_host
        self.dry_run = dry_run
        self._datasets_cache: Optional[List[Dict]] = None

    def _log_dry_run(self, message: str) -> None:
        """Log a dry-run message."""
        logger.info(f"[DRY RUN] {message}")

    def _make_request(
        self,
        method: str,
        url: str,
        **kwargs
    ) -> Optional[Dict[str, Any]]:
        """
        Make an authenticated API request with error handling.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            url: Full URL for the request
            **kwargs: Additional arguments for requests

        Returns:
            Response JSON as dict, empty dict for no-content success, or None for failure
        """
        try:
            logger.debug(f"API Request: {method} {url}")
            if 'json' in kwargs:
                logger.debug(f"Payload: {kwargs['json']}")

            response = requests.request(
                method,
                url,
                headers=self.auth.get_headers(),
                **kwargs
            )
            response.raise_for_status()

            # Handle empty responses (204 No Content, etc.)
            if not response.text or response.text.strip() == "":
                return {}

            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {method} {url}")
            logger.error(f"Error: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Status: {e.response.status_code}")
                logger.error(f"Response: {e.response.text}")
            return None

    # =========================================================================
    # Dataset Discovery
    # =========================================================================

    def fetch_all_datasets(self, force_reload: bool = False) -> List[Dict]:
        """
        Fetch all datasets from the workspace with pagination.

        Args:
            force_reload: If True, bypass cache and fetch fresh data

        Returns:
            List of dataset objects
        """
        if self._datasets_cache is not None and not force_reload:
            logger.debug(f"Using cached datasets ({len(self._datasets_cache)} datasets)")
            return self._datasets_cache

        datasets = []
        offset = 0
        page_size = 25

        logger.info("Fetching all datasets from Pennsieve...")

        while True:
            url = (
                f"{self.api_host}/datasets/paginated"
                f"?limit={page_size}&offset={offset}"
                f"&orderBy=Name&orderDirection=Asc"
                f"&includeBannerUrl=false&includePublishedDataset=false"
            )

            result = self._make_request("GET", url)
            if not result:
                break

            batch = result.get("datasets", [])
            if not batch:
                break

            datasets.extend(batch)
            total_count = result.get("totalCount", 0)
            logger.debug(f"Fetched {len(datasets)}/{total_count} datasets")

            offset += page_size
            if offset >= total_count:
                break

        logger.info(f"Fetched {len(datasets)} datasets total")
        self._datasets_cache = datasets
        return datasets

    def find_dataset_by_name(self, name: str) -> Optional[Dict]:
        """
        Find a dataset by its name.

        Args:
            name: Dataset name to search for

        Returns:
            Dataset object if found, None otherwise
        """
        datasets = self.fetch_all_datasets()

        for ds in datasets:
            if ds.get("content", {}).get("name") == name:
                return ds

        logger.warning(f"Dataset not found: {name}")
        return None

    def get_dataset_id(self, dataset: Dict) -> str:
        """Extract dataset ID from dataset object."""
        return dataset.get("content", {}).get("id")

    def get_dataset_name(self, dataset: Dict) -> str:
        """Extract dataset name from dataset object."""
        return dataset.get("content", {}).get("name", "Unknown")
