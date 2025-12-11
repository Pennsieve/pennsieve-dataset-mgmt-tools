"""
Reference operations for DatasetManager.

Handles external publications/references (DOIs) management.
"""

import logging

import requests

from .core import DatasetManagerCore

logger = logging.getLogger(__name__)


# Valid DataCite relationship types
RELATIONSHIP_TYPES = [
    "IsCitedBy", "Cites", "IsSupplementTo", "IsSupplementedBy",
    "IsContinuedBy", "Continues", "IsDescribedBy", "Describes",
    "HasMetadata", "IsMetadataFor", "HasVersion", "IsVersionOf",
    "IsNewVersionOf", "IsPreviousVersionOf", "IsPartOf", "HasPart",
    "IsReferencedBy", "References", "IsDocumentedBy", "Documents",
    "IsCompiledBy", "Compiles", "IsVariantFormOf", "IsOriginalFormOf",
    "IsIdenticalTo", "IsReviewedBy", "Reviews", "IsDerivedFrom",
    "IsSourceOf", "IsRequiredBy", "Requires", "IsObsoletedBy", "Obsoletes"
]


class ReferenceOperationsMixin:
    """Mixin providing external publication/reference management operations."""

    def add_reference(
        self: DatasetManagerCore,
        dataset_id: str,
        doi: str,
        relationship_type: str = "IsDescribedBy"
    ) -> bool:
        """
        Add an external publication/reference to a dataset.

        Args:
            dataset_id: Dataset node ID
            doi: DOI string (e.g., "10.1016/j.example.2025.01.001")
            relationship_type: DataCite relationship type (default: "IsDescribedBy")

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Adding reference to dataset {dataset_id}")
        logger.info(f"  DOI: {doi}")
        logger.info(f"  Relationship: {relationship_type}")

        if relationship_type not in RELATIONSHIP_TYPES:
            logger.warning(f"  '{relationship_type}' may not be a valid relationship type")

        if self.dry_run:
            self._log_dry_run(f"Would add reference {doi} as {relationship_type}")
            return True

        url = (
            f"{self.api_host}/datasets/{dataset_id}/external-publications"
            f"?doi={doi}&relationshipType={relationship_type}"
        )

        result = self._make_request("PUT", url)
        if result is not None:
            logger.info(f"  Successfully added reference: {doi}")
            return True

        logger.error("  Failed to add reference")
        return False

    def remove_reference(
        self: DatasetManagerCore,
        dataset_id: str,
        doi: str,
        relationship_type: str = "IsDescribedBy"
    ) -> bool:
        """
        Remove an external publication/reference from a dataset.

        Args:
            dataset_id: Dataset node ID
            doi: DOI string to remove
            relationship_type: DataCite relationship type

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Removing reference from dataset {dataset_id}")
        logger.info(f"  DOI: {doi}")
        logger.info(f"  Relationship: {relationship_type}")

        if self.dry_run:
            self._log_dry_run(f"Would remove reference {doi}")
            return True

        url = (
            f"{self.api_host}/datasets/{dataset_id}/external-publications"
            f"?doi={doi}&relationshipType={relationship_type}"
        )

        try:
            response = requests.delete(url, headers=self.auth.get_headers())

            if response.status_code == 404:
                logger.info(f"  Reference {doi} not found (already removed)")
                return True
            elif response.status_code in [200, 204]:
                logger.info(f"  Successfully removed reference: {doi}")
                return True
            else:
                logger.error(f"  Unexpected status: {response.status_code}")
                return False

        except requests.RequestException as e:
            logger.error(f"  Failed to remove reference: {e}")
            return False
