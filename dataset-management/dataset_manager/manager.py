"""
DatasetManager - unified class combining all operations.

This module provides the main DatasetManager class that combines
all operation mixins into a single unified interface.
"""

import logging
from typing import List, Optional

from .core import DatasetManagerCore
from .metadata import MetadataOperationsMixin
from .collaborators import CollaboratorOperationsMixin
from .contributors import ContributorOperationsMixin
from .references import ReferenceOperationsMixin
from .packages import PackageOperationsMixin

logger = logging.getLogger(__name__)


class DatasetManager(
    DatasetManagerCore,
    MetadataOperationsMixin,
    CollaboratorOperationsMixin,
    ContributorOperationsMixin,
    ReferenceOperationsMixin,
    PackageOperationsMixin
):
    """
    Unified manager for Pennsieve dataset operations.

    Supports:
    - Updating dataset name, subtitle (description), tags, license
    - Updating banner image and readme
    - Managing owner, teams, and user collaborators
    - Managing contributors
    - Managing external publications/references
    - Package operations and duplicate cleanup
    """

    def process_dataset(
        self,
        dataset_name: str,
        name: Optional[str] = None,
        subtitle: Optional[str] = None,
        tags: Optional[List[str]] = None,
        add_tags: Optional[List[str]] = None,
        remove_tags: Optional[List[str]] = None,
        license_name: Optional[str] = None,
        readme: Optional[str] = None,
        banner: Optional[str] = None,
        owner: Optional[str] = None,
        add_team: Optional[str] = None,
        add_team_role: str = "viewer",
        remove_team: Optional[str] = None,
        add_user: Optional[str] = None,
        add_user_role: str = "viewer",
        remove_user: Optional[str] = None,
        contributors: Optional[List[int]] = None,
        remove_contributors: Optional[List[int]] = None,
        add_reference: Optional[str] = None,
        reference_type: str = "IsDescribedBy",
        remove_reference: Optional[str] = None
    ) -> bool:
        """
        Process a single dataset with specified updates.

        Only applies updates for parameters that are explicitly provided.

        Returns:
            True if all operations succeeded, False otherwise
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing dataset: {dataset_name}")
        logger.info(f"{'='*60}")

        # Find dataset
        dataset = self.find_dataset_by_name(dataset_name)
        if not dataset:
            logger.error(f"Dataset not found: {dataset_name}")
            return False

        dataset_id = self.get_dataset_id(dataset)
        logger.info(f"Dataset ID: {dataset_id}")

        success = True
        actions = 0

        # Metadata updates
        if name is not None:
            if not self.update_name(dataset_id, name):
                success = False
            actions += 1

        if subtitle is not None:
            if not self.update_subtitle(dataset_id, subtitle):
                success = False
            actions += 1

        if tags is not None:
            if not self.update_tags(dataset_id, tags):
                success = False
            actions += 1

        if add_tags:
            for tag in add_tags:
                if not self.add_tag(dataset_id, tag):
                    success = False
                actions += 1

        if remove_tags:
            for tag in remove_tags:
                if not self.remove_tag(dataset_id, tag):
                    success = False
                actions += 1

        if license_name is not None:
            if not self.update_license(dataset_id, license_name):
                success = False
            actions += 1

        if readme is not None:
            if not self.update_readme(dataset_id, readme):
                success = False
            actions += 1

        if banner is not None:
            if not self.update_banner(dataset_id, banner):
                success = False
            actions += 1

        # Owner & collaborators
        if owner is not None:
            if not self.update_owner(dataset_id, owner):
                success = False
            actions += 1

        if add_team is not None:
            if not self.add_team(dataset_id, add_team, add_team_role):
                success = False
            actions += 1

        if remove_team is not None:
            if not self.remove_team(dataset_id, remove_team):
                success = False
            actions += 1

        if add_user is not None:
            if not self.add_user(dataset_id, add_user, add_user_role):
                success = False
            actions += 1

        if remove_user is not None:
            if not self.remove_user(dataset_id, remove_user):
                success = False
            actions += 1

        # Contributors
        if contributors is not None or remove_contributors is not None:
            if not self.update_contributors(
                dataset_id,
                contributors or [],
                remove_contributors
            ):
                success = False
            actions += 1

        # References
        if add_reference is not None:
            if not self.add_reference(dataset_id, add_reference, reference_type):
                success = False
            actions += 1

        if remove_reference is not None:
            if not self.remove_reference(dataset_id, remove_reference, reference_type):
                success = False
            actions += 1

        if actions == 0:
            logger.info("  No update actions specified")

        logger.info(f"  Completed with {actions} action(s), success={success}")
        return success
