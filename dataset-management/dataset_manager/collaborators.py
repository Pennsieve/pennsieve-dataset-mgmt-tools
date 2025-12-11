"""
Collaborator operations for DatasetManager.

Handles owner, team, and user collaborator management.
"""

import logging

import requests

from .core import DatasetManagerCore

logger = logging.getLogger(__name__)


class CollaboratorOperationsMixin:
    """Mixin providing owner and collaborator management operations."""

    def update_owner(self: DatasetManagerCore, dataset_id: str, owner_id: str) -> bool:
        """
        Update dataset owner.

        Args:
            dataset_id: Dataset node ID
            owner_id: New owner's user node ID (e.g., "N:user:xxxx-xxxx")

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Updating owner for dataset {dataset_id}")
        logger.info(f"  New owner: {owner_id}")

        if self.dry_run:
            self._log_dry_run(f"Would update owner to: {owner_id}")
            return True

        url = f"{self.api_host}/datasets/{dataset_id}/collaborators/owner"
        payload = {
            "id": owner_id,
            "role": "owner"
        }

        result = self._make_request("PUT", url, json=payload)
        if result is not None:
            logger.info(f"  Successfully updated owner to: {owner_id}")
            return True

        logger.error("  Failed to update owner")
        return False

    def add_team(
        self: DatasetManagerCore,
        dataset_id: str,
        team_id: str,
        role: str = "viewer"
    ) -> bool:
        """
        Add a team as collaborator to a dataset.

        Args:
            dataset_id: Dataset node ID
            team_id: Team node ID (e.g., "N:team:xxxx-xxxx")
            role: Role for the team ("viewer", "editor", or "manager")

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Adding team to dataset {dataset_id}")
        logger.info(f"  Team: {team_id}")
        logger.info(f"  Role: {role}")

        if role not in ["viewer", "editor", "manager"]:
            logger.error(f"  Invalid role: {role}. Must be viewer, editor, or manager")
            return False

        if self.dry_run:
            self._log_dry_run(f"Would add team {team_id} as {role}")
            return True

        url = f"{self.api_host}/datasets/{dataset_id}/collaborators/teams"
        payload = {
            "id": team_id,
            "role": role
        }

        result = self._make_request("PUT", url, json=payload)
        if result is not None:
            logger.info(f"  Successfully added team {team_id} as {role}")
            return True

        logger.error("  Failed to add team")
        return False

    def remove_team(self: DatasetManagerCore, dataset_id: str, team_id: str) -> bool:
        """
        Remove a team from dataset collaborators.

        Args:
            dataset_id: Dataset node ID
            team_id: Team node ID to remove

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Removing team from dataset {dataset_id}")
        logger.info(f"  Team: {team_id}")

        if self.dry_run:
            self._log_dry_run(f"Would remove team {team_id}")
            return True

        url = f"{self.api_host}/datasets/{dataset_id}/collaborators/teams"
        payload = {"id": team_id}

        try:
            response = requests.delete(
                url,
                headers=self.auth.get_headers(),
                json=payload
            )

            if response.status_code == 404:
                logger.info(f"  Team {team_id} not found (already removed)")
                return True
            elif response.status_code in [200, 204]:
                logger.info(f"  Successfully removed team {team_id}")
                return True
            else:
                logger.error(f"  Unexpected status: {response.status_code}")
                return False

        except requests.RequestException as e:
            logger.error(f"  Failed to remove team: {e}")
            return False

    def add_user(
        self: DatasetManagerCore,
        dataset_id: str,
        user_id: str,
        role: str = "viewer"
    ) -> bool:
        """
        Add a user as collaborator to a dataset.

        Args:
            dataset_id: Dataset node ID
            user_id: User node ID (e.g., "N:user:xxxx-xxxx")
            role: Role for the user ("viewer", "editor", or "manager")

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Adding user to dataset {dataset_id}")
        logger.info(f"  User: {user_id}")
        logger.info(f"  Role: {role}")

        if role not in ["viewer", "editor", "manager"]:
            logger.error(f"  Invalid role: {role}. Must be viewer, editor, or manager")
            return False

        if self.dry_run:
            self._log_dry_run(f"Would add user {user_id} as {role}")
            return True

        url = f"{self.api_host}/datasets/{dataset_id}/collaborators/users"
        payload = {
            "id": user_id,
            "role": role
        }

        result = self._make_request("PUT", url, json=payload)
        if result is not None:
            logger.info(f"  Successfully added user {user_id} as {role}")
            return True

        logger.error("  Failed to add user")
        return False

    def remove_user(self: DatasetManagerCore, dataset_id: str, user_id: str) -> bool:
        """
        Remove a user from dataset collaborators.

        Args:
            dataset_id: Dataset node ID
            user_id: User node ID to remove

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Removing user from dataset {dataset_id}")
        logger.info(f"  User: {user_id}")

        if self.dry_run:
            self._log_dry_run(f"Would remove user {user_id}")
            return True

        url = f"{self.api_host}/datasets/{dataset_id}/collaborators/users"
        payload = {"id": user_id}

        try:
            response = requests.delete(
                url,
                headers=self.auth.get_headers(),
                json=payload
            )

            if response.status_code == 404:
                logger.info(f"  User {user_id} not found (already removed)")
                return True
            elif response.status_code in [200, 204]:
                logger.info(f"  Successfully removed user {user_id}")
                return True
            else:
                logger.error(f"  Unexpected status: {response.status_code}")
                return False

        except requests.RequestException as e:
            logger.error(f"  Failed to remove user: {e}")
            return False
