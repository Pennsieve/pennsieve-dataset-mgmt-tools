"""
Pennsieve Authentication Module

Handles authentication via Cognito using API key + secret to obtain a Bearer token.
All API calls should use this token-based authentication.
"""

import logging
from typing import Optional, Dict

import boto3
import requests

from .config import API_HOST

logger = logging.getLogger(__name__)


class PennsieveAuth:
    """
    Handle Pennsieve authentication via Cognito.

    Usage:
        auth = PennsieveAuth()
        auth.authenticate(api_key, api_secret)

        # Then use auth.token or auth.get_headers() for API calls
        response = requests.get(url, headers=auth.get_headers())
    """

    def __init__(self, api_host: str = API_HOST):
        self.api_host = api_host
        self._token: Optional[str] = None

    def authenticate(self, api_key: str, api_secret: str) -> str:
        """
        Authenticate with Pennsieve using API key and secret.

        Args:
            api_key: Pennsieve API key
            api_secret: Pennsieve API secret

        Returns:
            Access token string

        Raises:
            Exception: If authentication fails
        """
        logger.info("Authenticating with Pennsieve...")
        url = f"{self.api_host}/authentication/cognito-config"

        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            cognito_app_client_id = data["tokenPool"]["appClientId"]
            cognito_region = data["region"]

            logger.debug(f"Cognito region: {cognito_region}")
            logger.debug(f"App client ID: {cognito_app_client_id}")

            cognito_idp_client = boto3.client(
                "cognito-idp",
                region_name=cognito_region,
                aws_access_key_id="",
                aws_secret_access_key="",
            )

            login_response = cognito_idp_client.initiate_auth(
                AuthFlow="USER_PASSWORD_AUTH",
                AuthParameters={"USERNAME": api_key, "PASSWORD": api_secret},
                ClientId=cognito_app_client_id,
            )

            self._token = login_response["AuthenticationResult"]["AccessToken"]
            logger.info("Authentication successful")
            return self._token

        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            raise

    @property
    def token(self) -> str:
        """Get the current access token."""
        if not self._token:
            raise ValueError("Not authenticated. Call authenticate() first.")
        return self._token

    @property
    def is_authenticated(self) -> bool:
        """Check if currently authenticated."""
        return self._token is not None

    def get_headers(self) -> Dict[str, str]:
        """
        Get headers for authenticated API requests.

        Returns:
            Dict with Authorization header and content type
        """
        if not self._token:
            raise ValueError("Not authenticated. Call authenticate() first.")
        return {
            "accept": "application/json",
            "content-type": "application/json",
            "Authorization": f"Bearer {self._token}",
        }


# Global auth instance for convenience
_global_auth: Optional[PennsieveAuth] = None


def get_auth() -> PennsieveAuth:
    """Get the global auth instance."""
    global _global_auth
    if _global_auth is None:
        _global_auth = PennsieveAuth()
    return _global_auth


def authenticate(api_key: str, api_secret: str) -> str:
    """
    Authenticate using global auth instance.

    Convenience function for scripts that don't need multiple auth contexts.

    Args:
        api_key: Pennsieve API key
        api_secret: Pennsieve API secret

    Returns:
        Access token string
    """
    auth = get_auth()
    return auth.authenticate(api_key, api_secret)


def get_headers() -> Dict[str, str]:
    """
    Get headers from global auth instance.

    Convenience function for scripts that don't need multiple auth contexts.

    Returns:
        Dict with Authorization header

    Raises:
        ValueError: If not authenticated
    """
    return get_auth().get_headers()


def get_token() -> str:
    """
    Get token from global auth instance.

    Returns:
        Access token string

    Raises:
        ValueError: If not authenticated
    """
    return get_auth().token
