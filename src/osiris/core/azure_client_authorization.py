"""
Contains functions to authorize a client against Azure storage
"""
import logging
import time
import os
import atexit

import msal

from azure.core.credentials import AccessToken


logger = logging.getLogger(__name__)


class AzureCredential:  # pylint: disable=too-few-public-methods
    """
    Represents a sync Credential object. This is a hack to use a access token
    received from a client.
    """

    # NOTE: This doesn't necessarily correspond to the token lifetime,
    # however it doesn't matter as it gets recreated per request
    EXPIRES_IN = 1000

    def __init__(self, token: str):
        self.token = token
        self.expires_on = int(self.EXPIRES_IN + time.time())

    def get_token(self, *scopes, **kwargs) -> AccessToken:  # pylint: disable=unused-argument
        """
        Returns an AcccesToken object.
        """
        return AccessToken(self.token, self.expires_on)


class AzureCredentialAIO:  # pylint: disable=too-few-public-methods
    """
    Represents a async Credential object. This is a hack to use a access token
    received from a client.
    """

    # NOTE: This doesn't necessarily correspond to the token lifetime,
    # however it doesn't matter as it gets recreated per request
    EXPIRES_IN = 1000

    def __init__(self, token: str):
        self.token = token
        self.expires_on = int(self.EXPIRES_IN + time.time())

    async def get_token(self, *scopes, **kwargs) -> AccessToken:  # pylint: disable=unused-argument
        """
        Returns an AcccesToken object.
        """
        return AccessToken(self.token, self.expires_on)


class ClientAuthorization:
    """
    Class to authenticate client against Azure storage
    """
    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        """
        :param tenant_id: The tenant ID representing the organisation.
        :param client_id: The client ID (a string representing a GUID).
        :param client_secret: The client secret string.
        """
        if None in [tenant_id, client_id, client_secret]:
            raise TypeError

        cache = msal.SerializableTokenCache()
        if os.path.exists("my_cache.bin"):
            cache.deserialize(open("my_cache.bin", "r").read())
        atexit.register(lambda:
                        open("my_cache.bin", "w").write(cache.serialize())
                        # Hint: The following optional line persists only when state changed
                        if cache.has_state_changed else None
                        )

        self.confidential_client_app = msal.ConfidentialClientApplication(
            authority=f'https://login.microsoftonline.com/{tenant_id}',
            client_id=client_id,
            client_credential=client_secret,
            token_cache=cache
        )

        self.scopes = ['https://storage.azure.com/.default']

        self.result = None

    def get_credential_sync(self) -> AzureCredential:
        """
        Returns Azure credentials for sync methods.
        """
        return AzureCredential(self.get_access_token())

    def get_credential_async(self) -> AzureCredentialAIO:
        """
        Returns Azure credentials for async methods.
        """
        return AzureCredentialAIO(self.get_access_token())

    def get_access_token(self) -> str:
        """
        Returns Azure access token.
        """
        result = self.confidential_client_app.acquire_token_silent(self.scopes, account=None)

        if not result:
            result = self.confidential_client_app.acquire_token_for_client(scopes=self.scopes)

        try:
            return result['access_token']
        except KeyError as error:
            message = f'Unauthorized client: {result["error_description"]}'
            logger.error(message)
            raise PermissionError(message) from error
