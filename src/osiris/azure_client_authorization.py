"""
Contains functions to authorize a client against Azure storage
"""
import time

import msal
from azure.core.credentials import AccessToken


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
        self.expires_on = self.EXPIRES_IN + time.time()

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
        self.expires_on = self.EXPIRES_IN + time.time()

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

        self.confidential_client_app = msal.ConfidentialClientApplication(
            authority=f'https://login.microsoftonline.com/{tenant_id}',
            client_id=client_id,
            client_credential=client_secret
        )

        self.scopes = ['https://storage.azure.com/.default']

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

        return result['access_token']
