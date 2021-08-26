"""
Contains functions to authorize a client against Azure storage
"""
import logging
import time
from typing import Optional, Union

import msal
from azure.core.credentials import AccessToken
from azure.identity import ClientSecretCredential as ClientSecretCredentialSync
from azure.identity.aio import ClientSecretCredential as ClientSecretCredentialASync

logger = logging.getLogger(__name__)


class AzureCredential:  # pylint: disable=too-few-public-methods
    """
    Represents a sync Credential object. This is a hack to use a access token
    received from a client.
    """

    # NOTE: This doesn't necessarily correspond to the token lifetime,
    # however it doesn't matter as it gets recreated per request
    EXPIRES_IN = 3600

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
    EXPIRES_IN = 3600

    def __init__(self, token: str):
        self.token = token
        self.expires_on = int(self.EXPIRES_IN + time.time())

    async def get_token(self, *scopes, **kwargs) -> AccessToken:  # pylint: disable=unused-argument
        """
        Returns an AcccesToken object.
        """
        return AccessToken(self.token, self.expires_on)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class ClientAuthorization:
    """
    Class to authenticate client against Azure storage. Uses EITHER a service principal approach with tenant id,
    client id and client secret OR a supplied access token.
    """
    def __init__(self, tenant_id: str = None, client_id: str = None, client_secret: str = None,
                 access_token: str = None):
        """
        :param tenant_id: The tenant ID representing the organisation.
        :param client_id: The client ID (a string representing a GUID).
        :param client_secret: The client secret string.
        :param access_token: An access token directly provided by the caller.
        """
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = access_token

        self.confidential_client_app: Optional[msal.ConfidentialClientApplication] = None
        self.scopes = ['https://storage.azure.com/.default']
        if access_token:
            if any([tenant_id, client_id, client_secret]):
                raise TypeError("Client Authorization must be done with either access token OR tenant_id, client_id "
                                "and client_secret. Cannot use both approaches")
            logger.info('Using access token value for client authorization')
        else:
            if not client_id:
                raise ValueError("client_id should be the id of an Azure Active Directory application")
            if not client_secret:
                raise ValueError("secret should be an Azure Active Directory application's client secret")
            if not tenant_id:
                raise ValueError(
                    "tenant_id should be an Azure Active Directory tenant's id (also called its 'directory id')"
                )

    def get_credential_sync(self) -> Union[ClientSecretCredentialSync, AzureCredential]:
        """
        Returns Azure credentials for sync methods.
        """
        if self.access_token:
            return AzureCredential(self.access_token)
        return ClientSecretCredentialSync(self.tenant_id, self.client_id, self.client_secret)

    def get_credential_async(self) -> Union[ClientSecretCredentialASync, AzureCredentialAIO]:
        """
        Returns Azure credentials for async methods.

        Usage example (to ensure that close is called):
        async with self.client_auth.get_credential_async() as credentials:
            async with OsirisFileClientAsync(self.account_url,
                                             self.filesystem_name, file_path,
                                             credential=credentials) as file_client:
                pass
        """
        if self.access_token:
            return AzureCredentialAIO(self.access_token)
        return ClientSecretCredentialASync(self.tenant_id, self.client_id, self.client_secret)

    def get_local_copy(self):
        """
        Returns a local copy of ClientAuthorization
        """
        return ClientAuthorization(self.tenant_id, self.client_id, self.client_secret, self.access_token)

    def get_access_token(self) -> str:
        """
        Returns Azure access token.
        """
        if self.access_token:
            return self.access_token

        # We lazyload this in order to keep it local
        if self.confidential_client_app is None:
            self.confidential_client_app = msal.ConfidentialClientApplication(
                authority=f'https://login.microsoftonline.com/{self.tenant_id}',
                client_id=self.client_id,
                client_credential=self.client_secret
            )

        result = self.confidential_client_app.acquire_token_silent(self.scopes, account=None)

        if not result:
            result = self.confidential_client_app.acquire_token_for_client(scopes=self.scopes)

        try:
            return result['access_token']
        except KeyError as error:
            message = f'Unauthorized client: {result["error_description"]}'
            logger.error(message)
            raise PermissionError(message) from error
