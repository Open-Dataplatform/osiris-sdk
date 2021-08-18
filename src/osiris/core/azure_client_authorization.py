"""
Contains functions to authorize a client against Azure storage
"""
import datetime as datetime
import logging

import msal
from azure.core.credentials import AccessToken
from azure.identity import ClientSecretCredential as ClientSecretCredentialSync
from azure.identity.aio import ClientSecretCredential as ClientSecretCredentialASync
from typing import Optional


logger = logging.getLogger(__name__)


class TokenCredential(ClientSecretCredentialSync):  # pylint: disable=too-few-public-methods
    """
    Represents a sync Credential object.
    """
    def __init__(self, token: AccessToken):
        self.token = token

    def get_token(self, *scopes, **kwargs) -> AccessToken:  # pylint: disable=unused-argument
        """
        Returns an AccessToken object.
        """
        return self.token


class TokenCredentialAIO(ClientSecretCredentialASync):  # pylint: disable=too-few-public-methods
    """
    Represents an async Credential object.
    """
    def __init__(self, token: AccessToken):
        self.token = token

    async def get_token(self, *scopes, **kwargs) -> AccessToken:  # pylint: disable=unused-argument
        """
        Returns an AccessToken object.
        """
        return self.token


class ClientAuthorization:
    """
    Class to authenticate client against Azure storage. Uses EITHER a service principal approach with tenant id,
    client id and client secret OR a supplied access token.
    """
    def __init__(self, tenant_id: str, client_id: str, client_secret: str, access_token: AccessToken = None):
        """
        :param tenant_id: The tenant ID representing the organisation.
        :param client_id: The client ID (a string representing a GUID).
        :param client_secret: The client secret string.
        :param access_token: An access token directly provided by the caller
        """
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = access_token

        self.confidential_client_app: Optional[msal.ConfidentialClientApplication] = None
        self.scopes = ['https://storage.azure.com/.default']
        if access_token:
            if None not in [tenant_id, client_id, client_secret]:
                logger.error("Client Authorization must be done with either access token OR tenant_id, client_id " +
                             "and client_secret. Cannot use both approaches")
                raise TypeError
            expire_date = datetime.datetime.fromtimestamp(access_token.expires_on)
            logger.info(f'Using access token value for client authorization. It expires at {expire_date}')
        else:
            if not client_id:
                raise ValueError("client_id should be the id of an Azure Active Directory application")
            if not client_secret:
                raise ValueError("secret should be an Azure Active Directory application's client secret")
            if not tenant_id:
                raise ValueError(
                    "tenant_id should be an Azure Active Directory tenant's id (also called its 'directory id')"
                )

    def get_credential_sync(self) -> ClientSecretCredentialSync:
        """
        Returns Azure credentials for sync methods.
        """
        if self.access_token:
            return TokenCredential(self.access_token)
        return ClientSecretCredentialSync(self.tenant_id, self.client_id, self.client_secret)

    def get_credential_async(self) -> ClientSecretCredentialASync:
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
            return TokenCredentialAIO(self.access_token)
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
            return self.access_token.token

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
