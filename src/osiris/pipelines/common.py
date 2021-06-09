"""
Contains common functions for pipelines modules.
"""

from ..core.azure_client_authorization import ClientAuthorization


def initialize_client_auth(func):
    """
    This function is used to initialize the ClientAuthorization in a lazy way because of Beam.
    """
    def wrapper(self, *args, **kwargs):

        if not self.client_auth:
            self.client_auth = ClientAuthorization(self.tenant_id, self.client_id, self.client_secret)

        return func(self, *args, **kwargs)

    return wrapper
