"""
Module for common pipeline classes and functions
"""
from ..core.enums import TimeResolution
from .azure_data_storage import DataSets


# pylint: disable=too-few-public-methods
class OsirisPipeline:
    """
    Superclass for pipelines. This class contains common inputs to the pipelines.
    """

    # pylint: disable=too-many-arguments
    def __init__(self,
                 storage_account_url: str,
                 filesystem_name: str,
                 tenant_id: str,
                 client_id: str,
                 client_secret: str,
                 source_dataset_guid: str,
                 destination_dataset_guid: str,
                 time_resolution: TimeResolution):
        """
        :param storage_account_url: The URL to Azure storage account.
        :param filesystem_name: The name of the filesystem.
        :param tenant_id: The tenant ID representing the organisation.
        :param client_id: The client ID (a string representing a GUID).
        :param client_secret: The client secret string.
        :param source_dataset_guid: The GUID for the source dataset.
        :param destination_dataset_guid: The GUID for the destination dataset.
        :param time_resolution: The time resolution to store the data in the destination dataset with.
        """

        if None in [storage_account_url, filesystem_name, tenant_id, client_id, client_secret,
                    source_dataset_guid, destination_dataset_guid, time_resolution]:
            raise TypeError

        self.storage_account_url = storage_account_url
        self.filesystem_name = filesystem_name
        self.source_dataset_guid = source_dataset_guid
        self.time_resolution = time_resolution

        self.datasets = DataSets(tenant_id, client_id, client_secret, storage_account_url, filesystem_name,
                                 source_dataset_guid, destination_dataset_guid, time_resolution)
