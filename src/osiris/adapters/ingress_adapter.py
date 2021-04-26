"""
Osiris-Ingress Adapter Framework.
"""
import abc
import logging
import sys
from datetime import datetime
from io import BytesIO

from ..apis.ingress import Ingress


logger = logging.getLogger(__name__)


class IngressAdapter:
    """
    This class is the basis class for Osiris-Ingress adapters. The class is implemented as a framework
    and it is the responsibility of the specific adapters who extends this class to provide the implementation
    for the method 'retrieve_data'.
    """
    __meta__ = abc.ABCMeta

    # pylint: disable=too-many-arguments
    def __init__(self, ingress_url: str, tenant_id: str, client_id: str, client_secret: str, dataset_guid: str):
        """
        :param ingress_url: The URL to the Osiris-ingress API.
        :param tenant_id: The tenant ID representing the organisation.
        :param client_id: The client ID (a string representing a GUID).
        :param client_secret: The client secret string.
        :param dataset_guid: The GUID for the dataset.
        """
        if None in [ingress_url, tenant_id, client_id, client_secret, dataset_guid]:
            logger.error('One or more arguments are None')
            raise TypeError

        self.ingress = Ingress(ingress_url, tenant_id, client_id, client_secret, dataset_guid)
        logger.debug('initialized')

    @abc.abstractmethod
    def retrieve_data(self) -> bytes:
        """
        Subclasses must implement this method to provide the data to be ingested to the DataPlatform using
        this Osiris-ingress API. The data must be converted to a bytes string.
        """

        return b''

    @staticmethod
    def __get_filename() -> str:
        return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    def upload_json_data(self, schema_validate: bool):
        """
        Makes a single run of the framework and ingest the data from the 'retrieve_data' method to the Platform
        with the file extension .json.
        """
        logger.debug('upload_json_data called')
        data = self.retrieve_data()
        file = BytesIO(data)
        file.name = self.__get_filename() + '.json'

        try:
            self.ingress.upload_json_file(file, schema_validate)
        except Exception as error:  # pylint: disable=broad-except
            logger.error('Exception occurred while running upload_json_data: %s', str(error))
            sys.exit(-1)

    def upload_data(self, file_extension: str):
        """
        Makes a single run of the framework and ingest the data from the 'retrieve_data' method to the Platform
        with the given file extension.
        """
        data = self.retrieve_data()
        file = BytesIO(data)
        file.name = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ') + '.' + file_extension

        try:
            self.ingress.upload_file(file)
        except Exception as error:  # pylint: disable=broad-except
            logger.error('Exception occurred while running upload_data: %s', str(error))
            sys.exit(-1)
