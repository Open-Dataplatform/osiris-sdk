"""
Osiris-Ingress Adapter Framework.

"""
import abc
import logging
import sys
from io import BytesIO
from typing import Optional

from ..apis.ingress import Ingress
from ..core.azure_client_authorization import ClientAuthorization

logger = logging.getLogger(__name__)


class IngressAdapter:
    """
    This class is the basis class for Osiris-Ingress adapters. The class is implemented as a framework
    and it is the responsibility of the specific adapters who extends this class to provide the implementation
    for the method 'retrieve_data'.

    The adapter has to main use cases:
    1. Adapter -> Ingress guid -> Transformation -> Egress guid
    2. Adapter -> Egress guid (no transformation needed)

    General note:
    The IngressAdapter handles the normal case where each run of the adapter will generate one file in a
    Ingress guid or Egress guid (dependent on case 1 or 2).

    If you need to make a more general adapter where each run makes more than one file in storage, then
    this is not the framework to use.

    Notes on case 1 and 2.
    Case 1 (Adapter -> Ingress guid -> Transformation -> Egress guid): This is the normal case where
    data needs a transformation afterwards. There is not a 1-1 mapping from data fetched in the adapter
    to where in storage it should be saved.

    Case 2 (Adapter -> Egress guid): This is a special case, where the data retrieved in the adapter
    needs to be written in one specific place in storage. Please note, that if there is data already
    it will be overwritten.
    """
    __meta__ = abc.ABCMeta

    # pylint: disable=too-many-arguments
    def __init__(self, client_auth: ClientAuthorization, ingress_url: str, dataset_guid: str):
        """
        :param client_auth: The Client Authorization to access the dataset.
        :param ingress_url: The URL to the Osiris-ingress API.
        :param dataset_guid: The GUID for the dataset.
        """
        if None in [client_auth, ingress_url, dataset_guid]:
            logger.error('One or more arguments are None')
            raise TypeError

        self.ingress = Ingress(client_auth, ingress_url, dataset_guid)
        logger.debug('initialized')

    @abc.abstractmethod
    def retrieve_data(self) -> Optional[bytes]:
        """
        Subclasses must implement this method to provide the data to be ingested to the DataPlatform using
        this Osiris-ingress API. The data must be converted to a bytes string.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_filename(self) -> str:
        """
        Subclasses must implement this method to provide the filename to be used when ingesting to the DataPlatform
        using this Osiris-ingress API. The data must be converted to a bytes string.
        """

        # this is one example of a filename
        # return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ') + '.json'
        raise NotImplementedError

    @abc.abstractmethod
    def get_event_time(self) -> str:
        """
        Subclasses must implement this method to provide the event time to be used when ingesting to the
        DataPlatform using this Osiris-ingress API. This function only needs to be implemented if you want to
        ingress using event time.
        """

        # this is one example of a datetime
        # return '2021-03-04T21:30:20'
        raise NotImplementedError

    def upload_json_data(self, schema_validate: bool):
        """
        Makes a single run of the framework and ingest the data from the 'retrieve_data' method to the Platform
        using the upload_json endpoint
        """
        logger.debug('upload_json_data called')
        data = self.retrieve_data()

        if not data:
            return

        file = BytesIO(data)
        file.name = self.get_filename()

        try:
            self.ingress.upload_json_file(file, schema_validate)
        except Exception as error:  # pylint: disable=broad-except
            logger.error('Exception occurred while running upload_json_data: %s', str(error))
            sys.exit(-1)

    def upload_data(self):
        """
        Makes a single run of the framework and ingest the data from the 'retrieve_data' method to the Platform
        using the upload endpoint
        """
        data = self.retrieve_data()

        if not data:
            return

        file = BytesIO(data)
        file.name = self.get_filename()

        try:
            self.ingress.upload_file(file)
        except Exception as error:  # pylint: disable=broad-except
            logger.error('Exception occurred while running upload_data: %s', str(error))
            sys.exit(-1)

    def upload_json_data_event_time(self, schema_validate: bool):
        """
        Makes a single run of the framework and ingest the data from the 'retrieve_data' method to the Platform
        using the json upload with a path corresponding to the given event time.
        """
        logger.debug('upload_json_data_event_time called')
        data = self.retrieve_data()

        if not data:
            return

        file = BytesIO(data)
        file.name = self.get_filename()

        try:
            self.ingress.upload_json_file_event_time(file, self.get_event_time(), schema_validate)
        except Exception as error:  # pylint: disable=broad-except
            logger.error('Exception occurred while running upload_json_data: %s', str(error))
            sys.exit(-1)

    def upload_data_event_time(self):
        """
        Makes a single run of the framework and ingest the data from the 'retrieve_data' method to the Platform
        using the upload with a path corresponding to the given event time.
        """
        data = self.retrieve_data()

        if not data:
            return

        file = BytesIO(data)
        file.name = self.get_filename()

        try:
            self.ingress.upload_file_event_time(file, self.get_event_time())
        except Exception as error:  # pylint: disable=broad-except
            logger.error('Exception occurred while running upload_data: %s', str(error))
            sys.exit(-1)
