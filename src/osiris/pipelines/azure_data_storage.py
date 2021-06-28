"""
Module to handle datasets IO
"""
import json
import logging
from datetime import datetime
from typing import List, Dict, AnyStr

from azure.core.exceptions import HttpResponseError

from .common import initialize_client_auth
from ..core.enums import TimeResolution
from ..core.io import get_file_path_with_respect_to_time_resolution, OsirisFileClient

logger = logging.getLogger(__name__)


# pylint: disable=too-many-instance-attributes
class DataSets:
    """
    Class to handle datasets IO
    """
    # pylint: disable=too-many-arguments
    def __init__(self,
                 tenant_id: str,
                 client_id: str,
                 client_secret: str,
                 account_url: str,
                 filesystem_name: str,
                 source: str,
                 destination: str,
                 time_resolution: TimeResolution):

        if None in [tenant_id, client_id, client_secret, account_url, filesystem_name, source,
                    destination, time_resolution]:
            raise TypeError

        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.account_url = account_url
        self.filesystem_name = filesystem_name
        self.source = source
        self.destination = destination
        self.time_resolution = time_resolution

        # We need to initialize the ClientAuthorization using lazy initializing because of Beam. Beam pickles
        # this object and pickles only allow simples values as instance variables and not objects.
        self.client_auth = None

    @initialize_client_auth
    def read_events_from_destination(self, date: datetime) -> List:
        """
        Read events from destination corresponding a given date
        """

        sub_file_path = get_file_path_with_respect_to_time_resolution(date, self.time_resolution, "data.json")
        file_path = f'{self.destination}/{sub_file_path}'

        with OsirisFileClient(self.account_url,
                              self.filesystem_name,
                              file_path,
                              credential=self.client_auth.get_credential_sync()) as file_client:  # type: ignore

            file_content = file_client.download_file().readall()
            return json.loads(file_content)

    @initialize_client_auth
    def upload_events_to_destination_json(self, date: datetime, events: List[Dict]):
        """
        Uploads events to destination based on the given date
        """
        sub_file_path = get_file_path_with_respect_to_time_resolution(date, self.time_resolution, "data.json")
        file_path = f'{self.destination}/{sub_file_path}'

        data = json.dumps(events)
        with OsirisFileClient(self.account_url,
                              self.filesystem_name,
                              file_path,
                              credential=self.client_auth.get_credential_sync()) as file_client:  # type: ignore
            try:
                file_client.upload_data(data, overwrite=True)
            except HttpResponseError as error:
                message = f'({type(error).__name__}) Problems uploading data file: {error}'
                logger.error(message)
                raise Exception(message) from error

    @initialize_client_auth
    def upload_data_to_destination(self, date: datetime, data: AnyStr, filename: str):
        """
        Uploads arbitrary `AnyStr` data to destination based on the given date
        """
        sub_file_path = get_file_path_with_respect_to_time_resolution(date, self.time_resolution, filename)
        file_path = f'{self.destination}/{sub_file_path}'

        with OsirisFileClient(self.account_url,
                              self.filesystem_name,
                              file_path,
                              credential=self.client_auth.get_credential_sync()) as file_client:  # type: ignore
            try:
                file_client.upload_data(data, overwrite=True)
            except HttpResponseError as error:
                message = f'({type(error).__name__}) Problems uploading data file: {error}'
                logger.error(message)
                raise Exception(message) from error
