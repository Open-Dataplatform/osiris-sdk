"""
Module to handle datasets IO
"""
import json
import logging
from datetime import datetime
from typing import List, Dict

from azure.core.exceptions import HttpResponseError
from azure.storage.filedatalake import DataLakeFileClient as DataLakeFileClientSync


from ..core.azure_client_authorization import AzureCredential


logger = logging.getLogger(__name__)


class _DataSets:
    """
    Class to handle datasets IO
    """
    # pylint: disable=too-many-arguments
    def __init__(self,
                 account_url: str,
                 filesystem_name: str,
                 source: str,
                 destination: str,
                 credential: AzureCredential):

        self.account_url = account_url
        self.filesystem_name = filesystem_name

        self.source = source
        self.destination = destination

        self.credential = credential

    def read_events_from_destination(self, date: datetime) -> List:
        """
        Read events from destination corresponding a given date
        """

        file_path = f'{self.destination}/year={date.year}/month={date.month:02d}/day={date.day:02d}/data.json'

        with DataLakeFileClientSync(self.account_url,
                                    self.filesystem_name, file_path,
                                    credential=self.credential) as file_client:
            try:
                file_content = file_client.download_file().readall()
                return json.loads(file_content)
            except HttpResponseError as error:
                message = f'({type(error).__name__}) Problems downloading data file: {error}'
                logger.error(message)
                raise Exception(message) from error

    def upload_events_to_destination(self, date: datetime, events: List[Dict]):
        """
        Uploads events to destination based on the given date
        """
        file_path = f'{self.destination}/year={date.year}/month={date.month:02d}/day={date.day:02d}/data.json'
        data = json.dumps(events)
        with DataLakeFileClientSync(self.account_url,
                                    self.filesystem_name,
                                    file_path,
                                    credential=self.credential) as file_client:
            try:
                file_client.upload_data(data, overwrite=True)
            except HttpResponseError as error:
                message = f'({type(error).__name__}) Problems uploading data file: {error}'
                logger.error(message)
                raise Exception(message) from error
