"""
Module to handle datasets IO
"""
import json
from datetime import datetime
from typing import List, Dict

from azure.storage.filedatalake import DataLakeFileClient as DataLakeFileClientSync


from src.osiris.core.azure_client_authorization import AzureCredential


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
            file_content = file_client.download_file().readall()
            return json.loads(file_content)

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
            file_client.upload_data(data, overwrite=True)
