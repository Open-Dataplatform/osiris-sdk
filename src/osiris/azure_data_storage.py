"""
Module to handle datasets IO
"""
import configparser
import json
from datetime import datetime
from typing import List, Dict

from azure.storage.filedatalake import DataLakeFileClient as DataLakeFileClientSync


from src.osiris.azure_client_authorization import ClientAuthorization


class _DataSets:
    """
    Class to handle datasets IO
    """
    def __init__(self,
                 account_url: str,
                 filesystem_name: str,
                 source: str,
                 destination: str,
                 client_auth: ClientAuthorization):
        self.config = configparser.ConfigParser()
        self.config.read('conf.ini')

        self.account_url = account_url
        self.filesystem_name = filesystem_name

        self.source = source
        self.destination = destination

        self.client_auth = client_auth

    def read_events_from_destination(self, date: datetime) -> List:
        """
        Read events from destination corresponding a given date
        """
        credential = self.client_auth.get_credential_sync()

        file_path = f'{self.destination}/year={date.year}/month={date.month:02d}/day={date.day:02d}/data.json'

        with DataLakeFileClientSync(self.account_url,
                                    self.filesystem_name, file_path,
                                    credential=credential) as file_client:
            file_content = file_client.download_file().readall()
            return json.loads(file_content)

    def upload_events_to_destination(self, date: datetime, events: List[Dict]):
        """
        Uploads events to destination based on the given date
        """
        credential = self.client_auth.get_credential_sync()

        file_path = f'{self.destination}/year={date.year}/month={date.month:02d}/day={date.day:02d}/data.json'
        data = json.dumps(events)
        with DataLakeFileClientSync(self.account_url,
                                    self.filesystem_name,
                                    file_path,
                                    credential=credential) as file_client:
            file_client.upload_data(data, overwrite=True)
