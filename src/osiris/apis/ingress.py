"""
Osiris-ingress API.
"""
import logging
from typing import Any

import requests

from .dependencies import check_status_code, handle_download_response
from ..core.azure_client_authorization import ClientAuthorization

logger = logging.getLogger(__name__)


class Ingress:
    """
    Contains functions for uploading data to the Osiris-ingress API.
    """
    def __init__(self,
                 client_auth: ClientAuthorization,
                 ingress_url: str,
                 dataset_guid: str):
        """
        :param client_auth: The Client Authorization to access the dataset.
        :param ingress_url: The URL to the Osiris-ingress API.
        :param dataset_guid: The GUID for the dataset if needed.
        """

        if None in [client_auth, ingress_url, dataset_guid]:
            message = 'One or more of the arguments are None.'
            logger.error(message)
            raise TypeError(message)

        self.client_auth = client_auth
        self.ingress_url = ingress_url
        self.dataset_guid = dataset_guid

    def upload_json_file(self, file, schema_validate: bool):
        """
        Uploads the given JSON file to <dataset_guid>.

        :param file: The JSON file to upload.
        :param schema_validate: Validate the content of the file? This requires that the validation schema is
                                supplied to the DataPlatform.
        """
        response = requests.post(
            url=f'{self.ingress_url}/{self.dataset_guid}/json',
            files={'file': file},
            params={'schema_validate': schema_validate},
            headers={'Authorization': self.client_auth.get_access_token()}
        )

        check_status_code(response)

    def upload_json_file_event_time(self, file, event_time: str, schema_validate: bool):
        """
        Uploads the given JSON file to <dataset_guid> with a path corresponding to the given event
        time.

        :param file: The JSON file to upload.
        :param event_time: Given event time. The path corresponds to this time.
        :param schema_validate: Validate the content of the file? This requires that the validation schema is
                                supplied to the DataPlatform.
        """
        response = requests.post(
            url=f'{self.ingress_url}/{self.dataset_guid}/event_time/json',
            files={'file': file},
            params=[('schema_validate', schema_validate),
                    ('event_time', event_time)],
            headers={'Authorization': self.client_auth.get_access_token()}
        )

        check_status_code(response)

    def upload_file(self, file):
        """
        Uploads the given arbitrary file to <dataset_guid>.

        :param file: The arbitrary file to upload.
        """
        response = requests.post(
            url=f'{self.ingress_url}/{self.dataset_guid}',
            files={'file': file},
            headers={'Authorization': self.client_auth.get_access_token()}
        )

        check_status_code(response)

    def upload_file_event_time(self, file, event_time: str):
        """
        Uploads the given arbitrary file to <dataset_guid> with a path corresponding to the given event
        time.

        :param file: The arbitrary file to upload.
        :param event_time: This string must be in the form '[year]-[month]-[day]T[hour]:[minutes] and
        decides the path where the data is stored.
        """
        response = requests.post(
            url=f'{self.ingress_url}/{self.dataset_guid}/event_time',
            files={'file': file},
            params={'event_time': event_time},
            headers={'Authorization': self.client_auth.get_access_token()}
        )

        check_status_code(response)

    def save_state(self, file):
        """
        Uploads the state file to <dataset_guid>. Can be downloaded from end-point in egress.

        :param file: File to be uploaded to <dataset_guid> and stored as state.json.
        """
        response = requests.post(
            url=f'{self.ingress_url}/{self.dataset_guid}/save_state',
            files={'file': file},
            headers={'Authorization': self.client_auth.get_access_token()}
        )

        check_status_code(response)

    def retrieve_state(self) -> Any:
        """
         Download state.json file from data storage from the given guid. This endpoint expects state.json to be
         stored in the folder {guid}/'.
        """
        response = requests.get(
            url=f'{self.ingress_url}/{self.dataset_guid}/retrieve_state',
            headers={'Authorization': self.client_auth.get_access_token()}
        )

        return handle_download_response(response)
