"""
Osiris-ingress API.
"""
from http import HTTPStatus

import requests

from ..core.azure_client_authorization import ClientAuthorization


class Ingress:
    """
    Contains functions for uploading data to the Osiris-ingress API.
    """
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
            raise TypeError

        self.ingress_url = ingress_url
        self.dataset_guid = dataset_guid

        self.client_auth = ClientAuthorization(tenant_id, client_id, client_secret)

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

        self.__check_status_code(response.status_code)

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

        self.__check_status_code(response.status_code)

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

        self.__check_status_code(response.status_code)

    @staticmethod
    def __check_status_code(status_code: int):
        if status_code == HTTPStatus.NOT_FOUND:
            raise FileNotFoundError('The dataset with GUID doesnt exist or JSON validation schema doesnt exist.')

        if status_code == HTTPStatus.BAD_REQUEST:
            raise ValueError('JSON validation error.')

        if status_code == HTTPStatus.INTERNAL_SERVER_ERROR:
            raise Exception('Internal server error')
