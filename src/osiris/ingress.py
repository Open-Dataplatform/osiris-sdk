"""
Osiris-ingress SDK.
"""
import json
from io import TextIOWrapper
from json.decoder import JSONDecodeError

import msal
import requests


class Ingress:
    """
    Contains functions for uploading data to the Osiris-ingress API.
    """

    def __init__(self, ingress_url: str, tenant_id: str, client_id: str, client_secret: str, dataset_guid: str):
        """
        :param ingress_url: The URL to the Osiris-ingress API.
        :param tenant_id: The tenant ID representing the organisation.
        :param client_id: The client ID (a string representing a GUID).
        :param client_secret: The client secret string.
        :param guid: The GUID for the dataset.
        """
        if None in [ingress_url, tenant_id, client_id, client_secret, dataset_guid]:
            raise TypeError

        self.ingress_url = ingress_url
        self.dataset_guid = dataset_guid

        self.confidential_client_app = msal.ConfidentialClientApplication(
            authority=f'https://login.microsoftonline.com/{tenant_id}',
            client_id=client_id,
            client_credential=client_secret
        )

        self.scopes = ['https://storage.azure.com/.default']

    def __get_access_token(self) -> str:
        result = self.confidential_client_app.acquire_token_silent(self.scopes, account=None)

        if not result:
            result = self.confidential_client_app.acquire_token_for_client(scopes=self.scopes)

        return result['access_token']

    def upload_json_file(self, file: TextIOWrapper, schema_validate: bool) -> int:
        """
        Uploads the given file to the given <dataset_guid>.

        :param file: The file to upload.
        :param schema_validate: Validate the content of the file? This requires that the validation schema is
                                supplied to the DataPlatform.
        :return: HTTP status code 201 if the file was uploaded successfully.
        """
        try:
            json.load(file)
        except JSONDecodeError:
            raise ValueError('File is not correctly JSON formatted.')

        response = requests.post(
            url=f'{self.ingress_url}/{self.dataset_guid}/json',
            files={'file': file},
            params={'schema_validate': schema_validate},
            headers={'Authorization': self.__get_access_token()}
        )

        return response.status_code
