"""
Osiris-egress SDK.
"""
import json
from datetime import date
from json.decoder import JSONDecodeError
from http import HTTPStatus
from typing import Any

import requests


from .azure_client_authorization import ClientAuthorization


class Egress:
    """
    Contains functions for downloading data from the Osiris-egress API.
    """
    # pylint: disable=too-many-arguments
    def __init__(self, egress_url: str, tenant_id: str, client_id: str, client_secret: str, dataset_guid: str):
        """
        :param egress_url: The URL to the Osiris-egress API.
        :param tenant_id: The tenant ID representing the organisation.
        :param client_id: The client ID (a string representing a GUID).
        :param client_secret: The client secret string.
        :param dataset_guid: The GUID for the dataset.
        """
        if None in [egress_url, tenant_id, client_id, client_secret, dataset_guid]:
            raise TypeError

        self.egress_url = egress_url
        self.dataset_guid = dataset_guid

        self.client_auth = ClientAuthorization(tenant_id, client_id, client_secret)

    def download_json_file(self, file_date: date) -> Any:
        """
         Download JSON file from data storage from the given date (UTC). This endpoint expects data to be
         stored in {guid}/year={date.year:02d}/month={date.month:02d}/day={date.day:02d}/data.json'.
        """
        response = requests.get(
            url=f'{self.egress_url}/{self.dataset_guid}/json',
            params={'file_date': str(file_date)},
            headers={'Authorization': self.client_auth.get_access_token()}
        )

        self.__check_status_code(response.status_code)

        try:
            return json.loads(response.content)
        except JSONDecodeError:
            raise ValueError('File is not correctly JSON formatted.') from JSONDecodeError

    def download_file(self, file_date: date) -> bytes:
        """
           Download file from data storage from the given date (UTC). This endpoint expects data to be
           stored in the folder {guid}/year={date.year:02d}/month={date.month:02d}/day={date.day:02d}/, but doesnt make
           any assumption about the filename and file extension.
        """

        response = requests.get(
            url=f'{self.egress_url}/{self.dataset_guid}',
            params={'file_date': str(file_date)},
            headers={'Authorization': self.client_auth.get_access_token()}
        )

        self.__check_status_code(response.status_code)

        return response.content

    @staticmethod
    def __check_status_code(status_code: int):
        if status_code == HTTPStatus.NOT_FOUND:
            raise FileNotFoundError('No file was found for that date or the dataset with GUID doesnt exist.')

        if status_code == HTTPStatus.INTERNAL_SERVER_ERROR:
            raise Exception('Internal server error.')
