"""
Osiris-egress API.
"""
import logging
from typing import Any, Optional, List

import requests

from .dependencies import handle_download_response, handle_parquet_response
from ..core.azure_client_authorization import ClientAuthorization
from ..core.enums import Horizon

logger = logging.getLogger(__name__)


# pylint: disable=too-few-public-methods
class Egress:
    """
    Contains functions for downloading data from the Osiris-egress API.
    """
    # pylint: disable=too-many-arguments
    def __init__(self,
                 egress_url: str,
                 tenant_id: str,
                 client_id: str,
                 client_secret: str,
                 dataset_guid: Optional[str] = None):
        """
        :param egress_url: The URL to the Osiris-egress API.
        :param tenant_id: The tenant ID representing the organisation.
        :param client_id: The client ID (a string representing a GUID).
        :param client_secret: The client secret string.
        :param dataset_guid: The GUID for the dataset if needed.
        """
        if None in [egress_url, tenant_id, client_id, client_secret]:
            message = 'One or more of the arguments are None.'
            logger.error(message)
            raise TypeError(message)

        self.egress_url = egress_url
        self.dataset_guid = dataset_guid

        self.client_auth = ClientAuthorization(tenant_id, client_id, client_secret)

    def download_json_file(self, from_date: Optional[str] = None, to_date: Optional[str] = None) -> Any:
        """
        Download JSON file from data storage from the given time period (UTC).'.
        """
        if to_date and from_date:
            response = requests.get(
                url=f'{self.egress_url}/v1/{self.dataset_guid}/json',
                params={'from_date': from_date, 'to_date': to_date},
                headers={'Authorization': self.client_auth.get_access_token()}
            )
            return handle_download_response(response)

        response = requests.get(
            url=f'{self.egress_url}/v1/{self.dataset_guid}/json',
            headers={'Authorization': self.client_auth.get_access_token()}
        )
        return handle_download_response(response)

    def download_dmi_file(self, lon: float, lat: float, from_date: str,
                          to_date: str) -> Any:
        """
        Download DMI file from data storage from the given time period (UTC). This method doesn't
        need to have a GUID. The GUID is decided on the server side.
        """
        response = requests.get(
            url=f'{self.egress_url}/v1/dmi',
            params={'lon': str(lon), 'lat': str(lat), 'from_date': from_date, 'to_date': to_date},
            headers={'Authorization': self.client_auth.get_access_token()}
        )
        return handle_parquet_response(response)

    def download_dmi_list(self, from_date: str) -> Any:
        """
        Download DMI list from data storage from the given time (UTC). It takes from_date: YYYY-MM

        This method doesn't need to have a GUID. The GUID is decided on the server side.
        """
        response = requests.get(
            url=f'{self.egress_url}/v1/dmi_list',
            params={'from_date': from_date},
            headers={'Authorization': self.client_auth.get_access_token()}
        )
        return handle_download_response(response)

    def download_parquet_file(self, from_date: Optional[str] = None, to_date: Optional[str] = None) -> Any:
        """
        Download Parquet file from data storage from the given time period (UTC).'.
        """
        if to_date and from_date:
            response = requests.get(
                url=f'{self.egress_url}/v1/{self.dataset_guid}/parquet',
                params={'from_date': from_date, 'to_date': to_date},
                headers={'Authorization': self.client_auth.get_access_token()}
            )
            return handle_parquet_response(response)

        response = requests.get(
            url=f'{self.egress_url}/v1/{self.dataset_guid}/parquet',
            headers={'Authorization': self.client_auth.get_access_token()}
        )
        return handle_parquet_response(response)

    def download_jao_file(self, horizon: Horizon, from_date: str,
                          to_date: str) -> Any:
        """
         Download JAO file from data storage from the given time period (UTC). This method doesn't
         need to have a GUID. The GUID is decided on the server side.
        """
        response = requests.get(
            url=f'{self.egress_url}/v1/jao',
            params={'horizon': horizon.name, 'from_date': from_date, 'to_date': to_date},
            headers={'Authorization': self.client_auth.get_access_token()}
        )
        return handle_download_response(response)

    def download_neptun_file(self, horizon: Horizon, from_date: str,
                             to_date: str, tags: List = None) -> Any:
        """
         Download Neptun file from data storage from the given time period (UTC). This method doesn't
         need to have a GUID. The GUID is decided on the server side.

         The data can be filtered by given a list of tags.
        """
        filters = ','.join(tags) if tags else ''
        response = requests.get(
            url=f'{self.egress_url}/v1/neptun',
            params={'horizon': horizon.name, 'from_date': from_date, 'to_date': to_date, 'tags': filters},
            headers={'Authorization': self.client_auth.get_access_token()}
        )
        return handle_download_response(response)

    def download_delfin_file(self, horizon: Horizon, from_date: str,
                             to_date: str, table_indices: List = None) -> Any:
        """
         Download Delfin file from data storage from the given time period (UTC). This method doesn't
         need to have a GUID. The GUID is decided on the server side.

         The data can be filtered by given a list of tags.
        """
        filters = ','.join(table_indices) if table_indices else ''

        response = requests.get(
            url=f'{self.egress_url}/v1/delfin',
            params={'horizon': horizon.name, 'from_date': from_date, 'to_date': to_date, 'table_indices': filters},
            headers={'Authorization': self.client_auth.get_access_token()}
        )
        return handle_download_response(response)

    # def download_file(self, file_date: datetime) -> bytes:
    #     """
    #        Download file from data storage from the given date (UTC). This endpoint expects data to be
    #        stored in the folder {guid}/year={date.year:02d}/month={date.month:02d}/day={date.day:02d}/, but doesnt
    #        make any assumption about the filename and file extension.
    #     """
    #
    #     response = requests.get(
    #         url=f'{self.egress_url}/{self.dataset_guid}',
    #         params={'file_date': str(file_date)},
    #         headers={'Authorization': self.client_auth.get_access_token()}
    #     )
    #
    #     check_status_code(response)
    #
    #     return response.content
