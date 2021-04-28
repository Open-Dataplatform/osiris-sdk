"""
Contains common functionalities for Egress and Ingress API's
"""
import json
from http import HTTPStatus
from json import JSONDecodeError
from typing import Any

from requests import Response


def handle_download_response(response: Response) -> Any:
    """
     Checks status codes and converts JSON string to Python objects.
    """

    check_status_code(response)

    try:
        return json.loads(response.content)
    except JSONDecodeError:
        raise ValueError('File is not correctly JSON formatted.') from JSONDecodeError


def check_status_code(response: Response):
    """
    Converts HTTP errors to Python Exceptions
    """
    if response.status_code == HTTPStatus.NOT_FOUND:
        detail = json.loads(response.text)['detail']
        raise FileNotFoundError(detail)

    if response.status_code == HTTPStatus.BAD_REQUEST:
        detail = json.loads(response.text)['detail']
        raise ValueError(detail)

    if response.status_code == HTTPStatus.FORBIDDEN:
        detail = json.loads(response.text)['detail']
        raise PermissionError(detail)

    if response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR:
        detail = json.loads(response.text)['detail']
        raise Exception(detail)
