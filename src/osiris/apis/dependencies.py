"""
Contains common functionalities for Egress and Ingress API's
"""
import json
import logging
from http import HTTPStatus
from json import JSONDecodeError
from typing import Any

from requests import Response


logger = logging.getLogger(__name__)


def handle_download_response(response: Response) -> Any:
    """
     Checks status codes and converts JSON string to Python objects. Returns empty list if no result was found.
    """

    check_status_code(response)

    if response.status_code == HTTPStatus.NO_CONTENT:
        return []

    try:
        return json.loads(response.content)
    except JSONDecodeError:
        message = 'File is not correctly JSON formatted.'
        logger.error(message)
        raise ValueError(message) from JSONDecodeError


def check_status_code(response: Response):
    """
    Converts HTTP errors to Python Exceptions
    """
    if response.status_code == HTTPStatus.NOT_FOUND:
        detail = json.loads(response.text)['detail']
        logger.error('(FileNotFoundError) %s', detail)
        raise FileNotFoundError(detail)

    if response.status_code == HTTPStatus.BAD_REQUEST:
        detail = json.loads(response.text)['detail']
        logger.error('(ValueError) %s', detail)
        raise ValueError(detail)

    if response.status_code == HTTPStatus.FORBIDDEN or \
       response.status_code == HTTPStatus.UNAUTHORIZED:
        detail = json.loads(response.text)['detail']
        logger.error('(PermissionError) %s', detail)
        raise PermissionError(detail)

    if response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR or \
       response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY:
        detail = json.loads(response.text)['detail']
        logger.error('(Exception) %s', detail)
        raise Exception(detail)
