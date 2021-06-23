"""
Contains Osiris common IO functions
"""
from datetime import datetime
import pandas as pd

from .enums import TimeResolution


def get_directory_path_with_respect_to_time_resolution(date: datetime, time_resolution: TimeResolution):
    """
    Returns the directory path which corresponds to the given time resolution. The GUID directory is not included!
    """
    if time_resolution == TimeResolution.NONE:
        return ''
    if time_resolution == TimeResolution.YEAR:
        return f'year={date.year}/'
    if time_resolution == TimeResolution.MONTH:
        return f'year={date.year}/month={date.month:02d}/'
    if time_resolution == TimeResolution.DAY:
        return f'year={date.year}/month={date.month:02d}/day={date.day:02d}/'
    if time_resolution == TimeResolution.HOUR:
        return f'year={date.year}/month={date.month:02d}/day={date.day:02d}/' + \
               f'hour={date.hour:02d}/'
    if time_resolution == TimeResolution.MINUTE:
        return f'year={date.year}/month={date.month:02d}/day={date.day:02d}/' + \
               f'hour={date.hour:02d}/minute={date.minute:02d}/'

    message = '(ValueError) Unknown time resolution giving.'
    raise ValueError(message)


def get_file_path_with_respect_to_time_resolution(date: datetime, time_resolution: TimeResolution, filename: str):
    """
    Returns the file path which corresponds to the given time resolution. The GUID directory is not included!
    """
    return f'{get_directory_path_with_respect_to_time_resolution(date, time_resolution)}{filename}'


def parse_date_str(date_str):
    """
    Returns the datetime and time resolution of the given date_str.
    """
    try:
        if len(date_str) == 4:
            return pd.to_datetime(date_str, format='%Y'), TimeResolution.YEAR
        if len(date_str) == 7:
            return pd.to_datetime(date_str, format='%Y-%m'), TimeResolution.MONTH
        if len(date_str) == 10:
            return pd.to_datetime(date_str, format='%Y-%m-%d'), TimeResolution.DAY
        if len(date_str) == 13:
            return pd.to_datetime(date_str, format='%Y-%m-%dT%H'), TimeResolution.HOUR
        if len(date_str) == 16:
            return pd.to_datetime(date_str, format='%Y-%m-%dT%H:%M'), TimeResolution.MINUTE

        raise ValueError('Wrong string format for date')
    except ValueError as error:
        raise ValueError('Wrong string format for date: ', error) from error
