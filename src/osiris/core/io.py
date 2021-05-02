"""
Contains Osiris common IO functions
"""
from datetime import datetime

from .enums import TimeResolution


def get_file_path_with_respect_to_time_resolution(date: datetime, time_resolution: TimeResolution, filename: str):
    """
    Returns the file path which corresponds to the given time resolution. The GUID directory is not included!
    """
    if time_resolution == TimeResolution.NONE:
        return f'{filename}'
    if time_resolution == TimeResolution.YEAR:
        return f'year={date.year}/{filename}'
    if time_resolution == TimeResolution.MONTH:
        return f'year={date.year}/month={date.month:02d}/{filename}'
    if time_resolution == TimeResolution.DAY:
        return f'year={date.year}/month={date.month:02d}/day={date.day:02d}/{filename}'
    if time_resolution == TimeResolution.HOUR:
        return f'year={date.year}/month={date.month:02d}/day={date.day:02d}/' + \
               f'hour={date.hour:02d}/{filename}'
    if time_resolution == TimeResolution.MINUTE:
        return f'year={date.year}/month={date.month:02d}/day={date.day:02d}/' + \
               f'hour={date.hour:02d}/minute={date.minute:02d}/{filename}'

    message = '(ValueError) Unknown time resolution giving.'
    raise ValueError(message)
