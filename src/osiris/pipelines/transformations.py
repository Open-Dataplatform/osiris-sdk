"""
Module to handle pipeline for timeseries
"""
from abc import ABC
from datetime import datetime
from typing import List, Tuple

import pandas as pd
import apache_beam.transforms.core as beam_core
from azure.core.exceptions import ResourceNotFoundError

from ..core.enums import TimeResolution
from .azure_data_storage import DataSets


class ConvertEventToTuple(beam_core.DoFn, ABC):
    """
    Takes a list of events and converts them to a list of tuples (datetime, event)
    """
    def __init__(self, date_key_name: str, date_format: str, time_resolution: TimeResolution):
        super().__init__()

        self.date_key_name = date_key_name
        self.date_format = date_format
        self.time_resolution = time_resolution

    def process(self, element, *args, **kwargs) -> List:
        """
        Overwrites beam.DoFn process.
        """
        res = []
        for event in element:
            datetime_obj = pd.to_datetime(event[self.date_key_name], format=self.date_format)
            res.append((self.__convert_datetime_to_time_resolution(datetime_obj), event))

        return res

    def __convert_datetime_to_time_resolution(self, datetime_obj: datetime):
        if self.time_resolution == TimeResolution.NONE:
            return '1970-01-01T00:00:00'
        if self.time_resolution == TimeResolution.YEAR:
            return f'{datetime_obj.year}-01-01T00:00:00'
        if self.time_resolution == TimeResolution.MONTH:
            return f'{datetime_obj.year}-{datetime_obj.month:02d}-01T00:00:00'
        if self.time_resolution == TimeResolution.DAY:
            return f'{datetime_obj.year}-{datetime_obj.month:02d}-{datetime_obj.day:02d}T00:00:00'
        if self.time_resolution == TimeResolution.HOUR:
            return f'{datetime_obj.year}-{datetime_obj.month:02d}-{datetime_obj.day:02d}T{datetime_obj.hour:02d}:00:00'
        if self.time_resolution == TimeResolution.MINUTE:
            return f'{datetime_obj.year}-{datetime_obj.month:02d}-{datetime_obj.day:02d}T{datetime_obj.hour:02d}:' + \
                   f'{datetime_obj.minute:02d}:00'
        message = 'Unknown enum type'
        raise ValueError(message)


class JoinUniqueEventData(beam_core.DoFn, ABC):
    """"
    Takes a list of events and join it with processed events, if such exists, for the particular event time.
    It will only keep unique pairs.
    """
    def __init__(self, datasets: DataSets):
        super().__init__()

        self.datasets = datasets

    def process(self, element, *args, **kwargs) -> List[Tuple]:
        """
        Overwrites beam.DoFn process.
        """
        date = pd.to_datetime(element[0])
        events = element[1]
        try:
            processed_events = self.datasets.read_events_from_destination_json(date)
            joined_events = events + processed_events
            # Only keep unique elements in the list
            joined_events = [i for n, i in enumerate(joined_events) if i not in joined_events[n + 1:]]

            return [(date, joined_events)]
        except ResourceNotFoundError:
            return [(date, events)]


class UploadEventsToDestination(beam_core.DoFn, ABC):
    """
    Uploads events to destination
    """

    def __init__(self, datasets: DataSets, parquet_execution: bool = False):
        super().__init__()
        self.datasets = datasets
        self.parquet_execution = parquet_execution

    def process(self, element, *args, **kwargs):
        """
        Overwrites beam.DoFn process.
        """
        date = element[0]
        events = element[1]

        if self.parquet_execution:
            self.datasets.upload_events_to_destination_parquet(date, events)
        else:
            self.datasets.upload_events_to_destination_json(date, events)
