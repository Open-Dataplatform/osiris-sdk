"""
Module to handle pipeline for timeseries
"""
from abc import ABC
from datetime import datetime
from typing import List, Tuple
import json

import pandas as pd
import apache_beam as beam
import apache_beam.transforms.core as beam_core
from apache_beam.options.pipeline_options import PipelineOptions
from azure.core.exceptions import ResourceNotFoundError

from .pipeline import OsirisPipeline
from ..core.enums import TimeResolution
from .azure_data_storage import _DataSets
from .file_io_connector import _DatalakeFileSource


class _ConvertEventToTuple(beam_core.DoFn, ABC):
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
            res.append((str(self.__convert_datetime_to_time_resolution(datetime_obj)), event))

        return res

    def __convert_datetime_to_time_resolution(self, datetime_obj: datetime):
        if self.time_resolution == TimeResolution.NONE:
            return datetime_obj.replace(year=1970, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        if self.time_resolution == TimeResolution.YEAR:
            return datetime_obj.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        if self.time_resolution == TimeResolution.MONTH:
            return datetime_obj.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if self.time_resolution == TimeResolution.DAY:
            return datetime_obj.replace(hour=0, minute=0, second=0, microsecond=0)
        if self.time_resolution == TimeResolution.HOUR:
            return datetime_obj.replace(minute=0, second=0, microsecond=0)
        if self.time_resolution == TimeResolution.MINUTE:
            return datetime_obj.replace(second=0, microsecond=0)
        message = 'Unknown enum type'
        raise ValueError(message)


class _JoinUniqueEventData(beam_core.DoFn, ABC):
    """"
    Takes a list of events and join it with processed events, if such exists, for the particular event time.
    It will only keep unique pairs.
    """
    def __init__(self, datasets: _DataSets):
        super().__init__()

        self.datasets = datasets

    def process(self, element, *args, **kwargs) -> List[Tuple]:
        """
        Overwrites beam.DoFn process.
        """
        date = pd.to_datetime(element[0])
        events = element[1]
        try:
            processed_events = self.datasets.read_events_from_destination(date)
            joined_events = events + processed_events
            # Only keep unique elements in the list
            joined_events = [i for n, i in enumerate(joined_events) if i not in joined_events[n + 1:]]

            return [(date, joined_events)]
        except ResourceNotFoundError:
            return [(date, events)]


class _UploadEventsToDestination(beam_core.DoFn, ABC):
    """
    Uploads events to destination
    """

    def __init__(self, datasets: _DataSets):
        super().__init__()
        self.datasets = datasets

    def process(self, element, *args, **kwargs):
        """
        Overwrites beam.DoFn process.
        """
        date = element[0]
        events = element[1]

        self.datasets.upload_events_to_destination_json(date, events)


class PipelineTimeSeries(OsirisPipeline):
    """
    Class to create pipelines for time series data
    """
    # pylint: disable=too-many-arguments, too-many-instance-attributes, too-few-public-methods
    def __init__(self, storage_account_url: str, filesystem_name: str, tenant_id: str, client_id: str,
                 client_secret: str, source_dataset_guid: str, destination_dataset_guid: str, date_format: str,
                 date_key_name: str, time_resolution: TimeResolution):
        """
        :param storage_account_url: The URL to Azure storage account.
        :param filesystem_name: The name of the filesystem.
        :param tenant_id: The tenant ID representing the organisation.
        :param client_id: The client ID (a string representing a GUID).
        :param client_secret: The client secret string.
        :param source_dataset_guid: The GUID for the source dataset.
        :param destination_dataset_guid: The GUID for the destination dataset.
        :param date_format: The date format used in the time series.
        :param date_key_name: The key in the record containing the date.
        :param time_resolution: The time resolution to store the data in the destination dataset with.
        """
        super().__init__(storage_account_url, filesystem_name, tenant_id, client_id, client_secret, source_dataset_guid,
                         destination_dataset_guid, time_resolution)

        if None in [date_format, date_key_name]:
            raise TypeError

        self.date_format = date_format
        self.date_key_name = date_key_name

    def transform_ingest_time_to_event_time(self, ingest_time: datetime = datetime.utcnow()):
        """
        Creates a pipeline to transform from ingest time to event on a daily time.
        :param ingest_time: the ingest time to parse - default to current time
        """
        datalake_connector = _DatalakeFileSource(ingest_time, self.client_auth.get_credential_sync(),
                                                 self.storage_account_url, self.filesystem_name,
                                                 self.source_dataset_guid)

        with beam.Pipeline(options=PipelineOptions(['--runner=DirectRunner'])) as pipeline:
            _ = (
                pipeline  # noqa
                | 'read from filesystem' >> beam.io.Read(datalake_connector)  # noqa
                | 'Convert from JSON' >> beam_core.Map(lambda x: json.loads(x))  # noqa pylint: disable=unnecessary-lambda
                | 'Create tuple for elements' >> beam_core.ParDo(_ConvertEventToTuple(self.date_key_name,  # noqa
                                                                                      self.date_format,  # noqa
                                                                                      self.time_resolution))  # noqa
                | 'Group by date' >> beam_core.GroupByKey()  # noqa
                | 'Merge from Storage' >> beam_core.ParDo(_JoinUniqueEventData(self.datasets))  # noqa
                | 'Write to Storage' >> beam_core.ParDo(_UploadEventsToDestination(self.datasets))  # noqa
            )
