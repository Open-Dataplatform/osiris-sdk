from abc import ABC
from datetime import datetime
from typing import List, Tuple

import pandas as pd
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from azure.core.exceptions import ResourceNotFoundError

from src.osiris.azure_client_authorization import ClientAuthorization
from src.osiris.azure_data_storage import _DataSets
from src.osiris.file_io_connector import _DatalakeFileSource


class _ConvertEventToTuple(beam.DoFn, ABC):
    """
    Takes a list of events and converts them to a list of tuples (datetime, event)
    """
    def __init__(self, date_key_name: str, date_format: str):
        super().__init__()

        self.date_key_name = date_key_name
        self.date_format = date_format

    def process(self, element, *args, **kwargs) -> List:
        """
        Overwrites beam.DoFn process.
        """
        res = []
        for event in element:
            datetime_obj = pd.to_datetime(event[self.date_key_name], format=self.date_format)
            res.append((datetime_obj.date(), event))

        return res


class _MergeEventData(beam.DoFn, ABC):
    """"
    Takes a list of events and merges it with processed events, if such exists, for the particular event time.
    """
    def __init__(self, date_key_name: str, datasets: _DataSets):
        super().__init__()

        self.date_key_name = date_key_name
        self.datasets = datasets

    def process(self, element, *args, **kwargs) -> List[Tuple]:
        """
        Overwrites beam.DoFn process.
        """
        date = element[0]
        events = element[1]
        try:
            processed_events = self.datasets.read_events_from_destination(date)
            merged_events = events + processed_events
            merged_events = list({event[self.date_key_name]: event for event in merged_events}.values())

            return [(date, merged_events)]
        except ResourceNotFoundError:
            return [element]


class _UploadEventsToDestination(beam.DoFn, ABC):
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

        self.datasets.upload_events_to_destination(date, events)


class PipelineTimeSeries:
    # pylint: disable=too-many-arguments
    def __init__(self,
                 storage_account_url: str,
                 filesystem_name: str,
                 tenant_id: str,
                 client_id: str,
                 client_secret: str,
                 source_dataset_guid: str,
                 destination_dataset_guid: str,
                 date_format: str,
                 date_key_name: str):
        """
        :param tenant_id: The tenant ID representing the organisation.
        :param client_id: The client ID (a string representing a GUID).
        :param client_secret: The client secret string.
        :param source_dataset_guid: The GUID for the source dataset.
        :param destination_dataset_guid: The GUID for the destination dataset.
        """
        if None in [storage_account_url, filesystem_name, tenant_id, client_id,
                    client_secret, source_dataset_guid, destination_dataset_guid,
                    date_format, date_key_name]:
            raise TypeError

        self.storage_account_url = storage_account_url
        self.filesystem_name = filesystem_name
        self.source_dataset_guid = source_dataset_guid
        self.destination_dataset_guid = destination_dataset_guid
        self.date_format = date_format
        self.date_key_name = date_key_name

        self.client_auth = ClientAuthorization(tenant_id, client_id, client_secret)
        self.datasets = _DataSets(storage_account_url, filesystem_name,
                                  source_dataset_guid, destination_dataset_guid, self.client_auth)

    def transform_ingest_time_to_event_time_daily(self, ingest_time: datetime = datetime.utcnow()):
        datalake_connector = _DatalakeFileSource(ingest_time, self.client_auth.get_credential_sync(),
                                                 self.storage_account_url, self.filesystem_name,
                                                 self.source_dataset_guid)

        with beam.Pipeline(options=PipelineOptions()) as pipeline:
            _ = (
                pipeline
                | 'read from filesystem' >> beam.io.Read(datalake_connector)  # noqa
                | 'Convert from JSON' >> beam.Map(lambda x: json.loads(x))  # noqa pylint: disable=unnecessary-lambda
                | 'Create tuple for elements' >> beam.ParDo(_ConvertEventToTuple(self.date_key_name, self.date_format))  # noqa
                | 'Group by date' >> beam.GroupByKey()  # noqa
                | 'Merge from Storage' >> beam.ParDo(_MergeEventData(self.date_key_name, self.datasets))  # noqa
                | 'Write to Storage' >> beam.ParDo(_UploadEventsToDestination())
            )
