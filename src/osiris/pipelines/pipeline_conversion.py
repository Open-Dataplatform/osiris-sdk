"""
Module for handling data format conversions
"""
import csv
from abc import ABC
from datetime import datetime
from io import StringIO, BytesIO

import pandas as pd
import apache_beam as beam
import apache_beam.transforms.core as beam_core
from apache_beam.options.pipeline_options import PipelineOptions

from ..core.azure_client_authorization import ClientAuthorization
from .azure_data_storage import _DataSets
from .file_io_connector import _DatalakeFileSource


class _LoadCSVToDF(beam_core.DoFn, ABC):
    """
    Loads CSV data (as `str`) into `pandas.DataFrame`.
    Useful for modular conversions or conversion to more than one other dataformat.
    The parameters are passed to `pandas.read_csv` internally.
    """
    def __init__(self, separator=';', quotechar='"', quoting=csv.QUOTE_NONNUMERIC, skipinitialspace=True):
        super().__init__()
        self.separator = separator
        self.quotechar = quotechar
        self.quoting = quoting
        self.skipinitialspace = skipinitialspace

    def process(self, element):
        df = pd.read_csv(
            StringIO(element),
            sep=self.separator,
            quotechar=self.quotechar,
            quoting=self.quoting,
            skipinitialspace=self.skipinitialspace
        )
        return [df]


class _ConvertDFToParquet(beam.DoFn):
    """
    Converts `pandas.DataFrame` elements into Parquet file format (stored as `io.BytesIO`).
    """
    def process(self, element):
        parquet_file = BytesIO()
        element.to_parquet(parquet_file, engine='pyarrow', compression='snappy')
        parquet_file.seek(0)
        return [parquet_file]


class _ConvertDFToJSON(beam.DoFn):
    """
    Converts `pandas.DataFrame` elements into JSON records.
    """
    def process(self, element):
        return [element.to_json(orient='records')]


class _ConvertCSVToJSON(beam_core.DoFn, ABC):
    """
    Converts CSV data (as `str`) into JSON records.
    """
    def __init__(self, separator=';', quotechar='"'):
        super().__init__()
        self.separator = separator
        self.quotechar = quotechar

    def process(self, element):
        df = pd.read_csv(
            StringIO(element),
            sep=self.separator,
            quotechar=self.quotechar,
            quoting=csv.QUOTE_NONNUMERIC,
            skipinitialspace=True
        )
        return [df.to_json(orient='records')]


class ConvertCSVToParquet(beam_core.DoFn, ABC):
    """
    Converts CSV data (as `str`) into Parquet file format (stored as `io.BytesIO`).
    """
    def __init__(self, separator=';', quotechar='"'):
        super().__init__()
        self.separator = separator
        self.quotechar = quotechar

    def process(self, element):
        df = pd.read_csv(
            StringIO(element),
            sep=self.separator,
            quotechar=self.quotechar,
            quoting=csv.QUOTE_NONNUMERIC,
            skipinitialspace=True
        )
        parquet_file = BytesIO()
        df.to_parquet(parquet_file, engine='pyarrow', compression='snappy')
        parquet_file.seek(0)
        return [parquet_file]


class _CombineDataFrames(beam.CombineFn):
    """
    Combines multiple `pd.DataFrame` into a single `pd.DataFrame`.
    """
    def create_accumulator(self):
        return pd.DataFrame()
    
    def add_input(self, mutable_accumulator, element):
        return mutable_accumulator.append(element, ignore_index=True)
    
    def merge_accumulators(self, accumulators):
        return pd.concat(accumulators, axis=0, ignore_index=True)
    
    def extract_output(self, accumulator):
        return accumulator


class _UploadDataToDestination(beam.DoFn):
    """
    Uploads arbitrary data to destination. 
    The optional filename prefix and suffix is inserted as:

        `{file_prefix}{filename}{file_suffix}`
    """

    def __init__(self, date: datetime, datasets: _DataSets, filename: str = 'data', file_prefix: str = '', file_suffix: str = ''):
        super().__init__()
        self.date = date
        self.datasets = datasets
        self.filename = filename
        self.file_prefix = file_prefix
        self.file_suffix = file_suffix

    def process(self, element, *args, **kwargs):
        _filename = f'{self.file_prefix}{self.filename}{self.file_suffix}'
        self.datasets.upload_data_to_destination(self.date, element, _filename)


class PipelineConversion:
    """
    Class to create pipelines for generic data conversion.
    """
    def __init__(self,
                 storage_account_url: str,
                 filesystem_name: str,
                 tenant_id: str,
                 client_id: str,
                 client_secret: str,
                 source_dataset_guid: str,
                 destination_dataset_guid: str):
        """
        :param storage_account_url: The URL to Azure storage account.
        :param filesystem_name: The name of the filesystem.
        :param tenant_id: The tenant ID representing the organisation.
        :param client_id: The client ID (a string representing a GUID).
        :param client_secret: The client secret string.
        :param source_dataset_guid: The GUID for the source dataset.
        :param destination_dataset_guid: The GUID for the destination dataset.
        """
        if None in [storage_account_url, filesystem_name, tenant_id, client_id,
                    client_secret, source_dataset_guid, destination_dataset_guid]:
            raise TypeError

        self.storage_account_url = storage_account_url
        self.filesystem_name = filesystem_name
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.source_dataset_guid = source_dataset_guid
        self.destination_dataset_guid = destination_dataset_guid

    def transform_convert_csv_to_json(self,
                                      ingest_time: datetime = datetime.utcnow(),
                                      separator: str = ',',
                                      quotechar: str = '"',
                                      quoting: int =csv.QUOTE_NONNUMERIC,
                                      skipinitialspace: bool = True):
        """
        Creates a pipeline to convert CSV data into JSON format.
        Writes the destination file to the same folder structure as the source file.
        :param ingest_time: the ingest time to parse - defaults to current time
        """
        client_auth = ClientAuthorization(self.tenant_id, self.client_id, self.client_secret)
        datalake_connector = _DatalakeFileSource(ingest_time, client_auth.get_credential_sync(),
                                                 self.storage_account_url, self.filesystem_name,
                                                 self.source_dataset_guid)

        datasets = _DataSets(self.storage_account_url, self.filesystem_name,
                             self.source_dataset_guid, self.destination_dataset_guid, client_auth.get_credential_sync())
        
        with beam.Pipeline(options=PipelineOptions()) as pipeline:
            _ = (
                pipeline 
                | 'Read from FS' >> beam.io.Read(datalake_connector)
                | 'Decode to str' >> beam.Map(lambda x: x.decode())
                | beam.ParDo(_LoadCSVToDF(separator=separator, quotechar=quotechar,
                                          quoting=quoting, skipinitialspace=skipinitialspace))
                | beam.CombineGlobally(_CombineDataFrames())
                | beam.ParDo(_ConvertDFToJSON())
                | beam.ParDo(_UploadDataToDestination(ingest_time, datasets, 'json'))
            )
    
    def transform_convert_csv_to_parquet(self,
                                         ingest_time: datetime = datetime.utcnow(),
                                         separator: str = ',',
                                         quotechar: str = '"',
                                         quoting: int = csv.QUOTE_NONNUMERIC,
                                         skipinitialspace: bool = True):
        """
        Creates a pipeline to convert CSV data into JSON format.
        Writes the destination file to the same folder structure as the source file.
        :param ingest_time: the ingest time to parse - defaults to current time
        """
        client_auth = ClientAuthorization(self.tenant_id, self.client_id, self.client_secret)
        datalake_connector = _DatalakeFileSource(ingest_time, client_auth.get_credential_sync(),
                                                 self.storage_account_url, self.filesystem_name,
                                                 self.source_dataset_guid)

        datasets = _DataSets(self.storage_account_url, self.filesystem_name,
                             self.source_dataset_guid, self.destination_dataset_guid, client_auth.get_credential_sync())
        
        with beam.Pipeline(options=PipelineOptions()) as pipeline:
            _ = (
                pipeline 
                | 'Read from FS' >> beam.io.Read(datalake_connector)
                | 'Decode to str' >> beam.Map(lambda x: x.decode())
                | beam.ParDo(_LoadCSVToDF(separator=separator, quotechar=quotechar,
                                          quoting=quoting, skipinitialspace=skipinitialspace))
                | beam.CombineGlobally(_CombineDataFrames())
                | beam.ParDo(_ConvertDFToParquet())
                | beam.ParDo(_UploadDataToDestination(ingest_time, datasets, 'snappy.parquet'))
            )
