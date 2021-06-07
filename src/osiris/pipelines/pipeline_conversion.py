"""
Module for handling data format conversions
"""
import csv
from abc import ABC
from datetime import datetime
from io import StringIO, BytesIO

import pandas as pd
import apache_beam.transforms.core as beam_core

from .azure_data_storage import DataSets


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

    def process(self, element, *args, **kwargs):
        dataframe = pd.read_csv(
            StringIO(element),
            sep=self.separator,
            quotechar=self.quotechar,
            quoting=self.quoting,
            skipinitialspace=self.skipinitialspace
        )
        return [dataframe]


class _ConvertDFToParquet(beam_core.DoFn, ABC):
    """
    Converts `pandas.DataFrame` elements into Parquet file format (stored as `io.BytesIO`).
    """
    def process(self, element, *args, **kwargs):
        parquet_file = BytesIO()
        element.to_parquet(parquet_file, engine='pyarrow', compression='snappy')
        parquet_file.seek(0)
        return [parquet_file]


class _ConvertDFToJSON(beam_core.DoFn, ABC):
    """
    Converts `pandas.DataFrame` elements into JSON records.
    """
    def process(self, element, *args, **kwargs):
        return [element.to_json(orient='records')]


class _ConvertCSVToJSON(beam_core.DoFn, ABC):
    """
    Converts CSV data (as `str`) into JSON records.
    """
    def __init__(self, separator=';', quotechar='"'):
        super().__init__()
        self.separator = separator
        self.quotechar = quotechar

    def process(self, element, *args, **kwargs):
        dataframe = pd.read_csv(
            StringIO(element),
            sep=self.separator,
            quotechar=self.quotechar,
            quoting=csv.QUOTE_NONNUMERIC,
            skipinitialspace=True
        )
        return [dataframe.to_json(orient='records')]


class _ConvertCSVToParquet(beam_core.DoFn, ABC):
    """
    Converts CSV data (as `str`) into Parquet file format (stored as `io.BytesIO`).
    """
    def __init__(self, separator=';', quotechar='"'):
        super().__init__()
        self.separator = separator
        self.quotechar = quotechar

    def process(self, element, *args, **kwargs):
        dataframe = pd.read_csv(
            StringIO(element),
            sep=self.separator,
            quotechar=self.quotechar,
            quoting=csv.QUOTE_NONNUMERIC,
            skipinitialspace=True
        )
        parquet_file = BytesIO()
        dataframe.to_parquet(parquet_file, engine='pyarrow', compression='snappy')
        parquet_file.seek(0)
        return [parquet_file]


class _CombineDataFrames(beam_core.CombineFn, ABC):
    """
    Combines multiple `pd.DataFrame` into a single `pd.DataFrame`.
    """
    def create_accumulator(self, *args, **kwargs):
        return pd.DataFrame()

    def add_input(self, mutable_accumulator, element, *args, **kwargs):
        return mutable_accumulator.append(element, ignore_index=True)

    def merge_accumulators(self, accumulators, *args, **kwargs):
        return pd.concat(accumulators, axis=0, ignore_index=True)

    def extract_output(self, accumulator, *args, **kwargs):
        return accumulator


class _UploadDataToDestination(beam_core.DoFn, ABC):
    """
    Uploads arbitrary data to destination.
    The optional filename prefix and suffix is inserted as:

        `{file_prefix}{filename}{file_suffix}`
    """
    # pylint: disable=too-many-arguments
    def __init__(self,
                 date: datetime,
                 datasets: DataSets,
                 filename: str = 'data',
                 file_prefix: str = '',
                 file_suffix: str = ''):
        super().__init__()
        self.date = date
        self.datasets = datasets
        self.filename = filename
        self.file_prefix = file_prefix
        self.file_suffix = file_suffix

    def process(self, element, *args, **kwargs):
        _filename = f'{self.file_prefix}{self.filename}{self.file_suffix}'
        self.datasets.upload_data_to_destination(self.date, element, _filename)

# Araz: I commented this code out. This code shouldn't be in here.
# class PipelineConversion(OsirisPipeline):
#     """
#     Class to create pipelines for generic data conversion.
#     """
#
#     # pylint: disable=too-many-arguments
#     def transform_convert_csv_to_json(self,
#                                       ingest_time: datetime = datetime.utcnow(),
#                                       separator: str = ',',
#                                       quotechar: str = '"',
#                                       quoting: int = csv.QUOTE_NONNUMERIC,
#                                       skipinitialspace: bool = True):
#         """
#         Creates a pipeline to convert CSV data into JSON format.
#         Writes the destination file to the same folder structure as the source file.
#         :param ingest_time: the ingest time to parse - defaults to current time
#         :param separator: the separator char to pass to `pandas.read_csv`
#         :param quotechar: the quote char to pass to `pandas.read_csv`
#         :param quoting: the quoting enum (from `csv`) to pass to `pandas.read_csv`
#         :param skipinitialspace: whether initial spaces in columns should be stripped, passed to `pandas.read_csv`
#         """
#         datalake_connector = DatalakeFileSource(self.tenant.,
#                                                 self.storage_account_url, self.filesystem_name,
#                                                 self.source_dataset_guid, ingest_time)
#
#         with beam.Pipeline(options=PipelineOptions()) as pipeline:
#             _ = (
#                 pipeline  # noqa
#                 | 'Read from FS' >> beam.io.Read(datalake_connector)  # noqa
#                 | 'Decode to str' >> beam_core.Map(lambda x: x.decode())  # noqa
#                 | beam_core.ParDo(_LoadCSVToDF(separator=separator, quotechar=quotechar,  # noqa
#                                                quoting=quoting, skipinitialspace=skipinitialspace))
#                 | beam_core.CombineGlobally(_CombineDataFrames())  # noqa
#                 | beam_core.ParDo(_ConvertDFToJSON())  # noqa
#                 | beam_core.ParDo(_UploadDataToDestination(ingest_time, self.datasets, 'json'))  # noqa
#             )
#
#     # pylint: disable=too-many-arguments
#     def transform_convert_csv_to_parquet(self,
#                                          ingest_time: datetime = datetime.utcnow(),
#                                          separator: str = ',',
#                                          quotechar: str = '"',
#                                          quoting: int = csv.QUOTE_NONNUMERIC,
#                                          skipinitialspace: bool = True):
#         """
#         Creates a pipeline to convert CSV data into JSON format.
#         Writes the destination file to the same folder structure as the source file.
#         :param ingest_time: the ingest time to parse - defaults to current time
#         :param separator: the separator char to pass to `pandas.read_csv`
#         :param quotechar: the quote char to pass to `pandas.read_csv`
#         :param quoting: the quoting enum (from `csv`) to pass to `pandas.read_csv`
#         :param skipinitialspace: whether initial spaces in columns should be stripped, passed to `pandas.read_csv`
#         """
#         datalake_connector = DatalakeFileSource(self.client_auth.get_credential_sync(),
#                                                 self.storage_account_url, self.filesystem_name,
#                                                 self.source_dataset_guid, ingest_time)
#
#         with beam.Pipeline(options=PipelineOptions()) as pipeline:
#             _ = (
#                 pipeline  # noqa
#                 | 'Read from FS' >> beam.io.Read(datalake_connector)  # noqa
#                 | 'Decode to str' >> beam_core.Map(lambda x: x.decode())  # noqa
#                 | beam_core.ParDo(_LoadCSVToDF(separator=separator, quotechar=quotechar,  # noqa
#                                                quoting=quoting, skipinitialspace=skipinitialspace))
#                 | beam_core.CombineGlobally(_CombineDataFrames())  # noqa
#                 | beam_core.ParDo(_ConvertDFToParquet())  # noqa
#                 | beam_core.ParDo(_UploadDataToDestination(ingest_time, self.datasets, 'snappy.parquet'))  # noqa
#             )
