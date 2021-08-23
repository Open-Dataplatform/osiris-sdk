"""
Module for handling data format conversions
"""
import csv
from abc import ABC
from io import StringIO, BytesIO

import pandas as pd
import apache_beam.transforms.core as beam_core


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
