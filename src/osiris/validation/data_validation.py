"""
Validate data by inheriting DataValidation class.
"""
from abc import ABC, abstractmethod
import pandas as pd


class DataValidation(ABC):
    """
    Use abstract methods to read from source and destination, then use the predefined validate functions to validate
    data.
    """

    @abstractmethod
    def read_from_source(self, **kwargs):
        """
        Read data from source.
        Return: pandas.DataFrame
        """

    @abstractmethod
    def read_from_destination(self, **kwargs):
        """
        Read data from destination.
        Return: pandas.DataFrame
        """

    @staticmethod
    def validate_column_names(source: pd.DataFrame, destination: pd.DataFrame):
        """
        Validate column names.
        """
        return bool(str(source.columns) == str(destination.columns))

    @staticmethod
    def validate_number_of_columns(source: pd.DataFrame, destination: pd.DataFrame):
        """
        Validate number of columns.
        """
        return bool(len(source.columns) == len(destination.columns))

    @staticmethod
    def validate_number_of_rows(source: pd.DataFrame, destination: pd.DataFrame):
        """
        Validate number of rows.
        """
        return bool(len(source.index) == len(destination.index))
