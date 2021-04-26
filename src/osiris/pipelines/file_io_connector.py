"""
A source to download files from Azure Datalake
"""
from datetime import timedelta, datetime
from typing import List, Optional, Generator

from apache_beam.io import OffsetRangeTracker, iobase
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.filedatalake import DataLakeServiceClient, DataLakeFileClient, FileSystemClient, PathProperties

from ..core.azure_client_authorization import AzureCredential


class _DatalakeFileSource(iobase.BoundedSource):  # noqa
    """
    A Class to download files from Azure Datalake
    """
    # pylint: disable=too-many-arguments
    def __init__(self,
                 ingest_time: datetime,
                 credential: AzureCredential,
                 account_url: str,
                 filesystem_name: str,
                 guid: str):
        self.account_url = account_url
        self.filesystem_name = filesystem_name
        self.credential = credential
        self.file_paths = self.__get_file_paths(ingest_time, guid)

    def __get_file_paths(self, ingest_time: datetime, guid: str) -> List[PathProperties]:
        folder_path1 = f'{guid}/year={ingest_time.year}/month={ingest_time.month:02d}/day={ingest_time.day:02d}' + \
                       f'/hour={ingest_time.hour:02d}'

        prev_hour = ingest_time - timedelta(hours=1)
        folder_path2 = f'{guid}/year={prev_hour.year}/month={prev_hour.month:02d}/day={prev_hour.day:02d}' + \
                       f'/hour={prev_hour.hour:02d}'

        paths = []
        with DataLakeServiceClient(self.account_url, credential=self.credential) as service_client:
            file_system_client = service_client.get_file_system_client(self.filesystem_name)
            paths += self.__get_file_paths_from_folder(folder_path1, file_system_client)
            paths += self.__get_file_paths_from_folder(folder_path2, file_system_client)
        return paths

    @staticmethod
    def __get_file_paths_from_folder(folder_path: str, file_system_client: FileSystemClient) -> List[PathProperties]:
        try:
            return list(file_system_client.get_paths(path=folder_path))
        except ResourceNotFoundError:
            return []

    def estimate_size(self) -> int:
        """
        Returns the number of files to process
        """
        return len(self.file_paths)

    def get_range_tracker(self, start_position: Optional[int], stop_position: Optional[int]) -> OffsetRangeTracker:
        """
        Creates and returns an OffsetRangeTracker
        """
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = len(self.file_paths)

        return OffsetRangeTracker(start_position, stop_position)

    def read(self, range_tracker: OffsetRangeTracker) -> Optional[Generator]:
        """
        Returns the content of the next file
        """
        for i in range(range_tracker.start_position(),
                       range_tracker.stop_position()):
            if not range_tracker.try_claim(i):
                return

            path = self.file_paths[i].name
            with DataLakeFileClient(self.account_url,
                                    self.filesystem_name, path,
                                    credential=self.credential) as file_client:

                content = file_client.download_file().readall()

                yield content

    def split(self,
              desired_bundle_size: int,
              start_position: Optional[int] = None,
              stop_position: Optional[int] = None) -> iobase.SourceBundle:
        """
        Splits a Tracker
        """
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = len(self.file_paths)

        bundle_start = start_position
        while bundle_start < stop_position:
            bundle_stop = min(stop_position, bundle_start + desired_bundle_size)
            yield iobase.SourceBundle(
                weight=(bundle_stop - bundle_start),
                source=self,
                start_position=bundle_start,
                stop_position=bundle_stop)
            bundle_start = bundle_stop
