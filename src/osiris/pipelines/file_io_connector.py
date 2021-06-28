"""
A source to download files from Azure Datalake
"""
import os
import json
from datetime import timedelta, datetime
from typing import List, Optional, Generator, Dict
import pandas as pd

from apache_beam.io import OffsetRangeTracker, iobase
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.filedatalake import FileSystemClient, PathProperties

from .common import initialize_client_auth
from ..core.azure_client_authorization import ClientAuthorization


# pylint: disable=too-many-instance-attributes
from ..core.io import OsirisFileClient


class DatalakeFileSource(iobase.BoundedSource):  # noqa
    """
    A Class to download files from Azure Datalake

    This datasource can be used in two different ways:

    1. You can initialize it with an ingest time. In this case it will process files from the current hour and the
    previous hour no matter if they have been processed before or not. It will also ignore the max_files argument.

    2. If you don't initialize it with an ingest time, it will use a state which it will store in a file (STATE_FILE)
    on the dataset. This state will contain the last modified timestamp of the last file it processed. Next time
    it runs it will only process files newer than this timestamp and according to the max_files argument.
        - If you use this approach you need to call close() after the pipeline to save the state otherwise it will
        keep processing the same files again and again.
        - First time it runs it will use the current UTC time.
    """

    STATE_FILE = 'transformation_state.json'

    # pylint: disable=too-many-arguments
    def __init__(self,
                 tenant_id: str,
                 client_id: str,
                 client_secret: str,
                 account_url: str,
                 filesystem_name: str,
                 guid: str,
                 ingest_time: datetime = None,
                 max_files: int = 10):

        if None in [tenant_id, client_id, client_secret, account_url, filesystem_name, guid]:
            raise TypeError

        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.account_url = account_url
        self.filesystem_name = filesystem_name
        self.guid = guid
        self.has_ingest_time = ingest_time is not None

        # We need to initialize the ClientAuthorization using lazy initializing because of Beam. Beam pickles
        # this object and pickles only allow simples values as instance variables and not objects.
        self.client_auth = None

        local_client_auth = ClientAuthorization(tenant_id, client_id, client_secret)
        self.file_paths = self.__get_file_paths(ingest_time, max_files, local_client_auth)

    def __get_file_paths(self, ingest_time: Optional[datetime], max_files: int,
                         local_client_auth: ClientAuthorization) -> List[PathProperties]:
        with FileSystemClient(self.account_url, self.filesystem_name,
                              credential=local_client_auth.get_credential_sync()) as filesystem_client:

            if ingest_time is None:
                return self.__get_paths_since_last_run(filesystem_client, max_files)

            return self.__get_paths_on_basis_of_ingest_time(filesystem_client, ingest_time)

    def __get_paths_on_basis_of_ingest_time(self, filesystem_client, ingest_time: datetime) -> List[PathProperties]:
        folder_path1 = f'{self.guid}/year={ingest_time.year}/month={ingest_time.month:02d}/' + \
                       f'day={ingest_time.day:02d}/hour={ingest_time.hour:02d}'

        prev_hour = ingest_time - timedelta(hours=1)
        folder_path2 = f'{self.guid}/year={prev_hour.year}/month={prev_hour.month:02d}/day={prev_hour.day:02d}' + \
                       f'/hour={prev_hour.hour:02d}'

        paths = []
        paths += self.__get_file_paths_from_folder(folder_path1, filesystem_client)
        paths += self.__get_file_paths_from_folder(folder_path2, filesystem_client)

        return paths

    def __get_paths_since_last_run(self, filesystem_client: FileSystemClient, max_files: int) -> List[PathProperties]:
        state = self.__retrieve_transformation_state(filesystem_client)

        if not state:
            now = datetime.utcnow()
            folder_path = f'{self.guid}/year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}'

            paths = self.__get_file_paths_from_folder(folder_path, filesystem_client)

            return paths[:max_files]

        # There is a state file:

        last_successful_run = datetime.strptime(state['last_successful_run'], '%Y-%m-%dT%H:%M:%S')
        now = datetime.utcnow()
        time_range = pd.date_range(last_successful_run, now, freq='H')

        paths = []
        for timeslot in time_range:
            folder_path = f'{self.guid}/year={timeslot.year}/month={timeslot.month:02d}/day={timeslot.day:02d}' + \
                          f'/hour={timeslot.hour:02d}'

            temp_paths = self.__get_file_paths_from_folder(folder_path, filesystem_client)

            for path in temp_paths:
                if path.last_modified > last_successful_run:
                    paths.append(path)

            if len(paths) > max_files:
                break

        return paths[:max_files]

    def __retrieve_transformation_state(self, filesystem_client: FileSystemClient) -> Optional[Dict]:
        with filesystem_client.get_file_client(f'{self.guid}/{self.STATE_FILE}') as file_client:
            try:
                state = file_client.download_file().readall()
                return json.loads(state)
            except ResourceNotFoundError:
                return None

    def __save_transformation_state(self, filesystem_client: FileSystemClient, state: Dict):
        with filesystem_client.get_file_client(f'{self.guid}/{self.STATE_FILE}') as file_client:
            json_data = json.dumps(state)
            file_client.upload_data(json_data, overwrite=True)

    @staticmethod
    def __get_file_paths_from_folder(folder_path: str, file_system_client: FileSystemClient) -> List[PathProperties]:
        try:
            paths = list(file_system_client.get_paths(path=folder_path))
            paths.sort(key=lambda x: x.last_modified)
            return paths
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

    @initialize_client_auth
    def read(self, range_tracker: OffsetRangeTracker) -> Optional[Generator]:
        """
        Returns the content of the next file
        """
        for i in range(range_tracker.start_position(), range_tracker.stop_position()):
            if not range_tracker.try_claim(i):
                return

            path = self.file_paths[i].name
            with OsirisFileClient(self.account_url,
                                  self.filesystem_name, path,
                                  credential=self.client_auth.get_credential_sync()) as file_client:  # type: ignore

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

    def close(self):
        """
        Updates the transformation state file after a successful run. Its important this method gets called
        after the pipeline has run or else the datasource will keep processing already processed files.
        """

        # If ingest time its set, we will not update or use the state. Close() should not be called in this
        # situation.
        if self.has_ingest_time:
            return

        local_client_auth = ClientAuthorization(self.tenant_id, self.client_id, self.client_secret)

        with FileSystemClient(self.account_url, self.filesystem_name,
                              credential=local_client_auth.get_credential_sync()) as filesystem_client:
            state = self.__retrieve_transformation_state(filesystem_client)

            # state file doesn't exist. We create a fresh one.
            if not state:
                state = {}

            if len(self.file_paths):
                latest_modified = self.file_paths[-1].last_modified  # file_paths is sorted ascending.
                state['last_successful_run'] = latest_modified.isoformat()
                self.__save_transformation_state(filesystem_client, state)




class DatalakeFileSourceWithFileName(DatalakeFileSource):  # noqa
    """
    A Class to download files from Azure Datalake with file name.
    """

    @initialize_client_auth
    def read(self, range_tracker: OffsetRangeTracker) -> Optional[Generator]:
        """
        Returns the content of the next file.
        """
        for i in range(range_tracker.start_position(), range_tracker.stop_position()):
            if not range_tracker.try_claim(i):
                return

            path = self.file_paths[i].name
            with OsirisFileClient(self.account_url,
                                  self.filesystem_name, path,
                                  credential=self.client_auth.get_credential_sync()) as file_client:  # type: ignore
                content = file_client.download_file().readall()

                file_name = os.path.basename(path)

                yield file_name, content
