from dataclasses import dataclass
from typing import List, Dict, Tuple
from datetime import datetime

from . import *


@dataclass
class SyncJob:
    '''
    Represents a synchronization task for a single Aqotec table.

    Each job includes the database and table to sync, the sensor columns and their
    corresponding NAO sensor IDs, and the NAO asset identifiers.

    Attributes:
        db_name (str): Name of the database.
        table_name (str): Name of the table.
        sensor_columns (List[str]): Aqotec sensor column names (e.g. DP_0Wert).
        sensor_ids (List[str]): Corresponding NAO sensor IDs (same order as sensor_columns).
        instance_id (str): NAO AssetID for assignment.
        asset_id (str): NAO TwinID for the template group.
        start_time (datetime): Timestamp from which to start synchronization.
        unsynced (bool): True if the job represents an initial synchronization (not yet covered in local state).
    '''
    db_name: str
    table_name: str
    sensor_columns: List[str]
    sensor_ids: List[str]
    instance_id: str
    asset_id: str
    start_time: datetime
    unsynced: bool = False


class SyncPlanner:
    '''
    Plans synchronization tasks based on the current Aqotec–NAO mapping and existing sync state.

    This class determines:
    - Which tables/sensors have not yet been synchronized at all (`unsynced=True` jobs).
    - Which tables/sensors are already being tracked and need continued sync (`unsynced=False`).

    Attributes:
        Mapper (AqotecNaoMapping): Mapping information from NAO labels and Aqotec structure.
        SyncManager (SyncStateManager): Sync state storage and retrieval logic.
        first_sinc_time (datetime): Default fallback timestamp for new sync jobs.
        sync_jobs_unsynced (List[SyncJob]): Initial jobs for unsynchronized assets or sensors.
        sync_jobs (List[SyncJob]): Regular incremental sync jobs.
    '''


    def __init__(self, Mapper: AqotecNaoMapping, 
                 SyncManager: SyncStateManager,
                 first_sinc_time:datetime=datetime(2017, 1, 1)) -> None:
        '''
        Initializes the SyncPlanner with necessary mapping and state management.

        Args:
            Mapper (AqotecNaoMapping): Contains the current asset-to-table mapping between NAO and Aqotec.
            SyncManager (SyncStateManager): Tracks which (db, table, sensor) combinations have already been synchronized.
            first_sinc_time (datetime, optional): Default starting point for full syncs of new assets or sensors.
                                                Defaults to January 1, 2017.

        Notes:
            - `sync_jobs_unsynced` will hold jobs that need full sync (either new tables or newly added sensors).
            - `sync_jobs` will hold standard incremental jobs.
            - You must run `setJobsUnsinct()` before `setJobs()` to ensure data consistency.
        '''
        self.Mapper = Mapper
        self.SincManager = SyncManager
        self.first_sinc_time = first_sinc_time
        self.sync_jobs_unsynced:List[SyncJob] = []
        self.sync_jobs: List[SyncJob] = []


    def setJobsUnsynced(self) -> List[SyncJob]:
        '''
        Detects and generates sync jobs for assets or sensors that have never been synchronized.

        Strategy:
        - If a table is unknown (not in sync state), schedule full sync from `first_sinc_time`
        for both the historical and current database (if both exist).
        - If a table is known, but new sensor columns have been added, schedule sync
        for only those new sensors (until they catch up to the rest).

        Returns:
            List[SyncJob]: List of unsynchronized job definitions.
        '''

        for entry in self.Mapper.mapping:
            key = (entry.aqotec_db, entry.aqotec_table)

            sensor_columns = [dp.dp_position for dp in entry.sensor_models]
            sensor_ids = [dp.nao_sensor_id for dp in entry.sensor_models]

            if key not in self.SincManager.sync_states:

                base_kwargs = dict(
                    table_name=entry.aqotec_table,
                    sensor_columns=sensor_columns,
                    sensor_ids=sensor_ids,
                    instance_id=entry.instance_id,
                    asset_id=entry.asset_id,
                    start_time=self.first_sinc_time,
                    unsynced=True
                )

                if entry.aqotec_db:
                    self.sync_jobs_unsynced.append(SyncJob(
                        db_name=entry.aqotec_db,
                        **base_kwargs
                    ))

                if entry.aqotec_db_history:

                    self.sync_jobs_unsynced.append(SyncJob(
                        db_name=entry.aqotec_db_history,
                        **base_kwargs
                    ))

            else:
                _, synced_sensors = self.SincManager.sync_states[key]
                unsynced = [
                    (dp.dp_position, dp.nao_sensor_id)
                    for dp in entry.sensor_models if dp.dp_position not in synced_sensors
                ]

                if len(unsynced) > 0:
                    columns, ids = zip(*unsynced)

                    base_kwargs = dict(
                        table_name=entry.aqotec_table,
                        sensor_columns=list(columns),
                        sensor_ids=list(ids),
                        instance_id=entry.instance_id,
                        asset_id=entry.asset_id,
                        start_time=self.first_sinc_time,
                        unsynced=True
                    )

                    if entry.aqotec_db:
                        self.sync_jobs_unsynced.append(SyncJob(
                            db_name=entry.aqotec_db,
                            **base_kwargs
                        ))

                    if entry.aqotec_db_history:

                        self.sync_jobs_unsynced.append(SyncJob(
                            db_name=entry.aqotec_db_history,
                            **base_kwargs
                        ))


    def setJobs(self) -> List[SyncJob]:
        '''
        Creates incremental sync jobs for all tables and sensors already present in sync state.

        Notes:
            - This function cannot run while there are still unsynchronized jobs pending.
            - Uses the timestamp and known sensor columns from the sync database.

        Returns:
            List[SyncJob]: List of standard sync jobs.
        
        Raises:
            BufferError: If unsynchronized jobs still exist (must be resolved first).
        '''
        if len(self.sync_jobs_unsynced):
            raise BufferError("Can't set regular sync jobs while unsynced jobs still exist")

        for entry in self.Mapper.mapping:
            key = (entry.aqotec_db, entry.aqotec_table)

            if key not in self.SincManager.sync_states:
                continue 
            
            last_synced, synced_columns = self.SincManager.sync_states[key]

            dp_pairs = [
                (dp.dp_position, dp.nao_sensor_id)
                for dp in entry.sensor_models if dp.dp_position in synced_columns
            ]
            if not dp_pairs:
                continue

            columns, ids = zip(*dp_pairs)

            self.sync_jobs.append(SyncJob(
                db_name=entry.aqotec_db,
                table_name=entry.aqotec_table,
                sensor_columns=list(columns),
                sensor_ids=list(ids),
                instance_id=entry.instance_id,
                asset_id=entry.asset_id,
                start_time=last_synced,
                unsynced=False
            ))
