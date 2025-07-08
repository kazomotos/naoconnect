import sqlite3
import json
from typing import List, Dict, Tuple
from datetime import datetime

from . import *

class SyncStateManager:
    '''
    Manages local synchronization state for Aqotec tables.

    Tracks the last synchronized timestamp and associated sensor columns for
    each (database, table) pair. Timestamps are handled as `datetime` objects.
    '''

    def __init__(self, db_path: str) -> None:
        '''
        Initializes the SQLite connection and ensures the sync_state table exists.

        Args:
            db_path (str): Path to the local SQLite file.
        '''
        self.conn = sqlite3.connect(db_path)
        self._initTable()
        self.sync_states:Dict[Tuple[str, str], Tuple[datetime, List[str]]] = self._initGetAllSyncStates()

    def _initTable(self) -> None:
        '''
        Creates the sync_state table if it doesn't already exist.
        '''
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS sync_state (
                db_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                last_synced TEXT NOT NULL,
                sensor_columns TEXT NOT NULL,
                PRIMARY KEY (db_name, table_name)
            );
        ''')
        self.conn.commit()

    def _initGetAllSyncStates(self) -> Dict[Tuple[str, str], Tuple[datetime, List[str]]]:
        '''
        Returns all current synchronization states.

        Returns:
            Dict[(db_name, table_name), (last_synced (datetime), sensor_columns (List[str]))]
        '''
        cur = self.conn.execute("SELECT db_name, table_name, last_synced, sensor_columns FROM sync_state")
        result = {}
        for db_name, table_name, last_synced_str, sensor_columns_json in cur.fetchall():
            last_synced = datetime.fromisoformat(last_synced_str)
            sensors = json.loads(sensor_columns_json)
            result[(db_name, table_name)] = (last_synced, sensors)
        return result

    def updateSyncTime(self, db_name: str, table_name: str, new_time: datetime) -> None:
        '''
        Updates the last synchronized timestamp for the given (db, table).

        Args:
            db_name (str): Name of the Aqotec database.
            table_name (str): Name of the table within the database.
            new_time (datetime): Timestamp to set as last synced.
        '''
        self.conn.execute('''
            UPDATE sync_state
            SET last_synced = ?
            WHERE db_name = ? AND table_name = ?;
        ''', (new_time.isoformat(), db_name, table_name))
        self.conn.commit()

    def updateSyncedColumns(self, db_name: str, table_name: str, columns: List[str], last_synced: datetime) -> None:
        '''
        Sets the synced sensor columns and last sync time for a given (db, table).

        Args:
            db_name (str): Name of the Aqotec database.
            table_name (str): Table name.
            columns (List[str]): List of Aqotec sensor column names.
            last_synced (datetime): Last synced timestamp.
        '''
        columns_json = json.dumps(columns)
        self.conn.execute('''
            INSERT INTO sync_state (db_name, table_name, last_synced, sensor_columns)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(db_name, table_name) DO UPDATE SET
                last_synced=excluded.last_synced,
                sensor_columns=excluded.sensor_columns;
        ''', (db_name, table_name, last_synced.isoformat(), columns_json))
        self.conn.commit()

    def updateWithFinishJob(self, job, time: datetime) -> None:
        '''
        Updates the internal synchronization state after a completed SyncJob.

        This method handles two cases:
        
        1. **Unsynced Jobs (`job.unsynced == True`)**
        - If the (db, table) already exists in sync state:
            - New sensor columns (if any) are added to the state.
            - The existing `last_synced` timestamp is retained (not updated).
        - If the (db, table) is new:
            - Inserts new entry with all job.sensor_columns and given time.
        
        2. **Regular Jobs (`job.unsynced == False`)**
        - Updates only the `last_synced` timestamp for the (db, table).
        - Keeps the set of sensor columns unchanged.

        Args:
            job (SyncJob): The finished sync job.
            time (datetime): The latest timestamp retrieved during this job (used for updates if applicable).
        '''
        if "_archiv" in job.table_name:
            key = (job.db_name, job.table_name[:-7])
        else:
            key = (job.db_name, job.table_name)

        if job.unsynced:
            existing = self.sync_states.get(key)

            if existing:
                old_time, old_sensors = existing
                updated_sensor_list = list(set(old_sensors).union(set(job.sensor_columns)))
                self.updateSyncedColumns(
                    job.db_name,
                    job.table_name,
                    updated_sensor_list,
                    old_time
                )
                self.sync_states[key] = (old_time, updated_sensor_list)

            else:
                self.updateSyncedColumns(
                    job.db_name,
                    job.table_name,
                    job.sensor_columns,
                    time
                )
                self.sync_states[key] = (time, job.sensor_columns)

        else:
            self.updateSyncTime(job.db_name, job.table_name, time)
            if key in self.sync_states:
                _, sensors = self.sync_states[key]
                self.sync_states[key] = (time, sensors)

    def closeConn(self) -> None:
        '''
        Closes the database connection.
        '''
        self.conn.close()
