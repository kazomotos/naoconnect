from contextlib import contextmanager
from typing import Optional, Dict, List, Tuple
from datetime import datetime, timezone
from zoneinfo import ZoneInfo 

import pyodbc

from . import *

class AqotecConnector():
    '''
    Utility class to handle connection and cursor management for the Aqotec database using pyodbc.
    '''

    def __init__(self,host,port,user,password,driver="{ODBC Driver 18 for SQL Server}") -> None:
        '''
        Initializes database connection string.

        Args:
            host (str): Database host.
            port (str): Port number.
            user (str): Username.
            password (str): Password.
            driver (str): ODBC driver name.
        '''
        self.connstring = f"DRIVER={driver};SERVER={host};PORT={port};UID={user};PWD={password};Encrypt=No"

    @contextmanager
    def withCursor(self, conn:Optional[pyodbc.Connection]=None):
        '''
        Context manager to safely open and close a database cursor.

        Args:
            conn (Optional[pyodbc.Connection]): Optional existing connection.

        Yields:
            pyodbc.Cursor: The opened database cursor.
        '''
        if conn:
            cursor = conn.cursor()
        else:
            cursor = self.conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    @contextmanager 
    def withConn(self):
        '''
        Context manager to safely open and close a database connection.

        Yields:
            pyodbc.Connection: The opened database connection.
        '''
        self.conn = pyodbc.connect(self.connstring)
        try:
            yield self.conn
        finally:
            self.conn.close()

    @contextmanager
    def withCursorConn(self):
        '''
        Context manager to safely open a new connection and its corresponding cursor.

        Yields:
            pyodbc.Cursor: The opened cursor.
        '''
        conn = pyodbc.connect(self.connstring)
        cursor = conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()
            conn.close()


class _AqotecColumnStruct():
    '''
    Internal class for retrieving and structuring column metadata from an Aqotec info table.
    '''

    def __init__(self, cursor:pyodbc.Cursor, table_name:str) -> None:
        '''
        Initializes column metadata and controller ID based on a given table.

        Args:
            cursor (pyodbc.Cursor): Database cursor for executing queries.
            table_name (str): Name of the info table (expected to end with "_b").
        '''
        self.__cursor = cursor
        self.table_name = table_name
        self.controller_id = None
        self.line_id = None
        self.all_columns = []

        self._setColumnsAndControllerID()


    def _setColumnsAndControllerID(self):
        '''
        Loads column names and controller/line IDs from the info table.

        Only works if table ends with "_b". Extracts column name and position.
        '''
        if self.table_name[-2:] != "_b":
            return([])
        
        self.__cursor.execute(f'Select * from "{self.table_name}"')
        data = self.__cursor.fetchall()
        
        if len(data) > 0:
            if str(data[0][3]) == self.table_name[:-2].split("R")[-1]:
                self.controller_id = data[0][3]
                self.line_id = data[0][2]
            else:
                pass
            
        self.all_columns = [(dat[5], dat[1]) for dat in data]
        

class _AqotecTableStruct():
    '''
    Class for reading and managing table-level structure of Aqotec databases.
    Extracts metadata from info tables and maps controller IDs.
    '''

    def __init__(self, connection:AqotecConnector, database_name:str) -> None:
        '''
        Initializes structure for a specific database.

        Args:
            connection (AqotecConnector): Database connector.
            database_name (str): Name of the database to process.
        '''
        self.AqotecConnetion = connection
        self.database_name = database_name
        self.columns:Dict[str, _AqotecColumnStruct] = {}

        self.all_tables = self._getTable()
        self.info_tables = self._searchInfoTables(self.all_tables)
        self._setColumns()


    def _setColumns(self) -> None:
        '''
        Reads and assigns column mappings for all info tables in the database.
        '''
        with self.AqotecConnetion.withCursor() as cursor:
            for tab in self.info_tables:
                self.columns[tab] = _AqotecColumnStruct(cursor=cursor, table_name=tab)

    def _getTable(self): 
        '''
        Retrieves a list of all table names from the selected Aqotec database.

        Returns:
            list: List of table name strings.
        '''
        with self.AqotecConnetion.withCursor() as cursor:
            cursor.execute(f'USE "{self.database_name}"')
            cursor.execute("SELECT table_name FROM information_schema.tables")
            res = [ val[0] for val in(cursor.fetchall())] 

        return( res )
    
    def _searchInfoTables(self, tables:list) -> None:
        '''
        Filters and returns only info tables (ending in "_b") from the table list.

        Args:
            tables (list): All tables in the database.

        Returns:
            list: Filtered list of info tables.
        '''
        res = []
        for tab in tables:
            if tab[-2:] == "_b":
                if tab[:-2] in tables:
                    res.append(tab)
        
        return(res)


class AqotecDatabaseStructure():
    '''
    High-level structure for scanning and organizing all Aqotec time-series databases.
    '''

    def __init__(self, connection:AqotecConnector):
        '''
        Initializes the database structure and scans all relevant time-series databases.

        Args:
            connection (AqotecConnector): Connector instance for Aqotec DB.
        '''
        self.AqotecConnetion = connection
        self.all_db_names = self._getDb()
        self.timeseries_db_names = self._searchTimeseriesDb(self.all_db_names)
        self.timeseries_tables:Dict[str, _AqotecTableStruct] = {}

        self._setTableDict()

    def _getDb(self) -> list:
        '''
        Lists all databases accessible from the Aqotec server.

        Returns:
            list: Names of all databases.
        '''
        with self.AqotecConnetion.withCursorConn() as cursor:
            cursor.execute("SELECT name FROM sys.databases")
            res = [ val[0] for val in(cursor.fetchall())]

        return(res)

    def _searchTimeseriesDb(self, dbs) -> list:
        '''
        Filters database names that represent time-series data.

        Args:
            dbs (list): List of all database names.

        Returns:
            list: Databases ending with "_Daten".
        '''
        res = [] 

        for db in dbs:
            if db[-6:] == "_Daten":
                res.append(db)

        return( res )
    
    def _setTableDict(self) -> None:
        '''
        Initializes and maps all tables belonging to time-series databases.
        '''
        with self.AqotecConnetion.withConn():
            for db_name in self.timeseries_db_names:
                self.timeseries_tables[db_name] = _AqotecTableStruct(connection=self.AqotecConnetion, database_name=db_name)


class AqotecJobExecutor:
    '''
    ...
    '''

    def __init__(self, connector:AqotecConnector, tz="utc"):
        '''
        ...
        '''
        self.tz = tz

        self.connector = connector

        if tz.lower() == "utc":
            self.tzinfo = timezone.utc
        else:
            # z.B. tz = "Europe/Berlin"
            self.tzinfo = ZoneInfo(tz)


    def fetchAndFormat(self, job) -> List[str]:
        '''
        Fetches sensor data for a given job and converts it to InfluxDB line protocol format.

        - Uses DATETIME2FROMPARTS for precise filtering.
        - Skips NaN values per sensor, not per row.
        - Only timestamps with at least one valid sensor value are included.

        Args:
            job (SyncJob): Job definition containing table, columns, and sync start.

        Returns:
            List[str]: List of line protocol strings.
        '''
        table = job.table_name
        table = job.table_name
        db = job.db_name
        dt = job.start_time
        columns = job.sensor_columns
        sensor_ids = job.sensor_ids
        columns_str = ", ".join(f'"{col}"' for col in columns)

        sql = f'''
            SELECT DP_Zeitstempel, {columns_str}
            FROM "{table[:-2]}"
            WHERE DP_Zeitstempel > DATETIME2FROMPARTS({dt.year}, {dt.month}, {dt.day}, {dt.hour}, {dt.minute}, {dt.second}, {dt.microsecond // 1000}, 3)
            ORDER BY DP_Zeitstempel ASC;
        '''

        lines: List[str] = []
        last_time:datetime = None
        values_count = 0

        try:
            with self.connector.withCursorConn() as cursor:
                cursor.execute(f"USE {db}")
                cursor.execute(sql)
                for row in cursor.fetchall():
                    ts = row[0]
                    values = row[1:]

                    if ts.tzinfo is None:
                        local_dt = ts.replace(tzinfo=self.tzinfo)
                    else:
                        # Falls DB wider Erwarten schon tzinfo hat, einfach nehmen
                        local_dt = ts

                    ts_utc = local_dt.astimezone(timezone.utc)

                    fields = []
                    for sid, val in zip(sensor_ids, values):
                        if val is not None:
                            values_count += 1
                            fields.append(f'{sid}={val}')

                    if fields:
                        last_time = ts_utc
                        # 3) Unix-Zeit in ns aus **UTC**-Zeit
                        timestamp_ns = int(ts_utc.timestamp() * 1e9)
                        line = (
                            f'{job.asset_id},instance={job.instance_id} '
                            + ",".join(fields)
                            + f' {timestamp_ns}'
                        )
                        lines.append(line)
                        
        except:
            if "_archiv" not in db:
                raise
            else:
                print("archive: \n", sql)

        return last_time, lines, values_count
