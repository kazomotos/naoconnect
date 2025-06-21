from contextlib import contextmanager
from typing import Optional
import pyodbc

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
            self.controller_id = data[0][3]
            self.line_id = data[0][2]

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
        self.columns = {}
        self.controller_ids = {}  

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
                if self.columns[tab].controller_id:
                    self.controller_ids[self.columns[tab].controller_id] = tab

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
            if tab[-2:] == "_b": # and len(tab) > 4:
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
        self.timeseries_tables = {}

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
    