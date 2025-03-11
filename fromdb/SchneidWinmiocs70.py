'Autor: Rupert Wieser -- Naotilus -- 20232209'
import psycopg2
from naoconnect.local_db import Driver, StationDatapoints, LablingNao, SyncronizationStatus
from pandas import Series
from typing import Dict, Union
import pandas as pd
from datetime import datetime, timedelta
from naoconnect.naoappV2 import NaoApp
from time import sleep, time
import sys
import csv
import re
import pytz
from os import path, listdir
from json import loads, dumps
from io import StringIO
from numpy import isnan, nan

'''

'''



# '''
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
#                                                         Params
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# '''

class SchneidParamWinmiocs70():
    '''
    Base class that provides shared constants and parameters for interacting with Schneid Winmiocs 70 devices and data.

    This class defines various constants, configuration parameters, and metadata identifiers that are commonly 
    used across all subclasses that interact with Schneid Winmiocs 70 systems. These values help standardize the 
    communication with Winmiocs 70 devices, data handling, and synchronization processes.

    Key attributes include:
        - **File formats** for different types of data (`HAST_FILE_FORMAT`, `WZ_FILE_FORMAT`, etc.).
        - **Database identifiers** and metadata keys (e.g., `DICTNAME_INFO_HEADER`, `DICTNAME_DB_NUMBER`).
        - **Synchronization-related constants** such as default times and sleep intervals for transferring data.
        - **Metadata fields** like serial numbers, station types, and customer names that are used for asset management.
    
    This class is designed to be inherited by other classes that need access to these shared variables.

    **Note**:
        If the scope of the class grows significantly in the future, it may be more appropriate to switch to using
        `dataclasses` or other data structures to handle attributes in a more modular and maintainable way.
    '''
    STATUS_CODE_GOOD = 204
    HAST_FILE_FORMAT = "_prot.csv"
    WZ_FILE_FORMAT = "_protWZ.csv"
    DATA_FILE_FORMAT = "prot"
    DATA_FILE_FORMAT_ENDING = ".csv"
    NAME_UG06_CSV = "UG06"
    NAME_UG08_CSV = "UG08"
    NAME_UG10_CSV = "UG10"
    NAME_UG12_CSV = "UG12"
    NAME_ACT_WMZ = "ACT_WMZ"
    NAME_ONLY_MBUS = "GEN_MBUS_"
    DICTNAME_INFO_HEADER = "info_header"
    DICTNAME_COLUMN_HEADER = "column_header"
    DICTNAME_DB_NUMBER = "database_number"
    DICTNAME_DATAPOINT_NAME = "datapoint_name"
    DICTNAME_POSITION = "position"
    DICTNAME_N = "N"
    DICTNAME_D = "D"
    DICTNAME_SH = "SH"
    DICTNAME_SCHNEID_TB_ID = "tablel_id"
    DICTNAME_SCHNEID_STATION_TYPE = "station_type"
    DICTNAME_LAST_WRITE_TIME = "last_write_time"
    DICTNAME_INTERVAL = "interval"
    DICTNAME_SCHNEID_ANID = "AnID"
    DICTNAME_CUSTOMER_NAME = "customer_name"
    DICTNAME_CUSTOMER_SECOND_NAME = "customer_second_name"
    DICTNAME_TEL_NUMBER = "tel_number"
    DICTNAME_STREED = "streed"
    DICTNAME_METER_TYPE = "meter_type"
    LT = "lt"
    GT = "gt"
    B1 = "b1"
    B2 = "b2"
    NAME_ASSET_ONLY_MBUS_WMZ = "only_mbus_wz"
    NAME_ASSET_UG06 = "ug06"
    NAME_ASSET_UG08 = "ug08"
    NAME_ASSET_UG10 = "ug10"
    NAME_ASSET_UG12 = "ug12"
    NAME__ID = "_id"
    NAME_ID = "id"
    NAME_TYPE = "type"
    NAME_VALUE = "value"
    NAME_DATABASE = "database"
    NAME_DP = "dp"
    NAME_DP_NAME = "name_dp"
    NAME_TABLE = "table"
    NAME_ASSET_ID = "_asset"
    NAME_DB_ASSET_ID = "asset_id"
    NAME_DP_POS = "dp_pos"
    NAME_NAME = "name"
    NAME_META_VALUES = "attributevalues"
    NAME_META_ID = "_attribute"
    NAME_NUMBER = "number"
    NAME_INTEGER = "integer"
    NAME_DB_INSTANCE_ID = "instance_id"
    NAME_META_ID_DB = "meta_id"
    NAME_SENSOR_ID = "sensor_id"
    NAME_SYNCRONICZIED = "syncronizied"
    NAME_UNSYCRONICIZIED = "unsyncronizied"
    NAME_TIME_SYNCRONICZIED_META = "time_syncronizied_meta"
    NAME_TIME_SYNCRONICZIED = "time_sincronizied"
    NAME_TIME_UNSYCRONICIZIED = "time_unsyncronizied"
    DEFAULT_TRANSFER_TIME_SCHNEID = 300
    DEFAULT_SCHNEID_TIMEZONE = 'Europe/Berlin'
    DEFAULT_TRASFER_SLEEPER_SECOND = 60*5
    DEFAULT_ERROR_SLEEP_SECOND = 300
    DEFAULT_BREAK_TELEGRAF_LEN = 50000
    DEFAULT_FIRST_TIME_SCHNEID = datetime(2014,1,1)




# '''
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
#                                                    Database Struct
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# '''

class DbStruct():
    '''
    A class to manage and categorize database tables based on Schneid station types.
    
    This class organizes tables into different station types (e.g., `ug06`, `ug08`, etc.), 
    where each station type contains a list of tables associated with a specific database.
    The station types represent different types of Schneid systems, and each category 
    stores the related tables for further processing.
    '''
    
    def __init__(self) -> None:
        '''
        Initializes the DbStruct object with empty dictionaries for each station type category.
        
        Each station type (e.g., `ug06`, `ug08`, etc.) is represented as a dictionary,
        where the key is the database name and the value is a list of tables.
        '''
        self.ug06 = {}  # Represents UG06 station type tables
        self.ug08 = {}  # Represents UG08 station type tables
        self.ug10 = {}  # Represents UG10 station type tables
        self.ug12 = {}  # Represents UG12 station type tables
        self.wzmbushast = {}  # Represents heat meter MBus as station type tables
        self.other = {}  # Represents other station types

    def putUg06(self, database, table):
        '''
        Adds a table to the 'ug06' category under the specified database.
        
        If the database is not already in the 'ug06' category, a new entry is created.
        
        Args:
            database (str): The name of the database.
            table (str): The name of the table to be added.
        '''
        if database not in self.ug06:
            self.ug06[database] = []
        self.ug06[database].append(table)

    def putUg08(self, database, table):
        '''
        Adds a table to the 'ug08' category under the specified database.
        
        If the database is not already in the 'ug08' category, a new entry is created.
        
        Args:
            database (str): The name of the database.
            table (str): The name of the table to be added.
        '''
        if database not in self.ug08:
            self.ug08[database] = []
        self.ug08[database].append(table)

    def putUg10(self, database, table):
        '''
        Adds a table to the 'ug10' category under the specified database.
        
        If the database is not already in the 'ug10' category, a new entry is created.
        
        Args:
            database (str): The name of the database.
            table (str): The name of the table to be added.
        '''
        if database not in self.ug10:
            self.ug10[database] = []
        self.ug10[database].append(table)

    def putUg12(self, database, table):
        '''
        Adds a table to the 'ug12' category under the specified database.
        
        If the database is not already in the 'ug12' category, a new entry is created.
        
        Args:
            database (str): The name of the database.
            table (str): The name of the table to be added.
        '''
        if database not in self.ug12:
            self.ug12[database] = []
        self.ug12[database].append(table)
    
    def putWzMbusHast(self, database, table):
        '''
        Adds a table to the 'wzmbushast' category under the specified database.
        
        If the database is not already in the 'wzmbushast' category, a new entry is created.
        
        Args:
            database (str): The name of the database.
            table (str): The name of the table to be added.
        '''
        if database not in self.wzmbushast:
            self.wzmbushast[database] = []
        self.wzmbushast[database].append(table)

    def putOther(self, database, table):
        '''
        Adds a table to the 'other' category under the specified database.
        
        If the database is not already in the 'other' category, a new entry is created.
        
        Args:
            database (str): The name of the database.
            table (str): The name of the table to be added.
        '''
        if database not in self.other:
            self.other[database] = []
        self.other[database].append(table)  




# '''
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
#                                                        CSV Data Class
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# '''

class SchneidCsvWinmiocs70(SchneidParamWinmiocs70):
    '''
    A class for handling the reading and processing of Schneid time series data stored in custom CSV formats.
    
    Schneid stores high-resolution time series data (5-minute intervals) in CSV files, which have a special header format. 
    The CSV files may change in structure when the station controller is updated, which means the number of columns may vary. 
    This class addresses such cases by managing the files, parsing their headers, and reading the latest data efficiently without 
    the need to process the entire file.
    
    The class can read data from various asset types, including house stations, heat generators, grid injection, heat meters, 
    electricity meters, and more. It can filter and process relevant assets based on their file names and metadata.
    '''
    CSV_ENCODING = 'ISO-8859-1'
    CSV_DELIMITER = ";"
    REVERSED_BUFFER_SIZE = 600
    COLUMN_COMPILER = re.compile(r'<COL\s+NR="(\d+)">(.*?)</COL>', re.DOTALL)
    TIME_COMPILER = re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')
    COLUMN_SENSOR_NAME_REG = r'<NAME>(.*?)</NAME>'
    COLUMN_N_REG = r'<N>(.*?)</N>'
    COLUMN_D_REG = r'<D>(.*?)</D>'
    COLUMN_SH_REG = r'<SH>(.*?)</SH>'
    TABLE_ID_REG = r'<PROTHDR ID="(.*?)">'
    TABLE_STATION_REG = r'<JOBID>(.*?)</JOBID>'
    TABLE_LAST_WRITE_TIME_REG = r'<LASTACT(.*?)</LASTACT>'
    TABLE_INTERVAL_REG = r'<INTVSEC>(.*?)</INTVSEC>'
    TIME_FORMAT_LAST_TIME = '%d.%m.%Y %H:%M:%S'
    TIME_FORMAT_TIMESTEPS = '%Y-%m-%d %H:%M:%S'


    def __init__(self, file_path: str = None) -> None:
        '''
        Initializes the SchneidCsvWinmiocs70 object, optionally setting the file path to the CSV data directory.
        
        Args:
            file_path (str, optional): The directory path where the CSV files are stored.
        '''
        self.file_path = file_path
        self.files = []
        self.hast_files = []
        self.files_columns = {}
        self.files_infos = {}

        if self.file_path: 
            self.restetFiles()


    def setFilesFromDirectory(self) -> None:
        '''
        Sets the list of files from the directory specified in `self.file_path`.
        
        If no file path is provided, the list will be empty.
        '''
        if self.file_path: 
            self.files:list = listdir(self.file_path)
        else: 
            self.files:list = []

    def setHastFiles(self) -> None:
        '''
        Filters the files to identify the "HAST" files, which are stored in `self.hast_files`.
        
        These files are filtered based on the `HAST_FILE_FORMAT` keyword in the filename.
        '''
        hast_files = []
        if not self.files: 
            self.hast_files:list=hast_files; return(-1)
        
        for file in self.files:
            if SchneidCsvWinmiocs70.HAST_FILE_FORMAT in file: hast_files.append(file)

        self.hast_files = hast_files
    
    def setFileInfos(self) -> None:
        '''
        Collects and stores the header information and columns from the CSV files that match the `DATA_FILE_FORMAT`.
        
        The header information is used to identify the structure of each file, and the column headers are parsed for each CSV.
        '''
        for file in self.files:
            if SchneidCsvWinmiocs70.DATA_FILE_FORMAT not in file: continue
            if SchneidCsvWinmiocs70.DATA_FILE_FORMAT_ENDING not in file: continue
            header = self.getFileHeader( file )
            self.files_columns[file] = self.getColsFromColumnHeader( header[SchneidCsvWinmiocs70.DICTNAME_COLUMN_HEADER] )
            self.files_infos[file] = self.getInfoFromInfoHeader( header[SchneidCsvWinmiocs70.DICTNAME_INFO_HEADER] )


    def restetFiles(self) -> None:
        '''
        Resets the file lists and refreshes the file information by calling `setFilesFromDirectory`, 
        `setHastFiles`, and `setFileInfos`.
        '''
        self.setFilesFromDirectory()
        self.setHastFiles()
        self.setFileInfos()


    def getFileHeader(self, file_name: str) -> dict:
        '''
        Retrieves the header information from a given CSV file.
        
        The header includes the info header and column header, which are used for further processing.
        
        Args:
            file_name (str): The name of the CSV file.
        
        Returns:
            dict: A dictionary containing the info and column headers.
        '''
        with open(self.file_path+"/"+file_name, mode='r', newline='', encoding=SchneidCsvWinmiocs70.CSV_ENCODING) as file:
            csv_reader = csv.reader(file, delimiter=SchneidCsvWinmiocs70.CSV_DELIMITER)
            try: 
                header = next(csv_reader)
            except: 
                header = ["", ""]

        return({SchneidCsvWinmiocs70.DICTNAME_INFO_HEADER:header[0],SchneidCsvWinmiocs70.DICTNAME_COLUMN_HEADER:header[1]})


    def getColsFromColumnHeader(self, header: str):
        '''
        Parses the column header to extract the sensor names, N, D, and SH values.
        
        Args:
            header (str): The column header string.
        
        Returns:
            dict: A dictionary of parsed column data with the sensor's name, N, D, and SH values.
        '''
        cols = SchneidCsvWinmiocs70.COLUMN_COMPILER.findall(header)
        cols_data = {}
        position_col = 0

        for nr, content in cols:
            name = re.search(SchneidCsvWinmiocs70.COLUMN_SENSOR_NAME_REG, content)
            name = name.group(1) if name else None

            if name == None: 
                position_col += 1
                continue

            n = re.search(SchneidCsvWinmiocs70.COLUMN_N_REG, content)
            n = n.group(1) if n else None
            d = re.search(SchneidCsvWinmiocs70.COLUMN_D_REG, content)
            d = d.group(1) if d else None
            sh = re.search(SchneidCsvWinmiocs70.COLUMN_SH_REG, content)
            sh = sh.group(1) if sh else None

            cols_data[name] = {
                SchneidCsvWinmiocs70.DICTNAME_DB_NUMBER: nr,
                SchneidCsvWinmiocs70.DICTNAME_DATAPOINT_NAME: name,
                SchneidCsvWinmiocs70.DICTNAME_N: n,
                SchneidCsvWinmiocs70.DICTNAME_D: d,
                SchneidCsvWinmiocs70.DICTNAME_SH: sh,
                SchneidCsvWinmiocs70.DICTNAME_POSITION: position_col
            }

            position_col += 1

        return(cols_data)

    def getInfoFromInfoHeader(self, header: str) -> dict:
        '''
        Extracts and parses the information from the info header.
        
        Args:
            header (str): The info header string.
        
        Returns:
            dict: A dictionary containing the parsed table ID, station type, last write time, and interval.
        '''
        internal_id = re.search(SchneidCsvWinmiocs70.TABLE_ID_REG, header)
        internal_id = internal_id.group(1) if internal_id else None
        station_type = re.search(SchneidCsvWinmiocs70.TABLE_STATION_REG, header)
        station_type = station_type.group(1) if station_type else None
        last_write_time = re.search(SchneidCsvWinmiocs70.TABLE_LAST_WRITE_TIME_REG, header)

        try: 
            last_write_time = datetime.strptime(last_write_time.group(1).split(">")[1],SchneidCsvWinmiocs70.TIME_FORMAT_LAST_TIME) if last_write_time else None
        except: 
            last_write_time = None

        interval = re.search(SchneidCsvWinmiocs70.TABLE_INTERVAL_REG, header)
        interval = interval.group(1) if interval else None

        return({
            SchneidCsvWinmiocs70.DICTNAME_SCHNEID_TB_ID: internal_id,
            SchneidCsvWinmiocs70.DICTNAME_SCHNEID_STATION_TYPE: station_type,
            SchneidCsvWinmiocs70.DICTNAME_LAST_WRITE_TIME: last_write_time,
            SchneidCsvWinmiocs70.DICTNAME_INTERVAL: interval
        })

    def readCsvDataReverseAsDataFrame(self, file_name: str, lines: int = 10) -> pd.DataFrame:
        '''
        Reads the CSV file and returns the most recent lines of data as a DataFrame.

        This method optimizes the process by reading the file in reverse, starting from the end of the file, 
        to avoid loading the entire file when only the latest data is needed. This is particularly useful for 
        large CSV files where only the most recent entries are required, thus saving memory and processing time.

        The file is read in chunks, and the method attempts to load only the required number of lines. If errors 
        occur while parsing the CSV, the method will attempt to recover by re-reading the file starting from the 
        error location.

        Args:
            file_name (str): The name of the CSV file to read.
            lines (int): The number of lines to read (default is 10). If 'all' is passed, the entire file is read.

        Returns:
            pd.DataFrame: The data read from the CSV file, converted into a Pandas DataFrame. The DataFrame will 
            have the following characteristics:
                - The first column will be converted to datetime format and set as the index.
                - The columns will be automatically numbered starting from 0.
                - The data will be read in reverse order, starting from the most recent entries.

        Detailed Explanation:
            - The method reads the file in reverse order by adjusting the file pointer to read from the end of 
            the file, which improves performance by not loading the entire file into memory when only the 
            most recent data is needed.
            - The file is read in chunks, and a loop is used to ensure that enough data is fetched to cover the 
            specified number of lines.
            - If the file contains errors or is not correctly formatted, the method tries to detect the error by 
            examining the line number and then attempts to reload the data from the error location onward.
            - Error locations typically occur when Schneid switches the controller (Regler), as this may cause 
            changes to the CSV structure. In such cases, the CSV format may change (e.g., different headers, 
            missing columns), leading to parsing errors. The method will attempt to handle these changes by 
            reading the file from the error point onward to ensure correct data extraction.
            - After reading the data, the method converts the first column (assumed to be the timestamp) into 
            a datetime object, which is essential for time series data.
            - The DataFrame's index is set to this datetime column, and the columns are automatically numbered 
            to maintain consistency in the DataFrame structure.

            This approach is designed for high-performance data processing, especially when working with large 
            time series data stored in CSV files where only the most recent records are required.
        '''
        if lines == 'all':
            with open(self.file_path + "/" + file_name, 'r', encoding=SchneidCsvWinmiocs70.CSV_ENCODING) as file:
                buffer = file.read().split("\n")

        else:
            muli = 1

            while 1==1:

                with open(self.file_path + "/" + file_name, 'r', encoding=SchneidCsvWinmiocs70.CSV_ENCODING) as file:
                    file.seek(0, 2)
                    file_end = file.tell()
                    position = file_end-SchneidCsvWinmiocs70.REVERSED_BUFFER_SIZE*lines*muli
                    position = max([0,position])
                    file.seek(position)
                    buffer = file.read(file_end - position)

                buffer = buffer.split("\n")
                muli+=1

                if len(buffer) > lines+1 or position==0:
                    break

                if buffer[-1]=="":
                    buffer=buffer[-lines-1:-1]

                else: 
                    buffer = buffer[-lines:]

        for buff in range(len(buffer)):
            if bool(SchneidCsvWinmiocs70.TIME_COMPILER.match(buffer[buff].split(SchneidCsvWinmiocs70.CSV_DELIMITER)[0])):
                break

        try:
            buffer = pd.read_csv(StringIO('\n'.join(buffer[buff:])), sep=SchneidCsvWinmiocs70.CSV_DELIMITER, header=None, encoding=SchneidCsvWinmiocs70.CSV_ENCODING)

        except pd.errors.ParserError as e:
            match = re.search(r'line (\d+)', str(e))
            line_number = int(match.group(1))

            try:
                buffer = pd.read_csv(StringIO('\n'.join(buffer[line_number+buff:])), sep=SchneidCsvWinmiocs70.CSV_DELIMITER, header=None, encoding=SchneidCsvWinmiocs70.CSV_ENCODING)
            
            except  pd.errors.ParserError as e:
                buffer = pd.read_csv(StringIO('\n'.join(buffer[line_number+100+buff:])), sep=SchneidCsvWinmiocs70.CSV_DELIMITER, header=None, encoding=SchneidCsvWinmiocs70.CSV_ENCODING)
        
        buffer[0] = pd.to_datetime(buffer[0], format=SchneidCsvWinmiocs70.TIME_FORMAT_TIMESTEPS)
        buffer.set_index(0, inplace=True)
        buffer.columns = range(len(buffer.columns))

        return(buffer)




# '''
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
#                                                    Connetct to Db Data Class
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# '''

class ScheindPostgresWinmiocs70(SchneidParamWinmiocs70):
    '''
    A class to interact with the Schneid Winmiocs 70 software's PostgreSQL database.

    This class provides methods to establish a connection to the PostgreSQL database,
    execute queries to retrieve and manipulate data related to the Winmiocs 70 system, 
    and disconnect from the database. It uses specific SQL queries defined in the 
    class variables to extract data from the system, such as node and serial information 
    for heat meters, as well as metadata about the system's nodes.   
    '''
    SQL_GET_NODES = "SELECT * FROM node;"
    SQL_TIME_FORMATTER = '%Y-%m-%d %H:%M:%S'
    SQL_GET_SERIAL_BY_ID = '''
        SELECT time, ser, err
        FROM cnt
        WHERE node = '%s' AND time > '%s'
        ORDER BY time;
    '''

    def __init__(self, hostname: str = "localhost", database: str = "postgres", username: str = "winmiocs", password: str = "") -> None:
        '''
        Initializes the database connection parameters.

        Args:
            hostname (str): The hostname of the database server (default is "localhost").
            database (str): The name of the database to connect to (default is "postgres").
            username (str): The username for database authentication (default is "winmiocs").
            password (str): The password for database authentication (default is an empty string).
        '''
        self.hostname = hostname
        self.database = database
        self.username = username
        self.password = password
        self.conn = None

    def connectToDb(self) -> None:
        '''
        Establishes a connection to the PostgreSQL database using the provided 
        connection parameters.
        '''
        self.conn = psycopg2.connect(
            host=self.hostname,
            dbname=self.database,
            user=self.username,
            password=self.password
        )

    def disconnectToDb(self) -> None:
        '''
        Closes the connection to the PostgreSQL database.
        '''
        self.conn.close()

    def getMetaDataGroupedById(self) -> dict:
        '''
        Retrieves metadata from the Schneid PostgreSQL database, grouped by 
        controller ID.

        This function queries the database for metadata records and organizes 
        them into a dictionary, where each key represents a controller ID 
        (`AnID`) and its value is a sub-dictionary containing associated metadata.

        Returns:
            dict: A dictionary containing metadata grouped by controller ID. 
                  Each key is the controller ID (`AnID`), and the value is a 
                  dictionary with the following keys:
                  - `AnID`: The controller ID.
                  - `customer_name`: The customer's name.
                  - `streed`: The street address.
                  - `tel_number`: The customer's telephone number.
                  - `customer_second_name`: A secondary name for the customer.
                  - `meter_type`: The type of meter.

        Notes:
            If an error occurs during the database connection or query 
            execution, the function catches the exception and returns an 
            empty dictionary `{}`.
        '''
        try:
            self.connectToDb()
            cur = self.conn.cursor()
            cur.execute(ScheindPostgresWinmiocs70.SQL_GET_NODES)
            rows = cur.fetchall()
            cur.close()
            self.disconnectToDb()
            meta = {}
            for row in rows:
                meta[row[1]] = {
                    ScheindPostgresWinmiocs70.DICTNAME_SCHNEID_ANID: row[1],
                    ScheindPostgresWinmiocs70.DICTNAME_CUSTOMER_NAME: row[2],
                    ScheindPostgresWinmiocs70.DICTNAME_STREED: row[3],
                    ScheindPostgresWinmiocs70.DICTNAME_TEL_NUMBER: row[4],
                    ScheindPostgresWinmiocs70.DICTNAME_CUSTOMER_SECOND_NAME: row[5],
                    ScheindPostgresWinmiocs70.DICTNAME_METER_TYPE: row[7]
                }
            return(meta)
        except: 
            return({})

    def getSerialSeriesByControllerId(self, controller_id:int, start_time:datetime) -> pd.DataFrame:
        ''' 
        Retrieves a time series of serial numbers and error states for a heat 
        meter from the Schneid PostgreSQL database.

        This function queries the database to return a pandas DataFrame 
        containing the serial numbers, error messages, and timestamps associated
        with a specific controller ID. The DataFrame includes the following columns:
        - `serial` (str): The serial number of the heat meter.
        - `error` (str): Any error messages associated with the heat meter.
        - `time` (datetime): The timestamp of the associated data.

        Args:
            controller_id (int): The ID of the controller to query for 
                                associated serial data.
            start_time (datetime): The starting point in time for the query.

        Returns:
            pd.DataFrame: A DataFrame with columns:
                - `serial` (str): The serial number of the heat meter.
                - `error` (str): The error message, if any.
                - `time` (datetime): The timestamp of the associated data.

        Raises:
            Any exception raised during database connection or query execution 
            will propagate upwards unless handled outside this function.
        '''
        str_start_time = start_time.strftime(ScheindPostgresWinmiocs70.SQL_TIME_FORMATTER)
        self.connectToDb()
        cur = self.conn.cursor()
        cur.execute(ScheindPostgresWinmiocs70.SQL_GET_SERIAL_BY_ID%(controller_id,str_start_time))
        rows = cur.fetchall()
        cur.close()
        self.disconnectToDb()
        time_list = []
        serial_list = []
        error_list = []
        for row in rows:
            if row[0]==None or row[1]==None:continue
            time_list.append(row[0])
            serial_list.append(row[1])
            error_list.append(row[2])
        result_frame = pd.DataFrame({"serial":serial_list,"error":error_list, "time":time_list})
        return( result_frame )


# '''
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
#                                                    Meta Data Class
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# '''

class SchneidMeta(SchneidParamWinmiocs70):

    def __init__(self,
                 SchneidPostgres:ScheindPostgresWinmiocs70,
                 SchneidCSV:SchneidCsvWinmiocs70,
                 NewDriver:Driver,
                 LablingPoints:StationDatapoints,
                 LablingNao:LablingNao,
                 SyncStatus:SyncronizationStatus,
                 NaoApp:NaoApp,
                 workspace_name:str,
                 work_defalut_name_instance:str="Daten",
                 only_mbus_wz_as_hast:bool=False,
                 seperator_name_work_instance:str=" \u2014 ") -> None:
        self.only_mbus_wz_as_hast = only_mbus_wz_as_hast
        self.seperator_name_work_instance = seperator_name_work_instance
        self.work_defalut_name_instance = work_defalut_name_instance
        self.workspace_name = workspace_name
        self.postgres = SchneidPostgres
        self.csvs = SchneidCSV
        self.driver_db = NewDriver
        self.labled_points = LablingPoints
        self.labled_nao = LablingNao
        self.sync_status = SyncStatus
        self.nao = NaoApp
        self.struct = DbStruct()
        self._setCsvStruct()
        self.user_id_nao = None
        self.check_point_buffer:pd.DataFrame = pd.DataFrame([])
            
    def _setCsvStruct(self):
        for csv in self.csvs.files_infos:
            if SchneidMeta.NAME_UG06_CSV in self.csvs.files_infos[csv][SchneidMeta.DICTNAME_SCHNEID_STATION_TYPE]:
                self.struct.putUg06(self.workspace_name,csv)
            elif SchneidMeta.NAME_UG08_CSV in self.csvs.files_infos[csv][SchneidMeta.DICTNAME_SCHNEID_STATION_TYPE]:
                self.struct.putUg08(self.workspace_name,csv)
            elif SchneidMeta.NAME_UG10_CSV in self.csvs.files_infos[csv][SchneidMeta.DICTNAME_SCHNEID_STATION_TYPE]:
                self.struct.putUg10(self.workspace_name,csv)
            elif SchneidMeta.NAME_UG12_CSV in self.csvs.files_infos[csv][SchneidMeta.DICTNAME_SCHNEID_STATION_TYPE]:
                self.struct.putUg12(self.workspace_name,csv)
            elif SchneidMeta.NAME_ACT_WMZ in self.csvs.files_infos[csv][SchneidMeta.DICTNAME_SCHNEID_STATION_TYPE] and SchneidMeta.NAME_ONLY_MBUS in self.csvs.files_infos[csv][SchneidMeta.DICTNAME_SCHNEID_TB_ID] and self.only_mbus_wz_as_hast:
                self.struct.putWzMbusHast(self.workspace_name,csv)
            else:
                self.struct.putOther(self.workspace_name,csv)

    def checkStationDatapoints(self):
        self._ceckWzMbusHast()
        self._ceckUg06()
        self._ceckUg08()
        self._ceckUg10()
        self._ceckUg12()

    def _ceckWzMbusHast(self):
        self._ceckTable(self.struct.wzmbushast, self.driver_db.ceckDriverCsvOnlyWzAsHast, SchneidMeta.NAME_ASSET_ONLY_MBUS_WMZ, True)

    def _ceckUg06(self):
        self._ceckTable(self.struct.ug06, self.driver_db.ceckDriverCsvUG06, SchneidMeta.NAME_ASSET_UG06, True)

    def _ceckUg08(self):
        self._ceckTable(self.struct.ug08, self.driver_db.ceckDriverCsvUG08, SchneidMeta.NAME_ASSET_UG08, True)

    def _ceckUg10(self):
        self._ceckTable(self.struct.ug10, self.driver_db.ceckDriverCsvUG10, SchneidMeta.NAME_ASSET_UG10, True)

    def _ceckUg12(self):
        self._ceckTable(self.struct.ug12, self.driver_db.ceckDriverCsvUG12, SchneidMeta.NAME_ASSET_UG12, True)

    def _ceckTable(self, table_struct, ceckDriver, asset_name, create_instance=True, instance_name_addive=""):
        asset_id = self.labled_nao.ceckAsset(asset_name=asset_name)
        if not asset_id:return(-1)
        for database in table_struct:
            workspace_id = None
            for table in table_struct[database]:
                instance_id = self.labled_nao.ceckInstance(instance_name_addive+table.split(".")[0],database=database,asset_id=asset_id)
                if not instance_id and not create_instance: continue
                self.check_point_buffer = self.csvs.readCsvDataReverseAsDataFrame(table,lines=3000).reset_index(drop=True)
                data_points = self.csvs.files_columns[table]
                for point in data_points:
                    # -------------------------------------------data point in driver ?---------------------------------------------------------
                    driver_meta = ceckDriver(data_points[point][SchneidMeta.DICTNAME_DATAPOINT_NAME],data_points[point][SchneidMeta.DICTNAME_POSITION])
                    if len(driver_meta)==0:continue 
                    # -------------------------------------------data point already in use ?---------------------------------------------------------
                    if not self.labled_points.ceckPoints(database, table, data_points[point][SchneidMeta.DICTNAME_POSITION]):continue
                    # -------------------------------------------data in aqotec database for this point ?---------------------------------------------------------
                    ifdata = self._ceckDataInPoint(
                        dp=data_points[point][SchneidMeta.DICTNAME_POSITION],
                        lt=driver_meta[SchneidMeta.LT],
                        gt=driver_meta[SchneidMeta.GT],
                        b1=driver_meta[SchneidMeta.B1],
                        b2=driver_meta[SchneidMeta.B2]
                    )
                    if not ifdata: continue
                    # -------------------------------------------creat workspace if not been created---------------------------------------------------------
                    if not workspace_id:
                        workspace_id = self.labled_nao.ceckWorkspace(self.workspace_name)
                        if not workspace_id:
                            ret=self.nao.createWorkspace(self.workspace_name)
                            workspace_id=ret[SchneidMeta.NAME__ID]
                            self.labled_nao.putWorkspace(ret)
                    # -------------------------------------------creat instance if not been created---------------------------------------------------------
                    if not instance_id:
                        instance_name = instance_name_addive+table.split(".")[0]
                        ret=self.nao.createInstance(
                            name=instance_name+self.seperator_name_work_instance+self.work_defalut_name_instance,
                            asset_id=asset_id,
                            description="Schneid_"+asset_name,
                            workspace_id=workspace_id
                        )
                        instance_id=ret[SchneidMeta.NAME__ID]
                        ret[SchneidMeta.NAME_NAME] = instance_name
                        self.labled_nao.putInstance(ret,database)
                        # if len(ret[SchneidMeta.NAME_META_VALUES])>0:
                        #     self._saveInitialMetaData(ret[SchneidMeta.NAME_META_VALUES],ret[SchneidMeta.NAME__ID],"?")
                        print(instance_name)
                    # --------------------------------------ceck if allready activatet with other station       ----------------------------
                    act_point = self.labled_points.getPointByInstanceSensorInstance(database,driver_meta[SchneidMeta.NAME_ID],instance_id)
                    # -------------------------------------------activate datapoint---------------------------------------------------------
                    if len(act_point)==0:
                        sleep(0.05)
                        ret=self.nao.activateDatapoint(
                            type_sensor=driver_meta[SchneidMeta.NAME_TYPE],
                            sensor_id=driver_meta[SchneidMeta.NAME_ID],
                            instance_id=instance_id,
                            config={
                                SchneidMeta.NAME_DATABASE:database,
                                SchneidMeta.NAME_TABLE:table,
                                SchneidMeta.NAME_DP:driver_meta[SchneidMeta.NAME_DP],
                                SchneidMeta.NAME_DP_NAME:driver_meta[SchneidMeta.NAME_DP_NAME]
                            }
                        )
                    # -----------------------------------save activated datapoint local-----------------------------------------------------
                    self.labled_points.savePoint(
                        database=database,
                        table_name=table,
                        name_dp=driver_meta[SchneidMeta.NAME_DP_NAME],
                        dp=driver_meta[SchneidMeta.NAME_DP],
                        instance_id=instance_id,
                        sensor_id=driver_meta[SchneidMeta.NAME_ID],
                        asset_id=asset_id
                    )
                    print("activate point")

    def _ceckDataInPoint(self,dp,lt,gt,b1,b2,returns=False):
        series:pd.Series = self.check_point_buffer[dp]
        series = series.mask(series==series.shift(1)).dropna()
        data_bool=Series([True]*len(series))
        if lt: data_bool=series.lt(lt)&data_bool
        if gt: data_bool=series.gt(gt)&data_bool
        if b1: data_bool=(series.lt(b1)|series.gt(b2))&data_bool
        if returns:
            if data_bool.any():
                return(series)
            else:
                return([])
        return(data_bool.any())
    
    def patchStationMeta(self):
        instances = self.labled_nao.getInstances()
        # -------------- Kundendaten --------------
        asset_meta = self._getAssetMetaQuery()
        if len(asset_meta) > 0:
            meta_postgres = self.postgres.getMetaDataGroupedById()
            for instance in instances:
                instance_anid = instance[SchneidMeta.NAME_NAME].split("_")
                if len(instance_anid)==2: instance_anid=instance_anid[0]
                elif len(instance_anid==3): instance_anid=instance_anid[1]
                else: continue
                try: data = meta_postgres[int(instance_anid)]
                except: continue
                if not asset_meta.get(instance[SchneidMeta.NAME_ASSET_ID]): continue
                if len(data)>0:self._patchStationMeta(data,instance,asset_meta[instance[SchneidMeta.NAME_ASSET_ID]], number=1)

    def patchLastDataPointMeta(self):
        asset_meta = self.driver_db.getAssetLastValueMeta()
        instances = self.labled_nao.getInstances()
        meta_all = self.labled_nao.getInstanceMetaAll()
        meta_instances = {}
        for met in meta_all: 
            if met[SchneidMeta.NAME_DP]==SchneidMeta.DICTNAME_SCHNEID_ANID:
                meta_instances[met[SchneidMeta.NAME_DB_INSTANCE_ID]] = met[SchneidMeta.NAME_VALUE]
        for instance in instances:
            if instance[SchneidMeta.NAME__ID] not in meta_instances: continue
            table_name = str(meta_instances[instance[SchneidMeta.NAME__ID]])+SchneidMeta.HAST_FILE_FORMAT
            if table_name not in self.csvs.files: 
                if not self.only_mbus_wz_as_hast: continue
                table_name = str(meta_instances[instance[SchneidMeta.NAME__ID]])+SchneidMeta.WZ_FILE_FORMAT
                if table_name not in self.csvs.files: continue
            table_columns = self.csvs.files_columns[table_name]
            for asset_values_drive in asset_meta:
                if asset_values_drive[SchneidMeta.NAME_DP_NAME] not in table_columns: continue
                if asset_values_drive[SchneidMeta.NAME_DP] != table_columns[asset_values_drive[SchneidMeta.NAME_DP_NAME]][SchneidMeta.DICTNAME_POSITION]: continue
                # if table for meta in database ?
                try:
                    self.check_point_buffer = self.csvs.readCsvDataReverseAsDataFrame(table_name,lines=3000).reset_index(drop=True)
                    # if a valid value in datatable ?
                    ifdata = self._ceckDataInPoint(
                        dp=asset_values_drive[SchneidMeta.NAME_DP],
                        lt=asset_values_drive[SchneidMeta.LT],
                        gt=asset_values_drive[SchneidMeta.GT],
                        b1=asset_values_drive[SchneidMeta.B1],
                        b2=asset_values_drive[SchneidMeta.B2],
                        returns=True
                    )
                    if len(ifdata) == 0: continue
                except:
                    continue
                self._patchStationMetaFromValue(ifdata.iloc[0],instance[SchneidMeta.NAME__ID],asset_values_drive)

    def _patchStationMetaFromValue(self, value, instance_id, driver_infos):
        # check if data valid
        if value=="" or value==None: return(-1)
        # check if meta labled before
        meta = self.labled_nao.getInstanceMetaByAttributeInstance(instance_id,driver_infos[SchneidMeta.NAME_META_ID_DB])
        if meta==[]:
            # get data from nao if not labled before
            instance_infos = self.nao.getInstanceInfos(instance_id)
            id_att = ""
            for info in instance_infos[SchneidMeta.NAME_META_VALUES]:
                if info[SchneidMeta.NAME_META_ID] == driver_infos[SchneidMeta.NAME_META_ID_DB]:
                    id_att = info[SchneidMeta.NAME__ID]
            if id_att=="":return(-1)
            # put initial meta data to local labling db
            self.labled_nao.putMetaInstance(
                value=None,
                meta_id=driver_infos[SchneidMeta.NAME_META_ID_DB],
                dp=driver_infos[SchneidMeta.NAME_DP],
                id=id_att,
                asset_id=driver_infos[SchneidMeta.NAME_DB_ASSET_ID],
                type=driver_infos[SchneidMeta.NAME_TYPE],
                dp_pos=None,
                instance_id=instance_id
            )
            meta = self.labled_nao.getInstanceMetaByAttributeInstance(instance_id,driver_infos[SchneidMeta.NAME_META_ID_DB])
        # ceck if meta has chanced
        if meta[0][SchneidMeta.NAME_TYPE] == SchneidMeta.NAME_NUMBER:
            try:
                if isinstance(value, (float,int)):
                    dat = float(dat)
                else:
                    dat = float(value.replace(",","."))
            except:
                dat = None
        elif meta[0][SchneidMeta.NAME_TYPE] == SchneidMeta.NAME_INTEGER: 
            try:
                if isinstance(value, (float,int)):
                    dat = int(value)
                else:
                    dat = int(value.replace(",", "."))
            except:
                dat = None
        else: dat = str(value)
        if meta[0][SchneidMeta.NAME_VALUE]!=dat:
            # patch meta data
            self.nao.patchInstanceMeta(meta[0][SchneidMeta.NAME_DB_INSTANCE_ID],meta[0][SchneidMeta.NAME_ID],dat)
            self.labled_nao.patchInstanceMetaValueByAttributeInstance(instance_id,driver_infos[SchneidMeta.NAME_META_ID_DB], dat)
            print("patch meta")

    def _getAssetMetaQuery(self, number=1):
        if number==1:
            asset_meta = self.driver_db.getAssetMeta()
        else:
            asset_meta = self.driver_db.getAssetMeta2()
        ret = {}
        for set in asset_meta:
            if set[SchneidMeta.NAME_DB_ASSET_ID] not in ret: ret[set[SchneidMeta.NAME_DB_ASSET_ID]]={}
            ret[set[SchneidMeta.NAME_DB_ASSET_ID]][set[SchneidMeta.NAME_DP]] = set[SchneidMeta.NAME_DP_POS]
        return(ret)

    def _saveInitialMetaData(self, attributevalues, instance_id, dp, number=1):
        for value in attributevalues:
            if number == 1:
                meta_driver = self.driver_db.getAssetMetaFromId(value[SchneidMeta.NAME_META_ID])
            else:
                meta_driver = self.driver_db.getAssetMetaFromId2(value[SchneidMeta.NAME_META_ID])
            if len(meta_driver)==0:
                continue
            meta_driver = meta_driver[0]
            old = self.labled_nao.getInstanceMetaByPosInstance(instance_id,meta_driver[SchneidMeta.NAME_DP_POS], dp)
            if len(old)!=0:continue
            self.labled_nao.putMetaInstance(
                value=None,
                meta_id=value[SchneidMeta.NAME_META_ID],
                dp=meta_driver[SchneidMeta.NAME_DP],
                id=value[SchneidMeta.NAME__ID],
                asset_id=meta_driver[SchneidMeta.NAME_DB_ASSET_ID],
                type=meta_driver[SchneidMeta.NAME_TYPE],
                dp_pos=meta_driver[SchneidMeta.NAME_DP_POS],
                instance_id=instance_id
            )

    def _patchStationMeta(self, data,instance,name_dp,number):
        for idx in data:
            sleep(0.05)
            if idx not in name_dp: continue
            if data[idx]=="" or data[idx]==None: continue
            meta = self.labled_nao.getInstanceMetaByPosInstance(instance[SchneidMeta.NAME__ID],name_dp[idx],idx)
            if meta==[]: 
                instance_infos = self.nao.getInstanceInfos(instance[SchneidMeta.NAME__ID])
                self._saveInitialMetaData(instance_infos[SchneidMeta.NAME_META_VALUES], instance[SchneidMeta.NAME__ID],name_dp[idx],number)
                meta = self.labled_nao.getInstanceMetaByPosInstance(instance[SchneidMeta.NAME__ID],name_dp[idx],idx)
            if meta[0][SchneidMeta.NAME_TYPE] == SchneidMeta.NAME_NUMBER:
                try:
                    dat = float(data[idx])
                except:
                    dat = None
            elif meta[0][SchneidMeta.NAME_TYPE] == SchneidMeta.NAME_INTEGER: 
                try:
                    dat = int(data[idx])
                except:
                    dat = None
            else: dat = str(data[idx])
            if meta[0][SchneidMeta.NAME_VALUE]!=dat:
                self.nao.patchInstanceMeta(meta[0][SchneidMeta.NAME_DB_INSTANCE_ID],meta[0][SchneidMeta.NAME_ID],dat)
                self.labled_nao.patchInstanceMetaValueByPosInstance(instance[SchneidMeta.NAME__ID],name_dp[idx],idx, dat)
                print("patch meta")

    def patchSyncStatus(self):
        data = self.labled_points.getNoSincPoints()
        for database in data:
            for dat in data[database]:
                self.sync_status.postUnsyncroniziedValue(
                    database=database, 
                    table_db=dat[SchneidMeta.NAME_TABLE], 
                    value=[{dat[SchneidMeta.NAME_DP]:dat[SchneidMeta.NAME_SENSOR_ID]}], 
                    asset_id=dat[SchneidMeta.NAME_DB_ASSET_ID], 
                    instance_id=dat[SchneidMeta.NAME_DB_INSTANCE_ID])
                self.labled_points.patchPointToSinc(dat[SchneidMeta.NAME_TABLE], dat[SchneidMeta.NAME_DP])
        #self.labled_points.patchAllPointsToSync()
            
    def startCheckAllMetaData(self):
        self.checkStationDatapoints()
        self.patchStationMeta()
        self.patchLastDataPointMeta()
        self.patchSyncStatus()





# '''
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
#                                          Data Transfer Class for CSV
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# '''

class SchneidTransferCsv(SchneidParamWinmiocs70):

    def __init__(self,interval:timedelta,SyncStatus:SyncronizationStatus,NaoApp:NaoApp,SchneidCSV:SchneidCsvWinmiocs70,
                 wrong_units:dict={}) -> None:
        '''
        wrong_units is a fix for wrong units, {<instance_id>:{<sensor_id>:<float(factor)>}}
        '''
        self.sync_status = SyncStatus
        self.interval = interval
        self.csvs = SchneidCSV
        self.status = self.getSyncStatus()
        self.nao = NaoApp
        self.new_status = {}
        self.status_count = {}
        self.wrong_units = wrong_units

    def startSyncronization(self, logfile=None, sleep_data_len=1, archiv_sync=False, transfer_sleeper_sec:int=None):
        if not transfer_sleeper_sec: transfer_sleeper_sec = SchneidTransferCsv.DEFAULT_TRASFER_SLEEPER_SECOND
        count = 0
        sync_timer = time()
        while 1==1:
            try: 
                if datetime.now().hour >= 23 and not archiv_sync:
                    self.setSyncStatus()
                    self.status=self.getSyncStatus()
                    break
                start_time = time()
                data_telegraf, sync_reset = self.getTelegrafData()
                if len(data_telegraf)>0:ret=self.nao.sendTelegrafData(data_telegraf)
                else:ret=SchneidTransferCsv.STATUS_CODE_GOOD    
                if ret==SchneidTransferCsv.STATUS_CODE_GOOD:
                    print(len(data_telegraf), " data posted; sec:",time()-start_time, datetime.now())
                    start_time = time()
                    count+=len(data_telegraf)
                    if sync_reset or time()-sync_timer:
                        sync_timer = time()
                        self.setSyncStatus()
                        self.status=self.getSyncStatus()
                    if archiv_sync and len(data_telegraf)==0:
                        break
                    elif len(data_telegraf)<sleep_data_len and not archiv_sync:                    
                        self.setSyncStatus()
                        self.status=self.getSyncStatus()  
                        sleep(transfer_sleeper_sec)
                        self.csvs.restetFiles()   
                else:
                    sleep(SchneidTransferCsv.DEFAULT_ERROR_SLEEP_SECOND)
            except:
                if logfile: logfile(str(sys.exc_info()))
                break
        if logfile:logfile(str(count)+" data sended")

    def setSyncStatus(self):
        for database in self.new_status:
            if len(self.new_status[database])<100:
                for table_db in self.new_status[database]:
                    if self.new_status[database][table_db].get(SchneidTransferCsv.NAME_TIME_UNSYCRONICIZIED):
                        self.sync_status.patchSincStatus(database,table_db,self.new_status[database][table_db][SchneidTransferCsv.NAME_TIME_UNSYCRONICIZIED],True)
                    if self.new_status[database][table_db].get(SchneidTransferCsv.NAME_TIME_SYNCRONICZIED):
                        self.sync_status.patchSincStatus(database,table_db,self.new_status[database][table_db][SchneidTransferCsv.NAME_TIME_SYNCRONICZIED],False)
            else:
                syncron_time = {}
                for table_db in self.new_status[database]:
                    if self.new_status[database][table_db].get(SchneidTransferCsv.NAME_TIME_UNSYCRONICIZIED):
                        self.sync_status.patchSincStatus(database,table_db,self.new_status[database][table_db][SchneidTransferCsv.NAME_TIME_UNSYCRONICIZIED],True)
                    if self.new_status[database][table_db].get(SchneidTransferCsv.NAME_TIME_SYNCRONICZIED):
                        syncron_time[table_db] = self.new_status[database][table_db][SchneidTransferCsv.NAME_TIME_SYNCRONICZIED]
                self.sync_status.patchSincStatusManyMany(database=database,data=syncron_time,isunsinc=False)
        self.new_status = {}

    def getSyncStatus(self):
        status = self.sync_status.getSyncStatusAll()
        reset = False
        for database in status:
            for table_dic in status[database]:
                if table_dic[SchneidTransferCsv.NAME_SYNCRONICZIED]!=[] and table_dic[SchneidTransferCsv.NAME_UNSYCRONICIZIED]!=[]:
                    if table_dic[SchneidTransferCsv.NAME_TIME_SYNCRONICZIED]==None and table_dic[SchneidTransferCsv.NAME_TIME_UNSYCRONICIZIED]==None:
                        reset=True
                        self.sync_status.postSyncroniziedValue(
                            database=database,
                            table_db=table_dic[SchneidTransferCsv.NAME_TABLE],
                            value=table_dic[SchneidTransferCsv.NAME_UNSYCRONICIZIED],
                            timestamp=table_dic[SchneidTransferCsv.NAME_TIME_UNSYCRONICIZIED]
                        )
                        self.sync_status.dropUnSincDps(database=database,table_dp=table_dic[SchneidTransferCsv.NAME_TABLE])
                if table_dic[SchneidTransferCsv.NAME_SYNCRONICZIED]==[] and table_dic[SchneidTransferCsv.NAME_UNSYCRONICIZIED]!=[]:
                    reset=True
                    self.sync_status.postSyncroniziedValue(
                        database=database,
                         table_db=table_dic[SchneidTransferCsv.NAME_TABLE],
                        value=table_dic[SchneidTransferCsv.NAME_UNSYCRONICIZIED],
                        timestamp=table_dic[SchneidTransferCsv.NAME_TIME_UNSYCRONICIZIED]
                    )
                    self.sync_status.dropUnSincDps(database=database,table_dp=table_dic[SchneidTransferCsv.NAME_TABLE])
                elif table_dic.get(SchneidTransferCsv.NAME_TIME_UNSYCRONICIZIED):
                     if datetime.fromisoformat(table_dic[SchneidTransferCsv.NAME_TIME_SYNCRONICZIED])<=datetime.fromisoformat(table_dic[SchneidTransferCsv.NAME_TIME_UNSYCRONICIZIED]):
                        self.sync_status.postSyncroniziedValue(
                            database=database,
                            table_db=table_dic[SchneidTransferCsv.NAME_TABLE],
                            value=table_dic[SchneidTransferCsv.NAME_UNSYCRONICIZIED],
                            timestamp=table_dic[SchneidTransferCsv.NAME_TIME_SYNCRONICZIED]
                        )
                        self.sync_status.dropUnSincDps(database=database,table_dp=table_dic[SchneidTransferCsv.NAME_TABLE])
                        reset=True
                if table_dic[SchneidTransferCsv.NAME_UNSYCRONICIZIED]==[] and table_dic[SchneidTransferCsv.NAME_TIME_UNSYCRONICIZIED] != None:
                    self.sync_status.dropUnSincDps(database=database,table_dp=table_dic[SchneidTransferCsv.NAME_TABLE])
                    reset=True              
        if reset: status=self.sync_status.getSyncStatusAll()
        return(status)

    def getTelegrafData(self):
        breaker = False
        all_data = False
        telegraf = []
        ext_telegraf = telegraf.extend
        for database in self.status:
            if database not in self.status_count:
                self.status_count[database] = 0
            if self.status_count[database] >= len(self.status[database]):
                all_data = True
                self.status_count[database] = 0
            count_add = 0
            for idc in range(len(self.status[database])-self.status_count[database]):
                status_instance = self.status[database][idc+self.status_count[database]]
                count_add += 1
                if status_instance[SchneidTransferCsv.NAME_TIME_SYNCRONICZIED]!=None:
                    if "WMZ" in status_instance[SchneidTransferCsv.NAME_TABLE]:
                        if  pytz.timezone(SchneidTransferCsv.DEFAULT_SCHNEID_TIMEZONE).localize(datetime.fromisoformat(status_instance[SchneidTransferCsv.NAME_TIME_SYNCRONICZIED])).astimezone(pytz.utc).replace(tzinfo=None) > datetime.utcnow()-timedelta(hours=26):
                            continue
                ext_telegraf(self._getTelegrafDataInstance(status_instance,database))
                if len(telegraf)>=SchneidTransferCsv.DEFAULT_BREAK_TELEGRAF_LEN:
                    breaker=True
                    break
            self.status_count[database] += count_add
            if breaker:
                break
        return(telegraf, all_data)
    
    def _getTelegrafDataInstance(self,status_instance:dict,database:str):
        if len(status_instance[SchneidTransferCsv.NAME_UNSYCRONICIZIED])!=0:
            return(self._getTelegrafDataInstance2(status_instance,database,SchneidTransferCsv.NAME_TIME_UNSYCRONICIZIED,SchneidTransferCsv.NAME_UNSYCRONICIZIED))
        else:
            return(self._getTelegrafDataInstance2(status_instance,database,SchneidTransferCsv.NAME_TIME_SYNCRONICZIED,SchneidTransferCsv.NAME_SYNCRONICZIED))
        
    def _getTelegrafDataInstance2(self,status_instance:dict, database:str, name_time:str, name_sync:str):
        if status_instance[SchneidTransferCsv.NAME_TABLE] not in self.csvs.files_infos: return([])
        if status_instance[name_time]: start_time=datetime.fromisoformat(status_instance[name_time])
        else: start_time = SchneidTransferCsv.DEFAULT_FIRST_TIME_SCHNEID
        if name_time == SchneidTransferCsv.NAME_TIME_UNSYCRONICIZIED:
            if status_instance[SchneidTransferCsv.NAME_TIME_SYNCRONICZIED]: stop_time = datetime.fromisoformat(status_instance[SchneidTransferCsv.NAME_TIME_SYNCRONICZIED])
            else: stop_time = self.csvs.files_infos[status_instance[SchneidTransferCsv.NAME_TABLE]][SchneidTransferCsv.DICTNAME_LAST_WRITE_TIME]
        else: stop_time = self.csvs.files_infos[status_instance[SchneidTransferCsv.NAME_TABLE]][SchneidTransferCsv.DICTNAME_LAST_WRITE_TIME]
        if (stop_time-start_time) < timedelta(seconds=SchneidTransferCsv.DEFAULT_TRANSFER_TIME_SCHNEID):
            return([])
        # delet duplicates of sensores in sync-status if regulator change
        ids_sensors = [value for item in status_instance[name_sync]  for value in item.values()]
        if len(ids_sensors)>len(set(ids_sensors)):
            new_sensors = []
            ids_duplicatet = []
            idx = 0
            for item in reversed(status_instance[name_sync]):
                ids = list(item.values())[0]
                if ids not in ids_duplicatet:
                    ids_duplicatet.append(ids)
                    idx+=1
                    new_sensors.append(item)
            self.sync_status.resetSensorIdsInstance(database=database,table_db=status_instance[SchneidTransferCsv.NAME_TABLE],sensors_list=new_sensors,sic_table=name_sync)
            status_instance[name_sync] = new_sensors
        timeseries=self._getTimeseries(
            database=database,
            table=status_instance[SchneidTransferCsv.NAME_TABLE],
            data_points=[int(dp) for item in status_instance[name_sync] for dp in item.keys()],
            start_time=start_time,
            stop_time=stop_time,
        )
        if len(timeseries)==0:return([])
        if database not in self.new_status:self.new_status[database]={}
        if status_instance[SchneidTransferCsv.NAME_TABLE] not in self.new_status[database]:self.new_status[database][status_instance[SchneidTransferCsv.NAME_TABLE]]={}
        self.new_status[database][status_instance[SchneidTransferCsv.NAME_TABLE]][name_time]=timeseries.index[-1]
        return(self._formatTimeseriesToTelegrafFrame(
            timeseries=timeseries,
            sensor_ids=[id for item in status_instance[name_sync] for id in item.values()],
            instance_id=status_instance[SchneidTransferCsv.NAME_DB_INSTANCE_ID],
            asset_id=status_instance[SchneidTransferCsv.NAME_DB_ASSET_ID]
        ))

    def _getTimeseries(self,database:str,table:str,data_points:list,start_time:datetime,stop_time:datetime):
        try:interval=timedelta(seconds=int(self.csvs.files_infos[table]["interval"]))
        except: interval = self.interval
        lines_to_read = int((stop_time-start_time)/interval)
        if lines_to_read > 500: lines_to_read = "all"
        dataframe = self.csvs.readCsvDataReverseAsDataFrame(file_name=table,lines=lines_to_read)
        columns_act = dataframe.columns
        new_cols = []
        for point in data_points:
            if point not in columns_act:
                new_cols.append(point)
        if len(new_cols)>0:
            dataframe[new_cols] = nan
        dataframe = dataframe[data_points]
        dataframe = dataframe[(dataframe.index>start_time)&(dataframe.index<=stop_time)]
        return(dataframe)
    
    def _formatTimeseriesToTelegrafFrame(self, timeseries:pd.DataFrame, sensor_ids:list, instance_id:str, asset_id:str):
        if len(sensor_ids)!=len(timeseries.columns): raise ValueError("bug in numer of sensors for instance-id:"+instance_id)
        telegraf_list = []
        add_telegraf = telegraf_list.append
        columns = list(timeseries.columns)
        for row in range(len(timeseries)):
            timestamp = str(int(pytz.timezone(SchneidTransferCsv.DEFAULT_SCHNEID_TIMEZONE).localize(timeseries.index[row]).astimezone(pytz.utc).replace(tzinfo=None).timestamp()*1e9))
            for idx in range(len(sensor_ids)):
                if isnan(timeseries[columns[idx]].iloc[row]):continue
                if instance_id in self.wrong_units:
                    if sensor_ids[idx] in self.wrong_units[instance_id]:
                        value = timeseries[columns[idx]].iloc[row]*self.wrong_units[instance_id][sensor_ids[idx]]
                    else:
                        value = timeseries[columns[idx]].iloc[row]
                else:
                     value = timeseries[columns[idx]].iloc[row]
                add_telegraf(f"{asset_id},instance={instance_id} {sensor_ids[idx]}={value} {timestamp}")
        return(telegraf_list)  
        


# '''
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
#                                                    Serial Data Postgres
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------
# '''


class StructSyncPoint():
    '''
    Represents a synchronization point containing information about a controller and its associated data.

    This class is designed to store and serialize data for synchronization points,
    including a controller ID, asset ID, instance ID, series ID, and the last synchronization timestamp.
    It provides a method for JSON serialization.
    '''

    def __init__(self, controller_id:int, asset_id:str, instance_id:str, 
                 series_id:str, last_time:Union[str,datetime]) -> None:
        '''
        Initializes a StructSyncPoint object.

        Args: also Attributes:
            controller_id (int): The unique ID of the controller, always stored as an integer.
            last_time (datetime): The last synchronization timestamp stored as a datetime object.
            asset_id (str): The ID of the associated asset.
            instance_id (str): The ID of the associated instance.
            series_id (str): The series ID for the synchronization.
        '''
        self.controller_id = controller_id if isinstance(controller_id,int) else int(controller_id)
        self.last_time = last_time if isinstance(last_time,datetime) else datetime.fromisoformat(last_time)
        self.asset_id = asset_id
        self.instance_id = instance_id
        self.series_id = series_id


    def toDict(self):
        '''
        Serializes the StructSyncPoint object into a dictionary suitable for JSON serialization.

        Returns:
            dict: A dictionary containing the object's data with `last_time` formatted as an ISO string.
        '''
        return({
            "controller_id": self.controller_id,
            "last_time": self.last_time.isoformat(),
            "asset_id": self.asset_id,
            "instance_id": self.instance_id,
            "series_id": self.series_id
        })
    

class StructSyncMetaPoint():
    '''
    '''

    def __init__(self, value:Union[str,int,float,None], attribute_id:str, asset_id:str, 
                 instance_id:str, self_id:str, controller_id:str) -> None:
        '''
        '''
        self.value = value
        self.attribute_id = attribute_id
        self.asset_id =  asset_id
        self.instance_id = instance_id
        self.self_id = self_id
        self.controller_id = controller_id


    def toDict(self):
        '''
        Serializes the StructSyncPoint object into a dictionary suitable for JSON serialization.

        Returns:
            dict: A dictionary containing the object's data-
        '''
        return({
            "value": self.value,
            "attribute_id": self.attribute_id,
            "asset_id":  self.asset_id,
            "instance_id": self.instance_id,
            "self_id": self.self_id,
            "controller_id": self.controller_id
        })
    

class ControllerIdSyncTime():
    '''
    Manages synchronization data for controllers, including their IDs and last synchronization timestamps.

    This class reads and writes synchronization data from/to a JSON file, 
    manages controller IDs, and ensures new controller IDs are added with a default synchronization timestamp.
    '''


    def __init__(self, file_path:str, default_start_time:datetime=datetime(2010,1,1)) -> None:
        '''
        Initializes the ControllerIdSyncTime object and reads the current synchronization status.

        Args:
            file_path (str): Path to the JSON file storing the synchronization data.
            default_start_time (datetime): The default timestamp for new synchronization entries.

        Attributes:
            file_path (str): The path to the synchronization status file.
            default_start_time (datetime): The default synchronization start time.
            sync_dic (dict): A dictionary mapping controller IDs to `StructSyncPoint` objects.
            meta_dict (dict): A dictionary mapping controller IDs to `StructSyncMetaPoint` objects.
            controller_ids (list): A list of all controller IDs currently managed.
        '''
        self.file_path:str = file_path
        self.default_start_time = default_start_time
        self.sync_dic:Dict[str,StructSyncPoint]={}
        self.meta_dic:Dict[str,StructSyncMetaPoint]={}
        self.controller_ids:list=[]
        self.readSyncStatus()


    def readSyncStatus(self) -> dict:
        '''
        Reads the synchronization status from the file and initializes internal variables.

        This method:
        1. Reads the synchronization file (if it exists).
        2. Parses the synchronization data into two separate dictionaries:
            - `sync_dic`: Maps controller IDs to `StructSyncPoint` objects representing synchronization points.
            - `meta_dic`: Maps controller IDs to `StructSyncPoint` objects containing meta-information.
        3. Updates the `controller_ids` list with all controller IDs from `sync_dic`.

        If the file is missing or empty, default structures are initialized:
        - `sync_dic` and `meta_dic` are empty dictionaries.
        - `controller_ids` is an empty list.
        '''
        try:
            with open(self.file_path, mode="r") as fi:
                result = fi.read()
        except FileNotFoundError:
            result=""

        if result=="":
            result = {"sync_time":[], "meta_data":[]}
        else:
            result = loads(result)

        self.sync_dic = {}
        self.controller_ids = []
        for sync in result["sync_time"]:
            self.sync_dic[sync["controller_id"]] = StructSyncPoint(
                controller_id=sync["controller_id"],
                asset_id=sync["asset_id"],
                instance_id=sync["instance_id"],
                series_id=sync["series_id"],
                last_time=sync["last_time"]
            )
            self.controller_ids.append(sync["controller_id"])
        
        self.meta_dic = {}
        for sync in result["meta_data"]:
            self.meta_dic[sync["controller_id"]] = StructSyncMetaPoint(
                value=sync["value"],
                attribute_id=sync["attribute_id"],
                instance_id=sync["instance_id"],
                asset_id=sync["asset_id"],
                self_id=sync["self_id"],
                controller_id=sync["controller_id"]
            )
        

    def writeSyncStatus(self) -> None:
        '''
        Writes the current synchronization status to the JSON file.

        This method:
        1. Serializes the following dictionaries into JSON format:
            - `sync_dic`: Contains synchronization point data.
            - `meta_dic`: Contains meta-information data.
        2. Saves the serialized data to the file at `self.file_path`.

        The JSON structure written to the file includes:
        - `sync_time`: A list of serialized synchronization points from `sync_dic`.
        - `meta_data`: A list of serialized meta-information points from `meta_dic`.

        Raises:
            Any exceptions during file writing will propagate upwards unless handled externally.
        '''
        sync_time = [ point.toDict() for point in list( self.sync_dic.values() ) ] 
        meta_data = [ point.toDict() for point in list( self.meta_dic.values() ) ] 

        with open(self.file_path, mode="w") as fi:
            fi.write(dumps( { "sync_time": sync_time, "meta_data": meta_data } ))

    
    def checkAndSetNewControllerIdsWithDefaultTime(self, controller_ids:list, asset_ids:list, instance_ids:list,
                                                    serial_id:str) -> None:
        '''
        Adds new controller IDs to the synchronization dictionary with a default timestamp.

        Args:
            controller_ids (list): A list of controller IDs to check and add if missing.
            asset_ids (list): Corresponding asset IDs for the controllers.
            instance_ids (list): Corresponding instance IDs for the controllers.
            serial_id (str): The series ID associated with these controllers.
        '''
        new_controller_ids = list( set( controller_ids ) - set( self.controller_ids ) )
        for idx in range(len(new_controller_ids)):
            self.controller_ids.append( controller_ids[idx] )
            self.sync_dic[controller_ids[idx]] = StructSyncPoint(
                controller_id=controller_ids[idx],
                asset_id=asset_ids[idx],
                instance_id=instance_ids[idx],
                series_id=serial_id,
                last_time=self.default_start_time
            )

        
    def setMetaPoint(self, value:Union[str,int,float,None], attribute_id:str, asset_id:str, 
                 instance_id:str, self_id:str, controller_id:str) -> None:
        '''
        '''
        self.meta_dic[controller_id] = StructSyncMetaPoint(
            value=value,
            attribute_id=attribute_id,
            asset_id=asset_id,
            instance_id=instance_id,
            self_id=self_id,
            controller_id=controller_id
        )
        


class SchneidPostgresHeatMeterSerialSync(SchneidParamWinmiocs70):
    '''
    Synchronizes meter serial numbers and error codes from the Schneid Winmiocs 70 PostgreSQL database 
    with the NAO system.

    **Purpose:**
    The existence of this class is necessitated by a limitation in the Schneid Winmiocs 70 system. In its primary 
    CSV-based database, meter serial numbers are incorrectly stored. Due to formatting issues, the last two 
    digits of large integers are replaced with "00" (e.g., a serial number like 9123456789 is stored as 9.1234567e7, 
    which truncates to 9123456700). As a result, the serial numbers must be retrieved from the PostgreSQL database, 
    which correctly stores them. 

    The PostgreSQL database for Schneid Winmiocs 70, however, only contains time series data for meter serial 
    numbers and error codes. Other time series data are not stored in this database. Consequently, this class 
    is solely responsible for synchronizing meter serial numbers and related error codes with the NAO system.

    **Functionality:**
    - Retrieves and processes time series data (serial numbers and error codes) from the PostgreSQL database.
    - Converts the data into Telegraf frames and sends them to the NAO system.
    - Ensures local synchronization status and metadata are updated to maintain consistency between the databases.
    - Manages metadata for serial numbers, ensuring any changes in the serial numbers are patched both locally and 
      in the NAO system.
    '''
    def __init__(self, path_and_file_sync_status:"str",NaoApp:NaoApp,SchneidPostgres:ScheindPostgresWinmiocs70,
                 LablingNaoInstance:LablingNao, serial_id:str, error_id:Union[str,None] = None, 
                 attribute_id:Union[str,None] = None) -> None:
        '''
        Initializes the synchronization manager with necessary configurations.

        Args:
            path_and_file_sync_status (str): Path to the synchronization status file.
            NaoApp (NaoApp): Instance of the NAO application for data transmission.
            SchneidPostgres (ScheindPostgresWinmiocs70): Instance of the Schneid PostgreSQL database handler.
            LablingNaoInstance (LablingNao): Instance of the NAO labeling manager.
            serial_id (str): ID of the series to be synchronized.
            error_id (Union[str, None], optional): ID of the error series, if applicable.
            attribute_id (Union[str, None], optional): ID of the attribute for metadata synchronization.
        '''
        self.sync_file = path_and_file_sync_status
        self.serial_id = serial_id
        self.error_id = error_id
        self.attribute_id = attribute_id
        self.naoapp = NaoApp
        self.postgres = SchneidPostgres
        self.naolabiling = LablingNaoInstance
        self.sync_status = ControllerIdSyncTime(self.sync_file)
        self.checkLabledInstances()

    
    def checkLabledInstances(self) -> None:
        '''
        Verifies and registers new NAO instances for assets.

        This method:
        1. Retrieves all labeled instances from NAO.
        2. Checks for instances that are not yet registered locally.
        3. Adds missing instances with default synchronization timestamps to the local synchronization dictionary.
        '''
        controller_ids = []
        asset_ids = []
        instance_ids = []
        instances = self.naolabiling.getInstances()
        for instance in instances:
            asset_ids.append(instance["_asset"])
            instance_ids.append(instance["_id"])
            controller_ids.append(int(instance["name"].split("_prot")[0]))

        self.sync_status.checkAndSetNewControllerIdsWithDefaultTime(
            controller_ids=controller_ids, 
            asset_ids=asset_ids, 
            instance_ids=instance_ids, 
            serial_id=self.serial_id
        )


    def getSelfSeriealIdFromNao(self, instance_id) -> str:
        '''
        Retrieves the metadata ID for the meter's serial number for a specific NAO instance.

        Args:
            instance_id (str): The ID of the NAO instance.

        Returns:
            str: The metadata ID for the serial number, or an empty string if not found.
        '''
        instance_infos = self.naoapp.getInstanceInfos(instance_id)

        id_att = ""
        for info in instance_infos[SchneidMeta.NAME_META_VALUES]:
            if info[SchneidMeta.NAME_META_ID] == self.attribute_id:
                id_att = info[SchneidMeta.NAME__ID]

        return( id_att )

    
    def syncTimeseries(self) -> None:
        '''
        Synchronizes time series data for each controller ID.

        This method:
        1. Fetches new data from the Schneid PostgreSQL database starting from the last synchronization time.
        2. Processes the data into Telegraf frames for serial numbers and errors.
        3. Sends the Telegraf frames to NAO.
        4. Updates the local synchronization status upon successful data transmission.
        5. Manages metadata for serial numbers, ensuring synchronization with NAO.

        Detailed Steps:
        - Retrieve new data from the database using the last synchronization timestamp.
        - Convert the data to Telegraf frames and send them to NAO.
        - Update the local synchronization status and save it to the synchronization status file.
        - If a new serial number is detected for a controller:
            - Fetch the corresponding metadata ID from NAO.
            - Update the serial number metadata on the NAO server and locally.
        '''
        for controller_id in self.sync_status.sync_dic:
            sync_data = self.sync_status.sync_dic[controller_id]

            dataframe = self.postgres.getSerialSeriesByControllerId(
                controller_id=controller_id,
                start_time=sync_data.last_time
            )
            dataframe["time2"] = dataframe["time"].astype(int)
            
            if len(dataframe) == 0: 
                continue

            telegraf_frame = dataframe[["time2", "serial"]].dropna().apply(
                lambda row: f'{sync_data.asset_id},instance={sync_data.instance_id} {sync_data.series_id}={repr(int(row["serial"]))} {int(row["time2"])}',
                axis=1
            ).to_list()

            if self.error_id!=None:
                telegraf_frame.extend(dataframe[["time2", "error"]].dropna().apply(
                    lambda row: f'{sync_data.asset_id},instance={sync_data.instance_id} {sync_data.series_id}={repr(row["error"])} {int(row["time2"])}',
                    axis=1
                ).to_list())
            
            status = self.naoapp.sendTelegrafData(
                payload=telegraf_frame
            )

            if status == 204:
                sync_data.last_time = dataframe["time"].iloc[-1]
            
            last_serial = dataframe["serial"].iloc[-1]
            if last_serial==None:
                continue
            else:
                last_serial = int(last_serial)

            if controller_id not in self.sync_status.meta_dic:
                meta_id = self.getSelfSeriealIdFromNao(instance_id=sync_data.instance_id)
                
                if meta_id == "":
                    continue

                self.sync_status.setMetaPoint(
                    value=None,
                    attribute_id=self.attribute_id,
                    instance_id=sync_data.instance_id,
                    asset_id=sync_data.asset_id,
                    controller_id=controller_id,
                    self_id=meta_id
                )

            meta_data = self.sync_status.meta_dic[controller_id]
            if meta_data.value != last_serial:
                res = self.naoapp.patchInstanceMeta(
                    instance_id = meta_data.instance_id,
                    meta_id = meta_data.self_id,
                    value = last_serial
                )

                if isinstance(res,dict):
                    if "_id" in res:
                        meta_data.value = last_serial
    
        self.sync_status.writeSyncStatus()


            




            

