from os import path
from datetime import datetime, timedelta

from naoconnect.local_db import Driver, StationDatapoints, LablingNao, SyncronizationStatus
from naoconnect.fromdb.SchneidWinmiocs70 import SchneidMeta, ScheindPostgresWinmiocs70, SchneidCsvWinmiocs70, SchneidTransferCsv, SchneidPostgresHeatMeterSerialSync
from naoconnect.naoappV2 import NaoApp, NaoLoggerMessage


'''
Instance of `NaoLoggerMessage` for developer communication.

This object is used to send status updates about the operation of data loggers, specifically 
whether they have been restarted successfully. Unlike other NAO instances in the system, this 
instance is dedicated to ensuring that the data acquisition processes are continuously monitored 
and reported.

Args:
    host (str): The NAO server host for communication.
    email (str): The email used for authentication with the NAO server.
    password (str): The password for authentication with the NAO server.
    asset (str): The ID of the asset used for logging these updates.
    instance (str): The ID of the instance linked to this asset.
    count_series (str): The ID of the series tracking the number of events.
    restart_series (str): The ID of the series indicating restart events.
    local (bool): Indicates whether this is a local operation. Defaults to `False`.

Methods:
    sendRestart(): Sends a message indicating a logger restart event to the NAO server.
'''
NaoMessager =  NaoLoggerMessage(
    host="developer_nao_server_instance",
    email="developer_logger_email",
    password="developer_logger_password",
    asset="developer_logger_asset_id",
    instance="developer_logger_instance_id",
    count_series="developer_logger_series_count_id",
    restart_series="developer_logger_series_restart_id",
    local=False
)
NaoMessager.sendRestart()


'''
Writes messages to a log file with a timestamp.

This utility function appends error or status messages to a log file. Each entry is prefixed with 
the current timestamp to provide a clear record of events.

Args:
    error (str): The error or status message to log.
    path_file (str, optional): The path to the log file. Defaults to a file named `logfile.txt` in the 
                               current script's directory.
'''
def LogfileWrite(error:str, path_file=path.dirname(path.abspath(__file__))+"/logfile.txt"):
    with open(path_file, "a") as fi: 
        fi.write(str(datetime.now())+" -- "+error + "\n") 


'''
Initialization of local cache files required for synchronization and automatic creation of new assets in NAO.

These components store intermediate data necessary for managing synchronization statuses, 
driver configurations, station datapoints, and asset labeling in the NAO system:
- SchneidDriver: Handles driver configurations for Schneid systems.
- SchneidStationPoints: Manages station-specific datapoints.
- SchneidLablingNAo: Tracks labeling information for assets in the NAO system.
- SchneidSyncStatus: Maintains synchronization status for all managed controllers.
'''
SchneidDriver = Driver(path.dirname(path.abspath(__file__))+"/driver_schneid.json")
SchneidStationPoints = StationDatapoints(path.dirname(path.abspath(__file__))+"/station_points.json")
SchneidLablingNAo = LablingNao(path.dirname(path.abspath(__file__)) + "/labling_nao.json")
SchneidSyncStatus = SyncronizationStatus(path.dirname(path.abspath(__file__))+"/syncronizied_status.json")


'''
Initialization of the NaoApp class for communication with the NAO server.

This class manages all interactions with the NAO system, including data transfer, metadata updates, 
and synchronization tasks. It acts as the central interface for sending and receiving information 
between the local system and the NAO server.

Args:
    host (str): The NAO server host for communication.
    email (str): The email used for authentication with the NAO server.
    password (str): The password for authentication with the NAO server.
    local (bool): Indicates whether this is a local operation. Defaults to `False`.
    Messager (NaoMessager): send total sended data count for developer communication.
'''
Nao = NaoApp(
    Messager=NaoMessager,
    host="my_nao_sever_instance",
    password="my_nao_user_password",
    email="my_nao_user_email",
    local=False
)


'''
This block of code marks the start of the synchronization procedure by:
1. Writing a log entry indicating that the synchronization process has started using the `LogfileWrite` function.
2. Setting the path to the order file, which contains necessary data for synchronization, and specifying the order file's name (`PROTCSV`).
'''
LogfileWrite("START SYNC ALL")
order_file_path:str = "C:\Winmiocs70"
order = "PROTCSV"


'''
Initialization of all classes required for communication with the Schneid Winmiocs 70 system.

This includes setting up instances for interacting with both the CSV-based and PostgreSQL-based data management systems, 
metadata handling, and synchronization processes. The classes work together to ensure accurate data transfer and synchronization 
between the Schneid Winmiocs 70 system and the NAO system.
'''
SchneidCsv = SchneidCsvWinmiocs70(file_path=order_file_path+'/'+order)
SchneidPostgres = ScheindPostgresWinmiocs70()
SchneidNewMeta = SchneidMeta(
    SchneidPostgres=SchneidPostgres,
    SchneidCSV=SchneidCsv,
    NewDriver=SchneidDriver,
    LablingPoints=SchneidStationPoints,
    LablingNao=SchneidLablingNAo,
    SyncStatus=SchneidSyncStatus,
    NaoApp=Nao,
    workspace_name="my_nao_workspace_name",
    work_defalut_name_instance="my_optional_auto_stations_naming_start"
)


'''
Synchronization of meter serial numbers and optional error codes from the Schneid Winmiocs 70 PostgreSQL database.

This script addresses the issue of incorrectly stored serial numbers in the CSV-based database (e.g., truncation of the last two digits).
By pulling serial numbers and error codes from PostgreSQL, it ensures data accuracy. The retrieved data is sent to NAO as Telegraf frames,
and metadata synchronization is performed if necessary.
'''
try:
    LogfileWrite("sync heat meter serial numbers")
    PostgresSerialSyncer = SchneidPostgresHeatMeterSerialSync(
        path_and_file_sync_status=path.dirname(path.abspath(__file__))+"/postgres_serial_sync.json",
        NaoApp=Nao,
        SchneidPostgres=SchneidPostgres,
        LablingNaoInstance=SchneidLablingNAo,
        serial_id="my_serial_series_id",
        attribute_id="my_serial_meta_id"
    )
    PostgresSerialSyncer.syncTimeseries()
    LogfileWrite("finish serial sync")
except Exception as e:
    LogfileWrite("ERROR in PostgresSerialSyncer: " + str( e ) )


'''
Synchronizes new metadata and station data.

This section of the code checks for new metadata and station points that need to be synchronized. 
The synchronization process is handled by the `SchneidNewMeta` class, which verifies and updates 
metadata in the system.

Process:
    1. The log function `LogfileWrite` is called to indicate the start of the metadata synchronization process.
    2. `SchneidNewMeta.startCheckAllMetaData()` is executed to perform the synchronization of metadata.
    3. Once the process completes, a success message is logged.
    4. If any errors occur, an exception is caught, and the error message is logged.

This code ensures that the system is up-to-date with the latest metadata and station points, 
keeping the NAO communication and data integrity in sync.
'''
try:
    LogfileWrite("check and sync new metadata and stations")
    SchneidNewMeta.startCheckAllMetaData()
    LogfileWrite("finish meta sync")
except Exception as e:
    LogfileWrite("ERROR in SchneidNewMeta: " + str( e ) )


'''
Starts the synchronization of time series data for Schneid system.

This block initializes and runs the synchronization process for the time series data (e.g., from CSV files), with logging to track progress and errors.

Process:
    1. **LogfileWrite("start time series sync")**: Logs the start of the time series synchronization process.
    2. Creates an instance of the `SchneidTransferCsv` class, which handles the synchronization of CSV-based time series data.
        - `interval=timedelta(minutes=5)`: Defines the time interval between synchronization actions (every 5 minutes).
        - `SyncStatus=SchneidSyncStatus`: Passes the synchronization status to track the process.
        - `NaoApp=Nao`: Passes the NAO application instance for communication.
        - `SchneidCSV=SchneidCsv`: Passes the CSV data handler.
    3. Calls the `startSyncronization` method to start the synchronization process:
        - `logfile=LogfileWrite`: Passes the logging function to capture progress and errors during synchronization.
        - `sleep_data_len=10`: Defines a sleep time after processing each data chunk (in seconds).
        - `transfer_sleeper_sec=60*15`: Defines a pause (15 minutes) between data transfers to regulate the sync process.
    4. **LogfileWrite("finish time series sync")**: Logs the completion of the time series synchronization.
    5. If any error occurs during the synchronization process, it is caught in the `except` block and logged.

This code ensures that the synchronization of time series data runs efficiently, with regular logging for progress and error reporting.
'''
try:
    LogfileWrite("start timseries sync")
    SchneidTransfer = SchneidTransferCsv(
        interval=timedelta(minutes=5),
        SyncStatus=SchneidSyncStatus,
        NaoApp=Nao,
        SchneidCSV=SchneidCsv
    )
    SchneidTransfer.startSyncronization(logfile=LogfileWrite,sleep_data_len=10, transfer_sleeper_sec=60*15)
    LogfileWrite("finish timseries sync")
except Exception as e:
    LogfileWrite("ERROR in SchneidTransfer: " + str( e ) )


'''
Logs the completion of the entire synchronization process.
'''
LogfileWrite("FINISH SYNC ALL")
