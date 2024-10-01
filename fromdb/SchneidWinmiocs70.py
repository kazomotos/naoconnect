'Autor: Rupert Wieser -- Naotilus -- 20232209'
import psycopg2
from naoconnect.local_db import Driver, StationDatapoints, LablingNao, SyncronizationStatus
from pandas import Series
import pandas as pd
from datetime import datetime, timedelta
from naoconnect.naoappV2 import NaoApp
from time import sleep, time
import sys
import csv
import re
import pytz
from os import path, listdir
from io import StringIO
from numpy import isnan, nan

''' DOC

'''


'''
--------------------------------------------------------------------------------------------------------------------
                                                        Params
--------------------------------------------------------------------------------------------------------------------
'''

class SchneidParamWinmiocs70():
    STATUS_CODE_GOOD = 204
    HAST_FILE_FORMAT = "_prot.csv"
    DATA_FILE_FORMAT = "prot"
    DATA_FILE_FORMAT_ENDING = ".csv"
    NAME_UG06_CSV = "UG06"
    NAME_UG08_CSV = "UG08"
    NAME_UG10_CSV = "UG10"
    NAME_UG12_CSV = "UG12"
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
    NAME_TIME_SYNCRONICZIED_META = "time_sincronizied_meta"
    NAME_TIME_SYNCRONICZIED = "time_sincronizied"
    NAME_TIME_UNSYCRONICIZIED = "time_unsyncronizied"
    DEFAULT_TRANSFER_TIME_SCHNEID = 300
    DEFAULT_SCHNEID_TIMEZONE = 'Europe/Berlin'
    DEFAULT_TRASFER_SLEEPER_SECOND = 60*5
    DEFAULT_ERROR_SLEEP_SECOND = 300
    DEFAULT_BREAK_TELEGRAF_LEN = 50000
    DEFAULT_FIRST_TIME_SCHNEID = datetime(2014,1,1)

class AqotecParams():
    NAME_ENDING_TABLE_ROW_META = "_b"
    NAME_TABLE_START = "table_start"
    NAME_STARTING_TABLE_UG07 = "UG07_"
    NAME_BASE_DATABASE = "aqotec_Daten"
    NAME_STARTING_TABLE_RM360 = "RM360_"
    NAME_BETWEEN_TABLE_BHKW = "BHKW"
    NAME_BETWEEN_TABLE_SUBZ = "SubZ_"
    NAME_SENSOR_ID = "sensor_id"
    NAME_NAME = "name"
    NAME_STARTING_TABLE_WMZ = "WMZ_"
    NAME_BETWEEN_NETWORK = "Netz"
    NAME_DB_INSTANCE_ID = "instance_id"
    NAME_DATABASE_END_CUSTOMER = "_Kunden"
    NAME_DATABASE_END_DATA = "_Daten"
    NAME_TABLE_CUSTOMER = "Tbl_Abnehmer"
    NAME_META_ID = "_attribute"
    NAME_META_ID_DB = "meta_id"
    NAME_DP_POS = "dp_pos"
    NAME_META_VALUES = "attributevalues"
    NAME_ASSET_ID = "_asset"
    NAME_ASSET_UG07 = "ug07"
    NAME_ASSET_RM360 = "rm360"
    NAME_ASSET_SZBZ = "subz"
    NAME_ASSET_NETWORK = "network"
    NAME_TYPE = "type"
    NAME_VALUE = "value"
    NAME_ID = "id"
    NAME_ASSET_WMZ = "wmz"
    NAME_NAME = "name"
    NAME_DATABASE = "database"
    NAME_ENDING_ARCHIV = "_archiv"
    NAME_DB_ASSET_ID = "asset_id"
    NAME_TABLE = "table"
    NAME_ORGANIZATION = "_organization"
    NAME_DP = "dp"
    NAME_SYNCRONICZIED = "syncronizied"
    NAME_UNSYCRONICIZIED = "unsyncronizied"
    NAME_TIME_SYNCRONICZIED_META = "time_sincronizied_meta"
    NAME_TIME_SYNCRONICZIED = "time_sincronizied"
    NAME_TIME_UNSYCRONICIZIED = "time_unsyncronizied"
    NAME_DP_NAME = "name_dp"
    NAME_NUMBER = "number"
    NAME_INTEGER = "integer"
    NAME__ID = "_id"
    LT = "lt"
    GT= "gt"
    B1 = "b1"
    B2 = "b2"
    POS_NAME_DP = 5
    POS_DP = 1
    DEFAULT_BREAK_TELEGRAF_LEN = 50000
    DEFAULT_MAX_TIMERANGE = timedelta(hours=24*5)
    DEFAULT_CHECK_ARCHIVE_TIMERANGE = timedelta(days=30)
    DEFAULT_TRANSFER_TIME_AQOTEC = 120
    DEFAULT_AQOTEC_TIMEZONE = 'Europe/Berlin'
    DEFAULT_TRASFER_SLEEPER_SECOND = 60*2
    DEFAULT_ERROR_SLEEP_SECOND = 300
    STATUS_CODE_GOOD = 204
    ERROR_HANDLING_START_DP = "DP_"
    INSTANCE_NAME_ADDITIVE_SUBZ = "SubZ-"
    DATABASE_NOTES_DEVAULT = "aqotec_Alarme"
    DATABASE_NOTES = "aqotec_%s_Alarme"
    NOTE_CREATED = "created"
    NOTE_NOTE = "note"
    NOTE_VISIB = "visibility"
    NOTE_INSTANCE = "_instance"
    NOTE_START = "start"
    NOTE_STOP = "stop"
    NOTE_USER = "_user"
    NOTE_TEXT = 'Änderung des Reglerwert "%s" von %s%s auf %s%s (Benutzer:%s)'
    NAME_DEVAULT_DATABASE = "Daten"
    NAME_NUMBER = "number"
    NAME_INTEGER = "integer"

'''
--------------------------------------------------------------------------------------------------------------------
                                                   Database Struct
--------------------------------------------------------------------------------------------------------------------
'''

class DbStruct():
    
    def __init__(self) -> None:
        self.ug06 = {}
        self.ug08 = {}
        self.ug10 = {}
        self.ug12 = {}
        self.other = {}

    def putUg06(self, database, table):
        if database not in self.ug06:
            self.ug06[database] = []
        self.ug06[database].append(table)

    def putUg08(self, database, table):
        if database not in self.ug08:
            self.ug08[database] = []
        self.ug08[database].append(table)

    def putUg10(self, database, table):
        if database not in self.ug10:
            self.ug10[database] = []
        self.ug10[database].append(table)

    def putUg12(self, database, table):
        if database not in self.ug12:
            self.ug12[database] = []
        self.ug12[database].append(table)

    def putOther(self, database, table):
        if database not in self.other:
            self.other[database] = []
        self.other[database].append(table)  

'''
--------------------------------------------------------------------------------------------------------------------
                                                       CSV Data Class
--------------------------------------------------------------------------------------------------------------------
'''


class SchneidCsvWinmiocs70(SchneidParamWinmiocs70):
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
        self.file_path = file_path
        self.files = []
        self.hast_files = []
        self.files_columns = {}
        self.files_infos = {}
        if self.file_path: 
            self.restetFiles()

    def setFilesFromDirectory(self) -> None:
        if self.file_path: self.files:list = listdir(self.file_path)
        else: self.files:list = []

    def setHastFiles(self) -> None:
        hast_files = []
        if not self.files: self.hast_files:list=hast_files; return(-1)
        for file in self.files:
            if SchneidCsvWinmiocs70.HAST_FILE_FORMAT in file: hast_files.append(file)
        self.hast_files = hast_files
    
    def setFileInfos(self) -> None:
        for file in self.files:
            if SchneidCsvWinmiocs70.DATA_FILE_FORMAT not in file: continue
            if SchneidCsvWinmiocs70.DATA_FILE_FORMAT_ENDING not in file: continue
            header = self.getFileHeader(file)
            self.files_columns[file] = self.getColsFromColumnHeader(header[SchneidCsvWinmiocs70.DICTNAME_COLUMN_HEADER])
            self.files_infos[file] = self.getInfoFromInfoHeader(header[SchneidCsvWinmiocs70.DICTNAME_INFO_HEADER])
        pass

    def restetFiles(self) -> None:
        self.setFilesFromDirectory()
        self.setHastFiles()
        self.setFileInfos()

    def getFileHeader(self, file_name: str) -> dict:
        with open(self.file_path+"/"+file_name, mode='r', newline='', encoding=SchneidCsvWinmiocs70.CSV_ENCODING) as file:
            csv_reader = csv.reader(file, delimiter=SchneidCsvWinmiocs70.CSV_DELIMITER)
            try: header = next(csv_reader)
            except: header = ["", ""]
        return({SchneidCsvWinmiocs70.DICTNAME_INFO_HEADER:header[0],SchneidCsvWinmiocs70.DICTNAME_COLUMN_HEADER:header[1]})

    def getColsFromColumnHeader(self, header: str):
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
        internal_id = re.search(SchneidCsvWinmiocs70.TABLE_ID_REG, header)
        internal_id = internal_id.group(1) if internal_id else None
        station_type = re.search(SchneidCsvWinmiocs70.TABLE_STATION_REG, header)
        station_type = station_type.group(1) if station_type else None
        last_write_time = re.search(SchneidCsvWinmiocs70.TABLE_LAST_WRITE_TIME_REG, header)
        try: last_write_time = datetime.strptime(last_write_time.group(1).split(">")[1],SchneidCsvWinmiocs70.TIME_FORMAT_LAST_TIME) if last_write_time else None
        except: last_write_time = None
        interval = re.search(SchneidCsvWinmiocs70.TABLE_INTERVAL_REG, header)
        interval = interval.group(1) if interval else None
        return({
            SchneidCsvWinmiocs70.DICTNAME_SCHNEID_TB_ID: internal_id,
            SchneidCsvWinmiocs70.DICTNAME_SCHNEID_STATION_TYPE: station_type,
            SchneidCsvWinmiocs70.DICTNAME_LAST_WRITE_TIME: last_write_time,
            SchneidCsvWinmiocs70.DICTNAME_INTERVAL: interval
        })

    def readCsvDataReverseAsDataFrame(self, file_name: str, lines: int = 10) -> pd.DataFrame:
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
                if buffer[-1]=="":buffer=buffer[-lines-1:-1]
                else: buffer = buffer[-lines:]
        for buff in range(len(buffer)):
            if bool(SchneidCsvWinmiocs70.TIME_COMPILER.match(buffer[buff].split(SchneidCsvWinmiocs70.CSV_DELIMITER)[0])):
                break
        try:
            buffer = pd.read_csv(StringIO('\n'.join(buffer[buff:])), sep=SchneidCsvWinmiocs70.CSV_DELIMITER, header=None, encoding=SchneidCsvWinmiocs70.CSV_ENCODING)
        except pd.errors.ParserError as e:
            match = re.search(r'line (\d+)', str(e))
            line_number = int(match.group(1))
            #buffer1 = pd.read_csv(StringIO('\n'.join(buffer[buff:line_number])), sep=SchneidCsvWinmiocs70.CSV_DELIMITER, header=None, encoding=SchneidCsvWinmiocs70.CSV_ENCODING)
            try:
                buffer = pd.read_csv(StringIO('\n'.join(buffer[line_number+buff:])), sep=SchneidCsvWinmiocs70.CSV_DELIMITER, header=None, encoding=SchneidCsvWinmiocs70.CSV_ENCODING)
            except  pd.errors.ParserError as e:
                buffer = pd.read_csv(StringIO('\n'.join(buffer[line_number+100+buff:])), sep=SchneidCsvWinmiocs70.CSV_DELIMITER, header=None, encoding=SchneidCsvWinmiocs70.CSV_ENCODING)
        buffer[0] = pd.to_datetime(buffer[0], format=SchneidCsvWinmiocs70.TIME_FORMAT_TIMESTEPS)
        buffer.set_index(0, inplace=True)
        buffer.columns = range(len(buffer.columns))
        return(buffer)

'''
--------------------------------------------------------------------------------------------------------------------
                                                   Connetct to Db Data Class
--------------------------------------------------------------------------------------------------------------------
'''

class ScheindPostgresWinmiocs70(SchneidParamWinmiocs70):
    SQL_GET_NODES = "SELECT * FROM node;"

    def __init__(self, hostname: str = "localhost", database: str = "postgres", username: str = "winmiocs", password: str = "") -> None:
        self.hostname = hostname
        self.database = database
        self.username = username
        self.password = password
        self.conn = None

    def connectToDb(self) -> None:
        self.conn = psycopg2.connect(
            host=self.hostname,
            dbname=self.database,
            user=self.username,
            password=self.password
        )

    def disconnectToDb(self) -> None:
        self.conn.close()

    def getMetaDataByID(self) -> dict:
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
        except: return({})

'''
--------------------------------------------------------------------------------------------------------------------
                                                   Meta Data Class
--------------------------------------------------------------------------------------------------------------------
'''

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
                 work_defalut_name_instance:str="Daten") -> None:
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
            else:
                self.struct.putOther(self.workspace_name,csv)

    def checkStationDatapoints(self):
        self._ceckUg06()
        self._ceckUg08()
        self._ceckUg10()
        self._ceckUg12()

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
                self.check_point_buffer = self.csvs.readCsvDataReverseAsDataFrame(table,lines=2000).reset_index(drop=True)
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
                            name=instance_name+" \u2014 "+ self.work_defalut_name_instance,
                            asset_id=asset_id,
                            description="Schneid_"+asset_name,
                            workspace_id=workspace_id
                        )
                        instance_id=ret[SchneidMeta.NAME__ID]
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
            meta_postgres = self.postgres.getMetaDataByID()
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
            table_name = str(meta_instances[instance[SchneidMeta.NAME__ID]])+SchneidMeta.HAST_FILE_FORMAT
            if table_name not in self.csvs.files: continue
            table_columns = self.csvs.files_columns[table_name]
            for asset_values_drive in asset_meta:
                if asset_values_drive[SchneidMeta.NAME_DP_NAME] not in table_columns: continue
                if asset_values_drive[SchneidMeta.NAME_DP] != table_columns[asset_values_drive[SchneidMeta.NAME_DP_NAME]][SchneidMeta.DICTNAME_POSITION]: continue
                # if table for meta in database ?
                try:
                    self.check_point_buffer = self.csvs.readCsvDataReverseAsDataFrame(table_name,lines=2000).reset_index(drop=True)
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
        #self.labled_points.patchAllPointsToSinc()
            
    def startCheckAllMetaData(self):
        self.checkStationDatapoints()
        self.patchStationMeta()
        self.patchLastDataPointMeta()
        self.patchSyncStatus()

'''
--------------------------------------------------------------------------------------------------------------------
                                                Data Transfer Class
--------------------------------------------------------------------------------------------------------------------
'''

class SchneidTransferCsv(SchneidParamWinmiocs70):

    def __init__(self,interval:timedelta,SyncStatus:SyncronizationStatus,NaoApp:NaoApp,SchneidCSV:SchneidCsvWinmiocs70) -> None:
        self.sync_status = SyncStatus
        self.interval = interval
        self.csvs = SchneidCSV
        self.status = self.getSyncStatus()
        self.nao = NaoApp
        self.new_status = {}
        self.status_count = {}

    def startSyncronization(self, logfile=None, sleep_data_len=1, archiv_sinc=False):
        count = 0
        is_sinct = False
        sinc_timer = time()
        while 1==1:
            # try: 
                if datetime.now().hour >= 23:
                    self.setSyncStatus()
                    self.status=self.getSyncStatus()
                    break
                start_time = time()
                data_telegraf, sinc_reset = self.getTelegrafData()
                if len(data_telegraf)>0:ret=self.nao.sendTelegrafData(data_telegraf)
                else:ret=SchneidTransferCsv.STATUS_CODE_GOOD    
                if ret==SchneidTransferCsv.STATUS_CODE_GOOD:
                    print(len(data_telegraf), " data posted; sec:",time()-start_time, datetime.now())
                    start_time = time()
                    count+=len(data_telegraf)
                    if sinc_reset or time()-sinc_timer:
                        sinc_timer = time()
                        self.setSyncStatus()
                        self.status=self.getSyncStatus()
                    if archiv_sinc and len(data_telegraf)==0:
                        break
                    if is_sinct:
                        sleep(SchneidTransferCsv.DEFAULT_TRASFER_SLEEPER_SECOND)
                    elif len(data_telegraf)<sleep_data_len:
                        self.csvs.restetFiles()                       
                        self.setSyncStatus()
                        self.status=self.getSyncStatus()  
                        is_sinct = True
                        sleep(SchneidTransferCsv.DEFAULT_TRASFER_SLEEPER_SECOND)
                else:
                    sleep(SchneidTransferCsv.DEFAULT_ERROR_SLEEP_SECOND)
            # except:
            #     if logfile: logfile(str(sys.exc_info()))
            #     sleep(SchneidTransferCsv.DEFAULT_ERROR_SLEEP_SECOND)
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
                sincron_time = {}
                for table_db in self.new_status[database]:
                    if self.new_status[database][table_db].get(SchneidTransferCsv.NAME_TIME_UNSYCRONICIZIED):
                        self.sync_status.patchSincStatus(database,table_db,self.new_status[database][table_db][SchneidTransferCsv.NAME_TIME_UNSYCRONICIZIED],True)
                    if self.new_status[database][table_db].get(SchneidTransferCsv.NAME_TIME_SYNCRONICZIED):
                        sincron_time[table_db] = self.new_status[database][table_db][SchneidTransferCsv.NAME_TIME_SYNCRONICZIED]
                self.sync_status.patchSincStatusManyMany(database=database,data=sincron_time,isunsinc=False)
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
                if status_instance["time_sincronizied"]!=None:
                    if "WMZ" in status_instance[SchneidTransferCsv.NAME_TABLE]:
                        if  pytz.timezone(SchneidTransferCsv.DEFAULT_SCHNEID_TIMEZONE).localize(datetime.fromisoformat(status_instance["time_sincronizied"])).astimezone(pytz.utc).replace(tzinfo=None) > datetime.utcnow()-timedelta(hours=26):
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
        # delet duplicates of sensores in sinc-status if regulator change
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
        dataframe = dataframe[(dataframe.index>=start_time)&(dataframe.index<=stop_time)]
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
                add_telegraf(f"{asset_id},instance={instance_id} {sensor_ids[idx]}={timeseries[columns[idx]].iloc[row]} {timestamp}")
        return(telegraf_list)  
        
