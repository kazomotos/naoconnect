'Autor: Rupert Wieser -- Naotilus -- 20232209'
import pyodbc
from naoconnect.local_db import Driver, StationDatapoints, LablingNao, SyncronizationStatus
from pandas import Series
from datetime import datetime, timedelta
from naoconnect.naoappV2 import NaoApp
from time import sleep, time
import sys
import pytz

''' DOC
-----------  Ubunut  -----------
sudo apt-get update
sudo apt-get install unixodbc
pip install pyodbc
----------- install Driver ODBC from Microsoft -------------
--> example for linux:
curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18

'''


'''
--------------------------------------------------------------------------------------------------------------------
                                                        Params
--------------------------------------------------------------------------------------------------------------------
'''

class AqotecParams():
    QUERY_DATABASES = "SELECT name FROM sys.databases"
    QUREY_USE = "USE %s"
    QUERY_TABLE_NAMES = "SELECT table_name FROM information_schema.tables"
    QUERY_TABLE_COL_NAMES = "SELECT * FROM %s"
    QUERY_DATA_POINT_CECK = "SELECT TOP 500 %s FROM %s ORDER BY DP_Zeitstempel DESC"
    QUERY_DATA_POINT_CECK_LAST = "SELECT TOP 1 %s FROM %s ORDER BY DP_Zeitstempel DESC"
    QUERY_SELECT_FIRST_TIME = "SELECT TOP 1 * FROM %s ORDER BY DP_Zeitstempel ASC"
    QUERY_SELECT_LAST_TIME = "SELECT TOP 1 * FROM %s ORDER BY DP_Zeitstempel DESC"
    QUERY_NOTES = "SELECT * FROM tbl_Historie WHERE his_Zeitstempel > DATETIME2FROMPARTS%s ORDER BY his_Zeitstempel ASC"
    QUERY_TIMESERIES = "SELECT %s, DP_Zeitstempel FROM %s WHERE DP_Zeitstempel > DATETIME2FROMPARTS%s AND DP_Zeitstempel < DATETIME2FROMPARTS%s ORDER BY DP_Zeitstempel DESC"
    QUERY_META_CUSTOMER_SELECT = "SELECT %s FROM Tbl_Abnehmer WHERE AnID = %s"
    QUERY_META_CUSTOMER_SELECT_2 = "SELECT %s FROM Tbl_Station WHERE AnID = %s"
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

'''
--------------------------------------------------------------------------------------------------------------------
                                                   Database Struct
--------------------------------------------------------------------------------------------------------------------
'''

class DbStruct():
    
    def __init__(self) -> None:
        self.ug07 = {}
        self.wmz = {}
        self.rm360 = {}
        self.bhkw = {}
        self.subz = {}
        self.other = {}
        self.network = {}

    def putUg07(self, database, table):
        if database not in self.ug07:
            self.ug07[database] = []
        self.ug07[database].append(table)

    def putWmz(self, database, table):
        if database not in self.wmz:
            self.wmz[database] = []
        self.wmz[database].append(table)

    def putRm360(self, database, table):
        if database not in self.rm360:
            self.rm360[database] = []
        self.rm360[database].append(table)

    def putBHKW(self, database, table):
        if database not in self.bhkw:
            self.bhkw[database] = []
        self.bhkw[database].append(table)

    def putSubZ(self, database, table):
        if database not in self.subz:
            self.subz[database] = []
        self.subz[database].append(table)

    def putNetwork(self, database, table):
        if database not in self.network:
            self.network[database] = []
        self.network[database].append(table)

    def putOher(self, database, table):
        if database not in self.other:
            self.other[database] = []
        self.other[database].append(table)

'''
--------------------------------------------------------------------------------------------------------------------
                                                   Connetct to Db Data Class
--------------------------------------------------------------------------------------------------------------------
'''

class AqotecConnectorV2(AqotecParams):

    def __init__(self,host,port,user,password,driver="{ODBC Driver 18 for SQL Server}") -> None:
        self.host = host
        self.port = str(port)
        self.user = user
        self.password = password
        self.driver = driver

    def connectToDb(self):
        try:
            self.conn = pyodbc.connect("DRIVER="+self.driver+";SERVER="+self.host+";PORT="+self.port+";UID="+self.user+";PWD="+self.password+";Encrypt=No")
        except:
            print("connection faild, nex connetction in 300 sec")
            sleep(2)
            self.conn = pyodbc.connect("DRIVER="+self.driver+";SERVER="+self.host+";PORT="+self.port+";UID="+self.user+";PWD="+self.password+";Encrypt=No")

    def disconnetToDb(self):
        try:
            self.conn.close()
        except:
            pass

'''
--------------------------------------------------------------------------------------------------------------------
                                                   Meta Data Class
--------------------------------------------------------------------------------------------------------------------
'''

class AqotecMetaV2(AqotecConnectorV2):

    def __init__(self,host,port,user,password,AqotecDriver:Driver,LablingPoints:StationDatapoints,LablingNao:LablingNao,SyncStatus:SyncronizationStatus,NaoApp:NaoApp,driver="{ODBC Driver 18 for SQL Server}") -> None:
        super().__init__(host, port, user, password, driver)
        self.driver_db = AqotecDriver
        self.labled_points = LablingPoints
        self.labled_nao = LablingNao
        self.sync_status = SyncStatus
        self.nao = NaoApp
        self.struct = DbStruct()
        self.connectToDb()
        self.databases = self._getDatabases()
        self._getTableStucture()
        self.disconnetToDb()
        self.user_id_nao=None
    
    def _getDatabases(self):
        cursor = self.conn.cursor()
        cursor.execute(self.QUERY_DATABASES)
        ret = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return(ret)
    
    def _getTableStucture(self):
        cursor = self.conn.cursor()
        for database in self.databases:
            try:
                cursor.execute(AqotecMetaV2.QUREY_USE%(database))
                cursor.execute(AqotecMetaV2.QUERY_TABLE_NAMES)
            except: continue
            tables = [row[0] for row in cursor.fetchall()]
            self._setTableNames(tables=tables,database=database)
        cursor.close()
            
    def _setTableNames(self, tables, database):
        for table in tables:
            if len(table)<2:continue
            if table[-2:]!=AqotecMetaV2.NAME_ENDING_TABLE_ROW_META:continue
            if AqotecMetaV2.NAME_STARTING_TABLE_UG07 in table:
                self.struct.putUg07(database,table)
            elif AqotecMetaV2.NAME_BETWEEN_NETWORK in table:
                self.struct.putNetwork(database,table)
            elif AqotecMetaV2.NAME_STARTING_TABLE_WMZ in table:
                self.struct.putWmz(database,table)
            elif AqotecMetaV2.NAME_STARTING_TABLE_RM360 in table:
                self.struct.putRm360(database,table)
            elif AqotecMetaV2.NAME_BETWEEN_TABLE_BHKW in table:
                self.struct.putBHKW(database,table)
            elif AqotecMetaV2.NAME_BETWEEN_TABLE_SUBZ in table:
                self.struct.putSubZ(database,table)
            else:
                self.struct.putOher(database,table)

    def checkStationDatapoints(self):
        self._ceckNetwork()
        self._ceckSubWmz()
        self._ceckWmzFromSubz()
        self._ceckUg07()
        self._ceckRm360()
        self._ceckStationWmz()

    def _ceckNetwork(self):
        self._ceckTable(self.struct.network, self.driver_db.ceckDriverNetwork, AqotecMetaV2.NAME_ASSET_NETWORK, True, "", True)

    def _ceckUg07(self):
        self._ceckTable(self.struct.ug07, self.driver_db.ceckDriverUG07, AqotecMetaV2.NAME_ASSET_UG07, True)

    def _ceckRm360(self):
        self._ceckTable(self.struct.rm360, self.driver_db.ceckDriverRM360, AqotecMetaV2.NAME_ASSET_RM360, True)

    def _ceckStationWmz(self):
        self._ceckTable(self.struct.wmz, self.driver_db.ceckDriverStationWMZ, AqotecMetaV2.NAME_ASSET_WMZ, False)

    def _ceckSubWmz(self):
        self._ceckTable(self.struct.subz, self.driver_db.ceckDriverSubWMZ, AqotecMetaV2.NAME_ASSET_SZBZ, True, AqotecMetaV2.INSTANCE_NAME_ADDITIVE_SUBZ)

    def _ceckWmzFromSubz(self):
        self._ceckTable(self.struct.wmz, self.driver_db.ceckDriverWMZfromSub, AqotecMetaV2.NAME_ASSET_SZBZ, False, AqotecMetaV2.INSTANCE_NAME_ADDITIVE_SUBZ )
    
    def _ceckTable(self, table_struct, ceckDriver, asset_name, create_instance=True, instance_name_addive="", full_name=False):
        self.connectToDb()
        cursor = self.conn.cursor()
        asset_id = self.labled_nao.ceckAsset(asset_name=asset_name)
        if not asset_id:return(-1)
        for database in table_struct:
            if len(database.split("_"))<3 and database!=AqotecMetaV2.NAME_BASE_DATABASE:continue
            try:cursor.execute(AqotecMetaV2.QUREY_USE%(database))
            except:continue
            workspace_id = None
            for table in table_struct[database]:
                if full_name:
                    instance_id = self.labled_nao.ceckInstance(instance_name_addive+table[:-2],database=database,asset_id=asset_id)
                else:
                    instance_id = self.labled_nao.ceckInstance(instance_name_addive+table.split("_")[-2],database=database,asset_id=asset_id)
                if not instance_id and not create_instance: continue
                try:cursor.execute(AqotecMetaV2.QUERY_TABLE_COL_NAMES%(table))
                except:continue
                data_points = cursor.fetchall()
                for point in data_points:
                    # -------------------------------------------data point in driver ?---------------------------------------------------------
                    driver_meta = ceckDriver(point[AqotecMetaV2.POS_NAME_DP],point[AqotecMetaV2.POS_DP])
                    if len(driver_meta)==0:continue 
                    # -------------------------------------------data point already in use ?---------------------------------------------------------
                    if not self.labled_points.ceckPoints(database, table, point[AqotecMetaV2.POS_DP]):continue
                    # -------------------------------------------data in aqotec database for this point ?---------------------------------------------------------
                    ifdata = self._ceckDataInPoint(
                        cursor=cursor,
                        dp=point[AqotecMetaV2.POS_DP],
                        table=table[:-2],
                        lt=driver_meta[AqotecMetaV2.LT],
                        gt=driver_meta[AqotecMetaV2.GT],
                        b1=driver_meta[AqotecMetaV2.B1],
                        b2=driver_meta[AqotecMetaV2.B2]
                    )
                    if not ifdata: continue
                    # -------------------------------------------creat workspace if not been created---------------------------------------------------------
                    if not workspace_id:
                        workspace_name = database.split("_")[1]
                        workspace_id = self.labled_nao.ceckWorkspace(workspace_name)
                        if not workspace_id:
                            ret=self.nao.createWorkspace(workspace_name)
                            workspace_id=ret[AqotecMetaV2.NAME__ID]
                            self.labled_nao.putWorkspace(ret)
                    # -------------------------------------------creat instance if not been created---------------------------------------------------------
                    if not instance_id:
                        if full_name:
                            instance_name = instance_name_addive+table[:-2]
                        else:
                            instance_name = instance_name_addive+table.split("_")[-2]
                        ret=self.nao.createInstance(
                            name=instance_name,
                            asset_id=asset_id,
                            description=table,
                            workspace_id=workspace_id
                        )
                        instance_id=ret[AqotecMetaV2.NAME__ID]
                        self.labled_nao.putInstance(ret,database)
                        # if len(ret[AqotecMetaV2.NAME_META_VALUES])>0:
                        #     self._saveInitialMetaData(ret[AqotecMetaV2.NAME_META_VALUES],ret[AqotecMetaV2.NAME__ID],"?")
                        print(instance_name)
                    # --------------------------------------ceck if allready activatet with other station (ug07)----------------------------
                    act_point = self.labled_points.getPointByInstanceSensorInstance(database,driver_meta[AqotecMetaV2.NAME_ID],instance_id)
                    # -------------------------------------------activate datapoint---------------------------------------------------------
                    if len(act_point)==0:
                        sleep(0.05)
                        ret=self.nao.activateDatapoint(
                            type_sensor=driver_meta[AqotecMetaV2.NAME_TYPE],
                            sensor_id=driver_meta[AqotecMetaV2.NAME_ID],
                            instance_id=instance_id,
                            config={
                                AqotecMetaV2.NAME_DATABASE:database,
                                AqotecMetaV2.NAME_TABLE:table,
                                AqotecMetaV2.NAME_DP:driver_meta[AqotecMetaV2.NAME_DP],
                                AqotecMetaV2.NAME_DP_NAME:driver_meta[AqotecMetaV2.NAME_DP_NAME]
                            }
                        )
                    # -----------------------------------save activated datapoint local-----------------------------------------------------
                    self.labled_points.savePoint(
                        database=database,
                        table_name=table,
                        name_dp=driver_meta[AqotecMetaV2.NAME_DP_NAME],
                        dp=driver_meta[AqotecMetaV2.NAME_DP],
                        instance_id=instance_id,
                        sensor_id=driver_meta[AqotecMetaV2.NAME_ID],
                        asset_id=asset_id
                    )
                    print("activate point")
        cursor.close()
        self.disconnetToDb()

    def _ceckDataInPoint(self,table,dp,cursor:pyodbc.Cursor,lt,gt,b1,b2,returns=False,last=False):
        if last:
            cursor.execute(AqotecMetaV2.QUERY_DATA_POINT_CECK%(dp,table))
        else:
            cursor.execute(AqotecMetaV2.QUERY_DATA_POINT_CECK%(dp,table))
        series = Series([row[0] for row in cursor.fetchall()])
        if len(series)==0:
            return(False)
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
        self.connectToDb()
        cursor = self.conn.cursor()
        instances = self.labled_nao.getInstances()
        # -------------- Stationsdaten --------------
        asset_meta, pos_dp = self._getAssetMetaQuery(number=2)
        name_db = ""
        for instance in instances:
            try: 
                if instance[AqotecMetaV2.NAME_DATABASE]!=name_db:cursor.execute(AqotecMetaV2.QUREY_USE%(
                    instance[AqotecMetaV2.NAME_DATABASE].split(AqotecMetaV2.NAME_DATABASE_END_DATA)[0]+AqotecMetaV2.NAME_DATABASE_END_CUSTOMER
                ))
            except: continue
            cursor.execute(AqotecMetaV2.QUERY_META_CUSTOMER_SELECT_2%(asset_meta[instance[AqotecMetaV2.NAME_ASSET_ID]][:-1],instance[AqotecMetaV2.NAME_NAME].split("R")[-1]))
            data = cursor.fetchall()
            if len(data)>0:self._patchStationMeta(data[0],instance, pos_dp, asset_meta[instance[AqotecMetaV2.NAME_ASSET_ID]].split(","), number=2)
        # -------------- Kundendaten --------------
        asset_meta, pos_dp = self._getAssetMetaQuery()
        name_db = ""
        for instance in instances:
            # not for subz
            if AqotecMetaV2.INSTANCE_NAME_ADDITIVE_SUBZ in instance[AqotecMetaV2.NAME_NAME]: continue
            try: 
                if instance[AqotecMetaV2.NAME_DATABASE]!=name_db:
                    cursor.execute(AqotecMetaV2.QUREY_USE%(
                        instance[AqotecMetaV2.NAME_DATABASE].split(AqotecMetaV2.NAME_DATABASE_END_DATA)[0]+AqotecMetaV2.NAME_DATABASE_END_CUSTOMER
                    ))
                    name_db = instance[AqotecMetaV2.NAME_DATABASE]
            except: continue
            cursor.execute(AqotecMetaV2.QUERY_META_CUSTOMER_SELECT%(asset_meta[instance[AqotecMetaV2.NAME_ASSET_ID]][:-1],instance[AqotecMetaV2.NAME_NAME].split("R")[-1]))
            data = cursor.fetchall()
            if len(data)>0:self._patchStationMeta(data[0],instance, pos_dp, asset_meta[instance[AqotecMetaV2.NAME_ASSET_ID]].split(","), number=1)
        cursor = self.conn.cursor()

        cursor.close()
        self.disconnetToDb()

    def _getDatetimeToSqlStrTuble(self, time:datetime):
        return(str((time.year,time.month,time.day,time.hour,time.minute,time.second,0,0)))
    
    def getNoteSchema(self, user=None, created=None, start=None, stop=None, visiblity=1, instance_id=None, note=None):
        return({
            AqotecMetaV2.NOTE_CREATED: created,
            AqotecMetaV2.NOTE_USER : user,
            AqotecMetaV2.NOTE_INSTANCE : instance_id,
            AqotecMetaV2.NOTE_VISIB : visiblity,
            AqotecMetaV2.NOTE_NOTE : note,
            AqotecMetaV2.NOTE_START : start,
            AqotecMetaV2.NOTE_STOP : stop
        })

    # "notes": {
    #     "1": {
    #         "name": "Daten",
    #         "time_sincronizied": "2024-03-26 09:16:11",
    #         "time_sincronizied_meta": "2024-03-24 14:33:07"
    #     }
    # },

    def patchNotesMeta(self, check_worcspace=False):
        # get user if undefined
        if not self.user_id_nao: self.user_id_nao = self.nao.getUserId()
        # notes last times
        notes_times = self.labled_nao.getNotesAll()
        # check workspaces
        workspaces = self.labled_nao.getWorkspaceMetaAll()
        workspace_ids = {}
        for work in workspaces:
            workspace_ids[work[AqotecMetaV2.NAME_NAME]] = work[AqotecMetaV2.NAME__ID]
        if check_worcspace:
            spaces = []
            for n_t in notes_times:
                spaces.append(n_t[AqotecMetaV2.NAME_NAME])
            for work in workspaces:
                if work[AqotecMetaV2.NAME_NAME] in spaces:continue
                self.labled_nao.putEmptyNode(work[AqotecMetaV2.NAME_NAME])
            notes_times = self.labled_nao.getNotesAll()
        if not notes_times: return(-1)
        if len(notes_times)==0: return(-1)
        for idx_sinc in range(len(notes_times)):
            sinc_time = notes_times[idx_sinc][AqotecMetaV2.NAME_TIME_SYNCRONICZIED]
            sinc_name = notes_times[idx_sinc][AqotecMetaV2.NAME_NAME]
            sinc_time_meta = notes_times[idx_sinc][AqotecMetaV2.NAME_TIME_SYNCRONICZIED_META]
            sinc_name_meta = notes_times[idx_sinc][AqotecMetaV2.NAME_NAME]
            if not sinc_time: sinc_time = datetime(2013,1,1,1,1,1)
            else: sinc_time = datetime.fromisoformat(sinc_time)
            if not sinc_time_meta: sinc_time_meta = datetime(2013,1,1,1,1,1)
            else: sinc_time_meta = datetime.fromisoformat(sinc_time_meta)
            # meta data status
            asset_meta = self.driver_db.getAssetNotesMeta()
            instances = self.labled_nao.getInstances(workspace_id=workspace_ids[notes_times[idx_sinc][AqotecMetaV2.NAME_NAME]])
            instance_dic = {}
            for instance in instances:
                try:instance_dic[instance[AqotecMetaV2.NAME_NAME].split("R")[1]] = instance
                except:continue
            metas = {}
            for asset in asset_meta:
                metas[asset[AqotecMetaV2.NAME_DP]] = asset
            # ------------  database -----------
            if notes_times[idx_sinc][AqotecMetaV2.NAME_NAME] == AqotecMetaV2.NAME_DEVAULT_DATABASE:
                database_use = AqotecMetaV2.DATABASE_NOTES_DEVAULT
            else:
                database_use = AqotecMetaV2.DATABASE_NOTES%(notes_times[idx_sinc][AqotecMetaV2.NAME_NAME])
            # --------     get new meta from notes    ----------
            self.connectToDb()
            cursor = self.conn.cursor()
            if len(metas) != 0:
                cursor.execute(AqotecMetaV2.QUREY_USE%(database_use))
                cursor.execute(AqotecMetaV2.QUERY_NOTES%(self._getDatetimeToSqlStrTuble(sinc_time_meta)))
                data_raw = [row for row in cursor.fetchall()]
                try:
                    for dat in data_raw:
                        if str(dat[4]) not in instance_dic: continue
                        if not dat[7] or dat[7] == "":continue
                        if dat[7] in metas:
                            self._patchStationMetaFromValue(value=dat[10],instance_id=instance_dic[str(dat[4])][AqotecMetaV2.NAME__ID],driver_infos=metas[dat[7]])
                            sinc_time_meta = dat[1]
                except:
                    self.labled_nao.updateNoteTimeMetaByName(time=sinc_time_meta,name=sinc_name_meta)
                    raise(KeyError("can't send notes to nao"))
                self.labled_nao.updateNoteTimeMetaByName(time=sinc_time_meta,name=sinc_name_meta)
            # --------     get new notes    ----------
            if len(instance_dic) != 0:
                cursor = self.conn.cursor()
                try:
                    cursor.execute(AqotecMetaV2.QUREY_USE%(database_use))
                    cursor.execute(AqotecMetaV2.QUERY_NOTES%(self._getDatetimeToSqlStrTuble(sinc_time)))
                except:
                    continue
                data_raw = [row for row in cursor.fetchall()]
                for dat in data_raw:
                    if str(dat[4]) not in instance_dic: continue
                    if not dat[7] or dat[7] == "":continue
                    if dat[16]:unit=dat[16]
                    else:unit=""
                    text =  AqotecMetaV2.NOTE_TEXT%(dat[7],dat[9],unit,dat[10],unit,dat[15])
                    note_payload = self.getNoteSchema(
                        note=text,
                        user=self.user_id_nao,
                        start=str(dat[1]),
                        stop=str(dat[1]),
                        created=str(dat[1]),
                        instance_id=instance_dic[str(dat[4])][AqotecMetaV2.NAME__ID]
                    )
                    try:
                        ret = self.nao.pushNote(asset_id=instance_dic[str(dat[4])][AqotecMetaV2.NAME_ASSET_ID],data_note=note_payload)         
                        print("post_notes")
                    except:
                        self.labled_nao.updateNoteTimeByName(time=sinc_time,name=sinc_name)
                        raise(KeyError("can't send notes to nao"))
                    sinc_time = dat[1]
                    print(1)
                self.labled_nao.updateNoteTimeByName(time=sinc_time,name=sinc_name)
            cursor.close()
            self.disconnetToDb()

    def patchLastDataPointMeta(self):
        self.connectToDb()
        cursor = self.conn.cursor()
        asset_meta = self.driver_db.getAssetLastValueMeta()
        instances = self.labled_nao.getInstances()
        name_db = ""
        for instance in instances:
            try: 
                if instance[AqotecMetaV2.NAME_DATABASE]!=name_db:
                    cursor.execute(AqotecMetaV2.QUREY_USE%(instance[AqotecMetaV2.NAME_DATABASE]))
                    name_db=instance[AqotecMetaV2.NAME_DATABASE]
            except: continue
            for asset_values_drive in asset_meta:
                # if table for meta in database ?
                try:
                    # if data name in datatable ?
                    cursor.execute(AqotecMetaV2.QUERY_TABLE_COL_NAMES%(asset_values_drive[AqotecMetaV2.NAME_TABLE_START]+instance[AqotecMetaV2.NAME_NAME].replace(AqotecMetaV2.INSTANCE_NAME_ADDITIVE_SUBZ,"")+AqotecConnectorV2.NAME_ENDING_TABLE_ROW_META))
                    col_names = cursor.fetchall()
                    ifcol = False
                    for col_name in col_names:
                        if col_name[AqotecMetaV2.POS_DP] == asset_values_drive[AqotecMetaV2.NAME_DP] and col_name[AqotecMetaV2.POS_NAME_DP] == asset_values_drive[AqotecMetaV2.NAME_DP_NAME]:
                            ifcol = True
                            break
                    if not ifcol: continue
                    # if a valid value in datatable ?
                    ifdata = self._ceckDataInPoint(
                        cursor=cursor,
                        dp=asset_values_drive[AqotecMetaV2.NAME_DP],
                        table=asset_values_drive[AqotecMetaV2.NAME_TABLE_START]+instance[AqotecMetaV2.NAME_NAME].replace(AqotecMetaV2.INSTANCE_NAME_ADDITIVE_SUBZ,""),
                        lt=asset_values_drive[AqotecMetaV2.LT],
                        gt=asset_values_drive[AqotecMetaV2.GT],
                        b1=asset_values_drive[AqotecMetaV2.B1],
                        b2=asset_values_drive[AqotecMetaV2.B2],
                        returns=True,
                        last=True
                    )
                    if len(ifdata) == 0: continue
                except:
                    continue
                self._patchStationMetaFromValue(ifdata[0],instance[AqotecMetaV2.NAME__ID],asset_values_drive)
        cursor.close()
        self.disconnetToDb()

    def _patchStationMetaFromValue(self, value, instance_id, driver_infos):
        # check if data valid
        if value=="" or value==None: return(-1)
        # check if meta labled before
        meta = self.labled_nao.getInstanceMetaByAttributeInstance(instance_id,driver_infos[AqotecMetaV2.NAME_META_ID_DB])
        if meta==[]:
            # get data from nao if not labled before
            instance_infos = self.nao.getInstanceInfos(instance_id)
            id_att = ""
            for info in instance_infos[AqotecMetaV2.NAME_META_VALUES]:
                if info[AqotecMetaV2.NAME_META_ID] == driver_infos[AqotecMetaV2.NAME_META_ID_DB]:
                    id_att = info[AqotecMetaV2.NAME__ID]
            if id_att=="":return(-1)
            # put initial meta data to local labling db
            self.labled_nao.putMetaInstance(
                value=None,
                meta_id=driver_infos[AqotecMetaV2.NAME_META_ID_DB],
                dp=driver_infos[AqotecMetaV2.NAME_DP],
                id=id_att,
                asset_id=driver_infos[AqotecMetaV2.NAME_DB_ASSET_ID],
                type=driver_infos[AqotecMetaV2.NAME_TYPE],
                dp_pos=None,
                instance_id=instance_id
            )
            meta = self.labled_nao.getInstanceMetaByAttributeInstance(instance_id,driver_infos[AqotecMetaV2.NAME_META_ID_DB])
        # ceck if meta has chanced
        if meta[0][AqotecMetaV2.NAME_TYPE] == AqotecMetaV2.NAME_NUMBER: dat = float(value.replace(",","."))
        elif meta[0][AqotecMetaV2.NAME_TYPE] == AqotecMetaV2.NAME_INTEGER: dat = int(value.replace(",", "."))
        else: dat = str(value)
        if meta[0][AqotecMetaV2.NAME_VALUE]!=dat:
            # patch meta data
            self.nao.patchInstanceMeta(meta[0][AqotecMetaV2.NAME_DB_INSTANCE_ID],meta[0][AqotecMetaV2.NAME_ID],dat)
            self.labled_nao.patchInstanceMetaValueByAttributeInstance(instance_id,driver_infos[AqotecMetaV2.NAME_META_ID_DB], dat)
            print("patch meta")

    def _getAssetMetaQuery(self, number=1):
        if number==1:
            asset_meta = self.driver_db.getAssetMeta()
        else:
            asset_meta = self.driver_db.getAssetMeta2()
        ret = {}
        ret2 = []
        for set in asset_meta:
            if set[AqotecMetaV2.NAME_DB_ASSET_ID] not in ret: ret[set[AqotecMetaV2.NAME_DB_ASSET_ID]]=""
            ret[set[AqotecMetaV2.NAME_DB_ASSET_ID]] += set[AqotecMetaV2.NAME_DP]+","
            ret2.append(set[AqotecMetaV2.NAME_DP_POS])
        return(ret, ret2)

    def _saveInitialMetaData(self, attributevalues, instance_id, dp, number=1):
        for value in attributevalues:
            if number == 1:
                meta_driver = self.driver_db.getAssetMetaFromId(value[AqotecMetaV2.NAME_META_ID])
            else:
                meta_driver = self.driver_db.getAssetMetaFromId2(value[AqotecMetaV2.NAME_META_ID])
            if len(meta_driver)==0:
                continue
            meta_driver = meta_driver[0]
            old = self.labled_nao.getInstanceMetaByPosInstance(instance_id,meta_driver[AqotecMetaV2.NAME_DP_POS], dp)
            if len(old)!=0:continue
            self.labled_nao.putMetaInstance(
                value=None,
                meta_id=value[AqotecMetaV2.NAME_META_ID],
                dp=meta_driver[AqotecMetaV2.NAME_DP],
                id=value[AqotecMetaV2.NAME__ID],
                asset_id=meta_driver[AqotecMetaV2.NAME_DB_ASSET_ID],
                type=meta_driver[AqotecMetaV2.NAME_TYPE],
                dp_pos=meta_driver[AqotecMetaV2.NAME_DP_POS],
                instance_id=instance_id
            )

    def _patchStationMeta(self, data,instance,pos_meta,name_dp,number):
        for idx in range(len(data)):
            sleep(0.05)
            if data[idx]=="" or data[idx]==None: continue
            meta = self.labled_nao.getInstanceMetaByPosInstance(instance[AqotecMetaV2.NAME__ID],pos_meta[idx],name_dp[idx])
            if meta==[]: 
                instance_infos = self.nao.getInstanceInfos(instance[AqotecMetaV2.NAME__ID])
                self._saveInitialMetaData(instance_infos[AqotecMetaV2.NAME_META_VALUES], instance[AqotecMetaV2.NAME__ID],name_dp[idx],number)
                meta = self.labled_nao.getInstanceMetaByPosInstance(instance[AqotecMetaV2.NAME__ID],pos_meta[idx],name_dp[idx])
            if meta[0][AqotecMetaV2.NAME_TYPE] == AqotecMetaV2.NAME_NUMBER: dat = float(data[idx])
            elif meta[0][AqotecMetaV2.NAME_TYPE] == AqotecMetaV2.NAME_INTEGER: dat = int(data[idx])
            else: dat = str(data[idx])
            if meta[0][AqotecMetaV2.NAME_VALUE]!=dat:
                self.nao.patchInstanceMeta(meta[0][AqotecMetaV2.NAME_DB_INSTANCE_ID],meta[0][AqotecMetaV2.NAME_ID],dat)
                self.labled_nao.patchInstanceMetaValueByPosInstance(instance[AqotecMetaV2.NAME__ID],pos_meta[idx],name_dp[idx], dat)
                print("patch meta")

    def patchSyncStatus(self):
        data = self.labled_points.getNoSincPoints()
        for database in data:
            for dat in data[database]:
                self.sync_status.postUnsyncroniziedValue(
                    database=database, 
                    table_db=dat[AqotecMetaV2.NAME_TABLE][:-2], 
                    value=[{dat[AqotecMetaV2.NAME_DP]:dat[AqotecMetaV2.NAME_SENSOR_ID]}], 
                    asset_id=dat[AqotecMetaV2.NAME_DB_ASSET_ID], 
                    instance_id=dat[AqotecMetaV2.NAME_DB_INSTANCE_ID])
                self.labled_points.patchPointToSinc(dat[AqotecMetaV2.NAME_TABLE], dat[AqotecMetaV2.NAME_DP])
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

class AqotecTransferV2(AqotecConnectorV2):

    def __init__(self,host,port,user,password,SyncStatus:SyncronizationStatus,NaoApp:NaoApp,driver="{ODBC Driver 18 for SQL Server}") -> None:
        super().__init__(host, port, user, password, driver)
        self.sync_status = SyncStatus
        self.status = self.getSyncStatus()
        self.nao = NaoApp
        self.new_status = {}

    def startSyncronization(self, logfile=None):
        count = 0
        is_sinct = False
        while 1==1:
            try:
                if datetime.now().hour >= 23:
                    break
                start_time = time()
                data_telegraf = self.getTelegrafData()
                if len(data_telegraf)>0:ret=self.nao.sendTelegrafData(data_telegraf)
                else:ret=AqotecTransferV2.STATUS_CODE_GOOD
                if ret==AqotecTransferV2.STATUS_CODE_GOOD:
                    print(len(data_telegraf), " data posted; sec:",time()-start_time, datetime.now())
                    start_time = time()
                    count+=len(data_telegraf)
                    self.setSyncStatus()
                    self.status=self.getSyncStatus()
                    if is_sinct:
                        sleep(AqotecTransferV2.DEFAULT_TRASFER_SLEEPER_SECOND)
                    elif len(data_telegraf)<1:
                        is_sinct = True
                        sleep(AqotecTransferV2.DEFAULT_TRASFER_SLEEPER_SECOND)
                else:sleep(AqotecTransferV2.DEFAULT_ERROR_SLEEP_SECOND)
            except:
                if logfile: logfile(str(sys.exc_info()))
                sleep(AqotecTransferV2.DEFAULT_ERROR_SLEEP_SECOND)
        if logfile:logfile(str(count)+" data sended")

    def setSyncStatus(self):
        for database in self.new_status:
            for table_db in self.new_status[database]:
                if self.new_status[database][table_db].get(AqotecTransferV2.NAME_TIME_UNSYCRONICIZIED):
                    self.sync_status.patchSincStatus(database,table_db,self.new_status[database][table_db][AqotecTransferV2.NAME_TIME_UNSYCRONICIZIED],True)
                if self.new_status[database][table_db].get(AqotecTransferV2.NAME_TIME_SYNCRONICZIED):
                    self.sync_status.patchSincStatus(database,table_db,self.new_status[database][table_db][AqotecTransferV2.NAME_TIME_SYNCRONICZIED],False)
        self.new_status = {}

    def getSyncStatus(self):
        status = self.sync_status.getSyncStatusAll()
        reset = False
        for database in status:
            for table_dic in status[database]:
                if table_dic[AqotecTransferV2.NAME_SYNCRONICZIED]!=[] and table_dic[AqotecTransferV2.NAME_UNSYCRONICIZIED]!=[]:
                    if table_dic[AqotecTransferV2.NAME_TIME_SYNCRONICZIED]==None and table_dic[AqotecTransferV2.NAME_TIME_UNSYCRONICIZIED]==None:
                        reset=True
                        self.sync_status.postSyncroniziedValue(
                            database=database,
                            table_db=table_dic[AqotecTransferV2.NAME_TABLE],
                            value=table_dic[AqotecTransferV2.NAME_UNSYCRONICIZIED],
                            timestamp=table_dic[AqotecTransferV2.NAME_TIME_UNSYCRONICIZIED]
                        )
                        self.sync_status.dropUnSincDps(database=database,table_dp=table_dic[AqotecTransferV2.NAME_TABLE])
                if table_dic[AqotecTransferV2.NAME_SYNCRONICZIED]==[] and table_dic[AqotecTransferV2.NAME_UNSYCRONICIZIED]!=[]:
                    reset=True
                    self.sync_status.postSyncroniziedValue(
                        database=database,
                         table_db=table_dic[AqotecTransferV2.NAME_TABLE],
                        value=table_dic[AqotecTransferV2.NAME_UNSYCRONICIZIED],
                        timestamp=table_dic[AqotecTransferV2.NAME_TIME_UNSYCRONICIZIED]
                    )
                    self.sync_status.dropUnSincDps(database=database,table_dp=table_dic[AqotecTransferV2.NAME_TABLE])
                elif table_dic.get(AqotecTransferV2.NAME_TIME_UNSYCRONICIZIED):
                     if datetime.fromisoformat(table_dic[AqotecTransferV2.NAME_TIME_SYNCRONICZIED])<=datetime.fromisoformat(table_dic[AqotecTransferV2.NAME_TIME_UNSYCRONICIZIED]):
                        self.sync_status.postSyncroniziedValue(
                            database=database,
                            table_db=table_dic[AqotecTransferV2.NAME_TABLE],
                            value=table_dic[AqotecTransferV2.NAME_UNSYCRONICIZIED],
                            timestamp=table_dic[AqotecTransferV2.NAME_TIME_SYNCRONICZIED]
                        )
                        self.sync_status.dropUnSincDps(database=database,table_dp=table_dic[AqotecTransferV2.NAME_TABLE])
                        reset=True
                if table_dic[AqotecTransferV2.NAME_UNSYCRONICIZIED]==[] and table_dic[AqotecTransferV2.NAME_TIME_UNSYCRONICIZIED] != None:
                    self.sync_status.dropUnSincDps(database=database,table_dp=table_dic[AqotecTransferV2.NAME_TABLE])
                    reset=True              
        if reset: status=self.sync_status.getSyncStatusAll()
        return(status)

    def getTelegrafData(self):
        self.connectToDb()
        cursor = self.conn.cursor()
        breaker = False
        telegraf = []
        ext_telegraf = telegraf.extend
        for database in self.status:
            for status_instance in self.status[database]:
                if status_instance["time_sincronizied"]!=None:
                    if "WMZ" in status_instance[AqotecTransferV2.NAME_TABLE]:
                        if  pytz.timezone(AqotecTransferV2.DEFAULT_AQOTEC_TIMEZONE).localize(datetime.fromisoformat(status_instance["time_sincronizied"])).astimezone(pytz.utc).replace(tzinfo=None) > datetime.utcnow()-timedelta(hours=26):
                            continue
                    else:
                        if  pytz.timezone(AqotecTransferV2.DEFAULT_AQOTEC_TIMEZONE).localize(datetime.fromisoformat(status_instance["time_sincronizied"])).astimezone(pytz.utc).replace(tzinfo=None) > datetime.utcnow()-timedelta(minutes=120):
                            continue
                ext_telegraf(self._getTelegrafDataInstance(status_instance,database, cursor))
                if len(telegraf)>=AqotecTransferV2.DEFAULT_BREAK_TELEGRAF_LEN:
                    breaker=True
                    break
            if breaker:
                break
        cursor.close()
        self.disconnetToDb()
        return(telegraf)
    
    def _getTelegrafDataInstance(self,status_instance:dict,database:str,cursor:pyodbc.Cursor):
        if len(status_instance[AqotecTransferV2.NAME_UNSYCRONICIZIED])!=0:
            return(self._getTelegrafDataInstance2(status_instance,database,cursor,AqotecTransferV2.NAME_TIME_UNSYCRONICIZIED,AqotecTransferV2.NAME_UNSYCRONICIZIED))
        else:
            return(self._getTelegrafDataInstance2(status_instance,database,cursor,AqotecTransferV2.NAME_TIME_SYNCRONICZIED,AqotecTransferV2.NAME_SYNCRONICZIED))
        
    def _getTelegrafDataInstance2(self,status_instance:dict,database:str,cursor:pyodbc.Cursor, name_time:str, name_sync:str):
        if status_instance[name_time]: start_time=datetime.fromisoformat(status_instance[name_time])
        else: 
            try:start_time=self._getFirstStartTime(database,status_instance[AqotecTransferV2.NAME_TABLE],cursor)
            except:return([])
        if name_time==AqotecTransferV2.NAME_TIME_UNSYCRONICIZIED:
            stop_time=self._getStopTime(start_time,datetime.fromisoformat(status_instance[AqotecTransferV2.NAME_TIME_SYNCRONICZIED])+timedelta(hours=1))
        else:stop_time = self._getStopTime(start_time,None)
        try:database_to_use=self._ifArchiveDatabase(database,start_time,status_instance[AqotecTransferV2.NAME_TABLE],cursor)
        except:return([])
        timeseries=self._getTimeseries(
            database=database_to_use,
            table=status_instance[AqotecTransferV2.NAME_TABLE],
            data_points=[dp for item in status_instance[name_sync] for dp in item.keys()],
            start_time=start_time,
            stop_time=stop_time,
            cursor=cursor
        )
        if timeseries==-1:
            database_to_use=database
            timeseries=[]
        if len(timeseries)==0:
            timeseries=self._getTimeseriesMeasureGaps(
                database=database_to_use,
                table=status_instance[AqotecTransferV2.NAME_TABLE],
                data_points=[dp for item in status_instance[name_sync] for dp in item.keys()],
                start_time=start_time,
                stop_time=stop_time,
                cursor=cursor
            )
        if len(timeseries)==0:return([])
        if database not in self.new_status:self.new_status[database]={}
        if status_instance[AqotecTransferV2.NAME_TABLE] not in self.new_status[database]:self.new_status[database][status_instance[AqotecTransferV2.NAME_TABLE]]={}
        self.new_status[database][status_instance[AqotecTransferV2.NAME_TABLE]][name_time]=timeseries[0][-1]
        return(self._formatTimeseriesToTelegrafFrame(
            timeseries=timeseries,
            sensor_ids=[id for item in status_instance[name_sync] for id in item.values()],
            instance_id=status_instance[AqotecTransferV2.NAME_DB_INSTANCE_ID],
            asset_id=status_instance[AqotecTransferV2.NAME_DB_ASSET_ID]
        ))

    def _ifArchiveDatabase(self, database, start_time, table, cursor):
        if start_time >= datetime.now()-AqotecTransferV2.DEFAULT_CHECK_ARCHIVE_TIMERANGE:return(database)
        last_time = self._getLastTime(database+AqotecTransferV2.NAME_ENDING_ARCHIV,table,cursor)
        if not last_time: return(database)
        if last_time<=start_time:return(database)
        else: return(database+AqotecTransferV2.NAME_ENDING_ARCHIV)

    def _getTimeseries(self,database:str,table:str,data_points:list,start_time:datetime,stop_time:datetime,cursor:pyodbc.Cursor):
        try:cursor.execute(AqotecTransferV2.QUREY_USE%(database))
        except:return([])
        try:
            cursor.execute(AqotecConnectorV2.QUERY_TIMESERIES%(
                self._getDataPointStrForQuery(data_points),
                table,
                self._getDatetimeToSqlStrTuble(start_time),
                self._getDatetimeToSqlStrTuble(stop_time)
            ))
            data = cursor.fetchall()
        except Exception as e:
            if AqotecConnectorV2.ERROR_HANDLING_START_DP in str(e):return(-1)
            else:return([])
        return(data)
    
    def _getTimeseriesMeasureGaps(self,database:str,table:str,data_points:list,start_time:datetime,stop_time:datetime,cursor:pyodbc.Cursor):
        last_time = self._getLastTime(database=database,table=table,cursor=cursor)
        if last_time == None: return([])
        if last_time <= start_time:return([])
        while 1==1:
            start_time=start_time+AqotecTransferV2.DEFAULT_MAX_TIMERANGE
            stop_time=self._getStopTime(start_time=start_time,max_time=last_time)
            if stop_time==last_time:return([])
            timeseries=self._getTimeseries(database,table,data_points,start_time,stop_time,cursor)
            if len(timeseries)>0:break
        return(timeseries)

    def _getStopTime(self,start_time:datetime,max_time:datetime=None) -> datetime:
        if max_time==None: max_time=datetime.now()
        if start_time+AqotecTransferV2.DEFAULT_MAX_TIMERANGE>max_time:return(max_time)
        else:return(start_time+AqotecTransferV2.DEFAULT_MAX_TIMERANGE)

    def _getDataPointStrForQuery(self, datapoints:list) -> str:
        return(",".join(datapoints))
    
    def _getDatetimeToSqlStrTuble(self, time:datetime):
        return(str((time.year,time.month,time.day,time.hour,time.minute,time.second,0,0)))
    
    def _getFirstStartTime(self,database,table,cursor:pyodbc.Cursor):
        try:
            time = self._getFirstTime(database+AqotecTransferV2.NAME_ENDING_ARCHIV,table,cursor)
        except:
            time = self._getFirstTime(database,table,cursor)
        if not time: return(self._getFirstTime(database,table,cursor))
        return(time)

    def _getFirstTime(self,database,table,cursor:pyodbc.Cursor):
        cursor.execute(AqotecTransferV2.QUREY_USE%(database))
        cursor.execute(AqotecTransferV2.QUERY_SELECT_FIRST_TIME%(table))
        data = cursor.fetchall()
        if len(data)==0:return(None)
        return(data[0][1])

    def _getLastTime(self,database,table,cursor:pyodbc.Cursor):
        try:
            cursor.execute(AqotecTransferV2.QUREY_USE%(database))
            cursor.execute(AqotecTransferV2.QUERY_SELECT_LAST_TIME%(table))
            data = cursor.fetchall()
        except:
            return(None)
        if len(data)==0:return(None)
        return(data[0][1])
    
    def _formatTimeseriesToTelegrafFrame(self, timeseries, sensor_ids, instance_id, asset_id):
        telegraf_list = []
        add_telegraf = telegraf_list.append
        for row in timeseries:
            timestamp = str(int(pytz.timezone(AqotecTransferV2.DEFAULT_AQOTEC_TIMEZONE).localize(row[-1]).astimezone(pytz.utc).replace(tzinfo=None).timestamp()*1e9))
            for idx in range(len(row[:-1])):
                if row[idx]==None:continue
                add_telegraf(f"{asset_id},instance={instance_id} {sensor_ids[idx]}={row[idx]} {timestamp}")
        return(telegraf_list)
        
