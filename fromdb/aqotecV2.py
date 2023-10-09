'Autor: Rupert Wieser -- Naotilus -- 20232209'
import pyodbc
from naoconnect.local_db import Driver, StationDatapoints, LablingNao, SyncronizationStatus
from pandas import Series
from datetime import datetime, timedelta
from naoconnect.naoappV2 import NaoApp
from time import sleep, time
import sys
import pytz

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
    QUERY_SELECT_FIRST_TIME = "SELECT TOP 1 * FROM %s ORDER BY DP_Zeitstempel ASC"
    QUERY_SELECT_LAST_TIME = "SELECT TOP 1 * FROM %s ORDER BY DP_Zeitstempel DESC"
    QUERY_TIMESERIES = "SELECT %s, DP_Zeitstempel FROM %s WHERE DP_Zeitstempel > DATETIME2FROMPARTS%s AND DP_Zeitstempel < DATETIME2FROMPARTS%s ORDER BY DP_Zeitstempel DESC"
    QUERY_META_CUSTOMER_SELECT = "SELECT %s FROM Tbl_Abnehmer WHERE AnID = %s"
    NAME_ENDING_TABLE_ROW_META = "_b"
    NAME_STARTING_TABLE_UG07 = "UG07_"
    NAME_STARTING_TABLE_RM360 = "RM360_"
    NAME_SENSOR_ID = "sensor_id"
    NAME_STARTING_TABLE_WMZ = "WMZ_"
    NAME_DB_INSTANCE_ID = "instance_id"
    NAME_DATABASE_END_CUSTOMER = "_Kunden"
    NAME_DATABASE_END_DATA = "_Daten"
    NAME_TABLE_CUSTOMER = "Tbl_Abnehmer"
    NAME_META_ID = "_attribute"
    NAME_DP_POS = "dp_pos"
    NAME_META_VALUES = "attributevalues"
    NAME_ASSET_ID = "_asset"
    NAME_ASSET_UG07 = "ug07"
    NAME_ASSET_RM360 = "rm360"
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
    NAME_TIME_SYNCRONICZIED = "time_sincronizied"
    NAME_TIME_UNSYCRONICIZIED = "time_unsyncronizied"
    NAME_DP_NAME = "name_dp"
    NAME_NUMBER = "number"
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
            sleep(300)
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
            if AqotecMetaV2.NAME_STARTING_TABLE_WMZ in table:
                self.struct.putWmz(database,table)
            if AqotecMetaV2.NAME_STARTING_TABLE_RM360 in table:
                self.struct.putRm360(database,table)

    def checkStationDatapoints(self):
        self._ceckUg07()
        self._ceckRm360()
        self._ceckStationWmz()
                 
    def _ceckUg07(self):
        self._ceckTable(self.struct.ug07, self.driver_db.ceckDriverUG07, AqotecMetaV2.NAME_ASSET_UG07, True)

    def _ceckRm360(self):
        self._ceckTable(self.struct.rm360, self.driver_db.ceckDriverRM360, AqotecMetaV2.NAME_ASSET_RM360, True)

    def _ceckStationWmz(self):
        self._ceckTable(self.struct.wmz, self.driver_db.ceckDriverStationWMZ, AqotecMetaV2.NAME_ASSET_WMZ, False)

    def _ceckTable(self, table_struct, ceckDriver, asset_name, create_instance=True):
        self.connectToDb()
        cursor = self.conn.cursor()
        asset_id = self.labled_nao.ceckAsset(asset_name=asset_name)
        if not asset_id:return(-1)
        for database in table_struct:
            if len(database.split("_"))<3:continue
            try:cursor.execute(AqotecMetaV2.QUREY_USE%(database))
            except:continue
            workspace_id = None
            for table in table_struct[database]:
                instance_id = self.labled_nao.ceckInstance(table.split("_")[-2],database=database)
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
                        instance_name = table.split("_")[-2]
                        ret=self.nao.createInstance(
                            name=instance_name,
                            asset_id=asset_id,
                            description=table,
                            workspace_id=workspace_id
                        )
                        instance_id=ret[AqotecMetaV2.NAME__ID]
                        self.labled_nao.putInstance(ret,database)
                        self._saveInitialMetaData(ret[AqotecMetaV2.NAME_META_VALUES],ret[AqotecMetaV2.NAME__ID])
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

    def _ceckDataInPoint(self,table,dp,cursor:pyodbc.Cursor,lt,gt,b1,b2):
        cursor.execute(AqotecMetaV2.QUERY_DATA_POINT_CECK%(dp,table))
        series = Series([row[0] for row in cursor.fetchall()])
        if len(series)==0:
            return(False)
        data_bool=Series([True]*len(series))
        if lt: data_bool=series.lt(lt)&data_bool
        if gt: data_bool=series.gt(gt)&data_bool
        if b1: data_bool=(series.lt(b1)|series.gt(b2))&data_bool
        return(data_bool.any())
    
    def patchStationMeta(self):
        self.connectToDb()
        cursor = self.conn.cursor()
        asset_meta, pos_dp = self._getAssetMetaQuery()
        instances = self.labled_nao.getInstances()
        name_db = ""
        for instance in instances:
            try: 
                if instance[AqotecMetaV2.NAME_DATABASE]!=name_db:cursor.execute(AqotecMetaV2.QUREY_USE%(
                    instance[AqotecMetaV2.NAME_DATABASE].split(AqotecMetaV2.NAME_DATABASE_END_DATA)[0]+AqotecMetaV2.NAME_DATABASE_END_CUSTOMER
                ))
            except: continue
            cursor.execute(AqotecMetaV2.QUERY_META_CUSTOMER_SELECT%(asset_meta[instance[AqotecMetaV2.NAME_ASSET_ID]][:-1],instance[AqotecMetaV2.NAME_NAME].split("R")[-1]))
            data = cursor.fetchall()
            if len(data)>0:self._patchStationMeta(data[0],instance, pos_dp)
        cursor.close()
        self.disconnetToDb()

    def _getAssetMetaQuery(self):
        asset_meta = self.driver_db.getAssetMeta()
        ret = {}
        ret2 = []
        for set in asset_meta:
            if set[AqotecMetaV2.NAME_DB_ASSET_ID] not in ret: ret[set[AqotecMetaV2.NAME_DB_ASSET_ID]]=""
            ret[set[AqotecMetaV2.NAME_DB_ASSET_ID]] += set[AqotecMetaV2.NAME_DP]+","
            ret2.append(set[AqotecMetaV2.NAME_DP_POS])
        return(ret, ret2)

    def _saveInitialMetaData(self, attributevalues, instance_id):
        for value in attributevalues:
            meta_driver = self.driver_db.getAssetMetaFromId(value[AqotecMetaV2.NAME_META_ID])[0]
            self.labled_nao.putMetaInstance(
                value=value[AqotecMetaV2.NAME_VALUE],
                meta_id=value[AqotecMetaV2.NAME_META_ID],
                dp=meta_driver[AqotecMetaV2.NAME_DP],
                id=value[AqotecMetaV2.NAME__ID],
                asset_id=meta_driver[AqotecMetaV2.NAME_DB_ASSET_ID],
                type=meta_driver[AqotecMetaV2.NAME_TYPE],
                dp_pos=meta_driver[AqotecMetaV2.NAME_DP_POS],
                instance_id=instance_id
            )
    
    def _patchStationMeta(self, data,instance,pos_meta):
        for idx in range(len(data)):
            sleep(0.05)
            if data[idx]=="" or data[idx]==None: continue
            meta = self.labled_nao.getInstanceMetaByPosInstance(instance[AqotecMetaV2.NAME__ID],pos_meta[idx])
            if meta[0][AqotecMetaV2.NAME_TYPE] == AqotecMetaV2.NAME_NUMBER: dat = float(data[idx])
            else: dat = str(data[idx])
            if meta[0][AqotecMetaV2.NAME_VALUE]!=dat:
                self.nao.patchInstanceMeta(meta[0][AqotecMetaV2.NAME_DB_INSTANCE_ID],meta[0][AqotecMetaV2.NAME_ID],dat)
                self.labled_nao.patchInstanceMetaValueByPosInstance(instance[AqotecMetaV2.NAME__ID],idx+1, dat)
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
            if datetime.now().hour > 23:
                break
        if logfile:logfile(str(count)+" data sended")

    def setSyncStatus(self):
        for database in self.new_status:
            for table_db in self.new_status[database]:
                if self.new_status[database][table_db].get(AqotecTransferV2.NAME_TIME_UNSYCRONICIZIED):
                    self.sync_status.patchSincStatus(database,table_db,self.new_status[database][table_db][AqotecTransferV2.NAME_TIME_UNSYCRONICIZIED],True)
                if self.new_status[database][table_db].get(AqotecTransferV2.NAME_TIME_SYNCRONICZIED):
                    self.sync_status.patchSincStatus(database,table_db,self.new_status[database][table_db][AqotecTransferV2.NAME_TIME_SYNCRONICZIED],False)

    def getSyncStatus(self):
        status = self.sync_status.getSyncStatusAll()
        reset = False
        for database in status:
            for table_dic in status[database]:
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
                    if table_dic[AqotecTransferV2.NAME_TIME_SYNCRONICZIED]<table_dic[AqotecTransferV2.NAME_TIME_UNSYCRONICIZIED]:
                        self.sync_status.postSyncroniziedValue(
                            database=database,
                            table_db=table_dic[AqotecTransferV2.NAME_TABLE],
                            value=table_dic[AqotecTransferV2.NAME_UNSYCRONICIZIED],
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
        time = self._getFirstTime(database+AqotecTransferV2.NAME_ENDING_ARCHIV,table,cursor)
        if not time: return(self._getFirstTime(database,table,cursor))
        return(time)

    def _getFirstTime(self,database,table,cursor:pyodbc.Cursor):
        cursor.execute(AqotecTransferV2.QUREY_USE%(database))
        cursor.execute(AqotecTransferV2.QUERY_SELECT_FIRST_TIME%(table))
        data = cursor.fetchall()
        if len(data)==0:return(None)
        return(data[0][1])

    def _getLastTime(self,database,table,cursor:pyodbc.Cursor):
        cursor.execute(AqotecTransferV2.QUREY_USE%(database))
        cursor.execute(AqotecTransferV2.QUERY_SELECT_LAST_TIME%(table))
        data = cursor.fetchall()
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
        
