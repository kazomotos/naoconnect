from tinydb import TinyDB, Query, operations
from os import path
from datetime import datetime
from typing import Union
from time import sleep
from datetime import datetime

class Par():
    NAME_DRIVER_UG07 = "driver_ug07"
    NAME_DRIVER_RM360 = "driver_rm360"
    NAME_DRIVER_STATION_WMZ = "driver_wmz_station"
    NAME_DRIVER_ASSET_META = "driver_asset_meta"
    NAME_DRIVER_ASSET_LAST_VALUE_META = "driver_asset_last_value_as_meta"
    NAME_DP_NAME = "name_dp"
    NAME_DP = "dp"
    NAME__ID = "_id"
    NAME_TABLE = "table"
    NAME_TABLE_WORKSPACE = "workspace"
    NAME_SENSOR_ID = "sensor_id"
    NAME_VALUE = "value"
    NAME_META_ID = "_attribute"
    NAME_DB_META_ID = "_attribute"
    NAME_INSTANCE_ID = "_instance"
    NAME_DB_INSTANCE_ID = "instance_id"
    NAME_TYPE = "type"
    NAME_TABLE_INSTANCE = "instance"
    NAME_TABLE_META_INSTANCE = "instance_meta"
    NAME_TABLE_ASSET = "asset"
    NAME_ACTIVE = "active"
    NAME_DATABASE = "database"
    NAME_DB_ASSET_ID = "asset_id"
    NAME_DP_POS = "dp_pos"
    NAME_ID = "id"
    NAME_NAME = "name"
    NAME_ASSET_ID = "_asset"
    NAME_SINC = "sinc"
    NAME_WORKSPACE_ID = "_workspace"
    NAME_ORGANIZATION_ID = "_organization"
    NAME__ID = "_id"
    NAME_SYNCRONICZIED = "syncronizied"
    NAME_UNSYCRONICIZIED = "unsyncronizied"
    NAME_TIME_SYNCRONICZIED = "time_sincronizied"
    NAME_TIME_UNSYCRONICIZIED = "time_unsyncronizied"

class Driver(Par):

    def __init__(self, database_name=path.dirname(path.abspath(__file__))+"/driver.json") -> None:
        self.db = TinyDB(database_name)

    def _ceckDriver(self, table_name, name_dp, dp):
        table = self.db.table(table_name)
        res = table.search((Query().name_dp==name_dp)&(Query().dp==dp))
        table.clear_cache()
        if res != []: res = res[0]
        return(res)

    def ceckDriverUG07(self, name_dp, dp):
        return(self._ceckDriver(Driver.NAME_DRIVER_UG07,name_dp,dp))

    def ceckDriverRM360(self, name_dp, dp):
        return(self._ceckDriver(Driver.NAME_DRIVER_RM360,name_dp,dp))      

    def ceckDriverStationWMZ(self, name_dp, dp):
        return(self._ceckDriver(Driver.NAME_DRIVER_STATION_WMZ,name_dp,dp))

    def getAssetMeta(self):
        table = self.db.table(Driver.NAME_DRIVER_ASSET_META)
        ret = table.all()
        table.clear_cache()
        return(ret)
    
    def getAssetLastValueMeta(self):
        table = self.db.table(Driver.NAME_DRIVER_ASSET_LAST_VALUE_META)
        ret = table.all()
        table.clear_cache()
        return(ret)       

    def getAssetMetaFromId(self,_attribute):
        table = self.db.table(Driver.NAME_DRIVER_ASSET_META)
        ret = table.search(Query().meta_id==_attribute)
        table.clear_cache()
        return(ret)

    def getAssetLastValueMetaFromId(self,_attribute):
        table = self.db.table(Driver.NAME_DRIVER_ASSET_LAST_VALUE_META)
        ret = table.search(Query().meta_id==_attribute)
        table.clear_cache()
        return(ret) 

class StationDatapoints(Par):
    
    def __init__(self, database_name=path.dirname(path.abspath(__file__))+"/station_points.json") -> None:
        self.db = TinyDB(database_name)

    def ceckPoints(self, database, table_name, dp):
        table = self.db.table(database)
        res = table.search((Query().dp==dp)&(Query().table==table_name))
        table.clear_cache()
        if res != []:False
        else: return(True)
    
    def getPointByInstanceSensorInstance(self, database, sensor_id, instance_id):
        table = self.db.table(database)
        ret = table.search((Query().sensor_id==sensor_id)&(Query().instance_id==instance_id))
        table.clear_cache()
        return(ret)

    def savePoint(self, database, table_name, name_dp, dp, instance_id ,sensor_id, asset_id ,sinc=False):
        table = self.db.table(database)
        table.insert({
            StationDatapoints.NAME_DB_ASSET_ID:asset_id,
            StationDatapoints.NAME_DP_NAME:name_dp,
            StationDatapoints.NAME_DP:dp,
            StationDatapoints.NAME_TABLE:table_name,
            StationDatapoints.NAME_DB_INSTANCE_ID: instance_id,
            StationDatapoints.NAME_SENSOR_ID:sensor_id,
            StationDatapoints.NAME_SINC:sinc
        })
        table.clear_cache()
    
    def getAll(self):
        ret = {}
        tables = self.db.tables()
        for table in tables: ret[table]=self.db.table(table).all()
        table.clear_cache()
        return(ret)
    
    def getNoSincPoints(self):
        ret = {}
        tables = self.db.tables()
        for table in tables:
            tab = self.db.table(table)
            ret[table]=tab.search(Query().sinc==False)
            tab.clear_cache()
        return(ret)

    def patchAllPointsToSinc(self):
        tables = self.db.tables()
        for table in tables:
            tab = self.db.table(table)
            tab.update({LablingNao.NAME_SINC:True}, Query().sinc==False)
            tab.clear_cache()

    def patchPointToSinc(self, table_db, dp):
        tables = self.db.tables()
        for table in tables:
            tab = self.db.table(table)
            tab.update({LablingNao.NAME_SINC:True}, (Query().table==table_db)&(Query().dp==dp))
            tab.clear_cache()

class LablingNao(Par):

    def __init__(self, database_name=path.dirname(path.abspath(__file__))+"/labling_nao.json") -> None:
        self.db = TinyDB(database_name)

    def ceckWorkspace(self, workspace) -> Union[str, None]:
        table = self.db.table(LablingNao.NAME_TABLE_WORKSPACE)
        try:
            res = table.search((Query().name==workspace))
            table.clear_cache()
        except:
            table.clear_cache()
            return(None)
        if len(res)>0:return(res[0][LablingNao.NAME__ID])
        else:return(None) 

    def putWorkspace(self, nao_ret):
        table = self.db.table(LablingNao.NAME_TABLE_WORKSPACE)
        table.insert({
            LablingNao.NAME_NAME:nao_ret[LablingNao.NAME_NAME],
            LablingNao.NAME_ORGANIZATION_ID:nao_ret[LablingNao.NAME_ORGANIZATION_ID],
            LablingNao.NAME__ID:nao_ret[LablingNao.NAME__ID]
        })
        table.clear_cache()

    def ceckInstance(self, instance_name, database) -> Union[str, None]:
        table = self.db.table(LablingNao.NAME_TABLE_INSTANCE)
        try:
            res = table.search((Query().name==instance_name)&(Query().database==database))
            table.clear_cache()
        except:
            table.clear_cache()
            return(None)
        if len(res)>0:return(res[0][LablingNao.NAME__ID])
        else:return(None)

    def putInstance(self, nao_ret, database):
        table = self.db.table(LablingNao.NAME_TABLE_INSTANCE)
        table.insert({
            LablingNao.NAME_NAME:nao_ret[LablingNao.NAME_NAME],
            LablingNao.NAME_ORGANIZATION_ID:nao_ret[LablingNao.NAME_ORGANIZATION_ID],
            LablingNao.NAME__ID:nao_ret[LablingNao.NAME__ID],
            LablingNao.NAME_ASSET_ID:nao_ret[LablingNao.NAME_ASSET_ID],
            LablingNao.NAME_WORKSPACE_ID:nao_ret[LablingNao.NAME_WORKSPACE_ID],
            LablingNao.NAME_DATABASE:database
        })
        table.clear_cache()

    def ceckAsset(self, asset_name) -> Union[str, None]:
        table = self.db.table(LablingNao.NAME_TABLE_ASSET)
        try:
            res = table.search((Query().name==asset_name))
            table.clear_cache()
        except:
            table.clear_cache()
            return(None)
        if len(res)>0:return(res[0][LablingNao.NAME__ID])
        else:return(None)

    def getInstances(self,database=None):
        table = self.db.table(LablingNao.NAME_TABLE_INSTANCE)
        if database: 
            ret = table.search(Query().database==database)
            table.clear_cache()
            return(ret)
        else: 
            ret = table.all()
            table.clear_cache()
            return(ret)
        
    def putMetaInstance(self, meta_id, dp, value, id, asset_id, instance_id, type, dp_pos):
        table = self.db.table(LablingNao.NAME_TABLE_META_INSTANCE)
        table.insert({
            LablingNao.NAME_META_ID:meta_id,
            LablingNao.NAME_DP:dp,
            LablingNao.NAME_VALUE:value,
            LablingNao.NAME_ID:id,
            LablingNao.NAME_DB_ASSET_ID:asset_id,
            LablingNao.NAME_DB_INSTANCE_ID:instance_id,
            LablingNao.NAME_TYPE:type,
            LablingNao.NAME_DP_POS:dp_pos
        })
        table.clear_cache()

    def getInstanceMetaByPosInstance(self, instance_id, dp_pos):
        table = self.db.table(LablingNao.NAME_TABLE_META_INSTANCE)
        ret = table.search((Query().instance_id==instance_id)&(Query().dp_pos==dp_pos))
        table.clear_cache()
        return(ret)
    
    def getInstanceMetaByAttributeInstance(self, instance_id, attribute_id):
        table = self.db.table(LablingNao.NAME_TABLE_META_INSTANCE)
        ret = table.search((Query().instance_id==instance_id)&(Query()._attribute==attribute_id))
        table.clear_cache()
        return(ret)

    def patchInstanceMetaValueByPosInstance(self, instance_id, dp_pos, value):
        table = self.db.table(LablingNao.NAME_TABLE_META_INSTANCE)
        table.update({LablingNao.NAME_VALUE:value}, (Query().instance_id==instance_id)&(Query().dp_pos==dp_pos))
        table.clear_cache()

    def patchInstanceMetaValueByAttributeInstance(self, instance_id, attribute_id, value):
        table = self.db.table(LablingNao.NAME_TABLE_META_INSTANCE)
        table.update({LablingNao.NAME_VALUE:value}, (Query().instance_id==instance_id)&(Query()._attribute==attribute_id))
        table.clear_cache()

class SyncronizationStatus(Par):

    def __init__(self, database_name=path.dirname(path.abspath(__file__))+"/syncronizied_status.json") -> None:
        self.db = TinyDB(database_name)

    def getSyncStatusAll(self):
        tables = self.db.tables()
        ret={}
        for table in tables:
            tabl_q = self.db.table(table)
            ret[table]=tabl_q.all()
            tabl_q.clear_cache()
        return(ret)

    def postUnsyncroniziedValue(self, database, table_db, value, asset_id, instance_id):
        table = self.db.table(database)
        if len(table.search(Query().table==table_db))==0:
            table.insert({
                SyncronizationStatus.NAME_DB_ASSET_ID:asset_id,
                SyncronizationStatus.NAME_DB_INSTANCE_ID:instance_id,
                SyncronizationStatus.NAME_TABLE:table_db,
                SyncronizationStatus.NAME_SYNCRONICZIED:[],
                SyncronizationStatus.NAME_UNSYCRONICIZIED:[],
                SyncronizationStatus.NAME_TIME_SYNCRONICZIED:None,
                SyncronizationStatus.NAME_TIME_UNSYCRONICIZIED:None,
                SyncronizationStatus.NAME_ACTIVE:True
            })
        table.update(operations.add(SyncronizationStatus.NAME_UNSYCRONICIZIED,value),Query().table==table_db)
        table.clear_cache()

    def postSyncroniziedValue(self, database, table_db, value, timestamp:datetime=None):
        table = self.db.table(database)
        table.update(operations.add(SyncronizationStatus.NAME_SYNCRONICZIED,value),Query().table==table_db)
        if timestamp: table.update({SyncronizationStatus.NAME_TIME_SYNCRONICZIED:str(timestamp)},Query().table==table_db)
        table.clear_cache()

    def patchSincStatus(self,database,table_db,timestamp:datetime,isunsinc=False):
        table = self.db.table(database)
        if isunsinc:time_col=SyncronizationStatus.NAME_TIME_UNSYCRONICIZIED
        else:time_col=SyncronizationStatus.NAME_TIME_SYNCRONICZIED
        table.update({time_col:str(timestamp)},Query().table==table_db)
        table.clear_cache()
        sleep(0.02)
    
    def dropUnSincDps(self,database,table_dp):
        table = self.db.table(database)
        table.update({
            SyncronizationStatus.NAME_TIME_UNSYCRONICIZIED:None,
            SyncronizationStatus.NAME_UNSYCRONICIZIED:[]
        }, Query().table==table_dp)
        table.clear_cache()
