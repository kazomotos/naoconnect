import json
import pandas as pd
import pyodbc
from time import sleep
from naoconnect.naoappV2 import NaoApp
from typing import Union
import ftfy
import numpy as np
from math import ceil
from decimal import Decimal
from datetime import datetime, timedelta, timezone
import re

class DesigoCC():
    SQL_LOGGING_DATAPOINTS = "SELECT [HDB_DpIdTs],[DpeName],[Sd],[SlLang0],[SlLang1] FROM [HDB].[dbo].[HDB_DpIdsTs];"
    SQL_SHOW_DATABASES = "SELECT name FROM sys.databases;"
    SQL_CHECK_DATA_IN_DATABASE = "SELECT TOP (3) [HDB_DpIdTs] FROM [%s].[dbo].[TiSe];"
    SQL_SELECT_VALUE_FROM_DP = "SELECT [SourceTime], [Value] FROM [%s].[dbo].[TiSe] WHERE [HDB_DpIdTs]=%s AND [SourceTime] > '%s' ORDER BY [SourceTime] ASC"
    SQL_COUNT_DATAPOINT_VALUES_FROM_DB = "SELECT  COUNT(Value) AS count, [HDB_DpIdTs] FROM [%s].[dbo].[TiSe] GROUP BY [HDB_DpIdTs];"
    DATABASE_TIMESERIES_REGEX = r"HDB_S"
    MESSAGE_WARNING_WRONG_LABLING_POINT = 'WARNUNG: Ein Datenpunkt ist in der Desigo Oberfläche einer falschen Station zugeordnet:\nDer Datenpunkt "%s"\nist statt dem Asset(Name) "%s", dem Asset(Name) "%s" zugeordent.\nDieser Datenpunkt wird in NAO automatisch der richtigen Station zugewiesen.'
    MESSAGE_INFO_ASSET_CREATED = 'INFO: In NAO wurde automatisch eine neues Asset für %s mit dem Namen "%s" (ID:%s) angelegt.'
    MESSAGE_INFO_ACTIVATED_POINTS = 'INFO: In NAO wurden für das Asset "%s" %s Datenpunkte aktiviert.'
    NAME_NAME = "name"
    NAME_SHORT_NAME = "short_name"
    NAME_GROUP = "group"
    NAME_ASSET_ID = "asset_id"
    NAME_SENSOR_ID = "sensor_id"
    NAME_REGEX = "regex"
    NAME_REGEX_NOT = "regex_not"
    NAME_DESCRIPTION = "description"
    NAME_FACTOR = "factor"
    NAME_META_ID = "meta_id"
    NAME_WORKSPACE = "workspace"
    NAME_ASSET = "asset"
    NAME_INSTANCE = "instance"
    NAME_INSTANCE_ID = "instance_id"
    NAME_INSTANCE_META = "instance_meta"
    NAME_ACTIVATED_DATAPOINTS = "activated_datapoint"
    NAME_SENSORS = "sensors"
    NAME_TP_ID_TS = "tp_id_ts"
    NAME_META1 = "meta_from_strig"
    NAME_TYPE = "type"
    NAME_VALUE = "value"
    NAO_ID_ID = "_id"
    NAO_ORGANIZATION_ID = "_organization"
    NAO_WORKSPACE_ID = "_workspace"
    NAO_ASSED_ID = "_asset"
    NAO_ATTRIBUTE_ID = "_attribute"
    NAO_ATTRIBUTEVALUES = "attributevalues"
    NAME_CHECK_MAX = "check_max"
    COLUMN_SD = "Sd"
    COLUMN_SL_LANG_0 = "SlLang0"
    COLUMN_SL_LANG_1 = "SlLang1"
    COLUMN_DPE_NAME = "DpeName"
    COLUMN_DEVICE = "device"
    COLUMN_WORKSPACE = "workspace"
    COLUMN_COUNT = "count"
    COLUMN_HDB_DP_ID_TS = "HDB_DpIdTs"
    COLUMN_VALUE = "Value"
    COLUMN_SOURCE_TIME = "SourceTime"
    META_ID1_SPLIT_1 = ".HAST."
    META_ID1_SPLIT_2 = ".Hausstationen."
    META_TYPE_STR = "string"
    META_TYPE_FLOAT = "float"
    META_TYPE_INT = "integer"
    REGEX_DRIVER = r"GmsDevice"
    REGEX_LOGICAL_VIEW = r"LogicalView:Logical"
    NOTE_CREATED = "created"
    NOTE_NOTE = "note"
    NOTE_VISIB = "visibility"
    NOTE_INSTANCE = "_instance"
    NOTE_START = "start"
    NOTE_STOP = "stop"
    NOTE_USER = "_user"
    TIMEDELTA_LAST_MIN_SINC = timedelta(minutes=2)
    TIMEDELTA_LAST_DB = timedelta(days=0.5)
    TIMEDELTA_SECOND_DB = timedelta(days=30)
    TELEGRAF_PUSH_LEN = 15000
    TELEGRAF_STATUS_CODE_GOOD = 204

    def __init__(self,NaoAppInstance:NaoApp,nao_driver_file:str,nao_labling_file:str,nao_sync_status_file:str,sql_host:str,sql_user:str,sql_password:str,sql_port:str="1433",sql_driver:str="{ODBC Driver 18 for SQL Server}",regex_test_mode=False,first_sync_time:datetime=datetime(2018,1,1,0,0,0)) -> None:
        self.first_sync_time = first_sync_time
        self.Nao = NaoAppInstance
        self.sql_driver = sql_driver
        self.sql_host = sql_host
        self.sql_user = sql_user
        self.sql_password = sql_password
        self.sql_port = sql_port
        self.nao_sync_status_file = nao_sync_status_file
        self.nao_sync_status_dict = self.getSyncStatus()
        self.nao_labling_file = nao_labling_file
        self.nao_labling_dic = self.getNaoLablingFileDict()
        self.activated_datapoints = self._getActivatedDatapoints()
        self.nao_driver_file = nao_driver_file
        self.nao_driver_dic = self.getNaoDriverFileDict()
        self.nao_driver_df = pd.DataFrame(self.nao_driver_dic[DesigoCC.NAME_SENSORS].values())
        self.validateDriver()
        self.workspace_dic = self._getWorkspaceDict()
        self.device_labeld_dict = self._getLabeldDeviceDict()
        self.sql_connection_str = "DRIVER="+self.sql_driver+";SERVER="+self.sql_host+";PORT="+self.sql_port+";UID="+self.sql_user+";PWD="+self.sql_password+";Encrypt=No"
        self.regex_test_mode = regex_test_mode
        self.user_id_nao = self.Nao.getUserId()
        self._patchSyncStatus()
        self.database_timeseries_list = self.getTimeseriesDatabases()
        self.timeseries_validator = self._getTimeseriesChecks()
        self.sleep_point = []

    def __del__(self):
        self.saveSyncStatus()

    def validateDriver(self) -> None:
        if self.nao_driver_df[DesigoCC.NAME_REGEX].duplicated().sum() > 0: raise("driver duplicate error")
        for group in self.nao_driver_df.groupby(by=DesigoCC.NAME_DESCRIPTION):
            if len(group[1][DesigoCC.NAME_SENSOR_ID][~group[1][DesigoCC.NAME_SENSOR_ID].duplicated()])>1:
                raise("driver wrong driver ids error")

    def writhDriverTable(self, file_path:str):
        group_sensors = self.nao_driver_df.groupby(by="description")
        fi = open(file_path, "w", encoding="utf-8")
        fi.writelines("Sensor;Kürzel;Kürzel-nicht;Umrechungsfaktor\n")
        for goup in group_sensors:
            fi.writelines("-------------------------------------------------------------;------------------------------------------------------------;-----------------------;----------------------\n")
            for idx in range(len(goup[1])):
                regex = goup[1]["regex"].iloc[idx]
                regex_not = goup[1]["regex_not"].iloc[idx]
                factor = goup[1]["factor"].iloc[idx]
                if np.isnan(factor):
                    factor="-"
                else: str(factor).replace(".", ",")
                fi.writelines(goup[0]+';'+str(regex)+";"+str(regex_not)+";"+str(factor)+"\n")
        fi.close()
    
    def connectToMsSql(self):
        try:
            self.conn = pyodbc.connect(self.sql_connection_str)
        except:
            print("connection faild, nex connetction in 300 sec")
            sleep(10)
            self.sql_conn = pyodbc.connect(self.sql_connection_str)

    def disconnetToMsSql(self):
        try:
            self.conn.close()
        except:
            pass

    def getSyncStatus(self) -> dict:
        ret = self._getJsonFileDict(self.nao_sync_status_file,return_empty_dict=True)
        return({int(k): v for k, v in ret.items()})

    def saveSyncStatus(self) -> dict:
        self._saveJsonFileDict(self.nao_sync_status_file, self.nao_sync_status_dict)

    def _patchSyncStatus(self) -> None:
        point_not_in_sync = list(set(self.activated_datapoints.keys())-set(self.nao_sync_status_dict.keys()))
        for point in point_not_in_sync:
            self.nao_sync_status_dict[point] = self.first_sync_time.isoformat()
        if len(point_not_in_sync)>0:
            self.saveSyncStatus() 
    
    def _getActivatedDatapoints(self) -> dict:
        ret = {}
        for point in self.nao_labling_dic[DesigoCC.NAME_ACTIVATED_DATAPOINTS]:
            ret[point[DesigoCC.NAME_TP_ID_TS]] = point
        return(ret)
    
    def getTimeseriesDatabases(self) -> list:
        self.connectToMsSql()
        database_all = pd.read_sql_query(DesigoCC.SQL_SHOW_DATABASES,con=self.conn,)
        database_all = database_all[DesigoCC.NAME_NAME][database_all[DesigoCC.NAME_NAME].str.contains(DesigoCC.DATABASE_TIMESERIES_REGEX, na=False)].sort_values().to_list()
        ret = []
        for database in database_all:
            try:
                check = pd.read_sql_query(DesigoCC.SQL_CHECK_DATA_IN_DATABASE%(database),con=self.conn,)
            except:
                continue
            if len(check)>2: ret.append(database)
        self.disconnetToMsSql()
        return(ret)
    
    def getDatabaseDatapointCounts(self) -> dict:
        ret = {}
        self.connectToMsSql()
        for database in self.database_timeseries_list:
            counts = pd.read_sql_query(DesigoCC.SQL_COUNT_DATAPOINT_VALUES_FROM_DB%(database),con=self.conn,)
            counts = counts[counts[DesigoCC.COLUMN_COUNT]>1]
            if len(counts)>1:
                ret[database] = counts
        self.disconnetToMsSql()
        return(ret)

    def getNoteSchema(self, created=None, start=None, stop=None, visiblity=1, instance_id=None, note=None):
        return({
            DesigoCC.NOTE_CREATED: created,
            DesigoCC.NOTE_USER : self.user_id_nao,
            DesigoCC.NOTE_INSTANCE : instance_id,
            DesigoCC.NOTE_VISIB : visiblity,
            DesigoCC.NOTE_NOTE : note,
            DesigoCC.NOTE_START : start,
            DesigoCC.NOTE_STOP : stop
        })

    def _getWorkspaceDict(self) -> dict:
        workspace_dict = {}
        for work_id in self.nao_labling_dic[DesigoCC.NAME_WORKSPACE]:
            workspace_dict[self.nao_labling_dic[DesigoCC.NAME_WORKSPACE][work_id][DesigoCC.NAME_NAME]] = work_id
        return(workspace_dict)
    
    def _getLabeldDeviceDict(self) -> list:
        frame = pd.DataFrame(self.nao_labling_dic[DesigoCC.NAME_INSTANCE].values())
        if len(frame) == 0: return({})
        device = frame[DesigoCC.COLUMN_DEVICE].to_list()
        instance = frame[DesigoCC.NAO_ID_ID].to_list()
        asset = frame[DesigoCC.NAO_ASSED_ID].to_list()
        ret = {}
        for idx in range(len(device)):
            ret[device[idx]] = {
                DesigoCC.NAME_INSTANCE_ID: instance[idx],
                DesigoCC.NAME_ASSET_ID: asset[idx]
            } 
        return(ret)

    def _createWorkspace(self, workspace_name) -> None:
        ret_nao = self.Nao.createWorkspace(workspace_name)
        work_id = ret_nao[DesigoCC.NAO_ID_ID]
        self.nao_labling_dic[DesigoCC.NAME_WORKSPACE][work_id] = {
            DesigoCC.NAME_NAME: workspace_name,
            DesigoCC.NAO_ID_ID: work_id,
            DesigoCC.NAO_ORGANIZATION_ID: ret_nao[DesigoCC.NAO_ORGANIZATION_ID]
        }
        self.workspace_dic[workspace_name] = work_id
        self.saveNaoLablingFileDict()

    def _setMetaData1(self, ret_nao:dict, meta_infos:dict) -> None:
        for attribute in ret_nao[DesigoCC.NAO_ATTRIBUTEVALUES]:
            if attribute[DesigoCC.NAO_ATTRIBUTE_ID] not in self.nao_driver_dic[DesigoCC.NAME_META1]: continue
            name_value = self.nao_driver_dic[DesigoCC.NAME_META1][attribute[DesigoCC.NAO_ATTRIBUTE_ID]][DesigoCC.NAME_NAME]
            if not meta_infos.get(name_value): continue
            type_meta = self.nao_driver_dic[DesigoCC.NAME_META1][attribute[DesigoCC.NAO_ATTRIBUTE_ID]][DesigoCC.NAME_TYPE]
            value = self._checkMetaType(value=meta_infos[name_value],type=type_meta)
            self.Nao.patchInstanceMeta(instance_id=ret_nao[DesigoCC.NAO_ID_ID], meta_id=attribute[DesigoCC.NAO_ID_ID],value=value)
            attribute[DesigoCC.NAME_VALUE] = value
            self.nao_labling_dic[DesigoCC.NAME_INSTANCE_META].append(attribute)

    def _checkMetaType(self, value, type) -> Union[str,int,float]:
        if type==DesigoCC.META_TYPE_STR:
            return(str(value))
        if type==DesigoCC.META_TYPE_FLOAT:
            return(float(value))
        if type==DesigoCC.META_TYPE_INT:
            return(int(value))
        else:
            raise(ValueError("no right meta type"))

    def _createInstance(self, name:str, description:str, asset_id:str, workspace_id:str, device:str,meta_infos:dict) -> None:
        ret_nao = self.Nao.createInstance(
            name=name,
            description=description,
            asset_id=asset_id,
            workspace_id=workspace_id,
        )
        if "statusCode" in ret_nao:
            raise(ValueError("doppelte Instancebennenung"))
            # ret_nao = self.Nao.createInstance(
            #     name=name + " (2)",
            #     description=description + " (doppelte Benennung)",
            #     asset_id=asset_id,
            #     workspace_id=workspace_id,
            # )
        instance_id = ret_nao[DesigoCC.NAO_ID_ID]
        self.nao_labling_dic[DesigoCC.NAME_INSTANCE][instance_id] = {
            DesigoCC.NAME_NAME: name,
            DesigoCC.NAO_ID_ID: instance_id,
            DesigoCC.NAO_ORGANIZATION_ID: ret_nao[DesigoCC.NAO_ORGANIZATION_ID],
            DesigoCC.NAO_ASSED_ID: asset_id,
            DesigoCC.NAO_WORKSPACE_ID: workspace_id,
            DesigoCC.NAME_DESCRIPTION: description,
            DesigoCC.COLUMN_DEVICE: device
        }
        self.device_labeld_dict[device] = {
            DesigoCC.NAME_INSTANCE_ID: instance_id,
            DesigoCC.NAME_ASSET_ID: asset_id
        }
        self._setMetaData1(ret_nao=ret_nao,meta_infos=meta_infos)
        self.saveNaoLablingFileDict()
        message = DesigoCC.MESSAGE_INFO_ASSET_CREATED%(self.nao_labling_dic[DesigoCC.NAME_ASSET][asset_id][DesigoCC.NAME_NAME], name, instance_id)
        note = self.getNoteSchema(
            created=str(datetime.now()),
            start=str(datetime.now()),
            stop=str(datetime.now()),
            note=message,
            instance_id=instance_id
        )
        self.Nao.pushNote(asset_id=asset_id,data_note=note)

    def saveNaoLablingFileDict(self) -> None:
        self._saveJsonFileDict(self.nao_labling_file, self.nao_labling_dic)

    def _getJsonFileDict(self, file_path_name:str, return_empty_dict:bool=False) -> dict:
        try: 
            fi = open(file_path_name)
            ret = fi.read()
            fi.close()
            return(json.loads(ret))
        except: 
            if return_empty_dict: return({})
            else: raise(ValueError("can't open file"))

    def _saveJsonFileDict(self, file_path_name:str, data:dict) -> None:
        fi = open(file_path_name, "w")
        try:
            fi.write(json.dumps(data))
        except:
            fi.write(str(data))
            raise(ValueError("can't save file"))
        fi.close()
    
    def getNaoLablingFileDict(self) -> dict:
        return(self._getJsonFileDict(self.nao_labling_file))

    def getNaoDriverFileDict(self) -> dict:
        driver_list = self._getJsonFileDict(self.nao_driver_file)
        driver_dict = {
            DesigoCC.NAME_SENSORS:{},
            DesigoCC.NAME_META1:{}
        }
        for row in driver_list[DesigoCC.NAME_SENSORS]: driver_dict[DesigoCC.NAME_SENSORS][row[DesigoCC.NAME_REGEX]] = row
        for row in driver_list[DesigoCC.NAME_META1]: driver_dict[DesigoCC.NAME_META1][row[DesigoCC.NAO_ATTRIBUTE_ID]] = row
        return(driver_dict)

    def _getLoggingDatapointsAll(self) -> pd.DataFrame:
        self.connectToMsSql()
        ret = pd.read_sql_query(DesigoCC.SQL_LOGGING_DATAPOINTS,con=self.conn,)
        self.disconnetToMsSql()
        ret = ret[ret[DesigoCC.COLUMN_SD].str.contains("",na=False)]
        ret = ret[ret[DesigoCC.COLUMN_SD].str.contains(DesigoCC.REGEX_LOGICAL_VIEW, na=False)]
        ret = ret[ret[DesigoCC.COLUMN_DPE_NAME].str.contains(DesigoCC.REGEX_DRIVER, na=False, regex=True)]
        ret[DesigoCC.COLUMN_SD] = ret[DesigoCC.COLUMN_SD].apply(lambda x: ".".join(x.split(".")[2:])).apply(ftfy.fix_text).str.replace("Ãœ", "Ü").str.replace("Ã¼", "ü")
        ret[DesigoCC.COLUMN_SL_LANG_0] = ret[DesigoCC.COLUMN_SL_LANG_0].apply(lambda x: ".".join(x.split(".")[2:])).apply(ftfy.fix_text).str.replace("Ãœ", "Ü").str.replace("Ã¼", "ü")
        ret[DesigoCC.COLUMN_SL_LANG_1] = ret[DesigoCC.COLUMN_SL_LANG_1].apply(lambda x: ".".join(x.split(".")[2:])).apply(ftfy.fix_text).str.replace("Ãœ", "Ü").str.replace("Ã¼", "ü")
        ret[DesigoCC.COLUMN_DEVICE] = ret[DesigoCC.COLUMN_DPE_NAME].apply(lambda x: x.split("_")[2])
        ret[DesigoCC.COLUMN_WORKSPACE] = ret[DesigoCC.COLUMN_SL_LANG_1].apply(lambda x: x.split(".")[0])
        return(ret)

    def getNewLoggingDatapointsWithDriver(self, wiht_driver=True) -> pd.DataFrame:
        ret = self._getLoggingDatapointsAll()
        combined_regex = "|".join(self.nao_driver_df[DesigoCC.NAME_REGEX].to_list())
        if wiht_driver:ret = ret[ret[DesigoCC.COLUMN_SD].str.contains(combined_regex, na=False, regex=True)]
        else:ret = ret[~ret[DesigoCC.COLUMN_SD].str.contains(combined_regex, na=False, regex=True)]
        ret = ret[ret[DesigoCC.COLUMN_SD].str.contains("",na=False)]
        return(ret)
    
    def _checkWorkspace(self, workspace_name:str, create_workspace:bool) -> bool:
        if workspace_name not in self.workspace_dic:
            if not create_workspace: return(False)
            self._createWorkspace(workspace_name)
        return(self.workspace_dic[workspace_name])

    def _checkInstance(self, device:str, data:pd.DataFrame, create_instance:bool, workspace_id:str) -> bool:
        if device not in self.device_labeld_dict:
            if not create_instance: return(False)
            idx, meta = self._metaFromPointStingWithCheck(data=data)
            asset_id = self.nao_driver_dic[DesigoCC.NAME_SENSORS][data.iloc[idx][DesigoCC.NAME_REGEX]][DesigoCC.NAME_ASSET_ID]
            self._createInstance(
                name=meta[DesigoCC.NAME_NAME],
                description=meta[DesigoCC.NAME_NAME]+" - "+self.nao_labling_dic[DesigoCC.NAME_WORKSPACE][workspace_id][DesigoCC.NAME_NAME],
                workspace_id=workspace_id,
                asset_id=asset_id,
                device=device,
                meta_infos=meta
            )
        return(self.device_labeld_dict[device][DesigoCC.NAME_INSTANCE_ID])
    
    def _metaFromPointStingWithCheck(self, data:pd.DataFrame) -> pd.DataFrame:
        meta_list = []
        for idx in range(len(data)):
            meta_list.append(self._metaFromPointSring(data.iloc[idx]))
        meta_df = pd.DataFrame(meta_list)
        if len(set(meta_df[DesigoCC.NAME_NAME]))==1:
            return(0,meta_list[0])
        name_max = meta_df[DesigoCC.NAME_NAME].value_counts().idxmax()
        idx = 0
        for meta in meta_list:
            if meta[DesigoCC.NAME_NAME] == name_max:
                break
            idx+=1
        return(idx,meta_list[idx])
    
    def _metaFromPointSring(self, data_col:pd.DataFrame) -> dict:
        if self.nao_driver_dic[DesigoCC.NAME_SENSORS][data_col["regex"]][DesigoCC.NAME_META_ID] == 1:
            return(self._metaId1FromPointSring(data_col=data_col))
        else:
            raise(ValueError("no meta driver"))

    def _metaId1FromPointSring(self, data_col:pd.DataFrame) -> dict:
        ret = {}
        sd = data_col[DesigoCC.COLUMN_SD]
        lang1 = data_col[DesigoCC.COLUMN_SL_LANG_1]
        if DesigoCC.META_ID1_SPLIT_1 in sd or DesigoCC.META_ID1_SPLIT_2 in sd:
            if DesigoCC.META_ID1_SPLIT_1 in sd:
                split1 = sd.split(DesigoCC.META_ID1_SPLIT_1)
            else:
                split1 = sd.split(DesigoCC.META_ID1_SPLIT_2)
            pos1 = len(split1[0].split("."))
            ret[DesigoCC.NAME_SHORT_NAME] = sd.split(".")[pos1+1]
            ret[DesigoCC.NAME_GROUP] = lang1.split(".")[pos1-1]
            ret[DesigoCC.NAME_NAME] = lang1.split(".")[pos1+1]
            ret[DesigoCC.COLUMN_DEVICE] = data_col[DesigoCC.COLUMN_DEVICE]
        else:
            ret[DesigoCC.NAME_NAME] = lang1.split(".")[1]
            ret[DesigoCC.COLUMN_DEVICE] = data_col[DesigoCC.COLUMN_DEVICE]
        return(ret)

    def _findMatchingRegex(self, test_string:str) -> str:
        for idx in range(len(self.nao_driver_df)):
            regex = self.nao_driver_df[DesigoCC.NAME_REGEX].iloc[idx]
            if re.search(regex, test_string):
                no_regex = self.nao_driver_df[DesigoCC.NAME_REGEX_NOT].iloc[idx]
                wrong = False
                for no_reg in no_regex:
                    if re.search(no_reg,test_string):
                        wrong=True
                if wrong: continue
                else: return(regex)
        if self.regex_test_mode:
            raise(ValueError("no regex"))
        else:
            return(None)
    
    def _activateDatapoint(self, instance_id:str, point_data:pd.Series) -> int:
        dp_id_point = int(point_data.loc[DesigoCC.COLUMN_HDB_DP_ID_TS])
        if dp_id_point in self.activated_datapoints: return(0)
        sensor_type = self.nao_driver_dic[DesigoCC.NAME_SENSORS][point_data.loc[DesigoCC.NAME_REGEX]][DesigoCC.NAME_TYPE]
        sensor_id = self.nao_driver_dic[DesigoCC.NAME_SENSORS][point_data.loc[DesigoCC.NAME_REGEX]][DesigoCC.NAME_SENSOR_ID]
        asset_id = self.nao_driver_dic[DesigoCC.NAME_SENSORS][point_data.loc[DesigoCC.NAME_REGEX]][DesigoCC.NAME_ASSET_ID]
        regex = point_data.loc[DesigoCC.NAME_REGEX]
        instance_name_labelt = self.nao_labling_dic[DesigoCC.NAME_INSTANCE][instance_id][DesigoCC.NAME_NAME]
        instance_name_datapoint = self._metaFromPointSring(data_col=point_data)[DesigoCC.NAME_NAME]
        if instance_name_datapoint != instance_name_labelt:
            message = DesigoCC.MESSAGE_WARNING_WRONG_LABLING_POINT%(point_data[DesigoCC.COLUMN_SL_LANG_1],instance_name_datapoint, instance_name_labelt)
            note = self.getNoteSchema(
                created=str(datetime.now()),
                start=str(datetime.now()-timedelta(days=14)),
                stop=str(datetime.now()+timedelta(days=14)),
                instance_id=instance_id,
                note=message,
            )
            self.Nao.pushNote(asset_id=asset_id,data_note=note)
        labling_dict = {
            DesigoCC.COLUMN_DPE_NAME: point_data.loc[DesigoCC.COLUMN_DPE_NAME],
            DesigoCC.COLUMN_SD: point_data.loc[DesigoCC.COLUMN_SD],
            DesigoCC.COLUMN_SL_LANG_0: point_data.loc[DesigoCC.COLUMN_SL_LANG_0],
            DesigoCC.COLUMN_SL_LANG_1: point_data.loc[DesigoCC.COLUMN_SL_LANG_1]
        }
        self.Nao.activateDatapoint(type_sensor=sensor_type,sensor_id=sensor_id,instance_id=instance_id,config=labling_dict)
        point_dict = {
            DesigoCC.NAME_TP_ID_TS: dp_id_point,
            DesigoCC.NAME_INSTANCE_ID: instance_id,
            DesigoCC.NAME_SENSOR_ID: sensor_id,
            DesigoCC.NAME_ASSET_ID: asset_id,
            DesigoCC.NAME_REGEX: regex
        }
        self.nao_labling_dic[DesigoCC.NAME_ACTIVATED_DATAPOINTS].append(point_dict)
        self.saveNaoLablingFileDict()
        self.activated_datapoints[dp_id_point] = point_dict
        return(1)

    def sendNewInstancesToNao(self,create_workspace:bool=True,create_instance:bool=True) -> None:
        datapoints_to_label = self.getNewLoggingDatapointsWithDriver()
        datapoints_to_label[DesigoCC.NAME_REGEX] = datapoints_to_label[DesigoCC.COLUMN_SD].apply(lambda x: self._findMatchingRegex(x))
        datapoints_to_label = datapoints_to_label[datapoints_to_label[DesigoCC.NAME_REGEX].str.contains("",na=False)]
        device_groups = datapoints_to_label.sort_values(by=DesigoCC.COLUMN_SD).groupby(DesigoCC.COLUMN_DEVICE)
        database_datapoint_counts = self.getDatabaseDatapointCounts()
        datapoint_counts_all = pd.concat(database_datapoint_counts.values(), ignore_index=True)
        for group in device_groups:
            '''
            TODO:
            - setze Datenpunkt in sincronisierungs-file (mit ersten Zeipunkt)
            '''
            data = group[1]
            device = group[0]
            data_len_total = len(data)
            data = data[data[DesigoCC.COLUMN_HDB_DP_ID_TS].isin(datapoint_counts_all[DesigoCC.COLUMN_HDB_DP_ID_TS])]
            data_len_logging = len(data)
            if len(data) < 3:
                continue
            if len(set(data[DesigoCC.COLUMN_WORKSPACE]))>1:
                raise(ValueError("workspace error"))
            workspace_name = data[DesigoCC.COLUMN_WORKSPACE].iloc[0]
            workspace_id = self._checkWorkspace(workspace_name=workspace_name,create_workspace=create_workspace)
            if not workspace_id: continue 
            instance_id = self._checkInstance(device=device,data=data,create_instance=create_instance,workspace_id=workspace_id)
            if not instance_id: continue
            activation_count = 0
            for idx in range(len(data)):
                activation_count+=self._activateDatapoint(instance_id=instance_id,point_data=data.iloc[idx])
            if activation_count > 0:
                asset_id = self.nao_labling_dic[DesigoCC.NAME_INSTANCE][instance_id][DesigoCC.NAO_ASSED_ID]
                instance_name = self.nao_labling_dic[DesigoCC.NAME_INSTANCE][instance_id][DesigoCC.NAME_NAME]
                message = DesigoCC.MESSAGE_INFO_ACTIVATED_POINTS%(instance_name,str(activation_count))
                note = self.getNoteSchema(
                    created=str(datetime.now()),
                    start=str(datetime.now()),
                    stop=str(datetime.now()),
                    note=message,
                    instance_id=instance_id
                )
                self.Nao.pushNote(asset_id=asset_id,data_note=note)
        self._patchSyncStatus()

    def getUtcNow(self) -> datetime:
        return(datetime.now(timezone.utc).replace(tzinfo=None))

    def _getTimeseriesFromDatabaseAndPoint(self, dp_point:int, last_time:str, database:str) -> pd.DataFrame:
        ret = pd.read_sql_query(DesigoCC.SQL_SELECT_VALUE_FROM_DP%(database,dp_point,last_time),con=self.conn,)
        return(ret)
    
    def _getNewTimeseriesFromPoint(self, dp_point:int) -> pd.DataFrame:
        last_time = self.nao_sync_status_dict[dp_point]
        start_time = datetime.fromisoformat(last_time)
        if self.getUtcNow()-DesigoCC.TIMEDELTA_LAST_MIN_SINC <= start_time:
            return(pd.DataFrame([]))
        # self.connectToMsSql()
        if self.getUtcNow()-DesigoCC.TIMEDELTA_LAST_DB <= start_time:
            last_db = -1
        elif self.getUtcNow()-DesigoCC.TIMEDELTA_SECOND_DB <= start_time:
            last_db = -2
        else:
            last_db = None
        for database in self.database_timeseries_list[last_db:]:
            timeseries = self._getTimeseriesFromDatabaseAndPoint(
                dp_point=dp_point,
                last_time=last_time,
                database=database
            )
            if len(timeseries)>0: break
        # self.disconnetToMsSql()
        if len(timeseries)==0:
            sleep_count = int(ceil(((self.getUtcNow()- start_time)/DesigoCC.TIMEDELTA_LAST_MIN_SINC)/2)+1)
            self.sleep_point.extend([dp_point]*sleep_count)
            print("sleep-point-count:", sleep_count)
        print("data-len:", len(timeseries))
        return(timeseries)
    
    def _validateTimeseries(self, timeseries:pd.DataFrame, validator:dict) -> pd.DataFrame:
        if validator.get(DesigoCC.NAME_FACTOR):
            timeseries[DesigoCC.COLUMN_VALUE] = timeseries[DesigoCC.COLUMN_VALUE]*validator[DesigoCC.NAME_FACTOR]
        else:
            print(1)
        if validator.get(DesigoCC.NAME_CHECK_MAX):
            if isinstance(validator[DesigoCC.NAME_CHECK_MAX], (int, float)):
                if timeseries[DesigoCC.COLUMN_VALUE].quantile(0.8) > validator[DesigoCC.NAME_CHECK_MAX]:
                    raise(ValueError("??"))
            else:
                if ((timedelta(hours=1)/timeseries[DesigoCC.COLUMN_SOURCE_TIME].diff())*timeseries[DesigoCC.COLUMN_VALUE].mask(timeseries[DesigoCC.COLUMN_VALUE]<1,np.nan).diff()).quantile(0.8) > float(validator[DesigoCC.NAME_CHECK_MAX]):
                    raise(ValueError("??"))
        return(timeseries)
                

    def _getNewTimeseriesAsTelegrafFrame(self,dp_point:int) -> set:
        timeseries = self._getNewTimeseriesFromPoint(dp_point=dp_point)
        if len(timeseries) == 0: return(-1,[])
        instance_id = self.activated_datapoints[dp_point][DesigoCC.NAME_INSTANCE_ID]
        asset_id = self.activated_datapoints[dp_point][DesigoCC.NAME_ASSET_ID]
        sensor_id = self.activated_datapoints[dp_point][DesigoCC.NAME_SENSOR_ID]
        last_time = timeseries[DesigoCC.COLUMN_SOURCE_TIME].iloc[-1].isoformat()
        if dp_point in self.timeseries_validator:
            timeseries = self._validateTimeseries(timeseries=timeseries, validator=self.timeseries_validator[dp_point])
        timeseries[DesigoCC.COLUMN_SOURCE_TIME] = timeseries[DesigoCC.COLUMN_SOURCE_TIME].astype(int)
        return(last_time, timeseries.apply(lambda row: f'{asset_id},instance={instance_id} {sensor_id}={repr(row[DesigoCC.COLUMN_VALUE])} {int(row[DesigoCC.COLUMN_SOURCE_TIME])}',axis=1).to_list())
    
    def _sendNaoTelegraf(self, telegraf_frame:list) -> bool:
        is_push=False
        for idx in range(3):
            ret=self.Nao.sendTelegrafData(telegraf_frame,max_sleep=0.15)
            if ret==DesigoCC.TELEGRAF_STATUS_CODE_GOOD:
                is_push=True
                break
            else:
                is_push=False
                sleep(10)
        return(is_push)
    
    def  _getTimeseriesChecks(self) -> dict:
        check_driver_dict = {}
        for regex in self.nao_driver_dic[DesigoCC.NAME_SENSORS]:
            if self.nao_driver_dic[DesigoCC.NAME_SENSORS][regex].get(DesigoCC.NAME_CHECK_MAX) or self.nao_driver_dic[DesigoCC.NAME_SENSORS][regex].get(DesigoCC.NAME_FACTOR):
                check_driver_dict[regex] = {
                    DesigoCC.NAME_CHECK_MAX: self.nao_driver_dic[DesigoCC.NAME_SENSORS][regex].get(DesigoCC.NAME_CHECK_MAX),
                    DesigoCC.NAME_FACTOR: self.nao_driver_dic[DesigoCC.NAME_SENSORS][regex].get(DesigoCC.NAME_FACTOR)
                }
        ret = {}
        for idx in range(len(self.nao_labling_dic[DesigoCC.NAME_ACTIVATED_DATAPOINTS])):
            if self.nao_labling_dic[DesigoCC.NAME_ACTIVATED_DATAPOINTS][idx][DesigoCC.NAME_REGEX] in check_driver_dict:
                ret[self.nao_labling_dic[DesigoCC.NAME_ACTIVATED_DATAPOINTS][idx][DesigoCC.NAME_TP_ID_TS]] = check_driver_dict[self.nao_labling_dic[DesigoCC.NAME_ACTIVATED_DATAPOINTS][idx][DesigoCC.NAME_REGEX]]
        return(ret)

    def sicAllDatapoints(self) -> bool:
        telegraf_frame = []
        last_point_times = {}
        self.connectToMsSql()
        points = []
        for dp_point in self.nao_sync_status_dict:
            # if dp_point in self.timeseries_validator:
            #     if self.timeseries_validator[dp_point].get("factor"):
            #         points.append(dp_point)
            #     else: continue
            # else:continue
            print("------------------------------",dp_point,"--------------------------------")
            if dp_point in self.sleep_point:
                self.sleep_point.pop(dp_point)
                continue
            last_time, frame = self._getNewTimeseriesAsTelegrafFrame(dp_point=dp_point)
            if last_time == -1: continue
            last_point_times[dp_point] = last_time
            telegraf_frame.extend(frame)
            if len(telegraf_frame)<DesigoCC.TELEGRAF_PUSH_LEN:continue
            if self._sendNaoTelegraf(telegraf_frame=telegraf_frame):
                telegraf_frame = []
                for dd in last_point_times: self.nao_sync_status_dict[dd] = last_point_times[dd]
                last_point_times = {}
            else:
                self.saveSyncStatus()  
                self.disconnetToMsSql()
                return(False)
        if len(telegraf_frame)==0: 
            self.saveSyncStatus()  
            self.disconnetToMsSql()
            return(True)
        if self._sendNaoTelegraf(telegraf_frame=telegraf_frame):
             for dd in last_point_times: self.nao_sync_status_dict[dd] = last_point_times[dd]
             self.saveSyncStatus()  
             self.disconnetToMsSql()
             return(True)
        else:
            self.saveSyncStatus()  
            self.disconnetToMsSql()
            return(False)

# regex_list = ["\\.FW_WMZ_Waermemenge$", "\\.CmnHtmHEg$"]
# for regex in self.nao_driver_dic["sensors"]:
#     if self.nao_driver_dic["sensors"][regex].get("factor"):
#         if factor
#         regex_list.append(regex)

# wrong_dbs = []
# for idx in range(len(self.nao_labling_dic["activated_datapoint"])):
#     if self.nao_labling_dic["activated_datapoint"][idx]["regex"] in regex_list:
#         wrong_dbs.append(self.nao_labling_dic["activated_datapoint"][idx]["tp_id_ts"])

# for dps in self.nao_sync_status_dict:
#     if dps in wrong_dbs:
#         self.nao_sync_status_dict[dps] = datetime(2018,1,1,0,0,0).isoformat()


