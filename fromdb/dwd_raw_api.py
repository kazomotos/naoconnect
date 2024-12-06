import http.client
import re
import zipfile
from copy import copy, deepcopy
from datetime import datetime, timedelta
from html.parser import HTMLParser
from io import BytesIO
from json import dumps, loads
from time import sleep
from numpy import nan, isnan

import pandas as pd

from naoconnect.NaoApp import NaoApp
from naoconnect.Param import Param
from naoconnect.TinyDb import TinyDb


class ParamDWD10m():
    SEP = ";"
    DATETIME_COLUMN = "MESS_DATUM"
    WORKSPACE_NAME = "DWD-Klimadaten"
    ASSET_NAME = "Wetterstationen"
    ASSET_DESCRIPTION = "Wetterstationen vom Deutschen Wetterdienst (DWD), die Daten werden aus der Open-Data-Schnittstelle von DWD (opendata.dwd.de) bezogen."
    META_CONTAINER = "Stationsdaten"
    META_STATION_NAME = "Stationsname"
    META_STATION_DESCRIPTION = "Name der Station aus den DWD-Metadaten"
    META_BUNDESLAND_NAME = "Bundesland"
    META_BUNDESLAND_DESCRIPTION = "Bundesland in welchem sich die Station befindet"
    META_STATION_ID_NAME = "Stations_id"
    META_STATION_ID_DESCRIPTION = "Stations-ID für die Zuordnung beim DWD"
    META_STATION_HOHE = "Stationshoehe"
    META_STATION_HOHE_DESCRIPTION = "Geografische Höhe der Station"
    META_INSTANCE_DESCRIPTION = "Wetterstation in %s im Bundesland %s vom DWD (opendata.dwd.de)."
    META_GEO_LAENGE = "geoLaenge"
    META_GEO_BREITE = "geoBreite"
    COLOR_NAO = "#49C2F2"
    COLOR = "color"
    DWD_STRUCT = {
        "10min_sensor": {
            "/wind/":{
                "name": "Wind-10min",
                "color": "#25D1EB",
                "instance": [],
                "sensor":  {
                    "FF_10": {
                        "description": "10min-Mittel der Windgeschwindigkeit",
                        "name": "Windgeschwindigkeit-10min",
                        "unit": "m/s",
                        "min": 0,
                        "max": 9,
                        "color": "#599C98",
                        "series": ""
                    },
                    "DD_10": {
                        "description": "10min-Mittel der Windrichtung",
                        "name": "Windrichtung-10min",
                        "unit": "°",
                        "min": 0,
                        "max": 360,
                        "color": "#859291",
                        "series": ""
                    }
                }
            },
            "/solar":{
                "name": "Solar-10min",
                "color": "#C3B60E",
                "instance": [],
                "sensor":  {
                    "DS_10": {
                        "description": "10min-Summe der diffusen solaren Strahlung",
                        "name": "Diffusstrahlung-10min",
                        "unit": "J/cm²",
                        "min": 0,
                        "max": 600,
                        "color": "#E0C743",
                        "series": ""
                    },
                    "GS_10": {
                        "description": "10min-Summe der Globalstrahlung",
                        "name": "Globalstrahlung-10min",
                        "unit": "J/cm²",
                        "min": 0,
                        "max": 600,
                        "color": "#D8BB20",
                        "series": ""
                    },
                    "SD_10": {
                        "description": "10min-Summe der Sonnenscheindauer",
                        "name": "Sonnenscheindauer-10min",
                        "unit": "h",
                        "min": 0,
                        "max": 10,
                        "color": "#7E743E",
                        "series": ""
                    },
                    "LS_10": {
                        "description": "10min-Summe der atmosphärischen Gegenstrahlung",
                        "name": "Athomsophärglobalstrahlung-10min",
                        "unit": "J/cm²",
                        "min": 0,
                        "max": 600,
                        "color": "#A7901A",
                        "series": ""
                    }
                }
            },
            "/air_temperature":{
                "name": "Luft-10min",
                "color": "#11AD01",
                "instance": [],
                "sensor":  {
                    "PP_10": {
                        "description": "Luftdruck auf Stationshöhe (Instantanwert)",
                        "name": "Luftdruck-10min",
                        "unit": "hPa",
                        "min": 400,
                        "max": 1200,
                        "color": "#9DB39B",
                        "series": ""
                    },
                    "TT_10": {
                        "description": "Lufttemperatur in 2m Höhe (Instantanwert)",
                        "name": "Lufttemperatur-2m-10min",
                        "unit": "°C",
                        "min": -60,
                        "max": 60,
                        "color": "#8DC7C4",
                        "series": ""
                    },
                    "TM5_10": {
                        "description": "Lufttemperatur 5cm Höhe (Instantanwert)",
                        "name": "Lufttemperatur-5cm-10min",
                        "unit": "°C",
                        "min": -60,
                        "max": 60,
                        "color": "#79A8A6",
                        "series": ""
                    },
                    "RF_10": {
                        "description": "relative Feuchte in 2m Höhe (Instantanwert)",
                        "name": "Luftfeuchte-2m-10min",
                        "unit": "%",
                        "min": 0,
                        "max": 100,
                        "color": "#6195E9",
                        "series": ""
                    },
                    "TD_10": {
                        "description": "Taupunkttemperatur in 2m Höhe (Mittelwert)",
                        "name": "Taupunkttemperatur-2m-10min",
                        "unit": "°C",
                        "min": -60,
                        "max": 60,
                        "color": "#11408B",
                        "series": ""
                    }
                }
            },
            "/precipitation":{
                "name": "Regen-10min",
                "color": "#0D5DAF",
                "instance": [],
                "sensor":  {
                    "RWS_DAU_10": {
                        "description": "Niederschlagsdauer der letzten 10-Minunten",
                        "name": "Niederschlagsdauer-10min",
                        "unit": "min",
                        "min": 0,
                        "max": 11,
                        "color": "#1875CA",
                        "series": ""
                    },
                    "RWS_10": {
                        "description": "Niederschlagshöhe der letzten 10-Minuten",
                        "name": "Niederschlagshöhe-10min",
                        "unit": "mm",
                        "min": 0,
                        "max": 10,
                        "color": "#0D508D",
                        "series": ""
                    }
                }
            }
        },
        "gradtag": {
            "/climate_environment/CDC/derived_germany/techn/daily/heating_degreedays/hdd_3807/":{
                "name": "Gradtage-1d",
                "color": "#DD6814",
                "instance": [],
                "sensor":  {
                    "Gradtage": {
                        "description": "Tägliche Gradtage nach VDI 3807 (Berechnet vom DWD)",
                        "name": "Gradtage-1d",
                        "unit": "Kd",
                        "min": 0,
                        "max": 250,
                        "color": "#E02EC4",
                        "series": ""
                    }
                }
            },
        },
        "instance": {},
        "asset": ""
    }
    UNITS = ["m/s", "J/cm²", "°", "h", "hPa", "°C", "%", "min", "mm", "Kd"]
    DWD_SENSOR = "sensor"
    DWD_10MIN_SENSOR = "10min_sensor"
    DWD_GRADTAG = "gradtag"
    URL_10MIN_CLIMATE = "/climate_environment/CDC/observations_germany/climate/10_minutes"
    URL_HOUR_CLIMATE = "/climate_environment/CDC/observations_germany/climate/hourly"
    URLS_META_NO_FOUND = [
        "/climate_environment/CDC/observations_germany/climate/monthly/more_precip/historical/",
        "/climate_environment/CDC/observations_germany/climate/hourly/wind/recent/"
    ]
    SUB_URLS_CLIMATE = [ "/wind", "/solar", "/air_temperature", "/precipitation" ]
    SUB_URL_HISTORICAL = "/historical"
    SUB_URL_RECENT = "/recent"
    SUB_URL_NOW = "/now"
    SUB_URLS_GRADTAG = ["/climate_environment/CDC/derived_germany/techn/daily/heating_degreedays/hdd_3807/"]
    NAME_NAME = "name"
    NAME_URL = "url"
    TIME_FORMAT = "%Y%m%d%H%M"

class _StructDWDHTMLContext10m(Param):
    default_time_format_struct_file_infos = "%d-%b-%Y %H:%M"
    string_format_beween_time = r' +.+  +'
    string_format_to_delet = '  +'
    data_format = '.zip'
    description_station = 'Beschreibung_Stationen'

    def __init__(self):
        self.station_data = {}
        self.last_station = False

    def handle_station(self, data:str):
        if _StructDWDHTMLContext10m.data_format in data:
            numbers = re.findall(r'\d+', data)
            for num in numbers:
                if len(num) == 5:
                    self.last_station = num
                    if not self.station_data.get(self.last_station):
                        self.station_data[self.last_station] = {_StructDWDHTMLContext10m.NAME_NAME: [data]}
                    else:
                        self.station_data[self.last_station][_StructDWDHTMLContext10m.NAME_NAME].append(data)
                    break
        elif _StructDWDHTMLContext10m.description_station in data:
            self.station_data[_StructDWDHTMLContext10m.NAME_DESCRIPTION] = data
        else:
            if self.last_station:
                self.station_data[self.last_station][_StructDWDHTMLContext10m.NAME_TIME] = datetime.strptime(
                    re.sub(_StructDWDHTMLContext10m.string_format_to_delet, '',re.findall(_StructDWDHTMLContext10m.string_format_beween_time, data)[0]),
                    _StructDWDHTMLContext10m.default_time_format_struct_file_infos
                ) 
                self.last_station = False

    def handle_time(self, data:str):
        if self.last_station:
            time_str = re.findall(_StructDWDHTMLContext10m.string_format_beween_time, data)
            if len(time_str) > 0:
                if time_str[0][0] == " " and time_str[0][1] != " ":
                    time_str[0] = time_str[0][1:]
                self.station_data[self.last_station][_StructDWDHTMLContext10m.NAME_TIME] = datetime.strptime(
                    re.sub(_StructDWDHTMLContext10m.string_format_to_delet, '',time_str[0]), # type: ignore
                    _StructDWDHTMLContext10m.default_time_format_struct_file_infos
                ) 
                self.last_station = False    

    def getStationData(self):
        data = copy(self.station_data)
        self.station_data = {}
        self.last_station = False
        return(data)

StructDWDHTMLContext10m = _StructDWDHTMLContext10m()

class _StructDWDHTMLParser(HTMLParser):

    def handle_starttag(self, tag, attrs):
        if len(attrs) > 0:
            if len(attrs[0]) > 1:
                StructDWDHTMLContext10m.handle_station(attrs[0][1]) # type: ignore
            
    def handle_data(self, data):
        StructDWDHTMLContext10m.handle_time(data)

StructDWDParser = _StructDWDHTMLParser()

class DWDData10m(Param, ParamDWD10m):
    SEC_TO_NANO = 1000000000

    def __init__(self, NaoAppLabling=None, auto_labling=False, host:str="opendata.dwd.de", path_labling_json:str="labling.json", path_transfer_conf:str="conf_dwd.json"):
        self.labling_path = path_labling_json
        self.conf_path = path_transfer_conf
        self.host = host
        self.auto_labling = auto_labling
        self.labling = self.getLablingJson(path_labling_json)
        self.db = TinyDb(path_transfer_conf)
        self.last_time = self._getLastTimestamps()
        self.last_time_hold:dict
        self.new_stat = []
        self.Nao:NaoApp = NaoAppLabling # type: ignore
        self.get_history = True
        self._lablingNaoStation10m(DWDData10m.SUB_URL_RECENT)

    def connect(self):
        self.con = http.client.HTTPSConnection(self.host)
    
    def disconnect(self):
        try:
            self.con.close()
        except:
            pass

    def refreshConnection(self):
        self.disconnect()

    def confirmTransfer(self):
        self.last_time = self.last_time_hold
        self._putLastTimestamps()

    def exit(self):
        pass

    def getOpenDataFileInfo(self, url):
        self.connect()
        self.con.request(DWDData10m.NAME_GET, url) # type: ignore
        res = self.con.getresponse().read().decode(DWDData10m.NAME_UTF8)
        StructDWDParser.feed(res)
        self.disconnect()
        return(StructDWDHTMLContext10m.getStationData())

    def convertDatetime(self, time_str):
        return(datetime.strptime(time_str, DWDData10m.TIME_FORMAT))

    def convertFloat(self, dat) -> float:
        try:
            return(float(dat))
        except:
            return(nan)

    def getDataFrameFromZIP(self, url):
        self.connect()
        self.con.request(DWDData10m.NAME_GET, url)
        data = self.con.getresponse().read()
        self.disconnect()
        data = zipfile.ZipFile(BytesIO(data),"r")
        data = pd.read_csv(
            BytesIO(data.open(data.infolist()[0].filename).read()), 
            sep=DWDData10m.SEP,  
            converters={DWDData10m.DATETIME_COLUMN: self.convertDatetime}, 
            index_col=DWDData10m.DATETIME_COLUMN, 
            na_values='######'
        )
        return(data)

    def getMetaDataFrame(self, url):
        self.connect()
        self.con.request(DWDData10m.NAME_GET, url)
        data = re.sub("  +", ";", self.con.getresponse().read().decode("iso-8859-1")).replace("-", "").split("\r\n")
        self.disconnect()
        keynames = data[0].replace(" ", ";").split(";")
        ret = {}
        for key in keynames: ret[key] = []
        for row in data[1:]:
            dat = row.replace(" ", ";", 3).split(";")
            if len(dat) > len(keynames):
                dat = dat[:len(keynames)]
            elif len(dat) < len(keynames):
                continue
            for idx in range(len(dat)):
                ret[keynames[idx]].append(dat[idx])
        return(pd.DataFrame(ret)) # type: ignore

    def getLablingJson(self, path):
        try:
            data_file = open(path, "r")
            return(loads(data_file.read()))
        except:
            return({})

    def getTelegrafData(self, max_data_len:int=200000):
        self.last_time_hold = deepcopy(self.last_time)
        if self.labling[DWDData10m.NAME_INSTANCE] == {}:
            if self.auto_labling:
                self._lablingNaoStation10m(DWDData10m.SUB_URL_HISTORICAL)
            else:
                return([])
        return(self._get10MinDWDTelegrafData(max_data=max_data_len))

    def _get10MinDWDTelegrafData(self, max_data:int):
        data_return = []
        ext_data = data_return.extend
        for sub_url2 in self.last_time_hold[DWDData10m.DWD_10MIN_SENSOR]:
            timestamps = self.last_time_hold[DWDData10m.DWD_10MIN_SENSOR]
            info_history = {}
            info_recent = {}
            info_now = {}
            for station_id in timestamps[sub_url2]:
                if station_id == DWDData10m.NAME_DESCRIPTION:
                    continue
                if len(data_return) > max_data:
                    return(data_return)
                if timestamps[sub_url2][station_id][0] == 100000:
                    if len(info_history) == 0:
                        info_history = self.getOpenDataFileInfo(url=DWDData10m.URL_10MIN_CLIMATE+sub_url2+DWDData10m.SUB_URL_HISTORICAL+"/")
                    if info_history.get(station_id) == None:
                        timestamps[sub_url2][station_id][0] = 1111111
                        continue
                    info_history[station_id][DWDData10m.NAME_NAME].sort()
                    data = []
                    data_add = data.append
                    for file_txt in info_history[station_id][DWDData10m.NAME_NAME]:
                        data_add(self.getDataFrameFromZIP(
                            url=DWDData10m.URL_10MIN_CLIMATE+sub_url2+DWDData10m.SUB_URL_HISTORICAL+"/"+file_txt
                        ))
                    for idx in range(len(data)):
                        ext_data(self._getTelegrafDataFromFrame(
                            data=data[idx],
                            sub_url=DWDData10m.DWD_10MIN_SENSOR,
                            sub_url2=sub_url2,
                            station_id=station_id
                        ))  
                    timestamps[sub_url2][station_id][0] = max(data[-1].index).timestamp()
                # elif  datetime.utcnow().timestamp() - timestamps[sub_url2][station_id][0] > 79200:
                #     if len(info_recent) == 0:
                #         info_recent = self.getOpenDataFileInfo(url=DWDData10m.URL_10MIN_CLIMATE+sub_url2+DWDData10m.SUB_URL_RECENT+"/")
                #     if info_recent.get(station_id) == None:
                #         continue
                #     if timestamps[sub_url2][station_id][1] == info_recent[station_id][DWDData10m.NAME_TIME].timestamp():
                #         continue
                #     data = self.getDataFrameFromZIP(
                #         url=DWDData10m.URL_10MIN_CLIMATE+sub_url2+DWDData10m.SUB_URL_RECENT+"/"+info_recent[station_id][DWDData10m.NAME_NAME]
                #     )
                #     data = data[datetime.fromtimestamp(timestamps[sub_url2][station_id][0])+timedelta(minutes=5):]
                #     ext_data(self._getTelegrafDataFromFrame(
                #         data=data,
                #         sub_url=DWDData10m.DWD_10MIN_SENSOR,
                #         sub_url2=sub_url2,
                #         station_id=station_id
                #     ))  
                #     if len(data) != 0:
                #         timestamps[sub_url2][station_id][0] = max(data.index).timestamp()
                #     try:
                #         timestamps[sub_url2][station_id][1] = info_recent[station_id][DWDData10m.NAME_TIME].timestamp()
                #     except:
                #         pass
                # else:
                #     if len(info_now) == 0:
                #         info_now = self.getOpenDataFileInfo(url=DWDData10m.URL_10MIN_CLIMATE+sub_url2+DWDData10m.SUB_URL_NOW+"/")
                #     if info_now.get(station_id) == None:
                #         continue
                #     if timestamps[sub_url2][station_id][1] == info_now[station_id][DWDData10m.NAME_TIME].timestamp():
                #         continue
                #     data = self.getDataFrameFromZIP(
                #         url=DWDData10m.URL_10MIN_CLIMATE+sub_url2+DWDData10m.SUB_URL_NOW+"/"+info_now[station_id][DWDData10m.NAME_NAME]
                #     )
                #     data = data[datetime.fromtimestamp(timestamps[sub_url2][station_id][0])+timedelta(minutes=5):]
                #     ext_data(self._getTelegrafDataFromFrame(
                #         data=data,
                #         sub_url=DWDData10m.DWD_10MIN_SENSOR,
                #         sub_url2=sub_url2,
                #         station_id=station_id
                #     ))  
                #     if len(data) != 0:
                #         timestamps[sub_url2][station_id][0] = max(data.index).timestamp()
                #     try:
                #         timestamps[sub_url2][station_id][1] = info_now[station_id][DWDData10m.NAME_TIME].timestamp()
                #     except:
                #         pass
        return(data_return)

    def _getTelegrafDataFromFrame(self, data:pd.DataFrame, sub_url, sub_url2, station_id):
        data_return = []
        add_data = data_return.append
        asset = self.labling[DWDData10m.NAME_ASSET]
        try:
            instance = self.labling[DWDData10m.NAME_INSTANCE][station_id]
        except:
            self._lablingOneNaoStation10mFromNotInMeta(station_id,sub_url2)
            instance = self.labling[DWDData10m.NAME_INSTANCE][station_id]
        for sensor in self.labling[sub_url][sub_url2][DWDData10m.DWD_SENSOR]:
            for timestamp_d in data.index:
                if not isnan(data[sensor][timestamp_d]):
                    add_data(self._buildTelegrafFrameForm(
                        twin=asset,
                        instance=instance,
                        series=self.labling[sub_url][sub_url2][DWDData10m.DWD_SENSOR][sensor][DWDData10m.NAME_SERIES],
                        value=data[sensor][timestamp_d],
                        timestamp=timestamp_d.timestamp()*DWDData10m.SEC_TO_NANO
                    ))  
        return(data_return)

    def _buildTelegrafFrameForm(self, twin, instance, series, value,timestamp):
        return(DWDData10m.FORMAT_TELEGRAFFRAMESTRUCT%(twin,instance,series,value,timestamp))

    def _lablingNaoStation10m(self, sub_url):
        for sub_url2 in DWDData10m.SUB_URLS_CLIMATE:
            url = DWDData10m.URL_10MIN_CLIMATE+sub_url2+sub_url+"/"
            info = self.getOpenDataFileInfo(url=url)
            meta = self.getMetaDataFrame(url=url+info[DWDData10m.NAME_DESCRIPTION])
            stations_ids_meta = set(meta[DWDData10m.META_STATION_ID_NAME])
            meta = meta.set_index(DWDData10m.META_STATION_ID_NAME)
            stations_to_create = stations_ids_meta.difference(set(self.labling[DWDData10m.NAME_INSTANCE].keys()))
            if len(stations_to_create) != 0:
                self._createInstancesInNao(meta, stations_to_create, self.labling[DWDData10m.NAME_ASSET])
            endpoints_to_create = stations_ids_meta.difference(set(self.labling[DWDData10m.DWD_10MIN_SENSOR][sub_url2][DWDData10m.NAME_INSTANCE]))
            if len(endpoints_to_create) != 0:
                self._createEndpoints(endpoints_to_create, sub_url2, DWDData10m.DWD_10MIN_SENSOR)
        self._setDefaultTime10m(DWDData10m.SUB_URL_HISTORICAL)
    
    def _lablingOneNaoStation10mFromNotInMeta(self, station, sub_url2):
        for url in DWDData10m.URLS_META_NO_FOUND:
            info = self.getOpenDataFileInfo(url=url)
            meta = self.getMetaDataFrame(url=url+info[DWDData10m.NAME_DESCRIPTION])
            meta = meta.set_index(DWDData10m.META_STATION_ID_NAME)
            if meta[DWDData10m.META_STATION_NAME].get(station):
                break
        stations_ids_meta = set([station])
        stations_to_create = stations_ids_meta.difference(set(self.labling[DWDData10m.NAME_INSTANCE].keys()))
        if len(stations_to_create) != 0:
            self._createInstancesInNao(meta, stations_to_create, self.labling[DWDData10m.NAME_ASSET]) # type: ignore
        endpoints_to_create = stations_ids_meta.difference(set(self.labling[DWDData10m.DWD_10MIN_SENSOR][sub_url2][DWDData10m.NAME_INSTANCE]))
        if len(endpoints_to_create) != 0:
            self._createEndpoints(endpoints_to_create, sub_url2, DWDData10m.DWD_10MIN_SENSOR)
        self._setDefaultTime10m(DWDData10m.SUB_URL_HISTORICAL)

    def _createEndpoints(self, instances, sub_url, types):
        for instance in instances:
            try:
                for sensor in self.labling[types][sub_url][DWDData10m.DWD_SENSOR]:
                    sleep(0.025)
                    self.Nao.postEnpointConifg(
                        conf={DWDData10m.NAME_URL: types+sub_url, DWDData10m.NAME_NAME: sensor},
                        _instance=self.labling[DWDData10m.NAME_INSTANCE][instance],
                        _series=self.labling[types][sub_url][DWDData10m.DWD_SENSOR][sensor][DWDData10m.NAME_SERIES],
                        _asset=self.labling[DWDData10m.NAME_ASSET]
                    )
                self.labling[types][sub_url][DWDData10m.NAME_INSTANCE].append(instance)
            except Exception as e:
                self._saveLabling()
                raise(e)
        self._saveLabling()

    def _setDefaultTime10m(self, sub_url):
        set_time = False
        for sub_url2 in DWDData10m.SUB_URLS_CLIMATE:
            info_recent = self.getOpenDataFileInfo(url=DWDData10m.URL_10MIN_CLIMATE+sub_url2+sub_url+"/")
            for station_id in info_recent:
                if self.last_time[DWDData10m.DWD_10MIN_SENSOR][sub_url2].get(station_id) == None:
                    set_time = True
                    self.last_time[DWDData10m.DWD_10MIN_SENSOR][sub_url2][station_id] = [100000,100000]
        if set_time:
            self._putLastTimestamps()

    def _createInstancesInNao(self, meta_data, station_ids, asset):
        for id_station in station_ids:
            try:
                instance = self.Nao.createInstance(
                    name=meta_data[DWDData10m.META_STATION_NAME][id_station],
                    description=DWDData10m.META_INSTANCE_DESCRIPTION%(
                        meta_data[DWDData10m.META_BUNDESLAND_NAME][id_station],
                        meta_data[DWDData10m.META_STATION_NAME][id_station]
                    ),  
                    _asset=asset,
                    geolocation=[
                        meta_data[DWDData10m.META_GEO_LAENGE][id_station], 
                        meta_data[DWDData10m.META_GEO_BREITE][id_station]
                    ]
                )[DWDData10m.NAME_ID_ID]
                inputs = self.Nao.sendInstanceInputMany([
                    [
                        id_station, 
                        self.labling[DWDData10m.NAME_INPUT][DWDData10m.META_STATION_ID_NAME],
                        instance
                    ],
                    [
                        meta_data[DWDData10m.META_STATION_HOHE][id_station], 
                        self.labling[DWDData10m.NAME_INPUT][DWDData10m.META_STATION_HOHE],
                        instance
                    ],
                    [
                        meta_data[DWDData10m.META_STATION_NAME][id_station], 
                        self.labling[DWDData10m.NAME_INPUT][DWDData10m.META_STATION_NAME],
                        instance
                    ],
                    [
                        meta_data[DWDData10m.META_BUNDESLAND_NAME][id_station], 
                        self.labling[DWDData10m.NAME_INPUT][DWDData10m.META_BUNDESLAND_NAME],
                        instance
                    ]
                ])
                self.labling[DWDData10m.NAME_INSTANCE][id_station] = instance
            except Exception as e:
                self._saveLabling()
                raise(e)
        self._saveLabling()

    def _saveLabling(self):
        json_file = open(self.labling_path, "w")
        json_file.writelines(dumps(self.labling))
        json_file.close()  

    def _getLastTimestamps(self):
        ''' {<id>: <timestamp>}'''
        try: 
            last_timestamps = self.db.getTinyTables(DWDData10m.NAME_TIME)[0]
        except:
            return(dict())
        return(last_timestamps)

    def _putLastTimestamps(self):
        ''' {<id>: <timestamp>}'''
        self.db.updateSimpleTinyTables(DWDData10m.NAME_TIME, self.last_time)

    def _getTransferChannels(self):
        conf_r = self.db.getTinyTables(DWDData10m.NAME_CONFIG)
        conf = {}
        for idx in conf_r:
            conf[idx[DWDData10m.NAME_NAME]] = idx
            conf[idx[DWDData10m.NAME_NAME]][DWDData10m.NAME_ID] = str(idx.doc_id)
        return(conf)

    def firstUseInitialConfigAndNaoAsset(self):
        if self.Nao == None:
            print("cant connect to NAO without NaoApp-Class, set NaoApp in init to use this method")
            return("cant connect to NAO without NaoApp-Class, set NaoApp in init to use this method")
        units_nao_raw = self.Nao.getUnits()[DWDData10m.NAME_RESULTS]
        units_nao = {}
        for dic_u in units_nao_raw: units_nao[dic_u[DWDData10m.NAME_NAME]] = dic_u[DWDData10m.NAME_ID_ID]
        for unit in DWDData10m.UNITS:
            if units_nao.get(unit) == None:
                units_nao[unit] = self.Nao.createUnit(unit)[DWDData10m.NAME_ID_ID]
        workspace = self.Nao.createWorkspace(
            name=DWDData10m.WORKSPACE_NAME
        )[DWDData10m.NAME_ID_ID]
        asset = self.Nao.createAsset(
            name=DWDData10m.ASSET_NAME, 
            _workspace=workspace, 
            description=DWDData10m.ASSET_DESCRIPTION,
            baseInterval="10m"
        )[DWDData10m.NAME_ID_ID]
        in_container = self.Nao.createInputcontainer(
            name=DWDData10m.META_CONTAINER,
            _asset=asset,
            description="",
            color=DWDData10m.COLOR_NAO
        )[DWDData10m.NAME_ID_ID]
        input_station_name = self.Nao.createInput(
            name = DWDData10m.META_STATION_NAME,
            _asset=asset,
            description=DWDData10m.META_STATION_DESCRIPTION
        )[DWDData10m.NAME_ID_ID]
        input_station_id = self.Nao.createInput(
            name = DWDData10m.META_STATION_ID_NAME,
            _asset=asset,
            description=DWDData10m.META_STATION_ID_DESCRIPTION
        )[DWDData10m.NAME_ID_ID]
        input_station_city = self.Nao.createInput(
            name = DWDData10m.META_BUNDESLAND_NAME,
            _asset=asset,
            description=DWDData10m.META_BUNDESLAND_DESCRIPTION
        )[DWDData10m.NAME_ID_ID]
        input_station_hohe = self.Nao.createInput(
            name = DWDData10m.META_STATION_HOHE,
            _asset=asset,
            description=DWDData10m.META_STATION_HOHE_DESCRIPTION
        )[DWDData10m.NAME_ID_ID]
        self.Nao.patchIpuntsInputcontainer(
            _inputcontainer=in_container,
            _inputs=[input_station_name, input_station_id, input_station_city, input_station_hohe]
        )
        new_struct = DWDData10m.DWD_STRUCT
        new_struct[DWDData10m.NAME_ASSET] = asset
        new_struct[DWDData10m.NAME_INPUT] = {
            DWDData10m.META_STATION_HOHE: input_station_hohe,
            DWDData10m.META_STATION_ID_NAME: input_station_id,
            DWDData10m.META_BUNDESLAND_NAME: input_station_city,
            DWDData10m.META_STATION_NAME: input_station_name
        }
        struct_10m = new_struct[DWDData10m.DWD_10MIN_SENSOR]
        for group10m in struct_10m:
            id_group = self.Nao.createPath(
                name=struct_10m[group10m][DWDData10m.NAME_NAME],
                _asset=asset,
                description="",
                color=struct_10m[group10m][DWDData10m.COLOR]
            )[DWDData10m.NAME_ID_ID]
            struct_sensor = struct_10m[group10m][DWDData10m.DWD_SENSOR]
            self._creatSeries(struct_sensor, asset, units_nao, id_group)
        struct_1d = new_struct[DWDData10m.DWD_GRADTAG]
        for group1d in struct_1d:
            id_group = self.Nao.createPath(
                name=struct_1d[group1d][DWDData10m.NAME_NAME],
                _asset=asset,
                description="",
                color=struct_1d[group1d][DWDData10m.COLOR]
            )[DWDData10m.NAME_ID_ID]
            struct_sensor = struct_1d[group1d][DWDData10m.DWD_SENSOR]
            self._creatSeries(struct_sensor, asset, units_nao, id_group)
        json_file = open(self.labling_path, "w")
        json_file.writelines(dumps(new_struct))
        json_file.close()
        json_file = open(self.conf_path, "w")
        last_time_file = {
            DWDData10m.NAME_CONFIG: {},
            DWDData10m.NAME_TIME: {DWDData10m.DWD_10MIN_SENSOR:{}, DWDData10m.DWD_GRADTAG:{}}
        }
        for sub_url in DWDData10m.SUB_URLS_CLIMATE:
            last_time_file[DWDData10m.DWD_10MIN_SENSOR][sub_url] = {}
        for sub_url in DWDData10m.SUB_URLS_GRADTAG:
            last_time_file[DWDData10m.DWD_GRADTAG][sub_url] = {}
        last_time_file[DWDData10m.NAME_TIME] = {"1":last_time_file[DWDData10m.NAME_TIME]}
        json_file.writelines(dumps(last_time_file))
        self.labling = self.getLablingJson(self.labling_path)
        self.db = TinyDb(self.conf_path)

    def _creatSeries(self, struct_sensor, asset, units_nao, id_group):
        if self.Nao == None:
            print("cant connect to NAO without NaoApp-Class, set NaoApp in init to use this method")
            return("cant connect to NAO without NaoApp-Class, set NaoApp in init to use this method")
        for sensor in struct_sensor:
            struct_sensor[sensor][DWDData10m.NAME_SERIES] = self.Nao.createSeries(
                type="sensor",
                name=struct_sensor[sensor][DWDData10m.NAME_NAME],
                description=struct_sensor[sensor][DWDData10m.NAME_DESCRIPTION],
                _asset=asset,
                _unit=units_nao[struct_sensor[sensor][DWDData10m.NAME_UNIT]],
                max=struct_sensor[sensor][DWDData10m.NAME_MAX_VALUE],
                min=struct_sensor[sensor][DWDData10m.NAME_MIN_VALUE],
                color=struct_sensor[sensor][DWDData10m.NAME_COLOR],
                _part=id_group,
                fill="null",
                fillValue=None
            )[DWDData10m.NAME_ID_ID]



'''
 DWD 1 Sundenwerte
'''



class ParamDWD1h():
    SEP = ";"
    DATETIME_COLUMN = "MESS_DATUM"
    WORKSPACE_NAME = "DWD-Klimadaten"
    ASSET_NAME = "Wetterstationen"
    ASSET_DESCRIPTION = "Wetterstationen vom Deutschen Wetterdienst (DWD), die Daten werden aus der Open-Data-Schnittstelle von DWD (opendata.dwd.de) bezogen."
    META_CONTAINER = "Stationsdaten"
    META_STATION_NAME = "Stationsname"
    META_STATION_DESCRIPTION = "Name der Station aus den DWD-Metadaten"
    META_BUNDESLAND_NAME = "Bundesland"
    META_BUNDESLAND_DESCRIPTION = "Bundesland in welchem sich die Station befindet"
    META_STATION_ID_NAME = "Stations_id"
    META_STATION_ID_DESCRIPTION = "Stations-ID für die Zuordnung beim DWD"
    META_STATION_HOHE = "Stationshoehe"
    META_STATION_HOHE_DESCRIPTION = "Geografische Höhe der Station"
    META_INSTANCE_DESCRIPTION = "Wetterstation in %s im Bundesland %s vom DWD (opendata.dwd.de)."
    META_GEO_LAENGE = "geoLaenge"
    META_GEO_BREITE = "geoBreite"
    COLOR_NAO = "#49C2F2"
    COLOR = "color"
    DWD_STRUCT = {
        "hourly": {
            "/wind":{
                "name": "Wind-1h",
                "color": "#25D1EB",
                "instance": [],
                "sensor":  {
                    "   F": {
                        "description": "1h-Mittel der Windgeschwindigkeit",
                        "name": "Windgeschwindigkeit-10min",
                        "unit": "m/s",
                        "min": 0,
                        "max": 9,
                        "color": "#599C98",
                        "series": ""
                    },
                    "   D": {
                        "description": "1h-Mittel der Windrichtung",
                        "name": "Windrichtung-1h",
                        "unit": "°",
                        "min": 0,
                        "max": 360,
                        "color": "#859291",
                        "series": ""
                    }
                }
            },
            # "/solar":{
            #     "name": "Solar-1h",
            #     "color": "#C3B60E",
            #     "instance": [],
            #     "sensor":  {
            #         "FD_LBERG": {
            #             "description": "1h-Summe der diffusen solaren Strahlung",
            #             "name": "Diffusstrahlung-1h",
            #             "unit": "J/cm²",
            #             "min": 0,
            #             "max": 600,
            #             "color": "#E0C743",
            #             "series": ""
            #         },
            #         "FG_LBERG": {
            #             "description": "1h-Summe der Globalstrahlung",
            #             "name": "Globalstrahlung-1h",
            #             "unit": "J/cm²",
            #             "min": 0,
            #             "max": 600,
            #             "color": "#D8BB20",
            #             "series": ""
            #         },
            #         "SD_LBERG": {
            #             "description": "1h-Summe der Sonnenscheindauer",
            #             "name": "Sonnenscheindauer-1h",
            #             "unit": "min",
            #             "min": 0,
            #             "max": 10,
            #             "color": "#7E743E",
            #             "series": ""
            #         },
            #         "ATMO_LBERG": {
            #             "description": "1h-Summe der atmosphärischen Gegenstrahlung",
            #             "name": "Athomsophärglobalstrahlung-10min",
            #             "unit": "J/cm²",
            #             "min": 0,
            #             "max": 600,
            #             "color": "#A7901A",
            #             "series": ""
            #         },
            #         "ZENIT": {
            #             "description": "1h Zenitwinkel der Sonne bei Intervallmitte",
            #             "name": "Zenitwinkel-1h",
            #             "unit": "°",
            #             "min": 0,
            #             "max": 360,
            #             "color": "#A7901A",
            #             "series": ""
            #         }
            #     }
            # },
            "/air_temperature":{
                "name": "Luft-1h",
                "color": "#11AD01",
                "instance": [],
                "sensor":  {
                    "TT_TU": {
                        "description": "1h Lufttemperatur in 2m Höhe (Instantanwert)",
                        "name": "Lufttemperatur-2m-1h",
                        "unit": "°C",
                        "min": -60,
                        "max": 60,
                        "color": "#8DC7C4",
                        "series": ""
                    },
                    "RF_TU": {
                        "description": "relative Feuchte (Instantanwert)",
                        "name": "Luftfeuchte-10min",
                        "unit": "%",
                        "min": 0,
                        "max": 100,
                        "color": "#6195E9",
                        "series": ""
                    }
                }
            },
            "/precipitation":{
                "name": "Regen-1h",
                "color": "#0D5DAF",
                "instance": [],
                "sensor":  {
                    "RS_IND": {
                        "description": "Niederschlag in der letzen Stunde [0,1]",
                        "name": "Niederschlag-1h",
                        "unit": "-",
                        "min": 0,
                        "max": 1,
                        "color": "#1875CA",
                        "series": ""
                    },
                    "  R1": {
                        "description": "Niederschlagshöhe der letzten Stunde",
                        "name": "Niederschlagshöhe-1h",
                        "unit": "mm",
                        "min": 0,
                        "max": 10,
                        "color": "#0D508D",
                        "series": ""
                    }
                }
            }
        },
        # "gradtag": {
        #     "/climate_environment/CDC/derived_germany/techn/daily/heating_degreedays/hdd_3807/":{
        #         "name": "Gradtage-1d",
        #         "color": "#DD6814",
        #         "instance": [],
        #         "sensor":  {
        #             "Gradtage": {
        #                 "description": "Tägliche Gradtage nach VDI 3807 (Berechnet vom DWD)",
        #                 "name": "Gradtage-1d",
        #                 "unit": "Kd",
        #                 "min": 0,
        #                 "max": 250,
        #                 "color": "#E02EC4",
        #                 "series": ""
        #             }
        #         }
        #     },
        # },
        "instance": {},
        "asset": ""
    }
    UNITS = ["m/s", "J/cm²", "°", "h", "hPa", "°C", "%", "min", "mm", "Kd", "-"]
    DWD_SENSOR = "sensor"
    DWD_HOURLY_SENSOR = "hourly"
    DWD_GRADTAG = "gradtag"
    URL_HOURLY_CLIMATE = "/climate_environment/CDC/observations_germany/climate/hourly"
    URLS_META_NO_FOUND = [
        "/climate_environment/CDC/observations_germany/climate/monthly/more_precip/historical/",
        "/climate_environment/CDC/observations_germany/climate/hourly/wind/recent/"
    ]
    SUB_URLS_CLIMATE = [ "/wind", "/air_temperature", "/precipitation"] #, "/solar", ]
    SUB_URL_HISTORICAL = "/historical"
    SUB_URL_RECENT = "/recent"
    SUB_URL_NOW = "/now"
    SUB_URLS_GRADTAG = ["/climate_environment/CDC/derived_germany/techn/daily/heating_degreedays/hdd_3807/"]
    NAME_NAME = "name"
    NAME_URL = "url"
    TIME_FORMAT = "%Y%m%d%H"

class _StructDWDHTMLContext1h(Param):
    default_time_format_struct_file_infos = "%d-%b-%Y %H:%M"
    string_format_beween_time = r' +.+  +'
    string_format_to_delet = '  +'
    data_format = '.zip'
    description_station = 'Beschreibung_Stationen'

    def __init__(self):
        self.station_data = {}
        self.last_station = False

    def handle_station(self, data:str):
        if _StructDWDHTMLContext1h.data_format in data:
            numbers = re.findall(r'\d+', data)
            for num in numbers:
                if len(num) == 5:
                    self.last_station = num
                    if not self.station_data.get(self.last_station):
                        self.station_data[self.last_station] = {_StructDWDHTMLContext1h.NAME_NAME: [data]}
                    else:
                        self.station_data[self.last_station][_StructDWDHTMLContext1h.NAME_NAME].append(data)
                    break
        elif _StructDWDHTMLContext1h.description_station in data:
            self.station_data[_StructDWDHTMLContext1h.NAME_DESCRIPTION] = data
        else:
            if self.last_station:
                self.station_data[self.last_station][_StructDWDHTMLContext1h.NAME_TIME] = datetime.strptime(
                    re.sub(_StructDWDHTMLContext1h.string_format_to_delet, '',re.findall(_StructDWDHTMLContext1h.string_format_beween_time, data)[0]),
                    _StructDWDHTMLContext1h.default_time_format_struct_file_infos
                ) 
                self.last_station = False

    def handle_time(self, data:str):
        if self.last_station:
            time_str = re.findall(_StructDWDHTMLContext1h.string_format_beween_time, data)
            if len(time_str) > 0:
                if time_str[0][0] == " " and time_str[0][1] != " ":
                    time_str[0] = time_str[0][1:]
                self.station_data[self.last_station][_StructDWDHTMLContext1h.NAME_TIME] = datetime.strptime(
                    re.sub(_StructDWDHTMLContext1h.string_format_to_delet, '',time_str[0]), # type: ignore
                    _StructDWDHTMLContext1h.default_time_format_struct_file_infos
                ) 
                self.last_station = False    

    def getStationData(self):
        data = copy(self.station_data)
        self.station_data = {}
        self.last_station = False
        return(data)

StructDWDHTMLContext = _StructDWDHTMLContext1h()

class _StructDWDHTMLParser1h(HTMLParser):

    def handle_starttag(self, tag, attrs):
        if len(attrs) > 0:
            if len(attrs[0]) > 1:
                StructDWDHTMLContext.handle_station(attrs[0][1]) # type: ignore
            
    def handle_data(self, data):
        StructDWDHTMLContext.handle_time(data)

StructDWDParser1h = _StructDWDHTMLParser1h()

class DWDData1h(Param, ParamDWD1h):
    SEC_TO_NANO = 1000000000

    def __init__(self, NaoAppLabling=None, auto_labling=False, host:str="opendata.dwd.de", path_labling_json:str="labling1h.json", path_transfer_conf:str="conf_dwd1h.json"):
        self.labling_path = path_labling_json
        self.conf_path = path_transfer_conf
        self.host = host
        self.auto_labling = auto_labling
        self.labling = self.getLablingJson(path_labling_json)
        self.db = TinyDb(path_transfer_conf)
        self.last_time = self._getLastTimestamps()
        self.last_time_hold:dict
        self.new_stat = []
        self.Nao:NaoApp = NaoAppLabling # type: ignore
        self.get_history = True
        if self.labling == {}:
            if self.auto_labling:
                self.firstUseInitialConfigAndNaoAsset()
        self._lablingNaoStation10m(DWDData1h.SUB_URL_RECENT)

    def connect(self):
        self.con = http.client.HTTPSConnection(self.host)
    
    def disconnect(self):
        try:
            self.con.close()
        except:
            pass

    def refreshConnection(self):
        self.disconnect()

    def confirmTransfer(self):
        self.last_time = self.last_time_hold
        self._putLastTimestamps()

    def exit(self):
        pass

    def getOpenDataFileInfo(self, url):
        self.connect()
        self.con.request(DWDData1h.NAME_GET, url) # type: ignore
        res = self.con.getresponse().read().decode(DWDData1h.NAME_UTF8)
        StructDWDParser1h.feed(res)
        self.disconnect()
        return(StructDWDHTMLContext.getStationData())

    def convertDatetime(self, time_str):
        return(datetime(
            year=int(time_str[0:4]),
            month=int(time_str[4:6]),
            day=int(time_str[6:8]),
            hour=int(time_str[8:10])
        ))
        #return(datetime.strptime(time_str, DWDData1h.TIME_FORMAT))

    def convertFloat(self, dat) -> float:
        try:
            return(float(dat))
        except:
            return(nan)

    def getDataFrameFromZIP(self, url):
        self.connect()
        self.con.request(DWDData1h.NAME_GET, url)
        data = self.con.getresponse().read()
        self.disconnect()
        data = zipfile.ZipFile(BytesIO(data),"r")
        idx = 0
        for file in data.infolist():
            if file.filename.split("_")[0] == "produkt":
                break
            idx += 1  
        data = pd.read_csv(
            BytesIO(data.open(data.infolist()[idx].filename).read()), 
            sep=DWDData1h.SEP,  
            converters={DWDData1h.DATETIME_COLUMN: self.convertDatetime}, 
            index_col=DWDData1h.DATETIME_COLUMN, 
            na_values='######'
        )
        return(data)

    def getMetaDataFrame(self, url):
        self.connect()
        self.con.request(DWDData1h.NAME_GET, url)
        data = re.sub("  +", ";", self.con.getresponse().read().decode("iso-8859-1")).replace("-", "").split("\r\n")
        self.disconnect()
        keynames = data[0].replace(" ", ";").split(";")
        ret = {}
        for key in keynames: ret[key] = []
        for row in data[1:]:
            dat = row.replace(" ", ";", 3).split(";")
            if len(dat) > len(keynames):
                dat = dat[:len(keynames)]
            elif len(dat) < len(keynames):
                continue
            for idx in range(len(dat)):
                ret[keynames[idx]].append(dat[idx])
        return(pd.DataFrame(ret)) # type: ignore

    def getLablingJson(self, path):
        try:
            data_file = open(path, "r")
            return(loads(data_file.read()))
        except:
            return({})

    def getTelegrafData(self, max_data_len:int=200000):
        self.last_time_hold = deepcopy(self.last_time)
        if self.labling[DWDData1h.NAME_INSTANCE] == {}:
            if self.auto_labling:
                self._lablingNaoStation10m(DWDData1h.SUB_URL_HISTORICAL)
            else:
                return([])
        return(self._get1hDWDTelegrafData(max_data=max_data_len))

    def _get1hDWDTelegrafData(self, max_data:int):
        data_return = []
        ext_data = data_return.extend
        for sub_url2 in self.last_time_hold[DWDData1h.DWD_HOURLY_SENSOR]:
            timestamps = self.last_time_hold[DWDData1h.DWD_HOURLY_SENSOR]
            info_history = {}
            info_recent = {}
            info_now = {}
            for station_id in timestamps[sub_url2]:
                if station_id == DWDData1h.NAME_DESCRIPTION:
                    continue
                if len(data_return) > max_data:
                    return(data_return)
                if timestamps[sub_url2][station_id][0] == 100000:
                    if len(info_history) == 0:
                        info_history = self.getOpenDataFileInfo(url=DWDData1h.URL_HOURLY_CLIMATE+sub_url2+DWDData1h.SUB_URL_HISTORICAL+"/")
                    if info_history.get(station_id) == None:
                        timestamps[sub_url2][station_id][0] = 1111111
                        continue
                    info_history[station_id][DWDData1h.NAME_NAME].sort()
                    data = []
                    data_add = data.append
                    for file_txt in info_history[station_id][DWDData1h.NAME_NAME]:
                        data_add(self.getDataFrameFromZIP(
                            url=DWDData1h.URL_HOURLY_CLIMATE+sub_url2+DWDData1h.SUB_URL_HISTORICAL+"/"+file_txt
                        ))
                    for idx in range(len(data)):
                        ext_data(self._getTelegrafDataFromFrame(
                            data=data[idx],
                            sub_url=DWDData1h.DWD_HOURLY_SENSOR,
                            sub_url2=sub_url2,
                            station_id=station_id
                        ))  
                    timestamps[sub_url2][station_id][0] = max(data[-1].index).timestamp()
                # elif  datetime.utcnow().timestamp() - timestamps[sub_url2][station_id][0] > 79200:
                #     if len(info_recent) == 0:
                #         info_recent = self.getOpenDataFileInfo(url=DWDData1h.URL_HOURLY_CLIMATE+sub_url2+DWDData1h.SUB_URL_RECENT+"/")
                #     if info_recent.get(station_id) == None:
                #         continue
                #     if timestamps[sub_url2][station_id][1] == info_recent[station_id][DWDData1h.NAME_TIME].timestamp():
                #         continue
                #     data = self.getDataFrameFromZIP(
                #         url=DWDData1h.URL_HOURLY_CLIMATE+sub_url2+DWDData1h.SUB_URL_RECENT+"/"+info_recent[station_id][DWDData1h.NAME_NAME]
                #     )
                #     data = data[datetime.fromtimestamp(timestamps[sub_url2][station_id][0])+timedelta(minutes=5):]
                #     ext_data(self._getTelegrafDataFromFrame(
                #         data=data,
                #         sub_url=DWDData1h.DWD_HOURLY_SENSOR,
                #         sub_url2=sub_url2,
                #         station_id=station_id
                #     ))  
                #     if len(data) != 0:
                #         timestamps[sub_url2][station_id][0] = max(data.index).timestamp()
                #     try:
                #         timestamps[sub_url2][station_id][1] = info_recent[station_id][DWDData1h.NAME_TIME].timestamp()
                #     except:
                #         pass
                # else:
                #     if len(info_now) == 0:
                #         info_now = self.getOpenDataFileInfo(url=DWDData1h.URL_HOURLY_CLIMATE+sub_url2+DWDData1h.SUB_URL_NOW+"/")
                #     if info_now.get(station_id) == None:
                #         continue
                #     if timestamps[sub_url2][station_id][1] == info_now[station_id][DWDData1h.NAME_TIME].timestamp():
                #         continue
                #     data = self.getDataFrameFromZIP(
                #         url=DWDData1h.URL_HOURLY_CLIMATE+sub_url2+DWDData1h.SUB_URL_NOW+"/"+info_now[station_id][DWDData1h.NAME_NAME]
                #     )
                #     data = data[datetime.fromtimestamp(timestamps[sub_url2][station_id][0])+timedelta(minutes=5):]
                #     ext_data(self._getTelegrafDataFromFrame(
                #         data=data,
                #         sub_url=DWDData1h.DWD_HOURLY_SENSOR,
                #         sub_url2=sub_url2,
                #         station_id=station_id
                #     ))  
                #     if len(data) != 0:
                #         timestamps[sub_url2][station_id][0] = max(data.index).timestamp()
                #     try:
                #         timestamps[sub_url2][station_id][1] = info_now[station_id][DWDData1h.NAME_TIME].timestamp()
                #     except:
                #         pass
        return(data_return)

    def _getTelegrafDataFromFrame(self, data:pd.DataFrame, sub_url, sub_url2, station_id):
        data_return = []
        add_data = data_return.extend
        asset = self.labling[DWDData1h.NAME_ASSET]
        try:
            instance = self.labling[DWDData1h.NAME_INSTANCE][station_id]
        except:
            self._lablingOneNaoStation10mFromNotInMeta(station_id,sub_url2)
            instance = self.labling[DWDData1h.NAME_INSTANCE][station_id]
        data[DWDData1h.NAME_INSTANCE] = instance
        data[DWDData1h.NAME_ASSET] = asset
        data[DWDData1h.NAME_TIME] = data.index.astype(int).astype(str)
        for sensor in self.labling[sub_url][sub_url2][DWDData1h.DWD_SENSOR]:
            data[DWDData1h.NAME_SERIES] = self.labling[sub_url][sub_url2][DWDData1h.DWD_SENSOR][sensor][DWDData1h.NAME_SERIES]
            data[sensor] = data[sensor].astype(str)
            add_data(
                list(data[DWDData1h.NAME_ASSET]+DWDData1h.FORMAT_TELEGRAFFRAMESTRUCT3+data[DWDData1h.NAME_INSTANCE]+" "+data[DWDData1h.NAME_SERIES]+"="+data[sensor]+" "+data[DWDData1h.NAME_TIME])
            )
        return(data_return)

    # def _buildTelegrafFrameForm(self, twin, instance, series, value,timestamp):
    #     return(DWDData1h.FORMAT_TELEGRAFFRAMESTRUCT%(twin,instance,series,value,timestamp))

    def _lablingNaoStation10m(self, sub_url):
        for sub_url2 in DWDData1h.SUB_URLS_CLIMATE:
            url = DWDData1h.URL_HOURLY_CLIMATE+sub_url2+sub_url+"/"
            info = self.getOpenDataFileInfo(url=url)
            meta = self.getMetaDataFrame(url=url+info[DWDData1h.NAME_DESCRIPTION])
            stations_ids_meta = set(meta[DWDData1h.META_STATION_ID_NAME])
            meta = meta.set_index(DWDData1h.META_STATION_ID_NAME)
            stations_to_create = stations_ids_meta.difference(set(self.labling[DWDData1h.NAME_INSTANCE].keys()))
            if len(stations_to_create) != 0:
                self._createInstancesInNao(meta, stations_to_create, self.labling[DWDData1h.NAME_ASSET])
            endpoints_to_create = stations_ids_meta.difference(set(self.labling[DWDData1h.DWD_HOURLY_SENSOR][sub_url2][DWDData1h.NAME_INSTANCE]))
            if len(endpoints_to_create) != 0:
                self._createEndpoints(endpoints_to_create, sub_url2, DWDData1h.DWD_HOURLY_SENSOR)
        self._setDefaultTime10m(DWDData1h.SUB_URL_HISTORICAL)
    
    def _lablingOneNaoStation10mFromNotInMeta(self, station, sub_url2):
        for url in DWDData1h.URLS_META_NO_FOUND:
            info = self.getOpenDataFileInfo(url=url)
            meta = self.getMetaDataFrame(url=url+info[DWDData1h.NAME_DESCRIPTION])
            meta = meta.set_index(DWDData1h.META_STATION_ID_NAME)
            if meta[DWDData1h.META_STATION_NAME].get(station):
                break
        stations_ids_meta = set([station])
        stations_to_create = stations_ids_meta.difference(set(self.labling[DWDData1h.NAME_INSTANCE].keys()))
        if len(stations_to_create) != 0:
            self._createInstancesInNao(meta, stations_to_create, self.labling[DWDData1h.NAME_ASSET]) # type: ignore
        endpoints_to_create = stations_ids_meta.difference(set(self.labling[DWDData1h.DWD_HOURLY_SENSOR][sub_url2][DWDData1h.NAME_INSTANCE]))
        if len(endpoints_to_create) != 0:
            self._createEndpoints(endpoints_to_create, sub_url2, DWDData1h.DWD_HOURLY_SENSOR)
        self._setDefaultTime10m(DWDData1h.SUB_URL_HISTORICAL)

    def _createEndpoints(self, instances, sub_url, types):
        for instance in instances:
            try:
                for sensor in self.labling[types][sub_url][DWDData1h.DWD_SENSOR]:
                    sleep(0.025)
                    self.Nao.postEnpointConifg(
                        conf={DWDData1h.NAME_URL: types+sub_url, DWDData1h.NAME_NAME: sensor},
                        _instance=self.labling[DWDData1h.NAME_INSTANCE][instance],
                        _series=self.labling[types][sub_url][DWDData1h.DWD_SENSOR][sensor][DWDData1h.NAME_SERIES],
                        _asset=self.labling[DWDData1h.NAME_ASSET]
                    )
                self.labling[types][sub_url][DWDData1h.NAME_INSTANCE].append(instance)
            except Exception as e:
                self._saveLabling()
                raise(e)
        self._saveLabling()

    def _setDefaultTime10m(self, sub_url):
        if self.last_time.get(DWDData1h.DWD_HOURLY_SENSOR) == None:
            self.last_time[DWDData1h.DWD_HOURLY_SENSOR] = {}
            for sub_url2 in DWDData1h.SUB_URLS_CLIMATE:
                self.last_time[DWDData1h.DWD_HOURLY_SENSOR][sub_url2] = {}
        set_time = False
        for sub_url2 in DWDData1h.SUB_URLS_CLIMATE:
            info_recent = self.getOpenDataFileInfo(url=DWDData1h.URL_HOURLY_CLIMATE+sub_url2+sub_url+"/")
            for station_id in info_recent:
                if self.last_time[DWDData1h.DWD_HOURLY_SENSOR][sub_url2].get(station_id) == None:
                    set_time = True
                    self.last_time[DWDData1h.DWD_HOURLY_SENSOR][sub_url2][station_id] = [100000,100000]
        if set_time:
            self._putLastTimestamps()

    def _createInstancesInNao(self, meta_data, station_ids, asset):
        for id_station in station_ids:
            try:
                instance = self.Nao.createInstance(
                    name=meta_data[DWDData1h.META_STATION_NAME][id_station],
                    description=DWDData1h.META_INSTANCE_DESCRIPTION%(
                        meta_data[DWDData1h.META_BUNDESLAND_NAME][id_station],
                        meta_data[DWDData1h.META_STATION_NAME][id_station]
                    ),  
                    _asset=asset,
                    geolocation=[
                        meta_data[DWDData1h.META_GEO_LAENGE][id_station], 
                        meta_data[DWDData1h.META_GEO_BREITE][id_station]
                    ]
                )[DWDData1h.NAME_ID_ID]
                inputs = self.Nao.sendInstanceInputMany([
                    [
                        id_station, 
                        self.labling[DWDData1h.NAME_INPUT][DWDData1h.META_STATION_ID_NAME],
                        instance
                    ],
                    [
                        meta_data[DWDData1h.META_STATION_HOHE][id_station], 
                        self.labling[DWDData1h.NAME_INPUT][DWDData1h.META_STATION_HOHE],
                        instance
                    ],
                    [
                        meta_data[DWDData1h.META_STATION_NAME][id_station], 
                        self.labling[DWDData1h.NAME_INPUT][DWDData1h.META_STATION_NAME],
                        instance
                    ],
                    [
                        meta_data[DWDData1h.META_BUNDESLAND_NAME][id_station], 
                        self.labling[DWDData1h.NAME_INPUT][DWDData1h.META_BUNDESLAND_NAME],
                        instance
                    ]
                ])
                self.labling[DWDData1h.NAME_INSTANCE][id_station] = instance
            except Exception as e:
                self._saveLabling()
                raise(e)
        self._saveLabling()

    def _saveLabling(self):
        json_file = open(self.labling_path, "w")
        json_file.writelines(dumps(self.labling))
        json_file.close()  

    def _getLastTimestamps(self):
        ''' {<id>: <timestamp>}'''
        try: 
            last_timestamps = self.db.getTinyTables(DWDData1h.NAME_TIME)[0]
        except:
            return(dict())
        return(last_timestamps)

    def _putLastTimestamps(self):
        ''' {<id>: <timestamp>}'''
        self.db.updateSimpleTinyTables(DWDData1h.NAME_TIME, self.last_time)

    def _getTransferChannels(self):
        conf_r = self.db.getTinyTables(DWDData1h.NAME_CONFIG)
        conf = {}
        for idx in conf_r:
            conf[idx[DWDData1h.NAME_NAME]] = idx
            conf[idx[DWDData1h.NAME_NAME]][DWDData1h.NAME_ID] = str(idx.doc_id)
        return(conf)

    def firstUseInitialConfigAndNaoAsset(self):
        if self.Nao == None:
            print("cant connect to NAO without NaoApp-Class, set NaoApp in init to use this method")
            return("cant connect to NAO without NaoApp-Class, set NaoApp in init to use this method")
        units_nao_raw = self.Nao.getUnits()[DWDData1h.NAME_RESULTS]
        units_nao = {}
        for dic_u in units_nao_raw: units_nao[dic_u[DWDData1h.NAME_NAME]] = dic_u[DWDData1h.NAME_ID_ID]
        for unit in DWDData1h.UNITS:
            if units_nao.get(unit) == None:
                units_nao[unit] = self.Nao.createUnit(unit)[DWDData1h.NAME_ID_ID]
        workspace = self.Nao.createWorkspace(
            name=DWDData1h.WORKSPACE_NAME
        )[DWDData1h.NAME_ID_ID]
        asset = self.Nao.createAsset(
            name=DWDData1h.ASSET_NAME, 
            _workspace=workspace, 
            description=DWDData1h.ASSET_DESCRIPTION,
            baseInterval="1h"
        )[DWDData1h.NAME_ID_ID]
        in_container = self.Nao.createInputcontainer(
            name=DWDData1h.META_CONTAINER,
            _asset=asset,
            description="",
            color=DWDData1h.COLOR_NAO
        )[DWDData1h.NAME_ID_ID]
        input_station_name = self.Nao.createInput(
            name = DWDData1h.META_STATION_NAME,
            _asset=asset,
            description=DWDData1h.META_STATION_DESCRIPTION
        )[DWDData1h.NAME_ID_ID]
        input_station_id = self.Nao.createInput(
            name = DWDData1h.META_STATION_ID_NAME,
            _asset=asset,
            description=DWDData1h.META_STATION_ID_DESCRIPTION
        )[DWDData1h.NAME_ID_ID]
        input_station_city = self.Nao.createInput(
            name = DWDData1h.META_BUNDESLAND_NAME,
            _asset=asset,
            description=DWDData1h.META_BUNDESLAND_DESCRIPTION
        )[DWDData1h.NAME_ID_ID]
        input_station_hohe = self.Nao.createInput(
            name = DWDData1h.META_STATION_HOHE,
            _asset=asset,
            description=DWDData1h.META_STATION_HOHE_DESCRIPTION
        )[DWDData1h.NAME_ID_ID]
        self.Nao.patchIpuntsInputcontainer(
            _inputcontainer=in_container,
            _inputs=[input_station_name, input_station_id, input_station_city, input_station_hohe]
        )
        new_struct = DWDData1h.DWD_STRUCT
        new_struct[DWDData1h.NAME_ASSET] = asset
        new_struct[DWDData1h.NAME_INPUT] = {
            DWDData1h.META_STATION_HOHE: input_station_hohe,
            DWDData1h.META_STATION_ID_NAME: input_station_id,
            DWDData1h.META_BUNDESLAND_NAME: input_station_city,
            DWDData1h.META_STATION_NAME: input_station_name
        }
        struct_10m = new_struct[DWDData1h.DWD_HOURLY_SENSOR]
        for group10m in struct_10m:
            id_group = self.Nao.createPath(
                name=struct_10m[group10m][DWDData1h.NAME_NAME],
                _asset=asset,
                description="",
                color=struct_10m[group10m][DWDData1h.COLOR]
            )[DWDData1h.NAME_ID_ID]
            struct_sensor = struct_10m[group10m][DWDData1h.DWD_SENSOR]
            self._creatSeries(struct_sensor, asset, units_nao, id_group)
        # struct_1d = new_struct[DWDData1h.DWD_GRADTAG]
        # for group1d in struct_1d:
        #     id_group = self.Nao.createPath(
        #         name=struct_1d[group1d][DWDData1h.NAME_NAME],
        #         _asset=asset,
        #         description="",
        #         color=struct_1d[group1d][DWDData1h.COLOR]
        #     )[DWDData1h.NAME_ID_ID]
        #     struct_sensor = struct_1d[group1d][DWDData1h.DWD_SENSOR]
        #     self._creatSeries(struct_sensor, asset, units_nao, id_group)
        json_file = open(self.labling_path, "w")
        json_file.writelines(dumps(new_struct))
        json_file.close()
        json_file = open(self.conf_path, "w")
        last_time_file = {
            DWDData1h.NAME_CONFIG: {},
            DWDData1h.NAME_TIME: {DWDData1h.DWD_HOURLY_SENSOR:{}, DWDData1h.DWD_GRADTAG:{}}
        }
        for sub_url in DWDData1h.SUB_URLS_CLIMATE:
            last_time_file[DWDData1h.NAME_TIME][DWDData1h.DWD_HOURLY_SENSOR][sub_url] = {}
        # for sub_url in DWDData1h.SUB_URLS_GRADTAG:
        #     last_time_file[DWDData1h.DWD_GRADTAG][sub_url] = {}
        last_time_file[DWDData1h.NAME_TIME] = {"1":last_time_file[DWDData1h.NAME_TIME]}
        json_file.writelines(dumps(last_time_file))
        self.labling = self.getLablingJson(self.labling_path)
        self.db = TinyDb(self.conf_path)

    def _creatSeries(self, struct_sensor, asset, units_nao, id_group):
        if self.Nao == None:
            print("cant connect to NAO without NaoApp-Class, set NaoApp in init to use this method")
            return("cant connect to NAO without NaoApp-Class, set NaoApp in init to use this method")
        for sensor in struct_sensor:
            struct_sensor[sensor][DWDData1h.NAME_SERIES] = self.Nao.createSeries(
                type="sensor",
                name=struct_sensor[sensor][DWDData1h.NAME_NAME],
                description=struct_sensor[sensor][DWDData1h.NAME_DESCRIPTION],
                _asset=asset,
                _unit=units_nao[struct_sensor[sensor][DWDData1h.NAME_UNIT]],
                max=struct_sensor[sensor][DWDData1h.NAME_MAX_VALUE],
                min=struct_sensor[sensor][DWDData1h.NAME_MIN_VALUE],
                color=struct_sensor[sensor][DWDData1h.NAME_COLOR],
                _part=id_group,
                fill="null",
                fillValue=None
            )[DWDData1h.NAME_ID_ID]


