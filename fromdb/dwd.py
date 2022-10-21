import http.client
from html.parser import HTMLParser
from datetime import datetime
from naoconnect.NaoApp import NaoApp
import re
import zipfile
from io import BytesIO
import pandas as pd
from copy import copy
from naoconnect.Param import Param, Labling
from json import dumps

class ParamDWD():
    DWD_SENSOR = "sensor"
    DWD_10MIN_SENSOR = "10min_sensor"
    DWD_1D_GRADTAG = "1d_gradtag"

class _StructDWDHTMLContext(Param):
    default_time_format_struct_file_infos = "%d-%b-%Y %H:%M"
    string_format_beween_time = r'  +.+  +'
    string_format_to_delet = '  +'
    data_format = '.zip'
    description_station = 'Beschreibung_Stationen'

    def __init__(self):
        self.station_data = {}
        self.last_station = False

    def handle_data(self, data:str):
        if _StructDWDHTMLContext.data_format in data:
            numbers = re.findall(r'\d+', data)
            for num in numbers:
                if len(num) == 5:
                    self.last_station = num
                    self.station_data[self.last_station] = {_StructDWDHTMLContext.NAME_NAME: data}
        elif _StructDWDHTMLContext.description_station in data:
            self.station_data[_StructDWDHTMLContext.NAME_DESCRIPTION] = data
        else:
            if self.last_station:
                self.station_data[self.last_station][_StructDWDHTMLContext.NAME_TIME] = datetime.strptime(
                    re.sub(_StructDWDHTMLContext.string_format_to_delet, '',re.findall(_StructDWDHTMLContext.string_format_beween_time, data)[0]),
                    _StructDWDHTMLContext.default_time_format_struct_file_infos
                ) 
                self.last_station = False
    
    def getStationData(self):
        data = copy(self.station_data)
        self.station_data = {}
        self.last_station = False
        return(data)

StructDWDHTMLContext = _StructDWDHTMLContext()

class _StructDWDHTMLParser(HTMLParser):

    def handle_data(self, data):
        StructDWDHTMLContext.handle_data(data)  # type: ignore

StructDWDParser = _StructDWDHTMLParser()

class DWDData(Param):
    SEP = ";"
    DATETIME_COLUMN = "MESS_DATUM"

    def __init__(self, host:str="opendata.dwd.de"):
        self.host = host

    def connect(self):
        self.con = http.client.HTTPSConnection(self.host)
    
    def disconnect(self):
        try:
            self.con.close()
        except:
            pass

    def getOpenDataFileInfo(self, url):
        self.connect()
        self.con.request(DWDData.NAME_GET, url)
        res = self.con.getresponse().read().decode(DWDData.NAME_UTF8)
        StructDWDParser.feed(res)
        self.disconnect()
        return(StructDWDHTMLContext.getStationData())

    def getDataFrameFromZIP(self, url):
        self.connect()
        self.con.request(DWDData.NAME_GET, url)
        data = self.con.getresponse().read()
        self.disconnect()
        data = zipfile.ZipFile(BytesIO(data),"r")
        data = pd.read_csv(BytesIO(data.open(data.infolist()[0].filename).read()), sep=DWDData.SEP)
        data.index = pd.DatetimeIndex(pd.to_datetime(data[DWDData.DATETIME_COLUMN].astype("string")))
        return(data)


    def getMetaDataFrame(self, url):
        self.connect
        self.con.request(DWDData.NAME_GET, url)
        data = re.sub("  +", ";", self.con.getresponse().read().decode("iso-8859-1")).replace("-", "").split("\r\n")
        self.disconnect
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
        return(pd.DataFrame(ret))


class DWDNaoInitial(DWDData, Labling, ParamDWD):

    WORKSPACE_NAME = "DWD-Klimadaten"
    ASSET_NAME = "Wetterstationen"
    ASSET_DESCRIPTION = "Wetterstationen vom Deutschen Wetterdienst (DWD), die Daten werden aus der Open-Data-Schnittstelle von DWD (opendata.dwd.de) bezogen."
    META_CONTAINER = "Stationsdaten"
    META_STATION_NAME = "Stationsname"
    META_STATION_DESCRIPTION = "Name der Station aus den DWD-Metadaten"
    META_BUNDESLAND_NAME = "Bundesland"
    META_BUNDESLAND_DESCRIPTION = "Bundesland in welchem sich die Station befindet"
    META_STATION_ID_NAME = "Station-ID"
    META_STATION_ID_DESCRIPTION = "Stations-ID für die Zuordnung beim DWD"
    COLOR_NAO = "#49C2F2"
    DWD_STRUCT = {
        "10min_sensor": {
            "/climate_environment/CDC/observations_germany/climate/10_minutes/wind/": {
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
            "/climate_environment/CDC/observations_germany/climate/10_minutes/solar/":{
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
            "/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/":{
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
            "/climate_environment/CDC/observations_germany/climate/10_minutes/precipitation/": {
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
        "1d_gradtag": {
            "/climate_environment/CDC/derived_germany/techn/daily/heating_degreedays/hdd_3807/": {
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
        "instance": [],
        "asset": ""
    }
    UNITS = ["m/s", "J/cm²", "°", "h", "hPa", "°C", "%", "min", "mm", "Kd"]

    def __init__(self, NaoApp:NaoApp, host:str="opendata.dwd.de"):
        self.host = host
        self.Nao = NaoApp

    def setConfig(self, path_name):
        units_nao_raw = self.Nao.getUnits()[DWDNaoInitial.NAME_RESULTS]
        units_nao = {}
        for dic_u in units_nao_raw: units_nao[dic_u[DWDNaoInitial.NAME_NAME]] = dic_u[DWDNaoInitial.NAME_ID_ID]
        for unit in DWDNaoInitial.UNITS:
            if units_nao.get(unit) == None:
                units_nao[unit] = self.Nao.createUnit(unit)[DWDNaoInitial.NAME_ID_ID]
        workspace = self.Nao.createWorkspace(
            name=DWDNaoInitial.WORKSPACE_NAME
        )[DWDNaoInitial.NAME_ID_ID]
        asset = self.Nao.createAsset(
            name=DWDNaoInitial.ASSET_NAME, 
            _workspace=workspace, 
            description=DWDNaoInitial.ASSET_DESCRIPTION,
            baseInterval="10m"
        )[DWDNaoInitial.NAME_ID_ID]
        in_container = self.Nao.createInputcontainer(
            name=DWDNaoInitial.META_CONTAINER,
            _asset=asset,
            description="",
            color=DWDNaoInitial.COLOR_NAO
        )[DWDNaoInitial.NAME_ID_ID]
        input_station_name = self.Nao.createInput(
            name = DWDNaoInitial.META_STATION_NAME,
            _asset=asset,
            description=DWDNaoInitial.META_STATION_DESCRIPTION
        )[DWDNaoInitial.NAME_ID_ID]
        input_station_id = self.Nao.createInput(
            name = DWDNaoInitial.META_STATION_ID_NAME,
            _asset=asset,
            description=DWDNaoInitial.META_STATION_ID_DESCRIPTION
        )[DWDNaoInitial.NAME_ID_ID]
        input_station_city = self.Nao.createInput(
            name = DWDNaoInitial.META_BUNDESLAND_NAME,
            _asset=asset,
            description=DWDNaoInitial.META_BUNDESLAND_DESCRIPTION
        )[DWDNaoInitial.NAME_ID_ID]
        self.Nao.patchIpuntsInputcontainer(
            _inputcontainer=in_container,
            _inputs=[input_station_name, input_station_id, input_station_city]
        )
        new_struct = DWDNaoInitial.DWD_STRUCT
        new_struct[DWDNaoInitial.ASSET_NAME] = asset
        struct_10m = new_struct[DWDNaoInitial.DWD_10MIN_SENSOR]
        for group10m in struct_10m:
            id_group = self.Nao.createPath(
                name=struct_10m[group10m][DWDNaoInitial.NAME_NAME],
                _asset=asset,
                description="",
                color=struct_10m[group10m][DWDNaoInitial.COLOR]
            )[DWDNaoInitial.NAME_ID_ID]
            struct_sensor = struct_10m[group10m][DWDNaoInitial.DWD_SENSOR]
            for sensor in struct_sensor:
                struct_sensor[sensor][DWDNaoInitial.NAME_SERIES] = self.Nao.createSeries(
                    type="sensor",
                    name=struct_sensor[sensor][DWDNaoInitial.NAME_NAME],
                    description=struct_sensor[sensor][DWDNaoInitial.NAME_DESCRIPTION],
                    _asset=asset,
                    _unit=units_nao[struct_sensor[sensor][DWDNaoInitial.NAME_UNIT]],
                    max=struct_sensor[sensor][DWDNaoInitial.NAME_MAX_VALUE],
                    min=struct_sensor[sensor][DWDNaoInitial.NAME_MIN_VALUE],
                    color=struct_sensor[sensor][DWDNaoInitial.NAME_COLOR],
                    _part=id_group,
                    fill="null",
                    fillValue=None
                )[DWDNaoInitial.NAME_ID_ID]
        struct_1d = new_struct[DWDNaoInitial.DWD_1D_GRADTAG]
        for group1d in struct_1d:
            id_group = self.Nao.createPath(
                name=struct_1d[group1d][DWDNaoInitial.NAME_NAME],
                _asset=asset,
                description="",
                color=struct_1d[group1d][DWDNaoInitial.COLOR]
            )[DWDNaoInitial.NAME_ID_ID]
            struct_sensor = struct_1d[group1d][DWDNaoInitial.DWD_SENSOR]
            for sensor in struct_sensor:
                struct_sensor[sensor][DWDNaoInitial.NAME_SERIES] = self.Nao.createSeries(
                    type="sensor",
                    name=struct_sensor[sensor][DWDNaoInitial.NAME_NAME],
                    description=struct_sensor[sensor][DWDNaoInitial.NAME_DESCRIPTION],
                    _asset=asset,
                    _unit=units_nao[struct_sensor[sensor][DWDNaoInitial.NAME_UNIT]],
                    max=struct_sensor[sensor][DWDNaoInitial.NAME_MAX_VALUE],
                    min=struct_sensor[sensor][DWDNaoInitial.NAME_MIN_VALUE],
                    color=struct_sensor[sensor][DWDNaoInitial.NAME_COLOR],
                    _part=id_group,
                    fill="null",
                    fillValue=None
                )[DWDNaoInitial.NAME_ID_ID] 
        json_file = open(path_name, "w")
        json_file.writelines(dumps(new_struct))
        json_file.close()
