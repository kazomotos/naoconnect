try:
    import pyodbc
except:
    pass
from re import A
import naoconnect.time2 as time2
import calendar
from copy import deepcopy
from time import time
from naoconnect.TinyDb import TinyDb
from naoconnect.Param import Param

# https://github.com/mkleehammer/pyodbc/wiki/Install


class Aquotec(Param):
    NAME_RM360              = "RM360"
    NAME_UG07               = "UG07"
    NAME_WMZ                = "WMZ"
    NAME_TABLENAME          = "tablename"
    NAME_COLUMNNAME         = "columnname"
    NAME_COLUMNS            = "columns"
    NAME_STATION_TYPE       = "station_type"
    NAME_DB_CUSTOMER        = "aqotec_Kunden"
    NAME_TABLE_CUSTOMER     = "Tbl_Abnehmer"
    LASTTIMESAVESEC         = 30
    RESET_TIME              = 1514761200
    SECTONANO               = 1000000000

    def __init__ (self, host, port, user, password, database=None, tds_version=7.4, driver='{FreeTDS}', tiny_db_name="aqotec_meta.json", sinc=False):
        self.host = host
        self.port = str(port)
        self.db = TinyDb(tiny_db_name)
        self.user = user
        self.password = password
        self.database = database
        self.version = str(tds_version)
        self.lasttimestamps = self._getLastTimestamps()
        self.marker_timestamps = None
        self.transfere = self._getTransferChannels()
        self.confirm_time = time()
        self.driver = driver
        self.__con = None
        self.__cur = None
        self.pauser = []
        self.sinc = sinc
        self.connectToDb()

    def connectToDb(self):
        if self.database == None:
            dbase = ""
        else:
            dbase = ";DATABASE=" + self.database 
        self.__con = pyodbc.connect("DRIVER=" + self.driver +
                                    ";SERVER=" + self.host +
                                    ";PORT=" + self.port +
                                    dbase +
                                    ";UID=" + self.user +
                                    ";PWD=" + self.password +
                                    ";TDS_Version=" + self.version + ";")
    
    def disconnetToDb(self):
        try:
            self.__con.close()
        except:
            pass

    def __buildCursor(self):
        self.__cur = self.__con.cursor()

    def getTables(self):
        self.connectToDb()
        self.__buildCursor()
        self.__cur.execute("SELECT TABLE_NAME FROM information_schema.tables")
        fetch = self.__cur.fetchall()
        self.disconnetToDb()
        return(fetch)

    def getCheckData(self, table_str, values_list_str, check_limit=5,time_column="DP_Zeitstempel"):
        self.connectToDb()
        values = time_column
        for i in values_list_str:
            values += ", " + i 
        self.__buildCursor()
        ret_data = []
        add_value = ret_data.append
        try:     
            for row in self.__cur.execute(" SELECT TOP(" + str(check_limit) + ") " + values +" FROM " + table_str):
                add_value(list(row[1:]))
        except pyodbc.ProgrammingError as e:
            print(e)
            return(None)
        self.disconnetToDb()
        return(ret_data)
    
    def getStationsInfos(self):
        self.connectToDb()
        self.__buildCursor()
        ret_data = {}
        keys = ["AnID", "AnName", "AnImName", "AnAdresse", "AnTelefonummer1", "AnkW_VV", "AnOrt", "AnDN"]
        values = "AnID, AnName, AnImName, AnAdresse, AnTelefonummer1, AnkW_VV, AnOrt, AnDN"  
        for row in self.__cur.execute(" SELECT " + values +" FROM " + Aquotec.NAME_TABLE_CUSTOMER):
            ret_data[str(row[0])] = dict(zip(keys, row))
        self.disconnetToDb()
        return(ret_data)

    def _buildTelegrafFrameForm(self, twin, instance, series):
        return(Aquotec.FORMAT_TELEGRAFFRAMESTRUCT2%(twin,instance,series)+"%f %.0f")

    def getTelegrafData(self, max_data_len=30000, maxtimerange=3600*24*5, time_column="DP_Zeitstempel"):
        ''' [ '<twin>,instance=<insatance>, <measurement>=<value> <timestamp>' ] '''
        self.connectToDb()
        ret_data = []
        ret_data_add = ret_data.append
        # sql cursor
        self.__buildCursor()
        data_len = 0
        self.marker_timestamps = deepcopy(self.lasttimestamps)
        for index in range(len(self.transfere)):
            if self.sinc:
                if self.transfere[index][Aquotec.NAME_TABLENAME] in self.pauser:
                    continue
            # set timesamp for new tables
            if self.marker_timestamps == {}:
                self.marker_timestamps[str(index)] = Aquotec.RESET_TIME
            if self.marker_timestamps.get(str(index)) == None:
                self.marker_timestamps[str(index)] = Aquotec.RESET_TIME
            # build sql time formate and set max timerange
            aftertimesql = str(tuple(time2.gmtime(float(self.marker_timestamps.get(str(index))))[0:6]))[0:-1] + ", 0, 0)"
            aftertimesql2 = str(tuple(time2.gmtime(float(self.marker_timestamps.get(str(index)))+maxtimerange)[0:6]))[0:-1] + ", 0, 0)"
            # build sql formated column list
            values = time_column
            telegraf_form_list = []
            telegraf_form_list_add = telegraf_form_list.append
            for column_dic in self.transfere[index][Aquotec.NAME_COLUMNS]:
                values += ", " + column_dic[Aquotec.NAME_COLUMNNAME]
                # build telegraf frame formate
                telegraf_form_list_add(self._buildTelegrafFrameForm(
                    twin=column_dic[Aquotec.NAME_TELEGRAF][0],
                    instance=column_dic[Aquotec.NAME_TELEGRAF][1],
                    series=column_dic[Aquotec.NAME_TELEGRAF][2]
                ))
            breaker = False
            while 1==1:  
                no_object = False
                try:
                    # get data from db
                    timestamp_list = []
                    timestamp_list_add = timestamp_list.append
                    for row in self.__cur.execute(" SELECT " + values +
                                            " FROM " + self.transfere[index][Aquotec.NAME_TABLENAME] +
                                            " WHERE " + time_column + 
                                            " > DATETIME2FROMPARTS" + aftertimesql + 
                                            " AND  " + time_column + 
                                            " < DATETIME2FROMPARTS" + aftertimesql2):
                        timestamp_sec = calendar.timegm(row[0].utctimetuple())
                        timestamp_list_add(timestamp_sec)
                        index_val = 0
                        # form data for telegraf
                        for col in list(row[1:]):
                            if col != None:
                                ret_data_add(telegraf_form_list[index_val]%(col, timestamp_sec*Aquotec.SECTONANO))
                                data_len += 1
                            index_val += 1
                    try:
                        self.marker_timestamps[str(index)] = max(timestamp_list)
                    except:
                        print("no_timestamp", self.transfere[index][Aquotec.NAME_TABLENAME], aftertimesql)
                        if self.sinc:
                            self.pauser.append(self.transfere[index][Aquotec.NAME_TABLENAME])
                except pyodbc.ProgrammingError as e:
                    print(e)
                    if "Invalid column name"    in str(e):
                        columname = str(e).split("Invalid column name")[1].split("\'")[1]
                        values = values.replace(", "+columname, "")
                        continue
                    if "Invalid object name" in str(e):
                        tablename = str(e).split("Invalid object name")[1].split("\'")[1]
                        no_object=True
                        print(tablename)
                    if "Ungültiger Objektname" in str(e):
                        tablename = str(e).split("Ungültiger Objektname")[1].split("\'")[1]
                        no_object=True
                        print(tablename)
                if timestamp_list != [] or no_object:
                    break
                elif breaker or int(self.marker_timestamps.get(str(index))) > time2.time()-3600*24*2:
                    break
                else:
                    # serach for first time stamp in database
                    new_aftertime = []
                    new_afteradd = new_aftertime.append
                    for row in self.__cur.execute(" SELECT " + values +
                            " FROM " + self.transfere[index][Aquotec.NAME_TABLENAME] +
                            " WHERE " + time_column + 
                            " > DATETIME2FROMPARTS" + aftertimesql):
                        new_afteradd(row[0].timestamp())
                    if new_aftertime == []:
                        break
                    aftertimesql = str(tuple(time2.gmtime(min(new_aftertime)-10)[0:6]))[0:-1] + ", 0, 0)"
                    aftertimesql2 = str(tuple(time2.gmtime((min(new_aftertime)-10)+maxtimerange)[0:6]))[0:-1] + ", 0, 0)"
                    breaker = True
            if data_len >= max_data_len:
                break
        self.disconnetToDb()
        return(ret_data)

    def _getTransferChannels(self):
        ''' 
        [{}] 
        '''
        return(self.db.getTinyTables(Aquotec.NAME_TRANSFERCHANNELS))

    def _putTransferChannel(self, tablename, columns_series_dict, instance, asset, station_type, interval):
        columns = []
        columns_add = columns.append
        for column in columns_series_dict:
            columns_add({
                Aquotec.NAME_COLUMNNAME: column,
                Aquotec.NAME_TELEGRAF: [
                    asset,
                    instance,
                    columns_series_dict[column]
                ]
            })
        data = {
            Aquotec.NAME_TABLENAME: tablename,
            Aquotec.NAME_COLUMNS: columns,
            Aquotec.NAME_STATION_TYPE: station_type,
            Aquotec.NAME_INTERVAL: interval
        }
        self.db.putTinyTables(Aquotec.NAME_TRANSFERCHANNELS, data)
        self.transfere = self._getTransferChannels()


    def _getLastTimestamps(self):
        ''' {<id>: <timestamp>}'''
        try:
            last_timestamps = self.db.getTinyTables(Aquotec.NAME_LASTTIME)[0]
        except:
            return(dict())
        return(last_timestamps)

    def _putLastTimestamps(self):
        ''' {<id>: <timestamp>}'''
        self.db.updateSimpleTinyTables(Aquotec.NAME_LASTTIME, self.lasttimestamps)

    def confirmTransfer(self):
        self.lasttimestamps = self.marker_timestamps
        if time()-self.confirm_time >= Aquotec.LASTTIMESAVESEC:
            self.confirm_time = time()
            self._putLastTimestamps()

    def exit(self):
        self.disconnetToDb()
        self._putLastTimestamps()

    def refreshConnection(self):
        print("refreshConnection aqotec")
        try:
            self.disconnetToDb()
        except:
            pass
        self.__con = self.connectToDb()


class AqotecLabling():
    sensorsS_HAST = [
        {'rm360': 'DP_0Wert', 'ug7': 'DP_3Wert', 'name': 'Außentemperatur', 'description': 'Wärmeübergabestation HK0 Fühler T10 Außentemperatur', 'unit': '°C', 'asset': 'HAST', 'component': 'HK-0', 'color': '#44b848', 'max': 50, 'min': -40, 'type': 'sensors'
        },
        {'rm360': 'DP_1Wert', 'ug7': 'DP_4Wert', 'name': 'Rücklauftemperatur pri.', 'description': 'Wärmeübergabestation HK0 Fühler T11 RL Prim', 'unit': '°C', 'asset': 'HAST', 'component': 'HK-0', 'color': '#3e30ba', 'max': 100, 'min': 5, 'type': 'sensors'
        },
        {'rm360': 'DP_2Wert', 'ug7': 'DP_5Wert', 'name': 'Vorlauftemperatur sec.', 'description': 'Wärmeübergabestation HK0 Fühler T12 sek VL', 'unit': '°C', 'asset': 'HAST', 'component': 'HK-0', 'color': '#e0315a', 'max': 100, 'min': 5, 'type': 'sensors'
        },
        {'rm360': 'DP_3Wert', 'ug7': 'DP_6Wert', 'name': 'Rücklauftemperatur sec.', 'description': 'Wärmeübergabestation HK0 Fühler T15 sek RL', 'unit': '°C', 'asset': 'HAST', 'component': 'HK-0', 'color': '#5433a1', 'max': 100, 'min': 5, 'type': 'sensors'
        },
        {'rm360': 'DP_4Wert', 'ug7': 'DP_7Wert', 'name': 'Sollvorlauftemperatur', 'description': 'Wärmeübergabestation HK0 Solltemperatur', 'unit': '°C', 'asset': 'HAST', 'component': 'HK-0', 'color': '#2e0404', 'max': 100, 'min': 0, 'type': 'setpoints'
        },
        {'rm360': 'DP_7Wert', 'ug7': 'DP_50Wert', 'name': 'Ventilstellung', 'description': 'Wärmeübergabestation HK0 errechnete Ventilstellung', 'unit': '%', 'asset': 'HAST', 'component': 'HK-0', 'color': '#000000', 'max': 100, 'min': 0, 'type': 'sensors'
        },
        {'rm360': 'DP_9Wert', 'ug7': 'DP_8Wert', 'name': 'RL-Temperaturbegrenzung', 'description': 'Wärmeübergabestation HK0 aktuell geltende RL Begrenzung', 'unit': '°C', 'asset': 'HAST', 'component': 'HK-0', 'color': '#080b40', 'max': 100, 'min': 10, 'type': 'sensors'
        },
        {'rm360': 'DP_16Wert', 'ug7': 'DP_12Wert', 'name': 'Sollvorlauftemperatur', 'description': 'HK1 VL-Solltemperatur', 'unit': '°C', 'asset': 'HAST', 'component': 'HK-1', 'color': '#590202', 'max': 100, 'min': 0, 'type': 'setpoints'
        },
        {'rm360': None, 'ug7': 'DP_11Wert', 'name': 'Vorlauftemperatur', 'description': 'HK1 VL-Temperatur', 'unit': '°C', 'asset': 'HAST', 'component': 'HK-1', 'color': '#c41414', 'max': 100, 'min': 5, 'type': 'sensors'
        },
        {'rm360': 'DP_34Wert', 'ug7': None, 'name': 'Rücklauftemperatur', 'description': 'HK1 RL Fühlerwert', 'unit': '°C', 'asset': 'HAST', 'component': 'HK-1', 'color': '#403bd1', 'max': 100, 'min': 5, 'type': 'sensors'
        },
        {'rm360': 'DP_35Wert', 'ug7': 'DP_20Wert', 'name': 'Vorlauftemperatur', 'description': 'HK2 Fühler T16 VL', 'unit': '°C', 'asset': 'HAST', 'component': 'HK-2', 'color': '#c41414', 'max': 100, 'min': 5, 'type': 'sensors'
        },
        {'rm360': 'DP_36Wert', 'ug7': 'DP_21Wert', 'name': 'Sollvorlauftemperatur', 'description': 'HK2 VL-Solltemperatur', 'unit': '°C', 'asset': 'HAST', 'component': 'HK-2', 'color': '#590202', 'max': 100, 'min': 0, 'type': 'setpoints'
        },
        {'rm360': 'DP_59Wert', 'ug7': None, 'name': 'Rücklauftemperatur', 'description': 'HK2 RL Fühlerwert', 'unit': '°C', 'asset': 'HAST', 'component': 'HK-2', 'color': '#403bd1', 'max': 100, 'min': 5, 'type': 'sensors'
        },
        {'rm360': 'DP_60Wert', 'ug7': 'DP_29Wert', 'name': 'Vorlauftemperatur', 'description': 'HK3 Fühler 3TMP VL', 'unit': '°C', 'asset': 'HAST', 'component': 'HK-3', 'color': '#c41414', 'max': 100, 'min': 5, 'type': 'sensors'
        },
        {'rm360': 'DP_61Wert', 'ug7': 'DP_30Wert', 'name': 'Sollvorlauftemperatur', 'description': 'HK3 VL-Solltemperatur', 'unit': '°C', 'asset': 'HAST', 'component': 'HK-3', 'color': '#590202', 'max': 100, 'min': 0, 'type': 'setpoints'
        },
        {'rm360': 'DP_94Wert', 'ug7': None, 'name': 'Rücklauftemperatur', 'description': 'HK3 RL Fühlerwert', 'unit': '°C', 'asset': 'HAST', 'component': 'HK-3', 'color': '#403bd1', 'max': 100, 'min': 5, 'type': 'sensors'
        },
        {'rm360': 'DP_96Wert', 'ug7': None, 'name': 'Vorlauftemperatur', 'description': 'HK4 Fühler 4TMP VL', 'unit': '°C', 'asset': 'HAST', 'component': 'HK-4', 'color': '#c41414', 'max': 100, 'min': 5, 'type': 'sensors'
        },
        {'rm360': 'DP_97Wert', 'ug7': None, 'name': 'Sollvorlauftemperatur', 'description': 'HK4 VL-Solltemperatur', 'unit': '°C', 'asset': 'HAST', 'component': 'HK-4', 'color': '#590202', 'max': 100, 'min': 0, 'type': 'setpoints'
        },
        {'rm360': 'DP_128Wert', 'ug7': None, 'name': 'Rücklauftemperatur', 'description': 'HK4 RL Fühlerwert', 'unit': '°C', 'asset': 'HAST', 'component': 'HK-4', 'color': '#403bd1', 'max': 100, 'min': 5, 'type': 'sensors'
        },
        {'rm360': 'DP_130Wert', 'ug7': 'DP_38Wert', 'name': 'Temperatur oben', 'description': 'Boiler -SpKr1 Speicherfühler oben T13', 'unit': '°C', 'asset': 'HAST', 'component': 'TES-1', 'color': '#b04105', 'max': 100, 'min': 5, 'type': 'sensors'
        },
        {'rm360': 'DP_131Wert', 'ug7': 'DP_39Wert', 'name': 'Temperatur unten', 'description': 'Boiler -SpKr1 Speicherfühler unten T14', 'unit': '°C', 'asset': 'HAST', 'component': 'TES-1', 'color': '#6305b0', 'max': 100, 'min': 5, 'type': 'sensors'
        },
        {'rm360': 'DP_136Wert', 'ug7': 'DP_40Wert', 'name': 'Sollvorlauftemperatur', 'description': 'Boiler -SpKr1 Ladesolltemperatur', 'unit': '°C', 'asset': 'HAST', 'component': 'TES-1', 'color': '#590a0a', 'max': 100, 'min': 0, 'type': 'setpoints'
        },
        {'rm360': 'DP_143Wert', 'ug7': 'DP_43Wert', 'name': 'Temperatur oben', 'description': 'Puffer -SpKr2 Speicherfühler oben T23', 'unit': '°C', 'asset': 'HAST', 'component': 'TES-2', 'color': '#b04105', 'max': 100, 'min': 5, 'type': 'sensors'
        },
        {'rm360': 'DP_144Wert', 'ug7': 'DP_44Wert', 'name': 'Temperatur unten', 'description': 'Puffer -SpKr2 Speicherfühler unten T24', 'unit': '°C', 'asset': 'HAST', 'component': 'TES-2', 'color': '#6305b0', 'max': 100, 'min': 5, 'type': 'sensors'
        },
        {'rm360': 'DP_145Wert', 'ug7': 'DP_48Wert', 'name': 'Einschalttemperatur oben', 'description': 'Puffer -SpKr2 var Einschalttemp Oben', 'unit': '°C', 'asset': 'HAST', 'component': 'TES-2', 'color': '#401a1a', 'max': 100, 'min': 0, 'type': 'sensors'
        },
        {'rm360': 'DP_146Wert', 'ug7': None, 'name': 'Solltemperatur oben', 'description': 'Puffer -SpKr2 var Solltemp oben', 'unit': '°C', 'asset': 'HAST', 'component': 'TES-2', 'color': '#521f2d', 'max': 100, 'min': 0, 'type': 'setpoints'
        },
        {'rm360': 'DP_147Wert', 'ug7': 'DP_49Wert', 'name': 'Einschalttemperatur unten', 'description': 'Puffer -SpKr2 var Einschalttemp Unten', 'unit': '°C', 'asset': 'HAST', 'component': 'TES-2', 'color': '#0e173b', 'max': 100, 'min': 0, 'type': 'sensors'
        },
        {'rm360': 'DP_148Wert', 'ug7': None, 'name': 'Solltemperatur unten', 'description': 'Puffer -SpKr2 var Solltemp unten', 'unit': '°C', 'asset': 'HAST', 'component': 'TES-2', 'color': '#361f52', 'max': 100, 'min': 0, 'type': 'setpoints'
        },
        {'rm360': 'DP_149Wert', 'ug7': 'DP_45Wert', 'name': 'Sollvorlauftemperatur', 'description': 'Puffer -SpKr2 Ladesolltemperatur', 'unit': '°C', 'asset': 'HAST', 'component': 'TES-2', 'color': '#590a0a', 'max': 100, 'min': 0, 'type': 'setpoints'
        },
        {'rm360': 'DP_156Wert', 'ug7': None, 'name': 'Solltemperatur 2 oben', 'description': 'Puffer -SpKr2 Puffersollwert oben', 'unit': '°C', 'asset': 'HAST', 'component': 'TES-2', 'color': '#590404', 'max': 100, 'min': 5, 'type': 'setpoints'
        },
        {'rm360': 'DP_157Wert', 'ug7': None, 'name': 'Auschalttemperatur unten', 'description': 'Puffer -SpKr2 Pufferausschaltpunkt unten', 'unit': '°C', 'asset': 'HAST', 'component': 'TES-2', 'color': '#412454', 'max': 100, 'min': 0, 'type': 'sensors'
        },
        {'rm360': 'DP_211Wert', 'ug7': 'DP_53Wert', 'name': 'Wärmemenge', 'description': 'WMZ1 Wärmemenge', 'unit': 'kWh', 'asset': 'HAST', 'component': 'WMZ', 'color': '#610052', 'max': 200, 'min': -1, 'type': 'meters'
        },
        {'rm360': 'DP_212Wert', 'ug7': 'DP_54Wert', 'name': 'Volumen', 'description': 'WMZ1 Volumen', 'unit': 'm3', 'asset': 'HAST', 'component': 'WMZ', 'color': '#0A635D', 'max': 10000, 'min': -1, 'type': 'meters'
        },
        {'rm360': 'DP_213Wert', 'ug7': 'DP_55Wert', 'name': 'Leistung', 'description': 'WMZ1 aktuelle Leistung', 'unit': 'kW', 'asset': 'HAST', 'component': 'WMZ', 'color': '#E41E98', 'max': 200, 'min': 0, 'type': 'sensors'
        },
        {'rm360': 'DP_214Wert', 'ug7': 'DP_56Wert', 'name': 'Volumenstrom', 'description': 'WMZ1 Durchfluss', 'unit': 'lph', 'asset': 'HAST', 'component': 'WMZ', 'color': '#1DC7BC', 'max': 10000, 'min': 0, 'type': 'sensors'
        },
        {'rm360': 'DP_215Wert', 'ug7': 'DP_58Wert', 'name': 'Vorlauftemperatur', 'description': 'WMZ1 Vorlauftemperatur', 'unit': '°C', 'asset': 'HAST', 'component': 'WMZ', 'color': '#FF0101', 'max': 100, 'min': 5, 'type': 'sensors'
        },
        {'rm360': 'DP_216Wert', 'ug7': 'DP_57Wert', 'name': 'Rücklauftemperatur', 'description': 'WMZ1 Rücklauftemperatur', 'unit': '°C', 'asset': 'HAST', 'component': 'WMZ', 'color': '#1800FF', 'max': 100, 'min': 5, 'type': 'sensors'
        },
        {'rm360': 'DP_217Wert', 'ug7': 'DP_59Wert', 'name': 'Spreizung', 'description': 'WMZ1 Spreizung', 'unit': 'K', 'asset': 'HAST', 'component': 'WMZ', 'color': '#8006d6', 'max': 90, 'min': -25, 'type': 'sensors'
        },
        {'rm360': 'DP_223Wert', 'ug7': None, 'name': 'Stichtagszählerstand', 'description': 'WMZ1 Stichtagszählerstand', 'unit': 'kWh', 'asset': 'HAST', 'component': 'WMZ', 'color': '#550469', 'max': 200, 'min': 0, 'type': 'sensors'
        }
    ]
    COMPONENT_COLOR_HAST={"HK-0":"#027a76", "HK-1":"#026c7a", "HK-2":"#026c7a", "HK-3":"#026c7a", "HK-4":"#02507a", "TES-1":"#027a02", "TES-2":"#7a0238", "WMZ":"#4e2175"}
    DESCRIPTION_ASSET={"HAST":"Wärmeübergabestation von Aquotec"}

    




