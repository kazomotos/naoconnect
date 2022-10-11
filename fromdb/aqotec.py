import pyodbc
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
    LASTTIMESAVESEC         = 240
    RESET_TIME              = 1514761200
    SECTONANO               = 1000000000

    def __init__ (self, host, port, user, password, database=None, tds_version=7.4, driver='{FreeTDS}', tiny_db_name="aqotec_meta.json"):
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
                except pyodbc.ProgrammingError as e:
                    print(e)
                    if "Invalid column name"    in str(e):
                        columname = str(e).split("Invalid column name")[1].split("\'")[1]
                        values = values.replace(", "+columname, "")
                        continue
                    if "Invalid object name" in str(e):
                        tablename = str(e).split("Invalid object name")[1].split("\'")[1]
                        print(tablename)
                if timestamp_list != []:
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