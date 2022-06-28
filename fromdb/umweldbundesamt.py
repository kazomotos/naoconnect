'Autor: Rupert Wieser -- Naotilus -- 20220622'
import http.client
from base64 import b64encode
from json import loads
from time import time
from random import random
from copy import copy
from naoconnect.TinyDb import TinyDb
from datetime import datetime
from naoconnect.Param import Param

class UmweldbundesamtV2 (Param):
    STATION = "station"
    COMPONENT = "component"
    SCOPE = "scope"
    DATA = "data"
    QUERY_DATA = "/api/air_data/v2/measures/json?station=%s&component=%s&scope=%s&date_from=%s&time_from=%s&date_to=%s&time_to=%s&lang=de"
    QUERY_CONFIG = "/api/air_data/v2/measures/limits"
    QUERY_STATION_META = "/api/air_data/v2/stations/json"
    INDEX_VALUE = 2
    TIMEOUT = 90
    LASTTIMESAVESEC = 120
    SECTONANO = 1000000000
    RESETTIME = 772466975
    SECTOMIL = 1000
    MILTOSEC = 0.001

    def __init__(self, host, tiny_db_name="umweldbundesamt.json"):
        self.playload = ""
        self.host = host
        self.db = TinyDb(tiny_db_name)
        self.transfere = self._getTransferChannels()
        self.confirm_time = time()
        self.lasttimestamps = self._getLastTimestamps()
        self._putLastTimestamps()
        self.marker_timestamps = None
        self.headers = {}
        self.__connection = None
        self.refreshConnection()

    def getTelegrafData(self, max_data_len):
        ''' [ '<twin>,instance=<insatance>, <measurement>=<value> <timestamp>' ] '''
        print("getTelegrafData")
        self.marker_timestamps = copy(self.lasttimestamps)
        data = []
        data_add = data.append
        count = 0
        for index in range(len(self.transfere)):
            try:
                first_time = self.lasttimestamps[str(index+1)] + self.transfere[index][UmweldbundesamtV2.NAME_INTERVAL]*0.5
            except:
                self.marker_timestamps[str(index+1)] = UmweldbundesamtV2.RESETTIME
                self.lasttimestamps[str(index+1)] = UmweldbundesamtV2.RESETTIME
                first_time = self.lasttimestamps[str(index+1)] + self.transfere[index][UmweldbundesamtV2.NAME_INTERVAL]*0.5
            last_time = self.lasttimestamps[str(index+1)] + self.transfere[index][UmweldbundesamtV2.NAME_INTERVAL]*max_data_len
            if last_time > (time()):
                last_time = (time())
            if first_time >= time():
                continue
            breaker = False
            timeout = False
            while 1==1:
                try:
                    history = self._getChannelHistory(
                        station= self.transfere[index][UmweldbundesamtV2.STATION],
                        component= self.transfere[index][UmweldbundesamtV2.COMPONENT],
                        scope= self.transfere[index][UmweldbundesamtV2.SCOPE],
                        timestamp_first= first_time,
                        timestamp_last= last_time
                    )
                except TimeoutError:
                    timeout = True
                    break
                if history[UmweldbundesamtV2.DATA] != {}:
                    break
                if breaker:
                    break
                else:
                    last_time += self.transfere[index][UmweldbundesamtV2.NAME_INTERVAL]*max_data_len
                    first_time += self.transfere[index][UmweldbundesamtV2.NAME_INTERVAL]*max_data_len
                if first_time > last_time:
                    first_time = last_time
                if last_time > (time()):
                    last_time = (time())
                    breaker = True
            if history[UmweldbundesamtV2.DATA] == {}:
                self.marker_timestamps[str(index+1)] = last_time
                continue
            if timeout:
                continue
            self.marker_timestamps[str(index+1)] = max(self.isotimeToTimestamp(list(history[UmweldbundesamtV2.DATA][str(self.transfere[index][UmweldbundesamtV2.STATION])].keys())))
            for time_key in history[UmweldbundesamtV2.DATA][str(self.transfere[index][UmweldbundesamtV2.STATION])]:
                if history[UmweldbundesamtV2.DATA][str(self.transfere[index][UmweldbundesamtV2.STATION])][time_key][UmweldbundesamtV2.INDEX_VALUE] != None:
                    count += 1
                    data_add(UmweldbundesamtV2.FORMAT_TELEGRAFFRAMESTRUCT % (
                        self.transfere[index][UmweldbundesamtV2.NAME_TELEGRAF][0], 
                        self.transfere[index][UmweldbundesamtV2.NAME_TELEGRAF][1],
                        self.transfere[index][UmweldbundesamtV2.NAME_TELEGRAF][2],
                        history[UmweldbundesamtV2.DATA][str(self.transfere[index][UmweldbundesamtV2.STATION])][time_key][UmweldbundesamtV2.INDEX_VALUE], 
                        self.isotimeToTimestamp(time_key)*UmweldbundesamtV2.SECTONANO
                    ))
            if count >= max_data_len: break
        self._disconnect()
        return(data) 

    def isotimeToTimestamp(self, isotime):
        '''list or string'''
        if isinstance(isotime,list):
            times = []
            times_add = times.append
            for tim in isotime:
                times_add(datetime.fromisoformat(tim).timestamp())
            return(times)
        else:
            return(datetime.fromisoformat(isotime).timestamp())

    def getConifgurationStation(self, component_ids:list, scope:int):
        data = self._GetRequest(
            UmweldbundesamtV2.NAME_GET,
            self.QUERY_CONFIG
        )
        station = {}  
        for index in data[UmweldbundesamtV2.DATA]:
            if int(data[UmweldbundesamtV2.DATA][index][0]) == scope and int(data[UmweldbundesamtV2.DATA][index][1]) in component_ids:
                if data[UmweldbundesamtV2.DATA][index][2] in station:
                    station[data[UmweldbundesamtV2.DATA][index][2]].append(data[UmweldbundesamtV2.DATA][index][1])
                else:
                    station[data[UmweldbundesamtV2.DATA][index][2]] = [data[UmweldbundesamtV2.DATA][index][1]]
        return(station)

    def getStationMetadata(self):
        return(self._GetRequest(
            UmweldbundesamtV2.NAME_GET,
            self.QUERY_STATION_META
        ))


    def confirmTransfer(self):
        self.lasttimestamps = self.marker_timestamps
        if time()-self.confirm_time >= UmweldbundesamtV2.LASTTIMESAVESEC:
            self.confirm_time = time()
            self._putLastTimestamps()

    def exit(self):
        self._disconnect()
        self._putLastTimestamps()

    def _putLastTimestamps(self):
        ''' {<id>: <timestamp>}'''
        self.db.updateSimpleTinyTables(UmweldbundesamtV2.NAME_LASTTIME, self.lasttimestamps)

    def refreshConnection(self):
        self.__connection = http.client.HTTPSConnection(self.host, timeout=UmweldbundesamtV2.TIMEOUT)
    
    def _connect(self):
        self.__connection.connect()

    def _disconnect(self):
        self.__connection.close()

    def _GetRequest(self, method, url):
        try:
            self.__connection.request(method, url, self.playload, self.headers)
        except:
            self._disconnect()
            self.refreshConnection()
            self.__connection.request(method, url, self.playload, self.headers)
        return(loads(self.__connection.getresponse().read().decode(UmweldbundesamtV2.NAME_UTF8)))

    def _getChannelHistory(self,station,component,scope,timestamp_first,timestamp_last):
        '''
        '''
        return(self._GetRequest(UmweldbundesamtV2.NAME_GET, 
            self.QUERY_DATA%(
                station, 
                component,
                scope,
                str(datetime.fromtimestamp(timestamp_first).date()),
                str(datetime.fromtimestamp(timestamp_first).time()),
                str(datetime.fromtimestamp(timestamp_last).date()),
                str(datetime.fromtimestamp(timestamp_last).time())
            )
        ))

    def _getTransferChannels(self):
        ''' 
        [{}] 
        '''
        return(self.db.getTinyTables(UmweldbundesamtV2.NAME_TRANSFERCHANNELS))

    def _putTransferChannel(self, station, component, scope, telegraf, interval):
        data = {
            UmweldbundesamtV2.STATION: station,
            UmweldbundesamtV2.COMPONENT: component,
            UmweldbundesamtV2.SCOPE: scope,
            UmweldbundesamtV2.NAME_TELEGRAF: telegraf,
            UmweldbundesamtV2.NAME_INTERVAL: interval
        }
        self.db.putTinyTables(UmweldbundesamtV2.NAME_TRANSFERCHANNELS, data)
        self.transfere = self._getTransferChannels()

    def _getLastTimestamps(self):
        ''' {<id>: <timestamp>}'''
        try:
            last_timestamps = self.db.getTinyTables(UmweldbundesamtV2.NAME_LASTTIME)[0]
        except:
            return({})
        return(last_timestamps)


