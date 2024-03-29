'Autor: Rupert Wieser -- Naotilus -- 20220218'
import http.client
from base64 import b64encode
from json import loads
from time import time
from random import random
from copy import copy
from naoconnect.TinyDb import TinyDb
from naoconnect.Param import Param

class OpenMuc (Param):
    RESTCHANNELS = "/rest/channels/"
    RESTDEVICES = "/rest/devices/"
    RESTCONF = "/configs"
    RESTDEVICE = "/deviceId"
    HISTORYFIRST = "/history?from="
    HISTORYLAST = "&until="
    SCAN = "/scan"
    CHANNELID = "id"
    CHANNEL = "channel"
    DEVICE = "device"
    DEVICEID = "deviceId"
    RECORDS = "records"
    FLAG = "flag"
    FLAG_NO_VALUE_YET = "NO_VALUE_RECEIVED_YET"
    FLAG_NO_DEVICE_BUSY = 'DEVICE_OR_INTERFACE_BUSY'
    FLAG_DRIVER_UNKNOW = 'DRIVER_THREW_UNKNOWN_EXCEPTION'
    FLAG_LOGGING_NOT_ACTIVE = 'DATA_LOGGING_NOT_ACTIVE'
    FLAG_VALID = "VALID"
    UNUSEDCHANNELS = "unused_channels"
    TIMEOUT = 90
    LASTTIMESAVESEC = 120
    MILTONANO = 1000000
    SECTOMIL = 1000
    RESETTIME = 1600000000000

    def __init__(self, host, port, username, password, tiny_db_name="muc.json"):
        self.playload = ""
        self.port = port
        self.host = host
        self.db = TinyDb(tiny_db_name)
        self.transfere = self._getTransferChannels()
        self.lasttimestamps = self._getLastTimestamps()
        self.marker_timestamps = None
        self.error_cannels = []
        self.confirm_time = time()
        self.headers = {OpenMuc.NAME_WEBAUTH: 'Basic '+b64encode(
            bytes(username+":"+password, OpenMuc.NAME_UTF8)
        ).decode("ascii")}
        self.__connection = None
        self.refreshConnection()

    def getTelegrafData(self, max_data_len):
        ''' [ '<twin>,instance=<insatance>, <measurement>=<value> <timestamp>' ] '''
        print("getTelegrafData")
        self.marker_timestamps = copy(self.lasttimestamps)
        data = []
        data_add = data.append
        count = 0
        for channelinfo in self.transfere:

            print(channelinfo)
            if channelinfo[OpenMuc.CHANNEL] in self.error_cannels:
                if 0.02 < random():
                    continue
            first_time = self.lasttimestamps[channelinfo[OpenMuc.CHANNEL]] + channelinfo[OpenMuc.NAME_INTERVAL]*0.5
            last_time = self.lasttimestamps[channelinfo[OpenMuc.CHANNEL]] + channelinfo[OpenMuc.NAME_INTERVAL]*max_data_len/4
            if last_time > (time())*OpenMuc.SECTOMIL:
                last_time = (time())*OpenMuc.SECTOMIL
            if first_time >= time()*OpenMuc.SECTOMIL:
                continue
            timeout = False
            breaker = False
            while 1==1:
                try:
                    history = self._getChannelHistory(channelinfo[OpenMuc.CHANNEL],first_time,last_time)
                except TimeoutError:    
                    self._disconnect()
                    self.error_cannels.append(channelinfo[OpenMuc.CHANNEL]+"_timeout")
                    timeout = True
                    history[OpenMuc.RECORDS] = None
                    break
                if breaker:
                    break
                if history[OpenMuc.RECORDS] != []:
                    break 
                else:
                    last_time += channelinfo[OpenMuc.NAME_INTERVAL]*max_data_len
                    first_time += channelinfo[OpenMuc.NAME_INTERVAL]*max_data_len
                if last_time > (time())*OpenMuc.SECTOMIL:
                    last_time = (time())*OpenMuc.SECTOMIL
                    breaker = True
            if history[OpenMuc.RECORDS] == []:
                self.marker_timestamps[channelinfo[OpenMuc.CHANNEL]] = last_time
                continue
            if timeout:
                continue
            self.marker_timestamps[channelinfo[OpenMuc.CHANNEL]] = history[OpenMuc.RECORDS][-1][OpenMuc.NAME_TIMESTAP]
            for data_set in history[OpenMuc.RECORDS]:
                if data_set[OpenMuc.FLAG] == OpenMuc.FLAG_VALID:
                    count += 1
                    data_add(OpenMuc.FORMAT_TELEGRAFFRAMESTRUCT % (
                        channelinfo[OpenMuc.NAME_TELEGRAF][0], channelinfo[OpenMuc.NAME_TELEGRAF][1], channelinfo[OpenMuc.NAME_TELEGRAF][2],
                        data_set[OpenMuc.NAME_VALUE], data_set[OpenMuc.NAME_TIMESTAP]*OpenMuc.MILTONANO
                    ))
                elif data_set[OpenMuc.FLAG] == OpenMuc.FLAG_DRIVER_UNKNOW or data_set[OpenMuc.FLAG] == OpenMuc.FLAG_NO_DEVICE_BUSY:
                    self.error_cannels.append(channelinfo[OpenMuc.CHANNEL] + data_set[OpenMuc.FLAG] )
                    print(channelinfo[OpenMuc.CHANNEL], "error")
                    break
                elif data_set[OpenMuc.FLAG] == OpenMuc.FLAG_LOGGING_NOT_ACTIVE:
                    continue
                else:
                    self.error_cannels.append(channelinfo[OpenMuc.CHANNEL] + data_set[OpenMuc.FLAG] )
                    break
            print(count)
            if count >= max_data_len: break
        self._disconnect()
        return(data) 

    def confirmTransfer(self):
        self.lasttimestamps = self.marker_timestamps
        if time()-self.confirm_time >= OpenMuc.LASTTIMESAVESEC:
            self.confirm_time = time()
            self._putLastTimestamps()

    def refreshConnection(self):
        self.__connection = http.client.HTTPConnection(self.host, self.port, timeout=OpenMuc.TIMEOUT)
    
    def exit(self):
        self._disconnect()
        self._putLastTimestamps()

    def _putLastTimestamps(self):
        ''' {<channel>: <timestamp>}'''
        self.db.updateSimpleTinyTables(OpenMuc.NAME_LASTTIME, self.lasttimestamps)

    def _connect(self):
        self.__connection.connect()

    def _disconnect(self):
        self.__connection.close()

    def _GetRequest(self, method, url):
        self.__connection.request(method, url, self.playload, self.headers)
        return(loads(self.__connection.getresponse().read().decode(OpenMuc.NAME_UTF8)))

    def _getChannels(self):
        '''
        {"records": [{
            "id": <id>, "valueType": <valueType>, "record": {
                    "timestamp": <timestamp>, "flag": <valid>, "value": <value>
                }
            }]
        }
        '''
        return(self._GetRequest(OpenMuc.NAME_GET, OpenMuc.RESTCHANNELS))

    def _getChannel(self, channel_id):
        return(self._GetRequest(OpenMuc.NAME_GET, OpenMuc.RESTCHANNELS + channel_id))

    def _getChannelConfigs(self, channel_id):
        return(self._GetRequest(OpenMuc.NAME_GET, OpenMuc.RESTCHANNELS + channel_id + OpenMuc.RESTCONF))

    def _getChannelDeviceId(self, channel_id):
        ''' {"deviceId": <deviceId>} '''
        return(self._GetRequest(OpenMuc.NAME_GET, OpenMuc.RESTCHANNELS + channel_id + OpenMuc.RESTDEVICE))

    def _getChannelHistory(self,channel_id,timestamp_first, timestamp_last):
        '''
        {"records": [{
                "timestamp": <timestamp>, "flag": <valid>, "value": <value>
            }]
        }
        '''
        return(self._GetRequest(OpenMuc.NAME_GET, 
               OpenMuc.RESTCHANNELS + channel_id + OpenMuc.HISTORYFIRST + 
               str(int(timestamp_first)) + OpenMuc.HISTORYLAST + str(int(timestamp_last)))
        )

    def _getDevices(self):
        return(self._GetRequest(OpenMuc.NAME_GET, OpenMuc.RESTDEVICES))

    def _getDeviceFild(self, device):
        return(self._GetRequest(OpenMuc.NAME_GET, OpenMuc.RESTDEVICES + device))

    def _getDeviceScan(self, device):
        return(self._GetRequest(OpenMuc.NAME_GET, OpenMuc.RESTDEVICES + device + OpenMuc.SCAN))

    def _getTransferChannels(self):
        ''' 
        [{
            "device:" <device>, "channel" <channel>, 
            "telegraf": [<twin>, <insatance>, <measurement>]
            "interval": <sec>
        }] 
        '''
        return(self.db.getTinyTables(OpenMuc.NAME_TRANSFERCHANNELS))

    def _getUnusedChannels(self):
        ''' [ {"device:" <device>, "channel" <channel>} ] '''
        return(self.db.getTinyTables(OpenMuc.UNUSEDCHANNELS))
    
    def _getMatchedChannels(self):
        ''' [ {"device:" <device>, "channel" <channel>} ] '''
        return(self._getTransferChannels() + self._getUnusedChannels())

    def _getNewChannels(self):
        ''' [ {"device:" <device>, "channel" <channel>} ] '''
        muc_channels = set([item[OpenMuc.CHANNELID] for item in self._getChannels()[OpenMuc.RECORDS]])
        matched_channels = set([item[OpenMuc.CHANNEL] for item in self._getMatchedChannels()])
        new_channels = muc_channels.difference(matched_channels)
        return([{
            OpenMuc.CHANNEL: item, OpenMuc.DEVICE: self._getChannelDeviceId(item)[OpenMuc.DEVICEID]
            } for item in new_channels
        ])

    def _getLastTimestamps(self):
        ''' {<channel>: <timestamp>}'''
        last_timestamps = self.db.getTinyTables(OpenMuc.NAME_LASTTIME)
        if last_timestamps == []:
            last_timestamps = {}
        else:
            last_timestamps = last_timestamps[0]
        missing_channels = list(
            set([item[OpenMuc.CHANNEL] for item in self._getTransferChannels()]).difference(
                set(list(last_timestamps.keys()))
        ))
        if len(missing_channels) > 0:
            for channel in missing_channels: last_timestamps[channel] = OpenMuc.RESETTIME
            self.db.putTinyTables(OpenMuc.NAME_LASTTIME, last_timestamps)
        return(last_timestamps)

    def _putTransferChannel(self, value):
        ''' 
        {
            "device:" <device>, "channel" <channel>, 
            "telegraf": [<twin>, <insatance>, <measurement>]
            "interval": <sec>
        } 
        '''
        self.db.putTinyTables(OpenMuc.NAME_TRANSFERCHANNELS, value)
        self.transfere = self._getTransferChannels()

    def _putUnusedChannel(self, value):
        ''' {"device:" <device>, "channel" <channel>} '''
        self.db.putTinyTables(OpenMuc.UNUSEDCHANNELS, value)

if __name__ == "__main__":
    'test'
    muc = OpenMuc("127.0.0.1", 8888, "admin", "admin")
    print(muc._getChannels())
    muc._disconnect()


