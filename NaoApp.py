'Autor: Rupert Wieser -- Naotilus -- 20220219'
import http.client
from logging.handlers import DatagramHandler
from sqlite3 import DatabaseError
from urllib.parse import quote
from copy import copy
from json import loads, dumps
from time import sleep, time
from threading import Thread
from pandas import isna
from naoconnect.TinyDb import TinyDb
from naoconnect.Param import Param


class NaoApp(Param):
    URLLOGIN = "/api/user/auth/login"
    URLTELEGRAF = "/api/telegraf/"
    URL_INSTANCE = "/api/nao/instance"
    URL_INPUT = "/api/nao/inputvalue"
    URL_INPUTS = "/api/nao/inputvalue/many"
    URL_WORKSPACE = "/api/nao/workspace"
    URL_ASSET = "/api/nao/asset"
    URL_PATH = "/api/nao/part"
    URL_UNITS = "/api/nao/units"
    URL_SERIES = "/api/nao/series/"
    HEADER_JSON = 'application/json'
    BEARER = "Bearer "
    TRANSFERCONFIG = "transfer_config"
    LOGINHEADER = {'Content-Type': 'application/x-www-form-urlencoded'}
    TRANSFERHEADER = {"Authorization": "", 'Content-Type': 'text/plain', 'Cookie': ""}
    MESSAGELOGIN = "MESSAGE: login nao"
    TRANSFERINTERVAL = "transferinterval"
    DATAPERTELEGRAF = "max_data_per_telegraf"
    ERRORSLEEP = "error_sleep_time"
    ERRORSLEEPLOGGER = "error_sleep_time_logger"
    TOTALTRANSFER = "total_number_of_sent_data"
    MAXBUFFERTIME = "max_buffer_time_DDR"
    LOGGINGINTERVAL = "logging_interval"
    STANDARD_MAXBUFFERTIME = 1800
    STANDARD_ERRORSLEEP = 120
    STANDARD_TRANSFERINTERVAL = 900
    STANDARD_LOGGINGINTERVAL = 60
    STANDARD_DATAPERTELEGRAF = 200000

    def __init__(self, host, email, password, DataFromDb=False, DataForLogging=False, DataForListener=False, tiny_db_name="nao.json", error_log=False):
        self.auth = {
            NaoApp.NAME_HOST:host,
            NaoApp.NAME_PAYLOAD:NaoApp.NAME_EMAIL+"="+quote(email)+"&"+NaoApp.NAME_PASSWD+"="+quote(password)
        }
        self.error_log = error_log
        self.__conneciton = None
        self.headers = NaoApp.TRANSFERHEADER
        self.db = TinyDb(tiny_db_name)
        self.DataFromDb = DataFromDb
        self.DataForLogging = DataForLogging
        self.DataForListener = DataForListener
        self.transfer_config = self._getTransferCofnig()
        self.end_transfer = False
        self.end_confirmation = False
        self.sending_counter = 0
        self.logging_data = []
        self.logging_data_add = self.logging_data.extend

    def sendTelegrafData(self, payload):
        ''' 
        [ '<twin>,instance=<insatance>, <measurement>=<value> <timestamp>' ] 
                                      or
          '<twin>,instance=<insatance>, <measurement>=<value> <timestamp>'
        '''
        if type(payload) == list:
            payload = NaoApp.FORMAT_TELEGRAFFRAMESEPERATOR*len(payload) % tuple(payload)
        try:
            self.__conneciton.request(NaoApp.NAME_POST, NaoApp.URLTELEGRAF, payload, self.headers)
            status = self.__conneciton.getresponse().status
            self.__conneciton.close()
        except:
            self._loginNao()
            self.__conneciton.request(NaoApp.NAME_POST, NaoApp.URLTELEGRAF, payload, self.headers)
            status = self.__conneciton.getresponse().status
            self.__conneciton.close()
        return(status)

    def _sendDataToNaoJson(self, method, url, payload):
        header = copy(self.headers)
        header[NaoApp.NAME_CONTENT_HEADER] = NaoApp.HEADER_JSON
        try:
            self.__conneciton.request(method, url, dumps(payload), header)
            data = self.__conneciton.getresponse().read()
            self.__conneciton.close()
        except:
            self._loginNao()
            header = copy(self.headers)
            header[NaoApp.NAME_CONTENT_HEADER] = NaoApp.HEADER_JSON
            self.__conneciton.request(method, url, dumps(payload), header)
            data = self.__conneciton.getresponse().read()
            self.__conneciton.close()
        if data == b'':
            return('')
        else:
            return(loads(data))
    
    def sendNewInstance(self, asset, name, discription, geolocation=None):
        data = {
            NaoApp.NAME_NAME: name,
            NaoApp.NAME_DESCRIPTION: discription,
            NaoApp.NAME_ASSET_ID: asset
        }
        if geolocation != None:
            data[NaoApp.NAME_GEOLOCATION] = geolocation
        return(self._sendDataToNaoJson(NaoApp.NAME_POST, NaoApp.URL_INSTANCE, data)[NaoApp.NAME_ID_ID])

    def sendInstanceInputMany(self, data_list:list):
        '''
        [[<value>, <input_id>, <instance_id>], ...]
        '''
        data = []
        data_add = data.append
        for data_set in data_list:
            data_add({
            NaoApp.NAME_VALUE: data_set[0],
            NaoApp.NAME_INPUT_ID: data_set[1],
            NaoApp.NAME_RELATION_ID: data_set[2]
            })
        return(self._sendDataToNaoJson(NaoApp.NAME_POST, NaoApp.URL_INPUTS, data))

    def sendInstanceInput(self,value,input_id,instance_id):
        data = {
          NaoApp.NAME_VALUE: value,
          NaoApp.NAME_INPUT_ID: input_id,
          NaoApp.NAME_RELATION_ID: instance_id
        }
        return(self._sendDataToNaoJson(NaoApp.NAME_POST, NaoApp.URL_INPUT, data))

    def startDataTransferFromDb(self):
        if not self.DataFromDb: 
            self.print("ERROR: no db in this class initialized")
            return(-1)
        Thread(target=self.__dataTransferFromDb, args=()).start()
        self.__transferInterrupt()
    
    def startDataTransferLogging(self):
        if not self.DataForLogging: 
            self.print("ERROR: no interface logger in this class initialized")
            return(-1)
        Thread(target=self.__DataTransferLogging, args=()).start()
        self.__transferInterrupt()

    def startDataTransferFromListener(self):
        if not self.DataForListener: 
            self.print("ERROR: no interface logger in this class initialized")
            return(-1)
        Thread(target=self.__dataTransferFromListener, args=()).start()
        self.__transferInterrupt()

    def __transferInterrupt(self):
        start_time = time()
        while 1==1:
            print("enter 'exit' for end data transfer to telegraf")
            print("priode of transfer time: " + str(round((time() - start_time)/60,2)) + " min, total sent value: " + str(self.sending_counter))
            input_str = input()
            if input_str == "exit" or input_str == "'exit'":
                print("process will end soon")
                self.end_transfer = True
                break
        while 1==1:
            if self.end_confirmation:
                break
            sleep(10)
        self.exit()     

    def exit(self):
        self._addAndUpdateTotalNumberOfSentData()
        print("ending")

    def __DataTransferLogging(self):
        Thread(target=self.__DataTransferLoggingData, args=()).start()
        start = time()
        while 1==1:
            sleep(self.transfer_config[NaoApp.TRANSFERINTERVAL])
            data = self.logging_data
            self.logging_data = []
            self.logging_data_add = self.logging_data.extend
            Thread(target=self.__DataTransferLoggingBuffer, args=(data,)).start()
            if time() - start > 800:
                self._addAndUpdateTotalNumberOfSentData()
                start = time()
            if self.end_transfer:
                sleep(self.transfer_config[NaoApp.TRANSFERINTERVAL])
                self.end_confirmation = True
                break     

    def __DataTransferLoggingData(self):    
        while 1==1:
            start = time()
            try:    
                self.logging_data_add(self.DataForLogging.getTelegrafData())
            except Exception as e:
                self.print("ERROR-FromDb: "+  str(e))
                sleep(self.transfer_config[NaoApp.ERRORSLEEPLOGGER])
                ending = self.DataForLogging.refreshConnection()
                if ending:
                    self.end_transfer = True
            diff = time() - start
            if self.end_transfer:
                self.DataForLogging.exit()
                break
            if diff < self.transfer_config[NaoApp.LOGGINGINTERVAL]:
                sleep(self.transfer_config[NaoApp.LOGGINGINTERVAL]-diff)

    def __DataTransferLoggingBuffer(self, data):
        start = time()
        while 1==1:
            try:
                data_len = len(data)
                status = self.sendTelegrafData(data)    
                if status == 204:
                    self.sending_counter += data_len
                    break
                elif status == 500:
                    self.print("ERROR: nao.status=" + str(status))
                    sleep(self.transfer_config[NaoApp.ERRORSLEEP])
                    self._loginNao()
                else:
                    self.print("ERROR: nao.status=" + str(status))
                    sleep(self.transfer_config[NaoApp.ERRORSLEEP])
            except Exception as e:
                self.print("ERROR-Nao:" + str(e))
                sleep(self.transfer_config[NaoApp.ERRORSLEEP])
            if self.end_transfer: break
            if time() - start > self.transfer_config[NaoApp.MAXBUFFERTIME]: 
                self.print("WARNING:" + str(len(data)) + " datasets destroyed")
                break 
            # TODO: hier könnte noch ein Buffer auf Festplatte gebaut werden

    def __dataTransferFromDb(self):
        while 1==1:
            start = time()
            try:    
                data = self.DataFromDb.getTelegrafData(max_data_len=self.transfer_config[NaoApp.DATAPERTELEGRAF])
                data_len = len(data)
            except TimeoutError:
                data = []
                self.print("ERROR-FromDb: TimeoutError")
                sleep(self.transfer_config[NaoApp.ERRORSLEEP])
                self.DataFromDb.refreshConnection()
                continue
            except Exception as e:
                data = []
                data_len = 0
                self.print("ERROR-FromDb:" + str(e))
                sleep(self.transfer_config[NaoApp.ERRORSLEEP])
                self.DataFromDb.refreshConnection()
            try:
                if data != []:
                    status = self.sendTelegrafData(data)
                    print(data_len, " data sendet")
                    if status == 204:
                        self.DataFromDb.confirmTransfer()
                        self.sending_counter += data_len
                    elif status == 500:
                        self.print("ERROR: nao.status=" + str(status))
                        self._loginNao()
                    else:
                        self.print("ERROR: nao.status=" + str(status))
                        self._loginNao()
            except Exception as e:
                self.print("ERROR-Nao:" + str(e))
                sleep(self.transfer_config[NaoApp.ERRORSLEEP])
                self.DataFromDb.refreshConnection()
            diff = time() - start
            if self.end_transfer:
                self.DataFromDb.exit()
                self.end_confirmation = True
                break
            if diff < self.transfer_config[NaoApp.TRANSFERINTERVAL]:
                sleep(self.transfer_config[NaoApp.TRANSFERINTERVAL]-diff)

    def __dataTransferFromListener(self):
        while 1==1:
            start = time() 
            data = self.DataForListener.getTelegrafData()
            data_len = len(data)
            self.logging_data_add(data)
            if (len(self.logging_data)) > NaoApp.STANDARD_DATAPERTELEGRAF:
                self.print("delteted data-len: " + str(int(len(self.logging_data)-NaoApp.STANDARD_DATAPERTELEGRAF)))
                self.logging_data[int(len(self.logging_data)-NaoApp.STANDARD_DATAPERTELEGRAF):]
            try:
                if self.logging_data != []:
                    status = self.sendTelegrafData(self.logging_data)    
                    if status == 204:
                        self.sending_counter += data_len
                        del self.logging_data
                        self.logging_data = []
                        self.logging_data_add = self.logging_data.extend
                    elif status ==500:
                        self.print("ERROR: nao.status=" + str(status))
                        self._loginNao()
                    else:
                        self.print("ERROR: nao.status=" + str(status))
            except Exception as e:
                self.print("ERROR-Nao:" + str(e))
                sleep(self.transfer_config[NaoApp.ERRORSLEEP])
                self.DataForListener.refreshConnection()
            if self.sending_counter > 10000:
                self._addAndUpdateTotalNumberOfSentData()
            diff = time() - start
            if self.end_transfer:
                self.DataForListener.exit()
                self.end_confirmation = True
                break
            if diff < self.transfer_config[NaoApp.TRANSFERINTERVAL]:
                sleep(self.transfer_config[NaoApp.TRANSFERINTERVAL]-diff)

    def _getTransferCofnig(self):
        config = self.db.getTinyTables(NaoApp.TRANSFERCONFIG)
        if config == []:
            config = [{
                NaoApp.DATAPERTELEGRAF: NaoApp.STANDARD_DATAPERTELEGRAF,
                NaoApp.TRANSFERINTERVAL: NaoApp.STANDARD_TRANSFERINTERVAL,
                NaoApp.ERRORSLEEP: NaoApp.STANDARD_ERRORSLEEP,
                NaoApp.ERRORSLEEPLOGGER: NaoApp.STANDARD_ERRORSLEEP,
                NaoApp.MAXBUFFERTIME: NaoApp.STANDARD_MAXBUFFERTIME,
                NaoApp.LOGGINGINTERVAL: NaoApp.STANDARD_LOGGINGINTERVAL
            }]
            self.db.putTinyTables(NaoApp.TRANSFERCONFIG, config[0])
        return(config[0])
    
    def _updateTransferConfig(self, config):
        new_config = self.db.getTinyTables(NaoApp.TRANSFERCONFIG)
        for conf in config:
            new_config[conf] = config[conf]
        self.db.updateSimpleTinyTables(NaoApp.TRANSFERCONFIG, new_config)
        self.transfer_config = new_config

    def _addAndUpdateTotalNumberOfSentData(self):
        old_number = self._getTotalNumberOfSentData()
        self.db.updateSimpleTinyTables(NaoApp.TOTALTRANSFER, {NaoApp.NAME_COUNT: old_number + self.sending_counter})
        self.sending_counter = 0

    def _getTotalNumberOfSentData(self):
        number = self.db.getTinyTables(NaoApp.TOTALTRANSFER)
        if number == []: 
            number = 0
            self.db.putTinyTables(NaoApp.TOTALTRANSFER, {NaoApp.NAME_COUNT: number})
        else:
            number = number[0][NaoApp.NAME_COUNT]
        return(number)

    def _loginNao(self):        
        self.print(NaoApp.MESSAGELOGIN)
        self.__conneciton = http.client.HTTPSConnection(self.auth[NaoApp.NAME_HOST])
        self.__conneciton.request(NaoApp.NAME_POST, NaoApp.URLLOGIN, self.auth[NaoApp.NAME_PAYLOAD], NaoApp.LOGINHEADER)
        res = self.__conneciton.getresponse()
        data = loads(res.read().decode(NaoApp.NAME_UTF8))
        self.headers[NaoApp.NAME_WEBAUTH] = NaoApp.BEARER + data[NaoApp.NAME_TOKENAC]
        # self.headers[NaoApp.NAME_COOKIE] = NaoApp.NAME_TOKENRE + data[NaoApp.NAME_TOKENRE]

    def print(self, log:str):
        if self.error_log:
            error_file = open(self.error_log, "a")
            error_file.writelines(log+'\n')
            error_file.close()
        else:
            print(log)

    def createWorkspace(self, name, avatar=None, tagitems=[]):
        payload = {
            NaoApp.NAME_NAME: name,
            NaoApp.NAME_AVATAR_ID: avatar,
            NaoApp.NAME_TAGITEMS_ID: tagitems
        }
        return(self._sendDataToNaoJson("POST", NaoApp.URL_WORKSPACE, payload))

    def createAsset(self, name, _workspace, description="", baseInterval="1m", useGeolocation=True, avatar=None, tagitems=[]):
        payload = {
            NaoApp.NAME_WORKSPACE_ID: _workspace,
            NaoApp.NAME_NAME: name,
            NaoApp.NAME_DESCRIPTION: description,
            NaoApp.NAME_AVATAR_ID: avatar,
            NaoApp.NAME_BASE_INTERVAL: baseInterval,
            NaoApp.NAME_USE_GEOLOCATION: useGeolocation
        }
        return(self._sendDataToNaoJson("POST", NaoApp.URL_ASSET, payload))

    def createPath(self, name, _asset, description="", color="#02c1de", _parent=None):
        payload = {
            NaoApp.NAME_ASSET_ID: _asset,
            NaoApp.NAME_NAME: name,
            NaoApp.NAME_DESCRIPTION: description,
            NaoApp.NAME_COLOR: color,
            NaoApp.NAME_PARENT_ID: _parent
        }
        return(self._sendDataToNaoJson("POST", NaoApp.URL_PATH, payload))

    def createUnit(self, name):
        payload = {
            NaoApp.NAME_NAME: name
        }
        return(self._sendDataToNaoJson("POST", NaoApp.URL_UNITS, payload))

    def createSeries(self, type, name, description, _asset, _part, _unit, max, min, fill, fillValue, color="#02c1de", _tagitems=None):
        if isna(fill):
            fill = "null"
        if isna(fillValue):
            fillValue = None
        if isna(max):
            max = None
        if isna(min):
            min = None
        payload = {
            NaoApp.NAME_COLOR: color,
            NaoApp.NAME_DESCRIPTION: description,
            NaoApp.NAME_FILL: fill,
            NaoApp.NAME_FILL_VALUE: fillValue,
            NaoApp.NAME_MAX_VALUE: max,
            NaoApp.NAME_MIN_VALUE: min,
            NaoApp.NAME_NAME: name,
            NaoApp.NAME_ASSET_ID: _asset,
            NaoApp.NAME_PART_ID: _part,
            NaoApp.NAME_TAGITEMS_ID: _tagitems,
            NaoApp.NAME_UNIT_ID: _unit
        }
        return(self._sendDataToNaoJson("POST", NaoApp.URL_SERIES + type, payload))

if __name__ == "__main__":
    'test'
    Nao = NaoApp("nao-app.de", "???", "????")
    Nao.startDataTransferFromDb()


