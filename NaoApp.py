'Autor: Rupert Wieser -- Naotilus -- 20220219'
import datetime
import http.client
from math import ceil
from copy import copy
from json import dumps, loads
from logging.handlers import DatagramHandler
from random import random
from threading import Thread
from time import sleep, time, time_ns
from typing import Union
from urllib.parse import quote

from naoconnect.Param import Param
from naoconnect.TinyDb import TinyDb


class NaoApp(Param):
    URLLOGIN = "/api/user/auth/login"
    URLTELEGRAF = "/api/telegraf/"
    URL_INSTANCE = "/api/nao/instance"
    URL_INPUT = "/api/nao/inputvalue"
    URL_INPUT_DESC = "/api/nao/input"
    URL_INPUTCONTAINER = "/api/nao/inputcontainer"
    URL_INPUTS = "/api/nao/inputvalue/many"
    URL_WORKSPACE = "/api/nao/workspace"
    URL_ASSET = "/api/nao/asset"
    URL_PATH = "/api/nao/part"
    URL_UNITS = "/api/nao/units"
    URL_SERIES = "/api/nao/series/"
    URL_SINGELVALUES = "/api/series/data/singlevalues"
    URL_PLOTTIMESERIES = "/api/series/data/plot"
    QUERY_GET_ENDPOINT = "?query=_instance=%s,_point=%s"
    QUERY_GET = "?query="
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
    NAME_ENDPOINT_ID = "_endpoint"
    SERIES_TYPE = "series_type"
    CONSOLIDATE = "template"
    STANDARD_MAXBUFFERTIME = 1800
    STANDARD_ERRORSLEEP = 120
    STANDARD_TRANSFERINTERVAL = 900
    STANDARD_LOGGINGINTERVAL = 60
    STANDARD_DATA_PER_FUNC_CALL = 200000
    STADARTD_DATA_PER_TELEGRAF = 10000

    def __init__(self, host, email, password, DataFromDb=False, DataForLogging=False, DataForListener=False, tiny_db_name="nao.json", error_log=False, break_hour:datetime.time=datetime.time(hour=23), local=False): # type: ignore
        self.auth = {
            NaoApp.NAME_HOST:host,
            NaoApp.NAME_PAYLOAD:NaoApp.NAME_EMAIL+"="+quote(email)+"&"+NaoApp.NAME_PASSWD+"="+quote(password)
        }
        self.error_log = error_log
        self.__conneciton = None # type: ignore
        self.headers = NaoApp.TRANSFERHEADER
        self.db = TinyDb(tiny_db_name)
        self.local=local
        self.DataFromDb = DataFromDb
        self.DataForLogging = DataForLogging
        self.DataForListener = DataForListener
        self.transfer_config = self._getTransferCofnig()
        self.end_transfer = False
        self.end_confirmation = False
        self.sending_counter = 0
        self.logging_data = []
        self.logging_data_add = self.logging_data.extend
        self.exit_hour = break_hour
        self.endwithexit = False

    def sendTelegrafData(self, payload):
        ''' 
        [ '<twin>,instance=<insatance>, <measurement>=<value> <timestamp>' ] 
                                      or
          '<twin>,instance=<insatance>, <measurement>=<value> <timestamp>'
        '''
        if type(payload) != list:
            return(self._sendTelegrafData(payload=payload))
        else:
            if len(payload) > NaoApp.STADARTD_DATA_PER_TELEGRAF:
                last_idx = 0
                for idx in range(int(ceil(len(payload)/NaoApp.STADARTD_DATA_PER_TELEGRAF))-1):
                    last_idx = idx
                    sta = self._sendTelegrafData(payload[int(idx*NaoApp.STADARTD_DATA_PER_TELEGRAF):int(idx*NaoApp.STADARTD_DATA_PER_TELEGRAF)+NaoApp.STADARTD_DATA_PER_TELEGRAF])
                    if sta != 204:
                        return(sta)
                    sleep(1+idx*0.07)
                return(self._sendTelegrafData(payload[int(last_idx*NaoApp.STADARTD_DATA_PER_TELEGRAF):]))
            else:
                return(self._sendTelegrafData(payload))

    def _sendTelegrafData(self, payload):
        if type(payload) == list:
            payload = NaoApp.FORMAT_TELEGRAFFRAMESEPERATOR*len(payload) % tuple(payload)
        try:
            self.__conneciton.request(NaoApp.NAME_POST, NaoApp.URLTELEGRAF, payload, self.headers) # type: ignore
            status = self.__conneciton.getresponse().status # type: ignore
            self.__conneciton.close() # type: ignore
        except:
            self._loginNao()
            self.__conneciton.request(NaoApp.NAME_POST, NaoApp.URLTELEGRAF, payload, self.headers) # type: ignore
            status = self.__conneciton.getresponse().status # type: ignore
            self.__conneciton.close() # type: ignore
        return(status)

    def _sendDataToNaoJson(self, method, url, payload) -> dict:
        header = copy(self.headers)
        header[NaoApp.NAME_CONTENT_HEADER] = NaoApp.HEADER_JSON
        if payload != None:
            payload = dumps(payload)
        try:
            self.__conneciton.request(method, url, payload, header) # type: ignore
            data = self.__conneciton.getresponse().read() # type: ignore
            self.__conneciton.close() # type: ignore
        except:
            self._loginNao()
            header = copy(self.headers)
            header[NaoApp.NAME_CONTENT_HEADER] = NaoApp.HEADER_JSON
            self.__conneciton.request(method, url, payload, header) # type: ignore
            data = self.__conneciton.getresponse().read() # type: ignore
            self.__conneciton.close() # type: ignore
        if data == b'':
            return('') # type: ignore
        else:
            try:
                return(loads(data))
            except:
                return(-1) # type: ignore
    
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
        start_time = datetime.datetime.now()
        self.print(" START process")
        while 1==1:
            if datetime.datetime.now().hour >= self.exit_hour.hour and datetime.datetime.now().day != start_time.day:
                print("process will end soon")
                self.end_transfer = True
                break
            if self.end_confirmation:
                break
            if self.endwithexit:
                self._addAndUpdateTotalNumberOfSentData()
                self.print("ERROR-Unknow: end process with exit()")
                exit()
            sleep(10)
        self.print(" EXIT process " + str(datetime.datetime.now()))
        sleep(2)
        self.exit()     

    def exit(self):
        self._addAndUpdateTotalNumberOfSentData()
        print("ending")

    def __DataTransferLogging(self):
        Thread(target=self.__DataTransferLoggingData, args=()).start()
        start = time()
        while 1==1:
            sleep(self.transfer_config[NaoApp.TRANSFERINTERVAL]) # type: ignore
            data = self.logging_data
            self.logging_data = []
            self.logging_data_add = self.logging_data.extend
            Thread(target=self.__DataTransferLoggingBuffer, args=(data,)).start()
            if time() - start > 800:
                self._addAndUpdateTotalNumberOfSentData()
                start = time()
            if self.end_transfer:
                sleep(self.transfer_config[NaoApp.TRANSFERINTERVAL]) # type: ignore
                self.end_confirmation = True
                break     

    def __DataTransferLoggingData(self):
        update_time = time()  
        while 1==1:
            start = time()
            try:    
                self.logging_data_add(self.DataForLogging.getTelegrafData()) # type: ignore
            except Exception as e:
                self.print("ERROR-FromDb: "+  str(e))
                sleep(self.transfer_config[NaoApp.ERRORSLEEPLOGGER]) # type: ignore
                ending = self.DataForLogging.refreshConnection() # type: ignore
                if ending:
                    self.end_transfer = True
            if time() - update_time > 800:
                self._addAndUpdateTotalNumberOfSentData()
                update_time = time()
            if self.end_transfer:
                self.DataForLogging.exit() # type: ignore
                break
            diff = time() - start
            if diff < self.transfer_config[NaoApp.LOGGINGINTERVAL]: # type: ignore
                sleep(self.transfer_config[NaoApp.LOGGINGINTERVAL]-diff) # type: ignore

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
                    sleep(self.transfer_config[NaoApp.ERRORSLEEP]) # type: ignore
                    self._loginNao()
                else:
                    self.print("ERROR: nao.status=" + str(status))
                    sleep(self.transfer_config[NaoApp.ERRORSLEEP]) # type: ignore
            except Exception as e:
                self.print("ERROR-Nao:" + str(e))
                sleep(self.transfer_config[NaoApp.ERRORSLEEP]) # type: ignore
            if self.end_transfer: break
            if time() - start > self.transfer_config[NaoApp.MAXBUFFERTIME]:  # type: ignore
                self.print("WARNING:" + str(len(data)) + " datasets destroyed")
                break 
            # TODO: hier könnte noch ein Buffer auf Festplatte gebaut werden

    def __dataTransferFromDb(self):
        while 1==1:
            start = time()
            try:    
                data = self.DataFromDb.getTelegrafData(max_data_len=self.transfer_config[NaoApp.DATAPERTELEGRAF]) # type: ignore
                data_len = len(data) 
            except TimeoutError:
                data = []
                self.print("ERROR-FromDb: TimeoutError")
                sleep(self.transfer_config[NaoApp.ERRORSLEEP]) # type: ignore
                self.DataFromDb.refreshConnection() # type: ignore
                continue
            except Exception as e:
                data = []
                data_len = 0
                self.print("ERROR-FromDb:" + str(e))
                sleep(self.transfer_config[NaoApp.ERRORSLEEP]) # type: ignore
                self.DataFromDb.refreshConnection() # type: ignore
            try:
                if data != []:
                    status = self.sendTelegrafData(data)
                    print(data_len, " data sendet")
                    if status == 204: # type: ignore
                        self.DataFromDb.confirmTransfer() # type: ignore
                        self.sending_counter += data_len
                    elif status == 500: # type: ignore
                        self.print("ERROR: nao.status=" + str(status)) # type: ignore
                        self._loginNao()
                    else: 
                        self.print("ERROR: nao.status=" + str(status)) # type: ignore
                        self._loginNao()
            except Exception as e:
                self.print("ERROR-Nao:" + str(e))
                sleep(self.transfer_config[NaoApp.ERRORSLEEP]) # type: ignore
                self.DataFromDb.refreshConnection() # type: ignore
            diff = time() - start
            if self.end_transfer:
                self.DataFromDb.exit() # type: ignore
                self.end_confirmation = True
                break
            if diff < self.transfer_config[NaoApp.TRANSFERINTERVAL]: # type: ignore
                sleep(self.transfer_config[NaoApp.TRANSFERINTERVAL]-diff) # type: ignore

    def __dataTransferFromListener(self):
        while 1==1:
            try: 
                start = time() 
                data = self.DataForListener.getTelegrafData() # type: ignore
                data_len = len(data)
                self.logging_data_add(data)
                if (len(self.logging_data)) > NaoApp.STANDARD_DATA_PER_FUNC_CALL:
                    self.print("delteted data-len: " + str(int(len(self.logging_data)-NaoApp.STANDARD_DATA_PER_FUNC_CALL)))
                    self.logging_data[int(len(self.logging_data)-NaoApp.STANDARD_DATA_PER_FUNC_CALL):]
                try:
                    if self.logging_data != []:
                        status = self.sendTelegrafData(self.logging_data)    
                        if status == 204:
                            self.sending_counter += data_len
                            del self.logging_data
                            self.logging_data = []
                            self.logging_data_add = self.logging_data.extend
                        else:
                            self.print("ERROR: nao.status=" + str(status))
                            self._loginNao()
                except Exception as e:
                    self.print("ERROR-Nao:" + str(e))
                    sleep(self.transfer_config[NaoApp.ERRORSLEEP]) # type: ignore
                    self.DataForListener.refreshConnection() # type: ignore
                    self._loginNao()
                if self.sending_counter > 10000:
                    self._addAndUpdateTotalNumberOfSentData()
                diff = time() - start
                if self.end_transfer:
                    self.DataForListener.exit() # type: ignore
                    self.end_confirmation = True
                    break
                if diff < self.transfer_config[NaoApp.TRANSFERINTERVAL]: # type: ignore
                    sleep(self.transfer_config[NaoApp.TRANSFERINTERVAL]-diff) # type: ignore
            except Exception as e:
                try:
                    self.print("ERROR-Unknow (329):" + str(e))
                    self._loginNao()
                    self.DataForListener.refreshConnection() # type: ignore
                except:
                    self.endwithexit = True

    def _getTransferCofnig(self):
        config = self.db.getTinyTables(NaoApp.TRANSFERCONFIG)
        if config == []:
            config = [{
                NaoApp.DATAPERTELEGRAF: NaoApp.STANDARD_DATA_PER_FUNC_CALL,
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

    def _loginNaoLocal(self):
        try:
            self.print(NaoApp.MESSAGELOGIN)
            self.__conneciton = http.client.HTTPConnection(self.auth[NaoApp.NAME_HOST])
            self.__conneciton.request(NaoApp.NAME_POST, NaoApp.URLLOGIN, self.auth[NaoApp.NAME_PAYLOAD], NaoApp.LOGINHEADER)
            res = self.__conneciton.getresponse()
            data = loads(res.read().decode(NaoApp.NAME_UTF8))
            self.headers[NaoApp.NAME_WEBAUTH] = NaoApp.BEARER + data[NaoApp.NAME_TOKENAC]
        except:
            try:
                sleep(self.transfer_config[NaoApp.ERRORSLEEP]) # type: ignore
                self.print(NaoApp.MESSAGELOGIN)
                self.__conneciton = http.client.HTTPConnection(self.auth[NaoApp.NAME_HOST])
                self.__conneciton.request(NaoApp.NAME_POST, NaoApp.URLLOGIN, self.auth[NaoApp.NAME_PAYLOAD], NaoApp.LOGINHEADER)
                res = self.__conneciton.getresponse()
                data = loads(res.read().decode(NaoApp.NAME_UTF8))
                self.headers[NaoApp.NAME_WEBAUTH] = NaoApp.BEARER + data[NaoApp.NAME_TOKENAC]
            except:
                self.__conneciton = http.client.HTTPConnection(self.auth[NaoApp.NAME_HOST])
                self.__conneciton.request(NaoApp.NAME_POST, NaoApp.URLLOGIN, self.auth[NaoApp.NAME_PAYLOAD], NaoApp.LOGINHEADER)
                res = self.__conneciton.getresponse()
                data = res.read().decode(NaoApp.NAME_UTF8)
                self.print("ERROR login, data: " + str(data))
                self.endwithexit = True

    def _loginNaoCloud(self):
        try:
            self.print(NaoApp.MESSAGELOGIN)
            self.__conneciton = http.client.HTTPSConnection(self.auth[NaoApp.NAME_HOST])
            self.__conneciton.request(NaoApp.NAME_POST, NaoApp.URLLOGIN, self.auth[NaoApp.NAME_PAYLOAD], NaoApp.LOGINHEADER)
            res = self.__conneciton.getresponse()
            data = loads(res.read().decode(NaoApp.NAME_UTF8))
            self.headers[NaoApp.NAME_WEBAUTH] = NaoApp.BEARER + data[NaoApp.NAME_TOKENAC]
        except:
            try:
                sleep(self.transfer_config[NaoApp.ERRORSLEEP]) # type: ignore
                self.print(NaoApp.MESSAGELOGIN)
                self.__conneciton = http.client.HTTPSConnection(self.auth[NaoApp.NAME_HOST])
                self.__conneciton.request(NaoApp.NAME_POST, NaoApp.URLLOGIN, self.auth[NaoApp.NAME_PAYLOAD], NaoApp.LOGINHEADER)
                res = self.__conneciton.getresponse()
                data = loads(res.read().decode(NaoApp.NAME_UTF8))
                self.headers[NaoApp.NAME_WEBAUTH] = NaoApp.BEARER + data[NaoApp.NAME_TOKENAC]
            except:
                self.__conneciton = http.client.HTTPSConnection(self.auth[NaoApp.NAME_HOST])
                self.__conneciton.request(NaoApp.NAME_POST, NaoApp.URLLOGIN, self.auth[NaoApp.NAME_PAYLOAD], NaoApp.LOGINHEADER)
                res = self.__conneciton.getresponse()
                data = res.read().decode(NaoApp.NAME_UTF8)
                self.print("ERROR login, data: " + str(data))
                self.endwithexit = True

    def _loginNao(self):
        if self.local:
            self._loginNaoLocal()
        else:
            self._loginNaoCloud()

    def print(self, log:str):
        if self.error_log:
            error_file = open(self.error_log, "a")
            error_file.writelines(str(datetime.datetime.now()) + log+'\n')
            error_file.close()
        else:
            print(log)

    def createWorkspace(self, name, avatar=None, tagitems=[]):
        payload = {
            NaoApp.NAME_NAME: name,
            NaoApp.NAME_AVATAR_ID: avatar,
            NaoApp.NAME_TAGITEMS_ID: tagitems
        }
        return(self._sendDataToNaoJson(NaoApp.NAME_POST, NaoApp.URL_WORKSPACE, payload))

    def createAsset(self, name, _workspace, description="", baseInterval="1m", useGeolocation=True, avatar=None, tagitems=[]):
        payload = {
            NaoApp.NAME_WORKSPACE_ID: _workspace,
            NaoApp.NAME_NAME: name,
            NaoApp.NAME_DESCRIPTION: description,
            NaoApp.NAME_AVATAR_ID: avatar,
            NaoApp.NAME_BASE_INTERVAL: baseInterval,
            NaoApp.NAME_USE_GEOLOCATION: useGeolocation
        }
        return(self._sendDataToNaoJson(NaoApp.NAME_POST, NaoApp.URL_ASSET, payload))

    def createPath(self, name, _asset, description="", color="#02c1de", _parent=None):
        payload = {
            NaoApp.NAME_ASSET_ID: _asset,
            NaoApp.NAME_NAME: name,
            NaoApp.NAME_DESCRIPTION: description,
            NaoApp.NAME_COLOR: color,
            NaoApp.NAME_PARENT_ID: _parent
        }
        return(self._sendDataToNaoJson(NaoApp.NAME_POST, NaoApp.URL_PATH, payload))

    def createInputcontainer(self, name, _asset, description="", color="#02c1de"):
        payload = {
            NaoApp.NAME_RELATION_ID: _asset,
            NaoApp.NAME_NAME: name,
            NaoApp.NAME_DESCRIPTION: description,
            NaoApp.NAME_COLOR: color
        }
        return(self._sendDataToNaoJson(NaoApp.NAME_POST, NaoApp.URL_INPUTCONTAINER, payload))

    def patchIpuntsInputcontainer(self, _inputcontainer, _inputs):
        payload = {
            NaoApp.NAME_INPUTS_ID: _inputs
        }
        return(self._sendDataToNaoJson(NaoApp.NAME_PATCH, NaoApp.URL_INPUTCONTAINER+"/"+_inputcontainer, payload))

    def createInput(self, name, _asset, value="", description="", component="input-text-field", props={}, rules={}):
        payload = {
            NaoApp.NAME_RELATION_ID: _asset,
            NaoApp.NAME_COMPONENT: component,
            NaoApp.NAME_VALUE: value,
            NaoApp.NAME_NAME: name,
            NaoApp.NAME_DESCRIPTION: description,
            NaoApp.NAME_PROPS: props,
            NaoApp.NAME_RULES: rules,
            NaoApp.NAME_FIELD: "8ee"+str(time_ns())[-5:]+"-513b-11ed-98eb-5dac241cd04e"
        }
        sleep(0.01)
        return(self._sendDataToNaoJson(NaoApp.NAME_POST, NaoApp.URL_INPUT_DESC, payload))

    def createUnit(self, name):
        payload = {
            NaoApp.NAME_NAME: name
        }
        return(self._sendDataToNaoJson(NaoApp.NAME_POST, NaoApp.URL_UNITS, payload))

    def createSeries(self, type, name, description, _asset, _part, _unit, max, min, fill, fillValue, color="#02c1de", _tagitems=None):
        if not isinstance(fill, (float, int)):
            fill = "null"
        if not isinstance(fillValue, (float, int)):
            fillValue = None
        if not isinstance(max, (float, int)):
            max = None
        if not isinstance(min, (float, int)):
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
        return(self._sendDataToNaoJson(NaoApp.NAME_POST, NaoApp.URL_SERIES + type, payload))

    def createInstance(self, name, description, _asset, geolocation=[], _tagitems=[]):
        payload = {
            NaoApp.NAME_NAME: name,
            NaoApp.NAME_DESCRIPTION: description,
            NaoApp.NAME_GEOLOCATION: geolocation,
            NaoApp.NAME_ASSET_ID: _asset,
            NaoApp.NAME_TAGITEMS_ID: _tagitems
        }
        return(self._sendDataToNaoJson(NaoApp.NAME_POST, NaoApp.URL_INSTANCE, payload))
        
    def patchEnpointConifg(self, conf:dict, _instance=None, _series=None, _asset=None, **args):
        """
        _asset (id), _instance (id) and _series (id) can be used as arguments \n
        or \n
        _endpoint (id) (actual instance-series) \n
        -->\n
        if no endpoint in NAO you can fix it with give args _asset, _instance, _series and series_type
        --> series_type can be Meter, Sensor, Actor or Setpoint
        """
        if args.get(NaoApp.NAME_ENDPOINT_ID):
            try:
                return(self._sendDataToNaoJson("PATCH", NaoApp.URL_SERIES+args[NaoApp.NAME_ENDPOINT_ID], dumps(
                    {NaoApp.NAME_CONFIG: dumps(conf)}
                )))
            except:
                raise Exception("no _endpoint in NAO, fix with give args _asset, _instance, _series and series-type")
        if _series == None and _instance == None:
            raise Exception("_series, _instance (or _endpoint) is missing")
        result = self._sendDataToNaoJson(NaoApp.NAME_GET, NaoApp.URL_SERIES+NaoApp.QUERY_GET_ENDPOINT%(_instance,_series), None)
        if result[NaoApp.NAME_RESULTS] == []:
            if _asset == None or args.get(NaoApp.NAME_POINT_MODEL):
                raise Exception("_asset or series_type is missing")
            return(self._sendDataToNaoJson(NaoApp.NAME_POST, NaoApp.URL_SERIES,{
                NaoApp.NAME_INSTANCE_ID:_instance,
                NaoApp.NAME_ASSET_ID:_asset,
                NaoApp.NAME_POINT_ID:_series,
                NaoApp.NAME_POINT_MODEL:args[NaoApp.SERIES_TYPE],
                NaoApp.NAME_CONFIG:dumps(conf)
            })[NaoApp.NAME_ID_ID])
        else:
            return(self._sendDataToNaoJson("PATCH", NaoApp.URL_SERIES+result[NaoApp.NAME_RESULTS][0][NaoApp.NAME_ID_ID], dumps(
                    {NaoApp.NAME_CONFIG: dumps(conf)}
            )))

    def postEnpointConifg(self, conf:dict, _instance=None, _series=None, _asset=None):
        """
        _asset (id), _instance (id) and _series (id) can be used as arguments \n
        """
        return(self._sendDataToNaoJson(NaoApp.NAME_POST, NaoApp.URL_SERIES,{
            NaoApp.NAME_INSTANCE_ID:_instance,
            NaoApp.NAME_ASSET_ID:_asset,
            NaoApp.NAME_POINT_ID:_series,
            NaoApp.NAME_CONFIG:dumps(conf)
        })[NaoApp.NAME_ID_ID])

    def getEndpoints(self, **args):
        if len(args) > 0:
            query = NaoApp.QUERY_GET
        else:
            query = ""
        for arg in args:
            query += arg + "=" + args[arg] + ","
        return(self._sendDataToNaoJson(NaoApp.NAME_GET, NaoApp.URL_SERIES+query, {}))

    def deleteEndpoint(self, _endpoint):
        self._sendDataToNaoJson(NaoApp.NAME_DELETE, NaoApp.URL_SERIES+_endpoint, {})
        return(NaoApp.NAME_DELETE)
    
    def getWorkspace(self, **args):
        if len(args) > 0:
            query = NaoApp.QUERY_GET
        else:
            query = ""
        for arg in args:
            query += arg + "=" + args[arg] + ","
        return(self._sendDataToNaoJson(NaoApp.NAME_GET, NaoApp.URL_WORKSPACE+query, {}))
    
    def getAssets(self, **args):
        if len(args) > 0:
            query = NaoApp.QUERY_GET
        else:
            query = ""
        for arg in args:
            query += arg + "=" + args[arg] + ","
        return(self._sendDataToNaoJson(NaoApp.NAME_GET, NaoApp.URL_ASSET+query, {}))

    def getInstances(self, **args):
        if len(args) > 0:
            query = NaoApp.QUERY_GET
        else:
            query = ""
        for arg in args:
            query += arg + "=" + args[arg] + ","
        return(self._sendDataToNaoJson(NaoApp.NAME_GET, NaoApp.URL_INSTANCE+query, {}))
    
    def getSeries(self, **args):
        if len(args) > 0:
            query = NaoApp.QUERY_GET
        else:
            query = ""
        for arg in args:
            query += arg + "=" + args[arg] + ","
        return(self._sendDataToNaoJson(NaoApp.NAME_GET, NaoApp.URL_SERIES+NaoApp.CONSOLIDATE+query, {}))

    def getUnits(self, **args):
        if len(args) > 0:
            query = NaoApp.QUERY_GET
        else:
            query = ""
        for arg in args:
            query += arg + "=" + args[arg] + ","
        return(self._sendDataToNaoJson(NaoApp.NAME_GET, NaoApp.URL_UNITS+query, {}))

    def getSingelValues(self, organizationId, first_time="-365d", last_time="now()", points=[{"id":"all"}], validates=False, aggregate="mean"):
        '''
        points ->   {
                        "id": str,
                        "asset": str,
                        "instance": str,
                        "series": str,
                    }
        '''
        payload = {
            NaoApp.NAME_SELECT:{
                NaoApp.NAME_ORGANIZATIONID:organizationId,
                NaoApp.NAME_POINTS:points,
                NaoApp.NAME_RANGE: {
                    NaoApp.NAME_START:first_time,
                    NaoApp.NAME_STOP:last_time
                },
                NaoApp.NAME_VALIDATES:validates,
            },
            NaoApp.NAME_AGGREGATE:aggregate
        }
        return(self._sendDataToNaoJson(NaoApp.NAME_POST, NaoApp.URL_SINGELVALUES, payload=payload))

    def getPlotformatetTimeseries(self, select):
        return(self._sendDataToNaoJson(NaoApp.NAME_POST, NaoApp.URL_PLOTTIMESERIES, payload=select))

#TODO refactoring BuildAssets
class BuildAssets():
    BASE_INTERVALL = "2m"

    def __init__(self, NaoApp):
        self.Nao = NaoApp
    
    def build(self, config_name, workspace, intervall=None):
        if not intervall:
            intervall=BuildAssets.BASE_INTERVALL
        config = self.readConfig(config_name)
        comp_color = config["component_color"]
        description_asset = config["description_asset"]
        sensors = config["sensors"]
        ret = {
            "_asset": config["_asset"],
            "_unit": config["_unit"]
        } 
        for sensor in sensors:
            if sensor.get("_asset"):
                continue
            # ASSET
            if sensor["asset"] not in ret["_asset"]:
                data_asset = self.Nao.createAsset(
                    name=sensor["asset"],
                    _workspace=workspace,
                    description=description_asset.get(sensor["asset"]), # type: ignore
                    baseInterval=intervall
                )
                ret["_asset"][sensor["asset"]] = {"_id":data_asset["_id"], "component":{}}
                asset = data_asset["_id"]
            else:
                asset = ret["_asset"][sensor["asset"]]["_id"]
            sensor["_asset"] = asset
            # COMONENT
            if sensor["component"] not in ret["_asset"][sensor["asset"]]["component"]:
                data_path = self.Nao.createPath(
                    name=sensor["component"],
                    _asset=asset,
                    color=comp_color.get(sensor["component"]) # type: ignore
                )
                ret["_asset"][sensor["asset"]]["component"][sensor["component"]] = data_path["_id"]
                component = data_path["_id"]
            else:
                component = ret["_asset"][sensor["asset"]]["component"][sensor["component"]]
            sensor["_component"] = component
            # UNIT
            if sensor["unit"] not in ret["_unit"]:
                data_unit = self.Nao.createUnit(
                    name=sensor["unit"]
                )
                ret["_unit"][sensor["unit"]] = data_unit["_id"]
                unit = data_unit["_id"]
            else:
                unit = ret["_unit"][sensor["unit"]]
            sensor["_unit"] = unit
            # SERIES
            series_data = self.Nao.createSeries(
                type=sensor["type"],
                name=sensor["name"],
                description=sensor["description"],
                _asset=asset,
                _part=component,
                _unit=unit,
                max=sensor["max"],
                min=sensor["min"],
                fill="null",
                fillValue=None,
                color=sensor["color"]
            )
            sensor["_series"] = series_data["_id"]
            self.writeConfig("data.json", {"sensors":sensors, "_asset": ret["_asset"], "_unit":ret["_unit"], "component_color":comp_color,"description_asset":description_asset})

    def writeConfig(self, name, conf):
        file = open(name, "w")
        file.writelines(dumps(conf))
        file.close()

    def readConfig(self, name):
        file = open(name, "r")
        conf = loads(file.read())
        file.close()
        return(conf)