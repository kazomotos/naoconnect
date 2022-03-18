'Autor: Rupert Wieser -- Naotilus -- 20220219'
import http.client
from logging.handlers import DatagramHandler
from sqlite3 import DatabaseError
from urllib.parse import quote
from json import loads
from time import sleep, time
from threading import Thread
from naoconnect.TinyDb import TinyDb
from naoconnect.Param import Param

class NaoApp(Param):
    URLLOGIN = "/api/nao/auth/login"
    URLTELEGRAF = "/api/telegraf/"
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


class NaoApp(Param):
    URLLOGIN = "/api/nao/auth/login"
    URLTELEGRAF = "/api/telegraf/"
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

    def __init__(self, host, email, password, DataFromDb=False, DataForLogging=False, DataForListener=False, tiny_db_name="nao.json"):
        self.auth = {
            NaoApp.NAME_HOST:host,
            NaoApp.NAME_PAYLOAD:NaoApp.NAME_EMAIL+"="+quote(email)+"&"+NaoApp.NAME_PASSWD+"="+quote(password)
        }
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
    
    def startDataTransferFromDb(self):
        if not self.DataFromDb: 
            print("ERROR: no db in this class initialized")
            return(-1)
        Thread(target=self.__dataTransferFromDb, args=()).start()
        self.__transferInterrupt()
    
    def startDataTransferLogging(self):
        if not self.DataForLogging: 
            print("ERROR: no interface logger in this class initialized")
            return(-1)
        Thread(target=self.__DataTransferLogging, args=()).start()
        self.__transferInterrupt()

    def startDataTransferFromListener(self):
        if not self.DataForListener: 
            print("ERROR: no interface logger in this class initialized")
            return(-1)
        Thread(target=self.__dataTransferFromListener, args=()).start()
        self.__transferInterrupt()

    def __transferInterrupt(self):
        start_time = time()
        while 1==1:
            print("enter 'exit' for end data transfer to telegraf")
            print("priode of transfer time:", round((time() - start_time)/60,2), "min, total sent value:",self.sending_counter)
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
            if time() - start > 3600:
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
                print("ERROR-FromDb:", e)
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
                    print("number of sent values:", data_len)
                    break
                elif status == 500:
                    print("ERROR: nao.status=", status)
                    sleep(self.transfer_config[NaoApp.ERRORSLEEP])
                    self._loginNao()
                else:
                    print("ERROR: nao.status=", status)
                    sleep(self.transfer_config[NaoApp.ERRORSLEEP])
            except Exception as e:
                print("ERROR-Nao:", e)
                sleep(self.transfer_config[NaoApp.ERRORSLEEP])
            if self.end_transfer: break
            if time() - start > self.transfer_config[NaoApp.MAXBUFFERTIME]: 
                print("WARNING:", len(data), "datasets destroyed")
                break 
            # TODO: hier könnte noch ein Buffer auf Festplatte gebaut werden

    def __dataTransferFromDb(self):
        while 1==1:
            start = time()
            try:    
                data = self.DataFromDb.getTelegrafData(max_data_len=self.transfer_config[NaoApp.DATAPERTELEGRAF])
                data_len = len(data)
            except Exception as e:
                data = []
                print("ERROR-FromDb:", e)
                sleep(self.transfer_config[NaoApp.ERRORSLEEP])
                self.DataFromDb.refreshConnection()
            try:
                status = self.sendTelegrafData(data)    
                if status == 204:
                    self.DataFromDb.confirmTransfer()
                    self.sending_counter += data_len
                    print("number of sent values:", data_len)
                elif status ==500:
                    print("ERROR: nao.status=", status)
                    self._loginNao()
                else:
                    print("ERROR: nao.status=", status)
            except Exception as e:
                print("ERROR-Nao:", e)
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
        self.DataForListener.refreshConnection()
        while 1==1:
            start = time() 
            data = self.DataForListener.getTelegrafData()
            data_len = len(data)
            self.logging_data_add(data)
            if (len(self.logging_data)) > NaoApp.STANDARD_DATAPERTELEGRAF:
                print("delteted data-len:", int(len(self.logging_data)-NaoApp.STANDARD_DATAPERTELEGRAF))
                self.logging_data[int(len(self.logging_data)-NaoApp.STANDARD_DATAPERTELEGRAF):]
            try:
                if self.logging_data != []:
                    status = self.sendTelegrafData(self.logging_data)    
                    if status == 204:
                        self.sending_counter += data_len
                        del self.logging_data
                        self.logging_data = []
                        self.logging_data_add = self.logging_data.extend
                        print("number of sent values:", data_len)
                    elif status ==500:
                        print("ERROR: nao.status=", status)
                        self._loginNao()
                    else:
                        print("ERROR: nao.status=", status)
            except Exception as e:
                print("ERROR-Nao:", e)
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
        print(NaoApp.MESSAGELOGIN)
        self.__conneciton = http.client.HTTPSConnection(self.auth[NaoApp.NAME_HOST])
        self.__conneciton.request(NaoApp.NAME_POST, NaoApp.URLLOGIN, self.auth[NaoApp.NAME_PAYLOAD], NaoApp.LOGINHEADER)
        res = self.__conneciton.getresponse()
        data = loads(res.read().decode(NaoApp.NAME_UTF8))
        self.headers[NaoApp.NAME_WEBAUTH] = NaoApp.BEARER + data[NaoApp.NAME_TOKENAC]
        # self.headers[NaoApp.NAME_COOKIE] = NaoApp.NAME_TOKENRE + data[NaoApp.NAME_TOKENRE]

if __name__ == "__main__":
    'test'
    Nao = NaoApp("nao-app.de", "???", "????")
    Nao.startDataTransferFromDb()

