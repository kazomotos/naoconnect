from naoconnect.TinyDb import TinyDb
from naoconnect.Param import Param
from datetime import datetime
from time import sleep
from threading import Thread
import paho.mqtt.subscribe as subscribe



class Mqtt(Param):
    SECTONANO = 1000000000

    def __init__ (self, broker, tiny_db_name="mqtt.json", error_log=False):
        self.db = TinyDb(tiny_db_name)
        self.broker = broker
        self.transfere = self._getTransferChannels()
        self.data = []
        self.add_data = self.data.append
        self.disconnect = False
        self.disconnected = True
        self.error_log=error_log
    
    def getTelegrafData(self):
        data = self.data
        self.data = []
        self.add_data = self.data.append
        return(data)
    
    def _connect(self):
        Thread(target=self.__connect, args=()).start()

    def __connect(self):
        self.disconnected = False
        subscribe.callback(
            self.__on_message, 
            list(self.transfere.keys()), 
            hostname=self.broker
        )
        self.print("mqtt connected")

    def __on_message(self, client, userdata, message):
        try:
            self.add_data(Mqtt.FORMAT_TELEGRAFFRAMESTRUCT % (
                    self.transfere[message.topic][0], 
                    self.transfere[message.topic][1], 
                    self.transfere[message.topic][2],
                    float(message.payload),
                    int(round(datetime.timestamp(datetime.utcnow()),0)*Mqtt.SECTONANO)
            ))
        except Exception as e:
            self.print(e)
            client.disconnect()
            self.disconnected = True
            self.print("mqtt disconneted")
            self.refreshConnection()
        if self.disconnect:
            client.disconnect()
            self.print("mqtt disconneted")
            self.disconnected = True

    def refreshConnection(self):
        self.print("refresh mqtt")
        self.disconnect = True
        while 1==1:
            sleep(5)
            if self.disconnected == True:
                break
        self.disconnect = False
        self._connect()        
    
    def exit(self):
        self.disconnect = True
        while 1==1:
            sleep(10)
            if self.disconnected == True:
                break
        return(-1)        

    def _getTransferChannels(self):
        ''' 
        {
            <channel>:  [<twin>, <insatance>, <measurement>]
        }
        '''
        channels = self.db.getTinyTables(Mqtt.NAME_TRANSFERCHANNELS)
        result = {}
        for channel in channels:
            result[channel[Mqtt.NAME_CHANNEL]] = channel[Mqtt.NAME_TELEGRAF]
        return(result)
    
    def _putTransferChannels(self, value):
        ''' 
        [{
            "channel": <channel>, 
            "telegraf": [<twin>, <insatance>, <measurement>]
        }] 
        '''
        for val in value:
            self.db.putTinyTables(Mqtt.NAME_TRANSFERCHANNELS, val)
        self.transfere = value

    def print(self, log:str):
        if self.error_log:
            error_file = open(self.error_log, "a")
            error_file.writelines(log+'\n')
            error_file.close()
        else:
            print(log)