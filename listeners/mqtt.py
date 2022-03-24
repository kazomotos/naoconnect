from http import client
from naoconnect.TinyDb import TinyDb
from naoconnect.Param import Param
from datetime import datetime
from time import sleep
from threading import Thread
from paho.mqtt.client import Client

class Mqtt(Param):
    SECTONANO = 1000000000

    def __init__ (self, broker, tiny_db_name="mqtt.json", error_log=False):
        self.db = TinyDb(tiny_db_name)
        self.Client = Client()
        self.Client.on_message = self.__on_message
        self.Client.on_connect = self.__on_connect
        self.Client.on_disconnect = self.__on_disconnect
        self.broker = broker
        self.transfere = self._getTransferChannels()
        self.data = []
        self.add_data = self.data.append
        self.error_log=error_log
        self._connect()

    def getTelegrafData(self):
        data = self.data
        self.data = []
        self.add_data = self.data.append
        return(data)

    def refreshConnection(self):
        self.print("refresh mqtt")
        try:
            self.Client.loop_stop()
        except:
            None
        self.Client.disconnect()
        self._connect()
    
    def exit(self):
        self.Client.disconnect()
        return(-1)        

    def _connect(self):
        self.Client.connect(host=self.broker)
        self._subscribe()
        Thread(target=self.Client.loop_start, args=()).start()

    def _subscribe(self):
        for channel in self.transfere:
            self.Client.subscribe(channel)

    def __on_connect(self, client, userdata, flags, rc):
        self.print("mqtt connected")
    
    def __on_disconnect(self, client, userdata, rc):
        self.print("mqtt disconnect")

    def __on_message(self, client, userdata, msg):
        try:
            self.add_data(Mqtt.FORMAT_TELEGRAFFRAMESTRUCT % (
                    self.transfere[msg.topic][0], 
                    self.transfere[msg.topic][1], 
                    self.transfere[msg.topic][2],
                    float(msg.payload),
                    int(round(datetime.timestamp(datetime.utcnow()),0)*Mqtt.SECTONANO)
            ))
        except Exception as e:
            self.print(e)
            self.refreshConnection()

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