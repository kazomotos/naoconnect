from http import client
from naoconnect.TinyDb import TinyDb
from naoconnect.Param import Param
from datetime import datetime
from time import sleep, time
from threading import Thread
from paho.mqtt.client import Client
import paho.mqtt.subscribe as subscribe

class Mqtt(Param):
    SECTONANO = 1000000000

    def __init__ (self, broker, tiny_db_name="mqtt.json", error_log=False, start_on_init=True, password="", username=""):
        self.password = password
        self.username = username
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
        self.stop_add_data = False
        if self.username != "":
            self.Client.username_pw_set(username=self.username, password=self.password)
        if start_on_init:
            self.startListenersFromConf()

    def getTelegrafData(self):
        self.stop_add_data = True
        data = self.data
        self.data = []
        self.stop_add_data = False
        self.add_data = self.data.append
        return(data)

    def refreshConnection(self):
        self.print("refresh mqtt")
        try:
            self.Client.loop_stop()
        except:
            None
        self.Client.disconnect()
        self.startListenersFromConf()
    
    def exit(self):
        self.Client.disconnect()
        return(-1)        

    def startListenersFromConf(self):
        self.Client.connect(host=self.broker)
        self._subscribeFromConf()
        Thread(target=self.Client.loop_start, args=()).start()

    def startListener(self):
        self.Client.connect(host=self.broker)
        Thread(target=self.Client.loop_start, args=()).start()

    def _subscribeFromConf(self):
        for channel in self.transfere:
            self.Client.subscribe(channel)
    
    def _subscribeOneTopic(self, topic):
        self.Client.subscribe(topic)

    def __on_connect(self, client, userdata, flags, rc):
        self.print("mqtt connected")
    
    def __on_disconnect(self, client, userdata, rc):
        self.print("mqtt disconnect")

    def __on_message(self, client, userdata, msg):
        try:
            while self.stop_add_data: sleep(1)
            self.add_data(Mqtt.FORMAT_TELEGRAFFRAMESTRUCT % (
                    self.transfere[msg.topic][0], 
                    self.transfere[msg.topic][1], 
                    self.transfere[msg.topic][2],
                    float(msg.payload),
                    int(round(datetime.timestamp(datetime.utcnow()),0)*Mqtt.SECTONANO)
            ))
        except Exception as e:
            self.print(str(e))
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
 
class MqttHelp(Param):

    def __init__ (self, broker, password="", username=""):
        self.password = password
        self.username = username        
        self.Client = Client()
        self.Client.on_message = self.__on_message
        self.Client.on_connect = self.__on_connect
        self.Client.on_disconnect = self.__on_disconnect
        self.broker = broker
        self.topic_set = set()
        self.add_topic = self.topic_set.add
        if self.username != "":
            self.Client.username_pw_set(username=self.username, password=self.password)

    def subscribeOneTopic(self, topic):
        self.Client.subscribe(topic)

    def startListener(self, max_data=100000, max_time=120):
        self.start_time = time()
        self.max_data = max_data
        self.max_time = max_time
        self.Client.connect(host=self.broker)
        Thread(target=self.Client.loop_start, args=()).start()

    def topicToTxt(self, file):
        data = open(file, "w")
        data.writelines(self.topic_set)
        data.close()

    def __on_message(self, client, userdata, msg):
        self.add_topic(
            str(msg.topic) + "\n"
        )
        if len(self.topic_set) >= self.max_data:
            self.disconnect()
        if time() - self.start_time >= self.max_time:
            self.disconnect()

    def __on_connect(self, client, userdata, flags, rc):
        print("mqtt connected")
    
    def __on_disconnect(self, client, userdata, rc):
        print("mqtt disconnect")

    def disconnect(self):
        self.Client.disconnect()  