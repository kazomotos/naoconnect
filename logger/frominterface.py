from nao.TinyDb import TinyDb
from nao.Param import Param
from datetime import datetime
from time import sleep

class vVissmann(Param):
    SECTONANO = 1000000000

    def __init__ (self, viControl, tiny_db_name="singel_interface.json"):
        self.viControl = viControl
        self.Control = viControl()
        self.Control.initComm()
        self.db = TinyDb(tiny_db_name)
        self.transfere = self._getTransferChannels()
        self.refreshcounter = 0
    
    def getTelegrafData(self):
        data = []
        data_add = data.append
        for channel in self.transfere:
            data_add(vVissmann.FORMAT_TELEGRAFFRAMESTRUCT % (
                channel[vVissmann.NAME_TELEGRAF][0], 
                channel[vVissmann.NAME_TELEGRAF][1], 
                channel[vVissmann.NAME_TELEGRAF][2],
                float(self.Control.execReadCmd(channel[vVissmann.NAME_CHANNEL]).value),
                int(round(datetime.timestamp(datetime.utcnow()),0)*vVissmann.SECTONANO)
            ))
        return(data)

    def refreshConnection(self):
        print("refresh viControl")
        try:
            if self.refreshConnection >= 5:
                del self.Control
                self.Control = None
                sleep(60)
                self.Control = self.viControl()
            self.Control.initComm()
        except:
            print("reconnect")
            
    
    def exit(self):
        None

    def _getTransferChannels(self):
        ''' 
        [{
            "channel": <channel>, 
            "telegraf": [<twin>, <insatance>, <measurement>]
        }] 
        '''
        return(self.db.getTinyTables(vVissmann.NAME_TRANSFERCHANNELS))
    
    def _putTransferChannels(self, value):
        ''' 
        [{
            "channel": <channel>, 
            "telegraf": [<twin>, <insatance>, <measurement>]
        }] 
        '''
        for val in value:
            self.db.putTinyTables(vVissmann.NAME_TRANSFERCHANNELS, val)
        self.transfere = value
