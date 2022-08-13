import mysql.connector
from copy import deepcopy
from time import time
from naoconnect.TinyDb import TinyDb
from naoconnect.Param import Param

class Monisoft(Param):
    TABLE_HISTORY           = "T_History"
    NAME_MONISOFT_ID        = "monisoft_id"
    COLUMN_TIME             = "TimeStamp"
    COLUMN_VALUE            = "Value"
    COLUMN_ID               = "T_Sensors_id_Sensors"
    POSITION_TIME           = 3
    POSITION_VALUE          = 2
    LASTTIMESAVESEC         = 240
    RESET_TIME              = 1400000000
    SECTONANO               = 1000000000

    def __init__ (self, host, user, password, database, port=3306, tiny_db_name="monisoft_meta.json"):
        self.host = host
        self.port = port
        self.db = TinyDb(tiny_db_name)
        self.user = user
        self.password = password
        self.database = database
        self.lasttimestamps = self._getLastTimestamps()
        self.marker_timestamps = None
        self.transfere = self._getTransferChannels()
        self.confirm_time = time()
        self.__con = None
        self.__cur = None
        self.connectToDb()

    def connectToDb(self):
        self.__con = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port
        )
    
    def disconnetToDb(self):
        self.__con.close()

    def __buildCursor(self):
        self.__cur = self.__con.cursor()

    def getTables(self):
        self.__buildCursor()
        self.__cur.execute("SELECT TABLE_NAME FROM information_schema.tables")
        fetch = self.__cur.fetchall()
        return(fetch)
    
    def _buildTelegrafFrameForm(self, twin, instance, series):
        return(Monisoft.FORMAT_TELEGRAFFRAMESTRUCT2%(twin,instance,series)+"%f %.0f")

    def getTelegrafData(self, max_data_len=30000, maxtimerange=None): #{'monisoft_id': '1000500',  'interval': 60}
        ''' [ '<twin>,instance=<insatance>, <measurement>=<value> <timestamp>' ] '''
        ret_data = []
        ret_data_add = ret_data.append
        # sql cursor
        self.__buildCursor()
        data_len = 0
        self.marker_timestamps = deepcopy(self.lasttimestamps)
        for index in range(len(self.transfere)):
            if maxtimerange == None:
                used_maxtimerange = self.transfere[index][Monisoft.NAME_INTERVAL] * (max_data_len+10-data_len)
            else:
                used_maxtimerange = maxtimerange
            # set timesamp for new tables
            if self.marker_timestamps == {}:
                self.marker_timestamps[str(index)] = Monisoft.RESET_TIME
            if self.marker_timestamps.get(str(index)) == None:
                self.marker_timestamps[str(index)] = Monisoft.RESET_TIME
            # build sql time formate and set max timerange
            aftertimesql = self.marker_timestamps.get(str(index))
            aftertimesql2 = self.marker_timestamps.get(str(index))+used_maxtimerange
            # build sql formated format
            telegraf_form = self._buildTelegrafFrameForm(
                    twin=self.transfere[index][Monisoft.NAME_TELEGRAF][0],
                    instance=self.transfere[index][Monisoft.NAME_TELEGRAF][1],
                    series=self.transfere[index][Monisoft.NAME_TELEGRAF][2]
            )
            breaker = False
            while 1==1:  
                # get data from db
                timestamp_list = []
                timestamp_list_add = timestamp_list.append
                query = self._buildQuery(aftertimesql, aftertimesql2, self.transfere[index][Monisoft.NAME_MONISOFT_ID])
                self.__cur.execute(query)
                result_sql = self.__cur.fetchall()
                for row in result_sql:
                    timestamp_list_add(row[Monisoft.POSITION_TIME])
                    # form data for telegraf
                    if row[Monisoft.POSITION_VALUE] != None:
                        ret_data_add(telegraf_form%(float(row[Monisoft.POSITION_VALUE]), row[Monisoft.POSITION_TIME]*Monisoft.SECTONANO))
                        data_len += 1
                try:
                    self.marker_timestamps[str(index)] = max(timestamp_list)
                except:
                    None
                if timestamp_list != []:
                    break
                elif breaker or int(self.marker_timestamps.get(str(index))) > time()-used_maxtimerange:
                    break
                else:
                    if aftertimesql == Monisoft.RESET_TIME:
                    # serach for first time stamp in database
                        self.__cur.execute("SELECT MIN("+Monisoft.COLUMN_TIME+") FROM "+Monisoft.TABLE_HISTORY+" WHERE "+Monisoft.COLUMN_ID+" = "+str(self.transfere[index][Monisoft.NAME_MONISOFT_ID]))
                        try:
                            aftertimesql = self.__cur.fetchall()[0][0]
                            aftertimesql2 = aftertimesql+used_maxtimerange
                        except:
                            #print("no data vor monisoft_id: ", self.transfere[index][Monisoft.NAME_MONISOFT_ID])
                            break
                        breaker = True
                    else:
                        break
            if data_len >= max_data_len:
                break
        return(ret_data)

    def _buildQuery(self, time1, time2, monisoft_id):
        return(" SELECT * FROM "+Monisoft.TABLE_HISTORY+" WHERE "+Monisoft.COLUMN_TIME+" > "+str(time1)+" AND  "+Monisoft.COLUMN_TIME+" < "+str(time2)+" AND "+Monisoft.COLUMN_ID+" = "+str(monisoft_id))
                                        

    def _getTransferChannels(self):
        ''' 
        [{}] 
        '''
        return(self.db.getTinyTables(Monisoft.NAME_TRANSFERCHANNELS))

    def _putTransferChannel(self, monisoft_id, asset, instance, series, interval=60):
        data = {
                Monisoft.NAME_MONISOFT_ID: monisoft_id,
                Monisoft.NAME_TELEGRAF: [
                    asset,
                    instance,
                    series
                ],
                Monisoft.NAME_INTERVAL: interval
            }
        self.db.putTinyTables(Monisoft.NAME_TRANSFERCHANNELS, data)
        self.transfere = self._getTransferChannels()


    def _getLastTimestamps(self):
        ''' {<id>: <timestamp>}'''
        try:
            last_timestamps = self.db.getTinyTables(Monisoft.NAME_LASTTIME)[0]
        except:
            return(dict())
        return(last_timestamps)

    def _putLastTimestamps(self):
        ''' {<id>: <timestamp>}'''
        self.db.updateSimpleTinyTables(Monisoft.NAME_LASTTIME, self.lasttimestamps)

    def confirmTransfer(self):
        self.lasttimestamps = self.marker_timestamps
        if time()-self.confirm_time >= Monisoft.LASTTIMESAVESEC:
            self.confirm_time = time()
            self._putLastTimestamps()

    def exit(self):
        self.disconnetToDb()
        self._putLastTimestamps()

    def refreshConnection(self):
        try:
            self.disconnetToDb()
        except:
            None
        self.__connection = self.connectToDb