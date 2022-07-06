import pyodbc
import naoconnect.time2 as time2
import calendar
from copy import deepcopy

# https://github.com/mkleehammer/pyodbc/wiki/Install


class MServer():

    def __init__ (self, host, port, user, password, database=None, tds_version=7.4, driver='{FreeTDS}',errorline=None):
        self.host = host
        self.port = str(port)
        self.user = user
        self.password = password
        self.database = database
        self.version = str(tds_version)   
        self.driver = driver
        self.errorline = errorline
        self.__con = None
        self.__cur = None
        self.connectToDb()

    def connectToDb(self):
        if self.database == None:
            dbase = ""
        else:
            dbase = ";DATABASE=" + self.database 
        self.__con = pyodbc.connect("DRIVER=" + self.driver +
                                    ";SERVER=" + self.host +
                                    ";PORT=" + self.port +
                                    dbase +
                                    ";UID=" + self.user +
                                    ";PWD=" + self.password +
                                    ";TDS_Version=" + self.version + ";")
    
    def disconnetToDb(self):
        self.__con.close()

    def __buildCursor(self):
        self.__cur = self.__con.cursor()

    def getTables(self):
        self.__buildCursor()
        self.__cur.execute("SELECT TABLE_NAME FROM information_schema.tables")
        fetch = self.__cur.fetchall()
        return(fetch)

    def getCheckData(self, table_str, values_list_str, check_limit=5,time_column="DP_Zeitstempel"):
        values = time_column
        for i in values_list_str:
            values += ", " + i 
        self.__buildCursor()
        ret_data = []
        add_value = ret_data.append
        try:     
            for row in self.__cur.execute(" SELECT TOP(" + str(check_limit) + ") " + values +" FROM " + table_str):
                add_value(list(row[1:]))
        except pyodbc.ProgrammingError as e:
            print(e)
            return(None)
        return(ret_data)

    def getData(self, table_str, values_list_str, aftertime_int=None, maxtimerange=3600*24,time_column="DP_Zeitstempel"):
        aftertimesql = str(tuple(time2.gmtime(float(aftertime_int))[0:6]))[0:-1] + ", 0, 0)"
        aftertimesql2 = str(tuple(time2.gmtime(float(aftertime_int)+maxtimerange)[0:6]))[0:-1] + ", 0, 0)"
        values = time_column
        for i in values_list_str:
            values += ", " + i 
        self.__buildCursor()
        ret_data = [[],[]]
        add_time = ret_data[0].append
        add_value = ret_data[1].append
        breaker = False
        while 1==1:  
            try:           
                for row in self.__cur.execute(" SELECT " + values +
                                        " FROM " + table_str +
                                        " WHERE " + time_column + 
                                        " > DATETIME2FROMPARTS" + aftertimesql + 
                                        " AND  " + time_column + 
                                        " < DATETIME2FROMPARTS" + aftertimesql2):
                    add_time(calendar.timegm(row[0].utctimetuple()))
                    add_value(list(row[1:]))
            except pyodbc.ProgrammingError as e:
                print(e)
                if "Invalid column name" in str(e):
                    columname = str(e).split("Invalid column name")[1].split("\'")[1]
                    self.errorline("MS-Server ("+self.database+")", "Spaltenname '"+columname+"' existiert nicht in Tabelle: "+table_str)
                    values = values.replace(", "+columname, "")
                    continue
                if "Invalid object name" in str(e):
                    tablename = str(e).split("Invalid object name")[1].split("\'")[1]
                    self.errorline("MS-Server ("+self.database+")", "Tabelle: '"+tablename+"' existiert nicht")
                break
            if ret_data != [[],[]]:
                break
            elif breaker or int(aftertime_int) > time2.time()-3600*24*2:
                break
            else:
                new_aftertime = []
                new_afteradd = new_aftertime.append
                for row in self.__cur.execute(" SELECT " + values +
                        " FROM " + table_str +
                        " WHERE " + time_column + 
                        " > DATETIME2FROMPARTS" + aftertimesql):
                    new_afteradd(row[0].timestamp())
                if new_aftertime == []:
                    break
                aftertimesql = str(tuple(time2.gmtime(min(new_aftertime)-10)[0:6]))[0:-1] + ", 0, 0)"
                aftertimesql2 = str(tuple(time2.gmtime((min(new_aftertime)-10)+maxtimerange)[0:6]))[0:-1] + ", 0, 0)"
                breaker = True
        return(ret_data)
        

class Aquotec():
    # -- constatn-class-variable
    # substation type
    NAME_RM360              = "RM360"
    NAME_UG07               = "UG07"
    NAME_WMZ                = "WMZ"


    def __init__ (self,condition,struct_db_values,struct_db_keys,MsClass,limit=500,filter_dic=None):
        '''
        reading_tables:     {'type_number': meter_counter,'type_number': meter_counter, ...]
        struct_db_values:   [table_typ1: {column1:[struct1, struct2], ...}, ...]
        condition:          {typ:{table:condition},...} or {typ:{table:[condition,{table:sub_condition}]},...}
        '''
        self.condition           = condition
        self.reading_tables      = {}
        self.new_reading_tables  = {}
        self.extra_check_tables  = {}
        self.Ms                  = MsClass
        self.struct_db_values    = struct_db_values
        self.struct_db_keys      = struct_db_keys
        self.limit               = limit
        self.meter_count         = None
        self.filter              = filter_dic


    def setReadingTables(self, reading_tables):
        self.reading_tables = reading_tables
        if self.reading_tables == {}:
            self.meter_count = 0
        else: 
            self.meter_count = max(self.reading_tables.values())


    def setNewReadingTables(self, new_reading_tables):
        self.new_reading_tables = new_reading_tables


    def setExtraCeckinTables(self, extra_check_tables):
        self.extra_check_tables = extra_check_tables


    def getNewReadingTables(self):
        return(self.new_reading_tables)


    def getExtraCeckinTables(self):
        return(self.extra_check_tables)


    def buildNewSensorFrames(self):
        '''
        return: {table_type_number1: [[column1, column2, ...], [build_dict1, build_dict2, ...]], ...}
        '''
        sensors = {}
        new_tables = self._checkNewTables()
        for types in new_tables:
            for table_number in new_tables[types]:
                table_type_number = types + "_" + table_number
                if table_type_number not in sensors:
                    sensors[table_type_number] = [[],[]]
                if table_number not in self.reading_tables:
                    self.meter_count += 1
                    count = self.meter_count
                    self.reading_tables[table_number] = count
                    self.new_reading_tables[table_number] = count
                else:
                    count = self.reading_tables[table_number]
                for column in new_tables[types][table_number]:
                    values = deepcopy(self.struct_db_values[types][column])
                    values[2] = values[2][0] + str(count) + values[2][1]
                    values[6] = values[6][0] + types + "_" + table_number + values[6][1]
                    sensors[table_type_number][0].append(column)
                    sensors[table_type_number][1].append(dict(zip(self.struct_db_keys,values)))
        return(sensors)
        

    def _checkNewTables(self):
        '''
        Ms.getCheckDat(): [[val1, val2, val3, ...], [val1, ...], ...]
        return: {typ1: {table_number2: [column1, column2, ...], table_number2: {... [...]}}, typ2: ...}
        '''
        new_tables = self._findNewTables()
        online_numbers = []
        online_columns = { # {typ1: {table_type_name1:[column1, column2, ...], ...}, ...}
            Aquotec.NAME_RM360: {},
            Aquotec.NAME_UG07:  {},
            Aquotec.NAME_WMZ:   {}
        } 
        # RM_360
        column_list = list(self.struct_db_values[Aquotec.NAME_RM360].keys())
        for number in new_tables[Aquotec.NAME_RM360]:
            data = self.Ms.getCheckData(
                table_str=Aquotec.NAME_RM360+"_"+number,
                values_list_str=column_list,
                check_limit=self.limit
            )
            if data == []:
                continue
            online = self.__ceckOnlineValues(Aquotec.NAME_RM360, data, column_list)
            if online != []:
                online_numbers.append(number)
                online_columns[Aquotec.NAME_RM360][number] =  online
        # UG07
        column_list = list(self.struct_db_values[Aquotec.NAME_UG07].keys())
        for number in new_tables[Aquotec.NAME_UG07]:
            if number not in online_numbers:
                data = self.Ms.getCheckData(
                    table_str=Aquotec.NAME_UG07+"_"+number,
                    values_list_str=column_list,
                    check_limit=self.limit
                )
                if data == []:
                    continue
                online = self.__ceckOnlineValues(Aquotec.NAME_UG07, data, column_list)
                if online != []:
                    online_numbers.append(number)
                    online_columns[Aquotec.NAME_UG07][number] =  online
        # WMZ
        column_list = list(self.struct_db_values[Aquotec.NAME_WMZ].keys())
        for number in new_tables[Aquotec.NAME_WMZ]:
            data = self.Ms.getCheckData(
                table_str=Aquotec.NAME_WMZ+"_"+number,
                values_list_str=column_list,
                check_limit=self.limit
            )
            if data == []:
                continue
            online = self.__ceckOnlineValues(Aquotec.NAME_WMZ, data, column_list)
            if online != []:
                if number not in online_numbers:
                    online_numbers.append(number)
                    online_columns[Aquotec.NAME_WMZ][number] =  online
                    self.extra_check_tables[number] = online
                else:
                    online_columns[Aquotec.NAME_WMZ][number] =  ["DP_21Wert"]

        return(online_columns)
        
        
    def _findNewTables(self):
        '''
        ...
        return: {typ1:[table_number, table_number, ...], typ2:[...], ...}
        Ms.getTables(): [('table_type_number',),('table_type_number',),...]
        '''
        table_list = self.Ms.getTables() 
        new_tables = {
            Aquotec.NAME_RM360: [],
            Aquotec.NAME_UG07:  [],
            Aquotec.NAME_WMZ:   []
        }
        for table in table_list:
            name_split = table[0].split("_")
            if len(name_split) == 2:
                if name_split[1] not in self.reading_tables:
                    if self.filter != None:
                        try:
                            number = int(name_split[1].split("R")[1])
                        except:
                            continue
                        if number > self.filter["max"] or number < self.filter["min"]:
                            continue
                    if name_split[0] == Aquotec.NAME_RM360:
                        new_tables[Aquotec.NAME_RM360].append(name_split[1])
                    elif name_split[0] == Aquotec.NAME_UG07:
                        new_tables[Aquotec.NAME_UG07].append(name_split[1])
                    elif name_split[0] == Aquotec.NAME_WMZ:
                        new_tables[Aquotec.NAME_WMZ].append(name_split[1])
        return(new_tables)


    def __ceckOnlineValues(self,table_type, data, column_list):
        '''
        ...
        '''
        online_column = []
        for column in self.condition[table_type]:
            if type(self.condition[table_type][column]) != list:
                index_column = column_list.index(column)
                condition = self.condition[table_type][column]
                if self.__checkConditions(data,index_column,condition):
                    online_column.append(column)
            else:
                index_column = column_list.index(column)
                condition = self.condition[table_type][column][0]
                if self.__checkConditions(data,index_column,condition):
                    for sub_column in self.condition[table_type][column][1]:
                        index_sub_column = column_list.index(sub_column)
                        sub_condition = self.condition[table_type][column][1][sub_column]
                        if self.__checkConditions(data,index_sub_column,sub_condition):
                            online_column.append(sub_column)
        return(online_column)


    def __checkConditions(self,data,index_column,sub_condition):
        online = False
        if sub_condition == 0:
            for index_value in range(len(data)):
                if data[index_value][index_column] != None:
                    online = True
                    break
        elif sub_condition == 1:
            for index_value in range(len(data)):
                if data[index_value][index_column] != None and data[index_value][index_column] != 0:
                    online = True
                    break
        elif sub_condition == 2:
            for index_value in range(len(data)):
                if data[index_value][index_column] != None and data[index_value][index_column] > 0 and data[index_value][index_column] < 250:
                    online = True
                    break
        elif sub_condition == 3:
            for index_value in range(len(data)):
                if data[index_value][index_column] != None and data[index_value][index_column] > 0 and data[index_value][index_column] < 98:
                    online = True
                    break
        elif sub_condition == 4:
            for index_value in range(len(data)):
                if data[index_value][index_column] != None and data[index_value][index_column] > 0 and data[index_value][index_column] < 120:
                    online = True
                    break
        elif sub_condition == 5:
            for index_value in range(len(data)):
                if data[index_value][index_column] != None and data[index_value][index_column] > 0 and data[index_value][index_column] != 12.6:
                    online = True
                    break
        return(online)

