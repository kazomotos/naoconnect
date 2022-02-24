'Autor: Rupert Wieser -- Naotilus -- 20220219'
from tkinter.messagebox import RETRY
from tinydb import TinyDB, where, Query

class TinyDb ():
    VARIABLE = "var"
    NAME = "name"

    def __init__(self, db_name):
        self.db = TinyDB(db_name)
        self.query = Query()

    def getTinyTables(self, table_name):
        table = self.db.table(table_name)
        return(table.all())

    def putTinyTables(self, table_name, value):
        table = self.db.table(table_name)
        table.insert(value)

    def updateSimpleTinyTables(self, table_name, value, filter_key=None, filter_value=None):
        table = self.db.table(table_name)
        if filter_value==None: 
            table.update(value)
        else: 
            table.update(value, where(filter_key)==filter_value)    

    def putSimpelValue(self, name, value):
        self.db.insert({TinyDb.VARIABLE:name, TinyDb.NAME:value})

    def getSimpleValue(self, name):
        var = self.db.search(self.query[TinyDb.NAME]==name)
        if var == []:
            return(var)
        else:
            return(var[TinyDb.VARIABLE])
            