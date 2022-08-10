import pandas as pd
from json import loads, dumps
from naoconnect.Param import Labling as lab
from naoconnect.Param import Param as par
from naoconnect.NaoApp import NaoApp

class FromPandasFrame():
    PATH_FILE_TABSTOPP_TEXT = "path_file_tabstop_text"

    def __init__(self, path_labeled_output:str, Nao:NaoApp, pandas_frame:pd.DataFrame=None, **args):
        self.Nao = Nao
        self.path_labeled = path_labeled_output
        self.workspace:dict = None
        self.asset:dict = None
        self.component:dict = None
        self.unit:dict = None
        self.series:dict = None
        self.instance:dict = None
        self.data:pd.DataFrame = pandas_frame
        self.labling:dict = self.readLabling()
        self.idx_row:int = None
        if self.data == None and args.get(FromPandasFrame.PATH_FILE_TABSTOPP_TEXT):
            self.data = self.readTabstoppText(args[FromPandasFrame.PATH_FILE_TABSTOPP_TEXT])
        
    def readTabstoppText(self, path_file):
        return(pd.read_csv(path_file, sep="\t", encoding="utf-16"))
    
    def readLabling(self):
        file_open = open(self.path_labeled, "r")
        labling = loads(file_open.read())
        file_open.close()
        return(labling)
    
    def writeLabling(self):
        file_open = open(self.path_labeled, "w")
        file_open.write(dumps(self.labling))
        file_open.close()
    
    def lablingAll(self, list_endpoint_conf:list=None):
        self._checkLabling()
        for self.idx_row in range(len(self.data)):
            if not self._checkLabling:
                print("missing data for row,", self.idx_row)
                continue
            self._setWorkspace()
            self._setAsset()
            self._setComponent()
            self._setSeries()
            self._setInstance()
            if list_endpoint_conf != None:
                self._setEnpointConifg(list_endpoint_conf)

    def _ceckRow(self):
        if pd.isna(self.data[lab.WORKSPACE][self.idx_row]):
            return(False)
        elif pd.isna(self.data[lab.ASSET][self.idx_row]):
            return(False)
        elif pd.isna(self.data[lab.COMPONENT_0][self.idx_row]):
            return(False)
        elif pd.isna(self.data[lab.SENSOR_TYPE][self.idx_row]):
            return(False)
        elif pd.isna(self.data[lab.NAME][self.idx_row]):
            return(False)
        else:
            return(True)
    
    def _checkLabling(self):
        if self.labling == "":
            self.labling = {
                par.NAME_WORKSPACE : [],
                par.NAME_UNIT: []
            }
    
    def _setWorkspace(self):
        self.workspace = list(filter(lambda config: config[par.NAME_NAME] == self.data[lab.WORKSPACE][self.idx_row], self.labling[par.NAME_WORKSPACE]))
        if self.workspace == []:
            data_workspace = self.Nao.createWorkspace(self.data[lab.WORKSPACE][self.idx_row])
            self.labling[par.NAME_WORKSPACE].append({
                par.NAME_NAME: data_workspace[par.NAME_NAME],
                par.NAME_ID: data_workspace[par.NAME_ID_ID],
                par.NAME_ASSET: []
            })
            self.workspace = list(filter(lambda config: config[par.NAME_NAME] == self.data[lab.WORKSPACE][self.idx_row], self.labling[par.NAME_WORKSPACE]))[0]
        else:
            self.workspace = self.workspace[0]
    
    def _setAsset(self):
        self.asset = list(filter(lambda config: config[par.NAME_NAME] == self.data[lab.ASSET][self.idx_row], self.workspace[par.NAME_ASSET]))
        if self.asset == []:
            data_asset = self.Nao.createAsset(
                self.data[lab.ASSET][self.idx_row], 
                self.workspace[lab.ID], 
                self.data[lab.ASSET][self.idx_row]
            )
            self.workspace[par.NAME_ASSET].append({
                par.NAME_NAME: data_asset[par.NAME_NAME],
                par.NAME_ID: data_asset[par.NAME_ID_ID],
                par.NAME_COMPONENT: [],
                par.NAME_INSTANCE: []
            })
            self.asset = list(filter(lambda config: config[par.NAME_NAME] == self.data[lab.ASSET][self.idx_row], self.workspace[par.NAME_ASSET]))[0]
        else:
            self.asset = self.asset[0]

    def _setComponent(self):
        self.component = list(filter(lambda config: config[par.NAME_NAME] == self.data[lab.COMPONENT_0][self.idx_row], self.asset[par.NAME_COMPONENT]))
        if self.component == []:
            data_path = self.Nao.createPath(
                self.data[lab.COMPONENT_0][self.idx_row], 
                self.asset[lab.ID], 
                self.data[lab.COMPONENT_0][self.idx_row],
            )
            self.asset[par.NAME_COMPONENT].append({
                par.NAME_NAME: data_path[lab.NAME],
                par.NAME_ID: data_path[par.NAME_ID_ID],
                par.NAME_SERIES: []
            })
            self.component = list(filter(lambda config: config[par.NAME_NAME] == self.data[lab.COMPONENT_0][self.idx_row], self.asset[par.NAME_COMPONENT]))[0]
        else:
            self.component = self.component[0]
    
    def _setUnit(self):
        if pd.isna(self.data[lab.UNIT][self.idx_row]):
            self.data[lab.UNIT][self.idx_row] = "-"
        self.unit = list(filter(lambda config: config[par.NAME_NAME] == self.data[lab.UNIT][self.idx_row], self.labling[par.NAME_UNIT]))
        if self.unit == []:
            data_unit = self.Nao.createUnit(
                self.data[lab.UNIT][self.idx_row]
            )
            self.labling[par.NAME_UNIT].append({
                par.NAME_NAME: data_unit[lab.NAME],
                par.NAME_ID: data_unit[par.NAME_ID_ID]
            })
            self.unit = list(filter(lambda config: config[par.NAME_NAME] == self.data[lab.UNIT][self.idx_row], self.labling[par.NAME_UNIT]))[0]
        else:
            self.unit = self.unit[0]
        
    def _setSeries(self):
        self.series = list(filter(lambda config: config[par.NAME_NAME] == self.data[lab.NAME][self.idx_row], self.component[par.NAME_SERIES]))
        if self.series == []:
            data_path = self.Nao.createSeries(
                type=self.data[lab.SENSOR_TYPE][self.idx_row],
                name=self.data[lab.NAME][self.idx_row],
                description=self.data[lab.DESCRIPTION][self.idx_row],
                _asset=self.asset[par.NAME_ID],
                _part=self.component[par.NAME_ID],
                _unit=self.unit[par.NAME_ID],
                max=self.data[lab.MAX_VALUE][self.idx_row],
                min=self.data[lab.MIN_VALUE][self.idx_row],
                fill=self.data[lab.FILL_METHOD][self.idx_row],
                fillValue=self.data[lab.FILL_VALUE][self.idx_row]
            )
            self.component[par.NAME_SERIES].append({
                par.NAME_NAME: data_path[lab.NAME],
                par.NAME_ID: data_path[par.NAME_ID_ID],
            })
            self.series = list(filter(lambda config: config[par.NAME_NAME] == self.data[lab.NAME][self.idx_row], self.component[par.NAME_SERIES]))[0]
        else:
            self.series = self.series[0]

    def _setInstance(self):
        self.instance = list(filter(lambda config: config[par.NAME_NAME] == self.data[lab.INSTANCE][self.idx_row], self.asset[par.NAME_INSTANCE]))
        if self.instance == []:
            data_path = self.Nao.createInstance(
                name=self.data[lab.INSTANCE][self.idx_row],
                description=self.data[lab.INSTANCE][self.idx_row],
                _asset=self.asset[par.NAME_ID]
            )
            self.asset[par.NAME_INSTANCE].append({
                par.NAME_NAME: data_path[lab.NAME],
                par.NAME_ID: data_path[par.NAME_ID_ID],
            })
            self.instance = list(filter(lambda config: config[par.NAME_NAME] == self.data[lab.INSTANCE][self.idx_row], self.asset[par.NAME_INSTANCE]))[0]
        else:
            self.instance = self.instance[0]

    def _setEnpointConifg(self, list_endpoint_conf:list):
        config = {}
        for conf in list_endpoint_conf:
            config[conf] = str(self.data[conf][self.idx_row])
        self.Nao.patchEnpointConifg(
            conf={par.NAME_CONFIG: dumps(config)},
            _asset=self.asset[par.NAME_ID],
            _instance=self.instance[par.NAME_ID],
            _series=self.series[par.NAME_ID],
            series_type=self.data[lab.SENSOR_TYPE][self.idx_row].capitalize()
        )