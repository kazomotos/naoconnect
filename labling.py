from naoconnect.NaoApp import NaoApp
from json import loads, dumps
import pandas as pd
from naoconnect.Param import Labling as lab, Param as par

Nao = NaoApp("nao-app.de", "rupertwieser@outlook.com", "V*z2eKmYWqdU", False, False, False,"th-nurnberg.json")


data = pd.read_csv("labling.txt", sep="\t", encoding="utf-16")
file_open = open("labling.json", "r")
labling = loads(file_open.read())
file_open.close()

data["workspace"] += " (rupert)"
for idx_work in range(len(data)):
    # check data
    if pd.isna(data[lab.WORKSPACE][idx_work]):
        continue
    elif pd.isna(data[lab.ASSET][idx_work]):
        continue
    elif pd.isna(data[lab.COMPONENT_0][idx_work]):
        continue
    elif pd.isna(data[lab.SENSOR_TYPE][idx_work]):
        continue
    elif pd.isna(data[lab.NAME][idx_work]):
        continue
    # empty labling
    if labling == "":
        labling = {
            par.NAME_WORKSPACE : [],
            par.NAME_UNIT: []
        }
    # workspace
    workspace = list(filter(lambda config: config[par.NAME_NAME] == data[lab.WORKSPACE][idx_work], labling[par.NAME_WORKSPACE]))
    if workspace == []:
        data_workspace = Nao.createWorkspace(data[lab.WORKSPACE][idx_work])
        labling[par.NAME_WORKSPACE].append({
            par.NAME_NAME: data_workspace[par.NAME_NAME],
            par.NAME_ID: data_workspace[par.NAME_ID_ID],
            par.NAME_ASSET: []
        })
        workspace = list(filter(lambda config: config[par.NAME_NAME] == data[lab.WORKSPACE][idx_work], labling[par.NAME_WORKSPACE]))[0]
    else:
        workspace = workspace[0]
    # asset
    asset = list(filter(lambda config: config[par.NAME_NAME] == data[lab.ASSET][idx_work], workspace[par.NAME_ASSET]))
    if asset == []:
        data_asset = Nao.createAsset(
            data[lab.ASSET][idx_work], 
            workspace[lab.ID], 
            data[lab.ASSET][idx_work]
        )
        workspace[par.NAME_ASSET].append({
            par.NAME_NAME: data_asset[par.NAME_NAME],
            par.NAME_ID: data_asset[par.NAME_ID_ID],
            par.NAME_COMPONENT: [],
            par.NAME_INSTANCE: []
        })
        asset = list(filter(lambda config: config[par.NAME_NAME] == data[lab.ASSET][idx_work], workspace[par.NAME_ASSET]))[0]
    else:
        asset = asset[0]
    # componet
    component = list(filter(lambda config: config[par.NAME_NAME] == data[lab.COMPONENT_0][idx_work], asset[par.NAME_COMPONENT]))
    if component == []:
        data_path = Nao.createPath(
            data[lab.COMPONENT_0][idx_work], 
            asset[lab.ID], 
            data[lab.COMPONENT_0][idx_work],
        )
        asset[par.NAME_COMPONENT].append({
            par.NAME_NAME: data_path[lab.NAME],
            par.NAME_ID: data_path[par.NAME_ID_ID],
            par.NAME_SERIES: []
        })
        component = list(filter(lambda config: config[par.NAME_NAME] == data[lab.COMPONENT_0][idx_work], asset[par.NAME_COMPONENT]))[0]
    else:
        component = component[0]
    # unit
    if pd.isna(data[lab.UNIT][idx_work]):
        data[lab.UNIT][idx_work] = "-"
    unit = list(filter(lambda config: config[par.NAME_NAME] == data[lab.UNIT][idx_work], labling[par.NAME_UNIT]))
    if unit == []:
        data_unit = Nao.createUnit(
            data[lab.UNIT][idx_work]
        )
        labling[par.NAME_UNIT].append({
            par.NAME_NAME: data_unit[lab.NAME],
            par.NAME_ID: data_unit[par.NAME_ID_ID]
        })
        unit = list(filter(lambda config: config[par.NAME_NAME] == data[lab.UNIT][idx_work], labling[par.NAME_UNIT]))[0]
    else:
        unit = unit[0]
    # series
    series = list(filter(lambda config: config[par.NAME_NAME] == data[lab.NAME][idx_work], component[par.NAME_SERIES]))
    if series == []:
        data_path = Nao.createSeries(
            type=data[lab.SENSOR_TYPE][idx_work],
            name=data[lab.NAME][idx_work],
            description=data[lab.DESCRIPTION][idx_work],
            _asset=asset[par.NAME_ID],
            _part=component[par.NAME_ID],
            _unit=unit[par.NAME_ID],
            max=data[lab.MAX_VALUE][idx_work],
            min=data[lab.MIN_VALUE][idx_work],
            fill=data[lab.FILL_METHOD][idx_work],
            fillValue=data[lab.FILL_VALUE][idx_work]
        )
        component[par.NAME_SERIES].append({
            par.NAME_NAME: data_path[lab.NAME],
            par.NAME_ID: data_path[par.NAME_ID_ID],
        })
        series = list(filter(lambda config: config[par.NAME_NAME] == data[lab.INSTANCE][idx_work], component[par.NAME_SERIES]))[0]
    else:
        series = series[0]
    print(series[lab.NAME])
    # instance
    instance = list(filter(lambda config: config[par.NAME_NAME] == data[lab.NAME][idx_work], component[par.NAME_SERIES]))
    if instance == []:
        data_path = Nao.createSeries(
            type=data[lab.SENSOR_TYPE][idx_work],
            name=data[lab.NAME][idx_work],
            description=data[lab.DESCRIPTION][idx_work],
            _asset=asset[par.NAME_ID],
            _part=component[par.NAME_ID],
            _unit=unit[par.NAME_ID],
            max=data[lab.MAX_VALUE][idx_work],
            min=data[lab.MIN_VALUE][idx_work],
            fill=data[lab.FILL_METHOD][idx_work],
            fillValue=data[lab.FILL_VALUE][idx_work]
        )
        component[par.NAME_SERIES].append({
            par.NAME_NAME: data_path[lab.NAME],
            par.NAME_ID: data_path[par.NAME_ID_ID],
        })
        instance = list(filter(lambda config: config[par.NAME_NAME] == data[lab.NAME][idx_work], component[par.NAME_SERIES]))[0]
    else:
        instance = instance[0]
    print(instance[lab.NAME])



file_open = open("labling.json", "w")
file_open.write(dumps(labling))
file_open.close()


if data["Einheit"][3] == '�C':
    print("true")



from datetime import datetime
from random import random
from json import dumps
from copy import copy
print(str(datetime.fromtimestamp(1626615264)))
start_time = 1626615264
z = []
x = []
y = []
for stat in range(150):
    y.append("station____xxxxx____number_"+str(stat))
    if random() < 0.8:
        z.append([1]*8760)
    elif random() < 0.95:
        dat = []
        for i in range(8760):
            if random() > 0.8:
                dat.append(None)
            else:
                dat.append(1)
        z.append(copy(dat))
    else:
        z.append([None]*8760)

for i in range(8760):
    x.append(str(datetime.fromtimestamp(start_time+i*3600)))

u = open("sersor_logging_test.json", "w")
u.writelines(dumps({"z":z,"x":x,"y":y}))
u.close()

