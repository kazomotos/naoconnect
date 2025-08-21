import os
import requests
from urllib.parse import quote
from json import loads, dumps
from copy import copy
from time import sleep
from datetime import datetime, timezone
from math import ceil
'''
Ähnlich wie V2, wird nur benötigt falls von einem Kritischem Netzwertk heraus Server überwacht werden sollen.
'''
class NaoApp():
    NAME_HOST = "host"
    NAME_PAYLOAD = "payload"
    NAME_PASSWD = "password"
    NAME_EMAIL = "email"
    NAME_WEBAUTH = "Authorization"
    NAME_POINT_ID = "_point"
    NAME_POINT_MODEL = "pointModel"
    NAME_POINT_CONFIG = "config"
    NAME_NAME = "name"
    NAME_CONTENT_HEADER = "Content-Type"
    NAME__ID = "_id"
    NAME_VALUE = "value"
    NAME_DESCRIPTION = "description"
    NAME_GEOLOCATION = "geolocation"
    NAME_ATTRIBUTEVALUES = "attributevalues"
    NAME_WORKSPACE_ID = "_workspace"
    NAME_ASSET_ID = "_asset"
    NAME_GET = "GET"
    NAME_POST = "POST"
    NAME_UTF8 = "utf-8"
    NAME_PATCH = "PATCH"
    NAME_DELETE = "DELETE"
    NAME_AVATAR_ID = "_avatar"
    URL_GET_USER_INFO = "/api/user/me"
    URL_PUT_NOTE = "/api/nao/assetnote"
    URL_PATCH_INSTANCE = "/api/nao/instance/%s"
    URL_TELEGRAF = "/api/telegraf"
    URL_INSTANCE = "/api/nao/instance"
    URL_INSTANCE_ATTREBIUTE = "/api/nao/instance/%s?select=attributevalues,_id"
    URL_PLOT_TIMESERIES = "/api/series/data/plot"
    URL_RAW_TIMESERIES = "/api/series/data/raw"
    URL_INSTANCE_MORE = "/api/nao/instance/more/%s"
    URL_WORKSPACE = "/api/nao/workspace"
    URL_ACTIVATE_DATAPOINT = "/api/nao/instance/%s/datapoints"
    URL_PATCH_META_INSTANCE = "/api/nao/instance/%s/attributevalues/%s"
    URL_ASSET = "/api/nao/asset"
    QUERY_HEADER_JSON = 'application/json'
    QUERY_BEARER = "Bearer "
    NAME_TOKENAC = "accessToken"
    QUERY_LOGINHEADER = {'Content-Type': 'application/x-www-form-urlencoded'}
    URL_LOGIN = "/api/user/auth/login"
    QUERY_TRANSFERHEADER = {"Authorization": "", 'Content-Type': 'text/plain', 'Cookie': ""}
    FORMAT_TELEFRAF_FRAME_SEPERATOR = "\n"
    STATUS_CODE_GOOD = 204
    QUERY_GET = "?query="
    TELEGRAF_FORMATER = "%s,instance=%s %s=%s %s"

    def __init__(self, host, email, password, local=False, data_per_telegraf_push=10000, Messager=False):
        self.auth = {
            NaoApp.NAME_HOST: host,
            NaoApp.NAME_EMAIL: email,
            NaoApp.NAME_PASSWD: password,
        }
        self.base_url = f"https://{host}"
        self.headers = NaoApp.QUERY_TRANSFERHEADER.copy()
        self.data_per_telegraf_push = data_per_telegraf_push
        self.local = local
        self.Messager = Messager
        self.session = requests.Session()

    def _loginNao(self):
        if self.local:
            raise NotImplementedError("Local login not implemented for requests-based version.")
        else:
            self._loginNaoCloud()

    def _loginNaoCloud(self):
        data = {
            "email": self.auth[NaoApp.NAME_EMAIL],
            "password": self.auth[NaoApp.NAME_PASSWD]
        }
        response = self.session.post(self.base_url + NaoApp.URL_LOGIN, headers=NaoApp.QUERY_LOGINHEADER, data=data)
        response.raise_for_status()
        token = response.json()[NaoApp.NAME_TOKENAC]
        self.headers[NaoApp.NAME_WEBAUTH] = NaoApp.QUERY_BEARER + token

    def _sendDataToNaoJson(self, method, url, payload=None):
        full_url = self.base_url + url
        headers = copy(self.headers)
        headers[NaoApp.NAME_CONTENT_HEADER] = NaoApp.QUERY_HEADER_JSON
        
        if payload is not None:
            payload = dumps(payload)
        
        method = method.upper()
        try:
            response = self.session.request(method, full_url, headers=headers, data=payload)
            response.raise_for_status()
            if response.text:
                return response.json()
            return ''
        except:
            print("LOGIN")
            self._loginNao()
            headers = copy(self.headers)
            headers[NaoApp.NAME_CONTENT_HEADER] = NaoApp.QUERY_HEADER_JSON
            response = self.session.request(method, full_url, headers=headers, data=payload)
            response.raise_for_status()
            if response.text:
                return response.json()
            return ''

    def sendSingleData(self, asset: str, instance: str, sensor: str, value: float, timestamp_use: datetime = None) -> int:
        if not timestamp_use:
            timestamp_use = datetime.now(timezone.utc)
        time_int = str(int(timestamp_use.timestamp() * 1_000_000_000))  # Nanosekunden
        return self._sendTelegrafData(NaoApp.TELEGRAF_FORMATER % (asset, instance, sensor, value, time_int))

    def getUserId(self):
        return self._sendDataToNaoJson(NaoApp.NAME_GET, NaoApp.URL_GET_USER_INFO)[NaoApp.NAME__ID]

    def pushNote(self, asset_id:str, data_note:dict):
        data_note["_asset"] = asset_id
        ret = self._sendDataToNaoJson(NaoApp.NAME_POST,url=NaoApp.URL_PUT_NOTE,payload=data_note)
        return(ret[NaoApp.NAME__ID])

    def createWorkspace(self, name, avatar=None):
        payload = {NaoApp.NAME_NAME: name, NaoApp.NAME_AVATAR_ID: avatar}
        return self._sendDataToNaoJson(NaoApp.NAME_POST, NaoApp.URL_WORKSPACE, payload)

    def createInstance(self, name, description, asset_id, workspace_id, geolocation=[], attributevalues=[]):
        payload = {
            NaoApp.NAME_NAME: name,
            NaoApp.NAME_DESCRIPTION: description,
            NaoApp.NAME_GEOLOCATION: geolocation,
            NaoApp.NAME_ASSET_ID: asset_id,
            NaoApp.NAME_WORKSPACE_ID: workspace_id,
            NaoApp.NAME_ATTRIBUTEVALUES: attributevalues,
        }
        return self._sendDataToNaoJson(NaoApp.NAME_POST, NaoApp.URL_INSTANCE, payload)

    def getInstanceInfos(self, instance_id):
        return self._sendDataToNaoJson(NaoApp.NAME_GET, NaoApp.URL_INSTANCE_MORE % instance_id)

    def getInstanceInfoAttrebuteValues(self, instance_id):
        return self._sendDataToNaoJson(NaoApp.NAME_GET, NaoApp.URL_INSTANCE_ATTREBIUTE % instance_id)

    def deleteInstance(self, instance_id):
        return self._sendDataToNaoJson(NaoApp.NAME_DELETE, f"{NaoApp.URL_INSTANCE}/{instance_id}")

    def activateDatapoint(self, type_sensor, sensor_id, instance_id, config={}):
        payload = {
            NaoApp.NAME_POINT_ID: sensor_id,
            NaoApp.NAME_POINT_MODEL: type_sensor,
            NaoApp.NAME_POINT_CONFIG: dumps(config)
        }
        return self._sendDataToNaoJson(NaoApp.NAME_POST, NaoApp.URL_ACTIVATE_DATAPOINT % instance_id, payload)

    def patchInstanceMeta(self, instance_id, meta_id, value):
        payload = {NaoApp.NAME__ID: instance_id, NaoApp.NAME_VALUE: value}
        return self._sendDataToNaoJson(NaoApp.NAME_PATCH, NaoApp.URL_PATCH_META_INSTANCE % (instance_id, meta_id), payload)

    def patchInstanceData(self, instance_id, payload):
        return self._sendDataToNaoJson(NaoApp.NAME_PATCH, NaoApp.URL_PATCH_INSTANCE % instance_id, payload)

    def getWorkspace(self, **args):
        query = NaoApp.QUERY_GET + ",".join(f"{k}={v}" for k, v in args.items()) if args else ""
        return self._sendDataToNaoJson(NaoApp.NAME_GET, NaoApp.URL_WORKSPACE + query)

    def getAssets(self, **args):
        query = NaoApp.QUERY_GET + ",".join(f"{k}={v}" for k, v in args.items()) if args else ""
        return self._sendDataToNaoJson(NaoApp.NAME_GET, NaoApp.URL_ASSET + query)

    def getInstances(self, **args):
        query = NaoApp.QUERY_GET + ",".join(f"{k}={v}" for k, v in args.items()) if args else ""
        return self._sendDataToNaoJson(NaoApp.NAME_GET, NaoApp.URL_INSTANCE + query)

    def getPlotformatetTimeseries(self, select):
        return self._sendDataToNaoJson(NaoApp.NAME_POST, NaoApp.URL_PLOT_TIMESERIES, payload=select)

    def getRawformatetTimeseries(self, select):
        return self._sendDataToNaoJson(NaoApp.NAME_POST, NaoApp.URL_RAW_TIMESERIES, payload=select)

    def _sendTelegrafData(self, payload):
        if isinstance(payload, list):
            payload = NaoApp.FORMAT_TELEFRAF_FRAME_SEPERATOR.join(payload)

        headers = copy(self.headers)
        try:
            response = self.session.post(
                self.base_url + NaoApp.URL_TELEGRAF,
                headers=headers,
                data=payload
            )
            if response.status_code != NaoApp.STATUS_CODE_GOOD:
                self._loginNao()
                headers = copy(self.headers)
                response = self.session.post(
                    self.base_url + NaoApp.URL_TELEGRAF,
                    headers=headers,
                    data=payload
                )
            return response.status_code
        except requests.RequestException:
            self._loginNao()
            headers = copy(self.headers)
            response = self.session.post(
                self.base_url + NaoApp.URL_TELEGRAF,
                headers=headers,
                data=payload
            )
            return response.status_code

    def sendTelegrafData(self, payload: list, max_sleep: float = 2):
        '''
        payload: Liste oder String im Telegraf Line Protocol-Format
        Beispiel:
            '<twin>,instance=<instance> <measurement>=<value> <timestamp>'
        '''
        if not isinstance(payload, list):
            sta = self._sendTelegrafData(payload=payload)
            if self.Messager:
                count = len(payload.split("\n"))
                self.Messager.sendCount(count)
            return sta
        else:
            count = len(payload)
            if count > self.data_per_telegraf_push:
                last_idx = 0
                for idx in range(int(ceil(count / self.data_per_telegraf_push)) - 1):
                    last_idx = idx
                    chunk = payload[int(idx * self.data_per_telegraf_push):int((idx + 1) * self.data_per_telegraf_push)]
                    sta = self._sendTelegrafData(payload=chunk)
                    if sta != NaoApp.STATUS_CODE_GOOD:
                        return sta
                    # adaptive sleep, damit der Proxy nicht überlastet wird
                    sleep(min(max_sleep, 0.1 + idx * 0.04))
                # letzten Block senden
                sta = self._sendTelegrafData(payload[int(last_idx * self.data_per_telegraf_push):])
            else:
                sta = self._sendTelegrafData(payload)
            if self.Messager:
                self.Messager.sendCount(count)
            return sta


class NaoLoggerMessage(NaoApp):

    def __init__(self, host, email, password, asset:str, instance:str, count_series:str, restart_series:str, local=False):
        super().__init__(host, email, password, local)
        self.asset = asset
        self.instance = instance
        self.count_series = count_series
        self.restart_series = restart_series

    def sendCount(self, count) -> None:
        try:self.sendSingleData(asset=self.asset,instance=self.instance,sensor=self.count_series,value=count)
        except:pass

    def sendRestart(self) -> None:
        try:self.sendSingleData(asset=self.asset,instance=self.instance,sensor=self.restart_series,value=1)
        except:pass
