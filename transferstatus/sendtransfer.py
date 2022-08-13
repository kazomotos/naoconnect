from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.encoders import encode_base64
import smtplib
import pandas as pd
from naoconnect.NaoApp import NaoApp
from datetime import datetime
import time
from naoconnect.Param import Param 

class ReportMailSsl(Param):
    MSG_FROM = "From"
    MSG_TO = "To"
    MSG_SUBJECT = "Subject"
    MSG_HTML = "html"
    MSG_KONTAKT_NAME = "KONTAKT_NAME"
    HTML_BASE = "mail_baseform.html"
    HTML_TITLE = "mail_title.html"
    HTML_ROW = "mail_rows.html"
    POS_YEAR = "{year}"
    POS_INSANCE_URL = "{instanceUrl}"
    POS_TITLE = "{title}"
    POS_SUBTITLE = "{sub_title}"
    POS_TEXT = "{text}"
    POS_ROWS = "{rows}"
    LAST_ALL = "last_all"
    COUNT_ALL = "count_all"
    LAST_30d = "last_30d"
    COUNT_30d = "count_30d"
    FORM_WORCSPACE_TITLE = "Workspace&nbsp;&ndash;&nbsp;%s"
    FORM_BASE_TEXT = "gesmater Zeitraum&nbsp;&ndash;&nbsp;Messwerte: %s,&nbsp;&nbsp;%s<br />letzen 30 Tage&nbsp;&ndash;&nbsp;Messwerte: %s,&nbsp;&nbsp;%s%s"
    FORM_MISSING_SENSORS = "%s Messpunkte liefern keine Messwerte"
    FORM_ASSET_TITLE = "Asset&nbsp;&ndash;&nbsp;%s"
    FORM_INSTANCE_TEXT_LAST = "<br />Instance&nbsp;&ndash;&nbsp;%s: seit %s werden keine Messwerte mehr geliefert"
    FORM_INSTANCE_TEXT_NO_DATA = "<br />Instance&nbsp;&ndash;&nbsp;%s: es wurden noch nie Messwerte geliefert"

    def __init__(self, NaoApp:NaoApp, host:str, port:int, from_mail:str, password:str, to_mails:list, to_names:list):
        self.Nao = NaoApp
        self.host = host
        self.port = port
        self.from_mail = from_mail
        self.password = password
        self.to_mails = to_mails
        self.to_names = to_names
        self.data_frame:pd.DataFrame = None
        self.client:smtplib.SMTP_SSL = None
        self.html_base = None
        self.html_input_title = None
        self.html_input_list:list = []
        self.html_input_add = self.html_input_list.append
    
    def login(self):
        self.client = smtplib.SMTP_SSL(host=self.host, port=self.port)
        self.client.login(self.from_mail, self.password)

    def sendHtmlMsg(self, subject="Report Datenerfassung", message="", filename=None, filecontent=None):
        self.login()
        msg_tamplet = MIMEText(message,ReportMailSsl.MSG_HTML)
        for name, email in zip(self.to_names, self.to_mails):
            msg = MIMEMultipart() 
            msg[ReportMailSsl.MSG_KONTAKT_NAME]=name
            msg[ReportMailSsl.MSG_FROM]=self.from_mail
            msg[ReportMailSsl.MSG_TO]=email
            msg[ReportMailSsl.MSG_SUBJECT]=subject
            msg.attach(msg_tamplet)
            if filename != None:
                file_data = MIMEBase('application', "octet-stream")
                file_data.set_payload(filecontent)
                encode_base64(file_data)
                file_data.add_header('Content-Disposition', 'attachment; filename="'+filename+'"')
                msg.attach(file_data)
            self.client.sendmail(self.from_mail, email, msg.as_string())
        self.client.close()

    def sendNaoReport(self, subject="Report Datenerfassung", message=""):
        self._getNaoData()
        self._getNaoHtmlModel()
        self._setHtmlInputs()
        self.sendHtmlMsg(subject=subject,message=self._buildHtmlMsg(), filename="nao_long_report.csv", filecontent=self.data_frame.to_csv(index=False))

    def _getNaoHtmlModel(self, InstanceUrl="https://nao-app.de", title="Report Datenerfassung"):
        path_split = __file__.split("/")
        path = ""
        for spl in path_split[:-1]:
            path += spl + "/"
        InstanceUrl = InstanceUrl
        self.html_base = open(path+ReportMailSsl.HTML_BASE, "r").read()
        self.html_base = self.html_base.replace(
                  ReportMailSsl.POS_YEAR, str(datetime.now().year)
        ).replace(ReportMailSsl.POS_INSANCE_URL, InstanceUrl
        ).replace(ReportMailSsl.POS_TITLE, title
        )
        self.html_input_title = open(path+ReportMailSsl.HTML_TITLE, "r").read()
        self.html_input_rows = open(path+ReportMailSsl.HTML_ROW, "r").read()

    def _setInput(self, title:str, text:str, sub_title:list, sub_text:list):
        html_input_title = self.html_input_title.replace(ReportMailSsl.POS_TITLE, title).replace(ReportMailSsl.POS_TEXT, text)
        html_sub = ""
        for index in range(len(sub_title)):
            html_sub += self.html_input_rows.replace(ReportMailSsl.POS_SUBTITLE,sub_title[index]).replace(ReportMailSsl.POS_TEXT,sub_text[index])
        self.html_input_add(html_input_title.replace(ReportMailSsl.POS_ROWS, html_sub))

    def _buildHtmlMsg(self):
        html_inputs = ""
        for html_input in self.html_input_list:
            html_inputs += html_input
        return(self.html_base.replace(ReportMailSsl.POS_ROWS, html_inputs))

    def _formatCount(self, count, seperator="."):
        return(format(int(count),",d").replace(",", seperator))

    def _getNaoData(self):
        data_frame = {
            ReportMailSsl.NAME_WORKSPACE: [],
            ReportMailSsl.NAME_WORKSPACE_ID: [],
            ReportMailSsl.NAME_ASSET: [],
            ReportMailSsl.NAME_ASSET_ID: [],
            ReportMailSsl.NAME_INSTANCE: [],
            ReportMailSsl.NAME_INSTANCE_ID: [],
            ReportMailSsl.NAME_SERIES: [],
            ReportMailSsl.NAME_SERIES_ID: [],
            ReportMailSsl.LAST_ALL: [],
            ReportMailSsl.COUNT_ALL: [],
            ReportMailSsl.LAST_30d: [],
            ReportMailSsl.COUNT_30d: []
        }
        add_worcspace = data_frame[ReportMailSsl.NAME_WORKSPACE].append
        add_worcspace_id = data_frame[ReportMailSsl.NAME_WORKSPACE_ID].append
        add_asset = data_frame[ReportMailSsl.NAME_ASSET].append
        add_asset_id = data_frame[ReportMailSsl.NAME_ASSET_ID].append
        add_instance = data_frame[ReportMailSsl.NAME_INSTANCE].append
        add_instance_id = data_frame[ReportMailSsl.NAME_INSTANCE_ID].append
        add_series = data_frame[ReportMailSsl.NAME_SERIES].append
        add_series_id = data_frame[ReportMailSsl.NAME_SERIES_ID].append
        add_lasttime_all = data_frame[ReportMailSsl.LAST_ALL].append
        add_count_all = data_frame[ReportMailSsl.COUNT_ALL].append
        add_lasttime_30d = data_frame[ReportMailSsl.LAST_30d].append
        add_count_30d = data_frame[ReportMailSsl.COUNT_30d].append
        # ----
        workspaces = self.Nao.getWorkspace()[ReportMailSsl.NAME_RESULTS]
        series_name_dict = {}
        for dict_workspace in workspaces:
            time.sleep(20)
            organistation_id = dict_workspace[ReportMailSsl.NAME_ORGANIZATION_ID]
            workspace_name = dict_workspace[ReportMailSsl.NAME_NAME]
            add_dict = {}
            select_points = []
            add_point = select_points.append
            assets = self.Nao.getAssets(_workspace=dict_workspace[ReportMailSsl.NAME_ID_ID])[ReportMailSsl.NAME_RESULTS]
            for dict_asset in assets:
                time.sleep(10)
                asset_name = dict_asset[ReportMailSsl.NAME_NAME]
                instances = self.Nao.getInstances(_asset=dict_asset[ReportMailSsl.NAME_ID_ID])[ReportMailSsl.NAME_RESULTS]
                for dict_instance in instances:
                    instance_name = dict_instance[ReportMailSsl.NAME_NAME]
                    endpoints = self.Nao.getEndpoints(_instance=dict_instance[ReportMailSsl.NAME_ID_ID])[ReportMailSsl.NAME_RESULTS]
                    for dict_enpoint in endpoints:
                        time.sleep(0.1)
                        series_name = series_name_dict.get(dict_enpoint[ReportMailSsl.NAME_POINT_ID])
                        if series_name == None:
                            series = self.Nao.getSeries(_id=dict_enpoint[ReportMailSsl.NAME_POINT_ID])[ReportMailSsl.NAME_RESULTS][0]
                            series_name_dict[series[ReportMailSsl.NAME_ID_ID]] = series[ReportMailSsl.NAME_NAME]
                            series_name = series_name_dict.get(dict_enpoint[ReportMailSsl.NAME_POINT_ID])
                        add_point({
                            ReportMailSsl.NAME_ID: dict_enpoint[ReportMailSsl.NAME_ID_ID],
                            ReportMailSsl.NAME_INSTANCE: dict_enpoint[ReportMailSsl.NAME_INSTANCE_ID],
                            ReportMailSsl.NAME_ASSET: dict_enpoint[ReportMailSsl.NAME_ASSET_ID],
                            ReportMailSsl.NAME_SERIES: dict_enpoint[ReportMailSsl.NAME_POINT_ID]
                        })
                        add_dict[dict_enpoint[ReportMailSsl.NAME_ID_ID]] = [
                            workspace_name, 
                            dict_workspace[ReportMailSsl.NAME_ID_ID],
                            asset_name, 
                            dict_enpoint[ReportMailSsl.NAME_ASSET_ID],
                            instance_name,
                            dict_enpoint[ReportMailSsl.NAME_INSTANCE_ID],
                            series_name,
                            dict_enpoint[ReportMailSsl.NAME_ID_ID]
                        ]
            dict_all = {}
            dict_30d = {}
            # count all
            if len(select_points) > 250:
                count_all = []
                count_all_add = count_all.extend
                for index in range(int(len(select_points)/250)):
                    count_all_add(self.Nao.getSingelValues(organistation_id, points=select_points[index*250:(index+1)*250], aggregate="count",first_time="-6000d")[ReportMailSsl.NAME_RESULT])
                    last_index = (index+1)*250
                count_all_add(self.Nao.getSingelValues(organistation_id, points=select_points[last_index:], aggregate="count",first_time="-6000d")[ReportMailSsl.NAME_RESULT])
            else:
                count_all = self.Nao.getSingelValues(organistation_id, points=select_points, aggregate="count",first_time="-6000d")[ReportMailSsl.NAME_RESULT]
            for dic in count_all:
                dict_all[dic[ReportMailSsl.NAME_ID]] = {ReportMailSsl.NAME_COUNT:dic[ReportMailSsl.NAME_VALUE]}
            del count_all
            #  last all
            if len(select_points) > 250:
                last_all = []
                last_all_add = last_all.extend
                for index in range(int(len(select_points)/250)):
                    last_all_add(self.Nao.getSingelValues(organistation_id, points=select_points[index*250:(index+1)*250], aggregate="last",first_time="-6000d")[ReportMailSsl.NAME_RESULT])
                    last_index = (index+1)*250
                last_all_add(self.Nao.getSingelValues(organistation_id, points=select_points[last_index:], aggregate="last",first_time="-6000d")[ReportMailSsl.NAME_RESULT])
            else:
                last_all = self.Nao.getSingelValues(organistation_id, points=select_points, aggregate="last",first_time="-6000d")[ReportMailSsl.NAME_RESULT]
            for dic in last_all:
                dict_all[dic[ReportMailSsl.NAME_ID]][ReportMailSsl.NAME_TIME] = dic[ReportMailSsl.NAME_TIME]
            del last_all
            # count 30d
            if len(select_points) > 250:
                count_30d = []
                count_30d_add = count_30d.extend
                for index in range(int(len(select_points)/250)):
                    count_30d_add(self.Nao.getSingelValues(organistation_id, points=select_points[index*250:(index+1)*250], aggregate="count",first_time="-30d")[ReportMailSsl.NAME_RESULT])
                    last_index = (index+1)*250
                count_30d_add(self.Nao.getSingelValues(organistation_id, points=select_points[last_index:], aggregate="count",first_time="-30d")[ReportMailSsl.NAME_RESULT])
            else:
                count_30d = self.Nao.getSingelValues(organistation_id, points=select_points, aggregate="count",first_time="-30d")[ReportMailSsl.NAME_RESULT]
            for dic in count_30d:
                dict_30d[dic[ReportMailSsl.NAME_ID]] = {ReportMailSsl.NAME_COUNT:dic[ReportMailSsl.NAME_VALUE]}
            del count_30d
            # last 30d
            if len(select_points) > 250:
                last_30d = []
                last_30d_add = last_30d.extend
                for index in range(int(len(select_points)/250)):
                    last_30d_add(self.Nao.getSingelValues(organistation_id, points=select_points[index*250:(index+1)*250], aggregate="last",first_time="-30d")[ReportMailSsl.NAME_RESULT])
                    last_index = (index+1)*250
                last_30d_add(self.Nao.getSingelValues(organistation_id, points=select_points[last_index:], aggregate="last",first_time="-30d")[ReportMailSsl.NAME_RESULT])
            else:
                last_30d = self.Nao.getSingelValues(organistation_id, points=select_points, aggregate="last",first_time="-30d")[ReportMailSsl.NAME_RESULT]
            for dic in last_30d:
                dict_30d[dic[ReportMailSsl.NAME_ID]][ReportMailSsl.NAME_TIME] = dic[ReportMailSsl.NAME_TIME]
            del last_30d
            for id_endpoint in dict_all:
                add_worcspace(add_dict[id_endpoint][0])
                add_worcspace_id(add_dict[id_endpoint][1])
                add_asset(add_dict[id_endpoint][2])
                add_asset_id(add_dict[id_endpoint][3])
                add_instance(add_dict[id_endpoint][4])
                add_instance_id(add_dict[id_endpoint][5])
                add_series(add_dict[id_endpoint][6])
                add_series_id(add_dict[id_endpoint][7])
                if dict_all[id_endpoint][ReportMailSsl.NAME_TIME] != None:
                    add_lasttime_all(datetime.fromisoformat(dict_all[id_endpoint][ReportMailSsl.NAME_TIME]))
                else:
                    add_lasttime_all(None)
                add_count_all(dict_all[id_endpoint][ReportMailSsl.NAME_COUNT])
                if dict_30d[id_endpoint][ReportMailSsl.NAME_TIME] != None:
                    add_lasttime_30d(datetime.fromisoformat(dict_30d[id_endpoint][ReportMailSsl.NAME_TIME]))
                else:
                    add_lasttime_30d(None)
                add_count_30d(dict_30d[id_endpoint][ReportMailSsl.NAME_COUNT])
        self.data_frame = pd.DataFrame(data_frame)

    def _setHtmlInputs(self):
        grouped_workspace = self.data_frame.groupby(ReportMailSsl.NAME_WORKSPACE_ID)
        workspaces_list_id = list(grouped_workspace.groups.keys())
        for workspace_id in workspaces_list_id:
            sub_title_list = []
            sub_text_list = []
            sub_title_add = sub_title_list.append
            sub_text_add = sub_text_list.append
            workspace_frame = grouped_workspace.get_group(workspace_id)
            title_workspace = ReportMailSsl.FORM_WORCSPACE_TITLE%(workspace_frame[ReportMailSsl.NAME_WORKSPACE].iloc[0])
            text_missing_all = workspace_frame[ReportMailSsl.COUNT_ALL].isna().sum()
            if text_missing_all > 0:
                text_missing_all = ReportMailSsl.FORM_MISSING_SENSORS%(self._formatCount(text_missing_all))
            else:
                text_missing_all = ""
            text_missing_30d = workspace_frame[ReportMailSsl.COUNT_30d].isna().sum()
            if text_missing_30d > 0:
                text_missing_30d = ReportMailSsl.FORM_MISSING_SENSORS%(self._formatCount(text_missing_30d))
            else:
                text_missing_30d = ""
            count_all = self._formatCount(workspace_frame[ReportMailSsl.COUNT_ALL].sum())
            count_30d = self._formatCount(workspace_frame[ReportMailSsl.COUNT_30d].sum())
            text_workspace = ReportMailSsl.FORM_BASE_TEXT%(count_all,text_missing_all,count_30d,text_missing_30d,"")
            # asset
            grouped_asset = workspace_frame.groupby(ReportMailSsl.NAME_ASSET_ID)
            asset_list_id = list(grouped_asset.groups.keys())
            for asset_id in asset_list_id:
                asset_frame = grouped_asset.get_group(asset_id)
                sub_title_add(ReportMailSsl.FORM_ASSET_TITLE%(asset_frame[ReportMailSsl.NAME_ASSET].iloc[0]))
                text_missing_all = asset_frame[ReportMailSsl.COUNT_ALL].isna().sum()
                if text_missing_all > 0:
                    text_missing_all = ReportMailSsl.FORM_MISSING_SENSORS%(self._formatCount(text_missing_all))
                else:
                    text_missing_all = ""
                text_missing_30d = asset_frame[ReportMailSsl.COUNT_30d].isna().sum()
                if text_missing_30d > 0:
                    text_missing_30d = ReportMailSsl.FORM_MISSING_SENSORS%(self._formatCount(text_missing_30d))
                else:
                    text_missing_30d = ""
                count_all = self._formatCount(asset_frame[ReportMailSsl.COUNT_ALL].sum())
                count_30d = self._formatCount(asset_frame[ReportMailSsl.COUNT_30d].sum())
                # instancen
                grouped_instance = asset_frame.groupby(ReportMailSsl.NAME_ASSET_ID)
                instance_list_id = list(grouped_instance.groups.keys())
                instance_text = ""
                for instance_id in instance_list_id:
                    instance_frame = grouped_instance.get_group(instance_id)
                    instance_name = instance_frame[ReportMailSsl.NAME_ASSET].iloc[0]
                    # missing_all = instance_frame[ReportMailSsl.COUNT_ALL].isna().sum()
                    # missing_30d = instance_frame[ReportMailSsl.COUNT_30d].isna().sum()
                    count_all = instance_frame[ReportMailSsl.COUNT_ALL].sum()
                    if count_all == 0:
                        instance_text += ReportMailSsl.FORM_INSTANCE_TEXT_NO_DATA%(instance_name)
                        continue
                    count_30d = instance_frame[ReportMailSsl.COUNT_30d].sum()
                    if count_30d == 0:
                        instance_text += ReportMailSsl.FORM_INSTANCE_TEXT_LAST %(instance_name, str(instance_frame[ReportMailSsl.LAST_ALL].max()).split("+")[0])


                sub_text_add(ReportMailSsl.FORM_BASE_TEXT%(count_all,text_missing_all,count_30d,text_missing_30d,instance_text))
            self._setInput(
                title=title_workspace, 
                text=text_workspace, 
                sub_title=sub_title_list, 
                sub_text=sub_text_list
            )