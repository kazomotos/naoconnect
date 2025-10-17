from typing import List, Dict
from urllib.request import urlopen
from json import loads

from naoconnect.naoappV2 import NaoApp
from . import *

class _NaoApiFromMetaModel:
    '''
    Class that manages the assignment of API data to a specific model instance.

    Contains references to various IDs such as asset, workspace, and controller IDs
    which are used to uniquely identify and assign data sources.
    '''

    def __init__(self, instance_dic:dict) -> None:
        '''
        Initializes a model instance with API identification values.

        Args:
            instance_dic (dict): Dictionary containing 'AssetID', 'TwinID', 'WorkspaceID', 'AqtecLinienID', and 'AqotecRegerID'.
        '''
        self.instance_id = instance_dic["AssetID"]
        self.asset_id = instance_dic["TwinID"]
        self.workspace_id = instance_dic["WorkspaceID"]
        self.aqotec_controller_id = instance_dic["AqotecRegerID"]
        self.aqotec_line_id = instance_dic["AqtecLinienID"]
        self.alternative_tabel_name = instance_dic["AlternativTabellenName"]


class NaoApiLablingFromMetaModel:
    '''
    Class for labeling and collecting metadata from a NAO API endpoint.
    Parses route-based token responses to generate structured label data.
    '''

    def __init__(self, route:str) -> None:
        '''
        Initializes label structure and sets labels using metadata from an API route.

        Args:
            route (str): API route to fetch label information.
        '''
        self.route = route
        self.labels:List[_NaoApiFromMetaModel] = []
        self.labels_dic = {} 
        self._setLables()


    def _setLables(self) -> list:
        '''
        Fetches and sets metadata for data acquisition.

        Returns:
            list: A list of NaoApiFromMetaModel instances representing labeled data.
        '''
        with urlopen(self.route) as response:
            nao_api_data = loads(response.read())
            nao_api_data = nao_api_data["results"]
        
        for dat in nao_api_data:
            meta = _NaoApiFromMetaModel(dat)
            self.labels.append(meta)
            self.labels_dic[meta.instance_id] = meta


class _WorkspaceNaoModel:
    '''
    Represents the NAO information schema for a single workspace.
    '''

    def __init__(self, aqotec_db_name, organization_id, workspace_id) -> None:
        '''
        Initializes a workspace model with relevant IDs.

        Args:
            aqotec_db_name (str): Name of the corresponding Aqotec database.
            organization_id (str): Organization ID.
            workspace_id (str): NAO workspace ID.
        '''
        self.aqotec_db_name = aqotec_db_name
        self.organization = organization_id
        self.workspace_id = workspace_id
        

class WorkspacesNao:
    '''
    Organizes and indexes labeled workspaces from NAO.
    '''

    def __init__(self) -> None:
        '''
        Initializes the workspace registry.
        '''
        self.labling_id:Dict[str,_WorkspaceNaoModel] = {}
        self.labling_name:dict = {}
        self.names:list = []


    def setFromV2(self, conf:dict) -> None:
        '''
        Loads workspace definitions from a config dictionary (AqotecV2 format).

        Args:
            conf (dict): Configuration dictionary containing "name", "_organization", and "_id" keys.
        '''
        for con in conf:

            self.names.append(con["name"])

            self.labling_id[con["_id"]] = _WorkspaceNaoModel(
                aqotec_db_name=con["name"],
                organization_id=con["_organization"],
                workspace_id=con["_id"]
            )
            self.labling_name[con["name"]] = self.labling_id[con["_id"]]
