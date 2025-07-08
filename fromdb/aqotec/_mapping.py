from typing import List
from dataclasses import dataclass

from . import *


@dataclass
class AssetTableMapping:
    '''
    Mapping object linking a NAO asset to an Aqotec measurement table.

    This data structure combines metadata from both systems, allowing the synchronizer
    to identify which table and which sensors in Aqotec correspond to which asset
    configuration in NAO.

    Attributes:
        instance_id (str): The NAO-internal asset ID (formerly known as "instance ID").
        asset_id (str): The NAO-internal Twin ID representing the asset type.
        workspace_id (str): The workspace identifier from NAO.
        aqotec_db (str): Name of the Aqotec live data database.
        aqotec_db_history (str): Name of the Aqotec historical data database (or None).
        aqotec_table (str): Name of the specific Aqotec table for the asset.
        controller_id (str): The Aqotec controller ID associated with the table.
        sensor_models (List[AqotecDataPointModel]): List of configured sensors (matching Aqotec columns).
    '''
    instance_id: str
    asset_id: str
    workspace_id: str
    aqotec_db: str
    aqotec_db_history: str
    aqotec_table: str
    controller_id: str
    sensor_models: List[AqotecDataPointModel] 


class AqotecNaoMapping:
    '''
    Responsible for generating the mapping between NAO assets and Aqotec measurement tables.

    This class combines workspace metadata, Aqotec database structure, and configured drivers
    to determine which Aqotec tables are eligible for synchronization and how they map to
    assets in NAO.
    '''

    def __init__(self, naolabling:NaoApiLablingFromMetaModel, driver_configs:List[DataPointsConfiguration],
                 aqotecstruct:AqotecDatabaseStructure, workspaces:WorkspacesNao) -> None:
        '''
        Initializes the mapping class with all required system inputs.

        Args:
            naolabling (NaoApiLablingFromMetaModel): Contains the list of current NAO asset mappings,
                including controller ID, workspace ID, asset ID and instance ID.
            driver_configs (List[DataPointsConfiguration]): Configured Aqotec driver definitions per sensor type.
            aqotecstruct (AqotecDatabaseStructure): Aqotec database and table structure including controller IDs.
            workspaces (WorkspacesNao): Workspace metadata used to derive Aqotec database names.
        '''
        self.naolabling = naolabling
        self.driver_configs = driver_configs
        self.aqotecstruct = aqotecstruct
        self.workspaces = workspaces
        self.mapping = self._mapping()


    def _mapping(self) -> List[AssetTableMapping]:
        '''
        Builds a list of asset-to-table mappings between NAO and Aqotec.

        For each NAO-labeled asset, the method checks:
        - Whether the workspace is valid and mapped to an Aqotec database
        - Whether the Aqotec database and optionally the historical database exist
        - Which tables match the given Aqotec controller ID
        - Whether a table is supported by one of the driver configurations
        - Whether any columns in the table match configured sensor definitions

        If all checks succeed, a mapping is created and added to the result list.

        Returns:
            List[AssetTableMapping]: List of all valid mappings between NAO assets and Aqotec tables.
        '''
        mapping = []

        for label in self.naolabling.labels:
            controller_id = label.aqotec_controller_id
            workspace_id = label.workspace_id

            db_model = self.workspaces.labling_id.get(workspace_id)
            if not db_model:
                continue 

            db_prefix = f"aqotec_{db_model.aqotec_db_name}_Daten"
            if not self.aqotecstruct.timeseries_tables.get(db_prefix):
                continue

            db_prefix_history = f"aqotec_{db_model.aqotec_db_name}_Daten_archiv"
            if not self.aqotecstruct.timeseries_tables.get(db_prefix):
                db_prefix_history = None

            db_struct = self.aqotecstruct.timeseries_tables.get(db_prefix)

            for table_name, col_struct in db_struct.columns.items():

                if col_struct.controller_id != controller_id:
                    continue

                for config in self.driver_configs:

                    if not config.checkTableName(table_name):
                        continue

                    dp_models = AqotecDataPointModel.searchMany(config.data_point, col_struct.all_columns)

                    if len(dp_models) == 0:
                        break

                    mapping.append(
                        AssetTableMapping(
                            instance_id=label.instance_id,
                            asset_id=label.asset_id, 
                            workspace_id=workspace_id,
                            aqotec_db=db_prefix,
                            aqotec_db_history=db_prefix_history,
                            aqotec_table=table_name,
                            controller_id=controller_id,
                            sensor_models=dp_models
                        )
                    )
                    break

        return(mapping)