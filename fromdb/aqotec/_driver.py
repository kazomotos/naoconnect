from typing import Optional, List


class AqotecDataPointModel:
    '''
    Model class representing a single Aqotec data point configuration.

    Each instance includes metadata such as the sensor ID, instance ID, asset ID, data point name and position,
    as well as threshold and boundary values used for data validation or processing logic.
    '''

    def __init__(self, nao_sensor_id:str, dp_name:str, 
                 dp_position:str, lt:float, gt:float, b1: float, b2:float) -> None:
        '''
        Initializes a new data point model with identification metadata and validation parameters.

        Args:
            nao_sensor_id (str): The sensor ID from the NAO system.
            dp_name (str): The descriptive name of the data point.
            dp_position (str): The internal key or position identifier.
            lt (float): Lower threshold value — used to determine if initial values are considered valid.
            gt (float): Upper threshold value — used to determine if initial values are considered valid.
            b1 (float): Lower boundary for exclusion zone. If a value falls within [b1, b2], it is considered invalid.
            b2 (float): Upper boundary for exclusion zone. If a value falls within [b1, b2], it is considered invalid.

        Note:
            Threshold checks (`lt`, `gt`, `b1`, `b2`) are only relevant for the **initial activation** of a sensor. 
            Once a valid value is recorded, the sensor is permanently considered active and will continue to be logged, 
            regardless of future validation status.
        '''
        self.nao_sensor_id = nao_sensor_id
        self.dp_name = dp_name
        self.dp_position = dp_position
        self.lt = lt
        self.gt = gt
        self.b1 = b1
        self.b2 = b2

    @classmethod
    def search(cls, liste: list["AqotecDataPointModel"], such_tupel: tuple[str, str]
               ) -> "AqotecDataPointModel | None":
        '''
        Searches a list of AqotecDataPointModel instances for a match based on (dp_name, dp_position).

        Args:
            list (List[AqotecDataPointModel]): The list to search in.
            such_tupel (tuple[str, str]): The (dp_name, dp_position) tuple to match.

        Returns:
            AqotecDataPointModel: The matching datapoint, or None if not found.
        '''

        for element in liste:
            if (element.dp_name, element.dp_position) == such_tupel:
                return element
        return None

    @classmethod
    def searchMany(cls, liste: list["AqotecDataPointModel"], such_tupel_liste: list[tuple[str, str]]
                ) -> list["AqotecDataPointModel"]:
        '''
        Sucht eine Liste von AqotecDataPointModel-Instanzen nach mehreren (dp_name, dp_position)-Tupeln ab.

        Args:
            liste (List[AqotecDataPointModel]): Die Liste, in der gesucht wird.
            such_tupel_liste (List[tuple[str, str]]): Liste von (dp_name, dp_position), nach denen gesucht wird.

        Returns:
            List[AqotecDataPointModel]: Liste aller passenden Modelle.
        '''
        such_set = set(such_tupel_liste)  # Für schnelle Suche
        return [element for element in liste if (element.dp_name, element.dp_position) in such_set]


class DataPointBase:
    '''
    Base class for all Aqotec data point types.

    Provides shared configuration structure and a method to generate data point model instances
    based on class-level configuration definitions (e.g., dp_config_raw).
    '''
    dp_config_raw: List[tuple[str, str]] = []
    gt: Optional[float] = None
    lt: Optional[float] = None
    b1: Optional[float] = None
    b2: Optional[float] = None

    def __init__(self, nao_sensor_id:str) -> None:
        '''
        Initializes base data point with NAO identifiers for sensor, instance, and asset.

        Args:
            nao_sensor_id (str): The NAO sensor ID.
            nao_instance_id (str): The NAO instance ID.
            nao_asset_id (str): The NAO asset ID.
        '''
        self.nao_sensor_id = nao_sensor_id

    @property
    def dp_config(self) -> List["AqotecDataPointModel"]:
        '''
        Returns the list of configured AqotecDataPointModel objects defined in dp_config_raw.

        This list is built dynamically from the raw (name, position) pairs defined at the class level.
        '''
        return( [
            AqotecDataPointModel(
                nao_sensor_id=self.nao_sensor_id,
                dp_name=name,
                dp_position=position,
                lt=self.lt,
                gt=self.gt,
                b1=self.b1,
                b2=self.b2
            )
            for name, position in self.dp_config_raw
        ] )
    

class DataPointHeatMeterEnergy(DataPointBase):
    '''
    Driver class for heat meter high-resolution energy.
    '''
    typ:str = "Meter"
    dp_config_raw:List[tuple[str, str]]  = [
        ("Wärmemenge ", "DP_0Wert"),
        ("Wärmemenge", "DP_0Wert"),
        ("Waermemenge_in_kWh", "DP_0Wert"),
        ("WZ Wärmemenge", "DP_51Wert"),
        ("Wärmemenge", "DP_96Wert"),
        ("WzEnergie", "DP_53Wert"),
        ("WMZ1 Wärmemenge", "DP_211Wert"),
        ("Waermemenge_in_kWh", "DP_1Wert"),
        ("Wärmemenge 1", "DP_0Wert")
    ]
    gt:Optional[float] = 10
    lt:Optional[float] = None
    b1:Optional[float] = 240
    b2:Optional[float] = 266


class DataPointHeatMeterVolume(DataPointBase):
    '''
    Driver class for heat meter high-resolution volume.
    '''
    typ:str = "Meter"
    dp_config_raw:List[tuple[str, str]]  = [
        ('Volumen', 'DP_1Wert'),
        ('Volumen_in_m3', 'DP_1Wert'),
        ('WZ Volumen', 'DP_52Wert'),
        ('Volumen', 'DP_97Wert'),
        ('WzVolumen', 'DP_54Wert'),
        ('WMZ1 Volumen', 'DP_212Wert'),
        ('Volumen_in_m3', 'DP_2Wert'),
    ]
    gt:Optional[float] = 2
    lt:Optional[float] = None
    b1:Optional[float] = 240
    b2:Optional[float] = 266


class DataPointHeatMeterPower(DataPointBase):
    '''
    Driver class for heat meter high-resolution power.
    '''
    typ:str = "Sensor"
    dp_config_raw:List[tuple[str, str]]  = [
        ('mom Leistung', 'DP_2Wert'),
        ('aktuelle Leistung', 'DP_8Wert'),
        ('aktuelle_Leistung_in_kW', 'DP_2Wert'),
        ('WzLeistung', 'DP_55Wert'),
        ('Leistung', 'DP_98Wert'),
        ('WZ Leistung', 'DP_53Wert'),
        ('WMZ1 aktuelle Leistung', 'DP_213Wert'),
        ('aktuelle_Leistung_in_kW', 'DP_3Wert'),
        ('mom.Leistung', 'DP_2Wert'),
    ]
    gt:Optional[float] = 0.25
    lt:Optional[float] = None
    b1:Optional[float] = 240
    b2:Optional[float] = 266


class DataPointHeatMeterFlow(DataPointBase):
    '''
    Driver class for heat meter high-resolution flow (volume).
    '''
    typ:str = "Sensor"
    dp_config_raw:List[tuple[str, str]]  = [
        ('akt Durchfluss', 'DP_3Wert'),
        ('Durchfluss_in_lph', 'DP_3Wert'),
        ('Durchfluß', 'DP_9Wert'),
        ('WzDurchfluss', 'DP_56Wert'),
        ('Duchfluss long', 'DP_99Wert'),
        ('WZ Duchfluss long', 'DP_54Wert'),
        ('WMZ1 Durchfluss', 'DP_214Wert'),
        ('Durchfluss_in_lph', 'DP_4Wert'),
        ('Durchfluß long', 'DP_3Wert'),
    ]
    gt:Optional[float] = 1
    lt:Optional[float] = None
    b1:Optional[float] = 240
    b2:Optional[float] = 266


class DataPointHeatMeterSupplyTemperature(DataPointBase):
    '''
    Driver class for heat meter high-resolution supply temperature.
    '''
    typ:str = "Sensor"
    dp_config_raw:List[tuple[str, str]]  = [
        ('Vorlauftemp', 'DP_5Wert'),
        ('Vorlauftemperatur Primär', 'DP_6Wert'),
        ('Vorlauftemp_in_C', 'DP_4Wert'),
        ('WzVLPrimär', 'DP_58Wert'),
        ('Vorlauftemp.', 'DP_101Wert'),
        ('WZ Vorlauftemp', 'DP_56Wert'),
        ('WMZ1 Vorlauftemperatur', 'DP_215Wert'),
        ('Vorlauftemp_in_C', 'DP_5Wert'),
        ('Vorlauftemp prim', 'DP_9Wert'),
        ('Vorlauftemp.prim.', 'DP_9Wert'),
    ]
    gt:Optional[float] = 5
    lt:Optional[float] = 130
    b1:Optional[float] = None
    b2:Optional[float] = None


class DataPointHeatMeterReturnTemperature(DataPointBase):
    '''
    Driver class for heat meter high-resolution return temperature.
    '''
    typ:str = "Sensor"
    dp_config_raw:List[tuple[str, str]]  = [
        ('Rücklauftemp', 'DP_4Wert'),
        ('Rücklauftemperatur Primär', 'DP_7Wert'),
        ('Ruecklauftemp_In_C', 'DP_5Wert'),
        ('WzRLPrimär', 'DP_57Wert'),
        ('Rücklauftemp.', 'DP_100Wert'),
        ('WZ Rücklauftemp', 'DP_55Wert'),
        ('WMZ1 Rücklauftemperatur', 'DP_216Wert'),
        ('Ruecklauftemp_In_C', 'DP_6Wert'),
        ('Rücklauftemp prim', 'DP_8Wert'),
        ('Rücklauftemp. prim.', 'DP_8Wert'),
    ]
    gt:Optional[float] = 5
    lt:Optional[float] = 130
    b1:Optional[float] = None
    b2:Optional[float] = None


class DataPointHeatMeterTemperaturSpread(DataPointBase):
    '''
    Driver class for heat meter high-resolution temperature spread beween supply and return temperature.
    '''
    typ:str = "Sensor"
    dp_config_raw:List[tuple[str, str]]  = [
        ('Spreizung', 'DP_6Wert'),
        ('Spreizung_in_K', 'DP_6Wert'),
        ('Spreizung', 'DP_5Wert'),
        ('WzSpreizung', 'DP_59Wert'),
        ('Spreizung', 'DP_102Wert'),
        ('WZ Spreizung', 'DP_57Wert'),
        ('WMZ1 Spreizung', 'DP_217Wert'),
    ]
    gt:Optional[float] = 5
    lt:Optional[float] = 130
    b1:Optional[float] = None
    b2:Optional[float] = None


class DataPointHeatMeterNumber(DataPointBase):
    '''
    Driver class for heat meter serial number.
    '''
    typ:str = "Sensor"
    dp_config_raw:List[tuple[str, str]]  = [
        ('Zählernummer', 'DP_21Wert'),
        ('Seriennummer', 'DP_21Wert'),
    ]
    gt:Optional[float] = 5
    lt:Optional[float] = 130
    b1:Optional[float] = None
    b2:Optional[float] = None


class DataPointsConfiguration:
    '''
    Configuration class to define allowed system types for asset mapping and associated data points.
    '''
    
    def __init__(self, rm360:bool=False, ug07:bool=False, bhkw:bool=False, ug07_sub_heat_meter:bool=False,
                 heat_meter:bool=False, rm360_sub_heat_meter:bool=False, network:bool=False) -> None:
        '''
        Initializes configuration and sets allowed system types.

        Args:
            rm360 (bool): Include RM360 systems.
            ug07 (bool): Include UG07 systems.
            bhkw (bool): Include BHKW systems.
            ug07_sub_heat_meter (bool): Include UG07 sub heat meters.
            heat_meter (bool): Include general heat meters.
            rm360_sub_heat_meter (bool): Include RM360 sub heat meters.
            network (bool): Include network systems.
        '''
        self.table_substrings = []
        self.data_point:List['DataPointBase'] = [] 
        if rm360: self.table_substrings.append("RM360_")
        if ug07: self.table_substrings.append("UG07_")
        if bhkw: self.table_substrings.append("BHKW")
        if heat_meter: self.table_substrings.append("WMZ_")
        if rm360_sub_heat_meter: self.table_substrings.append("RM360SubZ_")
        if ug07_sub_heat_meter: self.table_substrings.append("UG07SubZ_")
        if network: self.table_substrings.append("Netz")


    def _addDataPoint(self, data_point:'DataPointBase'):
        '''
        Adds configured data points to the current set.

        Args:
            data_point (DataPointBase): DataPointBase-derived object with configuration.
        '''
        self.data_point.extend(data_point.dp_config)

    def checkTableName(self, table_name) -> bool:
        '''
        Überprüft ob die Übergebene Tabelle für den Treiber verwendet werden darf.
        '''
        res = False
        for sub in  self.table_substrings:
            if sub in table_name:
                res = True
                break
        return(res)

    def addHeatmeterEnergy(self, nao_sensor_id) -> None:
        self._addDataPoint( DataPointHeatMeterEnergy(nao_sensor_id=nao_sensor_id) )

    def addHeatmeterPower(self, nao_sensor_id) -> None:
        self._addDataPoint( DataPointHeatMeterPower(nao_sensor_id=nao_sensor_id) )

    def addHeatmeterVolume(self, nao_sensor_id) -> None:
        self._addDataPoint( DataPointHeatMeterVolume(nao_sensor_id=nao_sensor_id) )
    
    def addHeatmeterFlow(self, nao_sensor_id) -> None:
        self._addDataPoint( DataPointHeatMeterFlow(nao_sensor_id=nao_sensor_id) )
    
    def addHeatmeterSupplyTemp(self, nao_sensor_id) -> None:
        self._addDataPoint( DataPointHeatMeterSupplyTemperature(nao_sensor_id=nao_sensor_id) )
    
    def addHeatmeterReturnTemp(self, nao_sensor_id) -> None:
        self._addDataPoint( DataPointHeatMeterReturnTemperature(nao_sensor_id=nao_sensor_id) )
    
    def addHeatmeterSpreadTemp(self, nao_sensor_id) -> None:
        self._addDataPoint( DataPointHeatMeterTemperaturSpread(nao_sensor_id=nao_sensor_id) )
    
    def addHeatmeterNumber(self, nao_sensor_id) -> None:
        self._addDataPoint( DataPointHeatMeterNumber(nao_sensor_id=nao_sensor_id) )