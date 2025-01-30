

from typing import Dict, Tuple, List, Any
import csv

import numpy as np
import pandas as pd

'''
file type 1 für csv von Prina abschauen
file type 2 für excel von GEG abschauen
'''

class ReadCsv:
    """
    A class to read and process CSV files containing sensor data.

    Attributes:
        column_labing (dict): A dictionary mapping CSV sensor names to their metadata.
        data_file_path (str): The directory path where CSV files are stored.
        file_list_to_read (list): A list of filenames to be read.
        file_type (int): The type of CSV file format (currently only supports type 0).
        getNameFromFile (callable, optional): A function to extract relevant parts of the filename.
    """

    def __init__(self, column_labing: Dict[str, Dict[str, Any]], data_file_path: str, 
                 file_list_to_read: List[str], file_type: int = 0, 
                 getNameFromFile: Any = None) -> None:
        """
        Initializes the ReadCsv class.

        Args:
            column_labing (dict): Dictionary defining sensor mapping and conversion factors.
            data_file_path (str): Path to the directory containing the CSV files.
            file_list_to_read (list): List of filenames to read.
            file_type (int, optional): Type of CSV format (only 0 is supported). Defaults to 0.
            getNameFromFile (callable, optional): Function to extract metadata from filenames.

        Raises:
            TypeError: If an unsupported file type is provided.
        """
        self.column_labing = column_labing
        self.getNameFromFile = getNameFromFile
        if file_type != 0:
            raise TypeError("Only file type 0 is supported.")
        self.file_type = file_type
        self.data_file_path = data_file_path
        self.file_list_to_read = file_list_to_read
        self._file_frame = None

    def _getCsvHeader(self, file_path_name: str) -> Tuple[List[str], str, int]:
        """
        Identifies the header row and determines the CSV delimiter.

        Args:
            file_path_name (str): Path to the CSV file.

        Returns:
            tuple: A tuple containing (header list, delimiter, header row index).

        Raises:
            ValueError: If no valid header is found.
        """
        delimiter = ";"
        header_col = 0

        with open(file_path_name, mode='r', newline='') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=";")
            header = next(csv_reader)

            while len(header) == 1:
                if header_col > 5:
                    raise ValueError(f"No header found in {file_path_name}")

                if len(header[0].split(",")) > 2:
                    header = header[0].split(",")
                    delimiter = ","
                    break

                header_col += 1
                header = next(csv_reader)

        return( header, delimiter, header_col )

    def _readType0Csv(self, file_path: str, file_name: str, dict_name: str) -> Dict[str, pd.DataFrame]:
        """
        Reads and processes CSV files of type 0.

        Args:
            file_path (str): Path to the CSV file.
            file_name (str): Name of the CSV file.
            dict_name (str): Name of the resulting DataFrame key.

        Returns:
            dict: A dictionary containing the processed DataFrame.
        """
        header, delimiter, header_col = self._getCsvHeader(file_path + file_name)
        name_time = header[0]

        if len(name_time.split('"')) == 3:
            name_time = name_time.split('"')[1]

        if header_col > 0:
            frame = pd.read_csv(
                file_path + file_name, 
                parse_dates=[name_time], 
                delimiter=delimiter, 
                header=header_col, 
            )
        else:
            frame = pd.read_csv(file_path + file_name, parse_dates=[name_time], delimiter=delimiter)

        frame.rename(columns={name_time: 'time'}, inplace=True)

        return( {dict_name: frame} )

    def _sortDataFrames(self, heat_meter_frames: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Sorts each DataFrame by time in ascending order.

        Args:
            heat_meter_frames (dict): Dictionary containing DataFrames.

        Returns:
            dict: Sorted DataFrames.
        """
        for meter_id in heat_meter_frames:
            heat_meter_frames[meter_id].sort_values(by="time", ascending=True, inplace=True)
            heat_meter_frames[meter_id].index = heat_meter_frames[meter_id]["time"]
        return( heat_meter_frames )

    def _dropDuplicates(self, heat_meter_frames: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Removes duplicate timestamps, keeping only the first occurrence.

        Args:
            heat_meter_frames (dict): Dictionary containing DataFrames.

        Returns:
            dict: DataFrames with duplicates removed.
        """
        for meter_id in heat_meter_frames:
            duplicate_mask = heat_meter_frames[meter_id]["time"].duplicated(keep="first")
            heat_meter_frames[meter_id].mask(duplicate_mask, np.nan, inplace=True)

        return( heat_meter_frames )

    def _getUnitConvertedFrame(self, heat_meter_frames: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Converts sensor values using predefined conversion factors.

        Args:
            heat_meter_frames (dict): Dictionary containing DataFrames.

        Returns:
            dict: DataFrames with converted sensor values.
        """
        for heat_meter_id in heat_meter_frames:
            columns = list(heat_meter_frames[heat_meter_id].columns)

            for sensor, properties in self.column_labing.items():
                if sensor in columns and properties.get("factor") is not None:
                    heat_meter_frames[heat_meter_id][sensor] *= properties["factor"]

        return( heat_meter_frames )

    def _convertTimestampToNano(self, heat_meter_frames: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Converts timestamps to nanoseconds for InfluxDB compatibility.

        Args:
            heat_meter_frames (dict): Dictionary containing DataFrames.

        Returns:
            dict: DataFrames with converted timestamps.
        """
        for heat_meter_id in heat_meter_frames:
            heat_meter_frames[heat_meter_id]["time"] = heat_meter_frames[heat_meter_id]["time"].astype(int)
        return( heat_meter_frames )

    def _readFilesAsFrameDict(self) -> Dict[str, pd.DataFrame]:
        """
        Reads and processes all specified CSV files.

        Returns:
            dict: Dictionary of processed DataFrames.
        """
        frame_dict = {}

        for file in self.file_list_to_read:
            dict_name = self.getNameFromFile(file) if self.getNameFromFile else file
            if self.file_type == 0:
                frame_dict.update(self._readType0Csv(file_path=self.data_file_path, file_name=file, dict_name=dict_name))

        return( frame_dict )

    @property
    def file_frames(self) -> Dict[str, pd.DataFrame]:
        """
        Returns the processed file frames. If not read yet, reads and processes them.

        Returns:
            dict: Dictionary containing processed DataFrames.
        """
        if self._file_frame is None:
            self._file_frame = self._readFilesAsFrameDict()
            self._file_frame = self._sortDataFrames(self._file_frame)
            self._file_frame = self._dropDuplicates(self._file_frame)
            self._file_frame = self._getUnitConvertedFrame(self._file_frame)
            self._file_frame = self._convertTimestampToNano(self._file_frame)

        return( self._file_frame )
