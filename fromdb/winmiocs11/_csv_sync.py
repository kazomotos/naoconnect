from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from io import StringIO
from math import isnan
from os import listdir, path
import csv
import json
import re
import sqlite3
from typing import Any

import pandas as pd


CSV_ENCODING = "ISO-8859-1"
CSV_DELIMITER = ";"
CSV_REVERSED_BUFFER_SIZE = 600
CSV_TIME_FORMAT_LAST_TIME = "%d.%m.%Y %H:%M:%S"
CSV_TIME_FORMAT_TIMESTEPS = "%Y-%m-%d %H:%M:%S"
CSV_TIMEZONE = "Europe/Berlin"
DEFAULT_INTERVAL_SECONDS = 300
DEFAULT_SAMPLE_LINES = 3000
SQLITE_SCHEMA_VERSION = 1

COLUMN_COMPILER = re.compile(r'<COL\s+NR="(\d+)">(.*?)</COL>', re.DOTALL)
TIME_COMPILER = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")
COLUMN_SENSOR_NAME_REG = r"<NAME>(.*?)</NAME>"
COLUMN_N_REG = r"<N>(.*?)</N>"
COLUMN_D_REG = r"<D>(.*?)</D>"
COLUMN_SH_REG = r"<SH>(.*?)</SH>"
TABLE_ID_REG = r'<PROTHDR ID="(.*?)">'
TABLE_STATION_REG = r"<JOBID>(.*?)</JOBID>"
TABLE_LAST_WRITE_TIME_REG = r"<LASTACT(.*?)</LASTACT>"
TABLE_INTERVAL_REG = r"<INTVSEC>(.*?)</INTVSEC>"
ARCHIVE_FOLDER_RE = re.compile(r"^PROTCSV_(\d{14})$")
HAST_TABLE_RE = re.compile(r"^(?P<controller_id>\d+)_prot\.csv$", re.IGNORECASE)
HAST_WMZ_TABLE_RE = re.compile(r"^(?P<controller_id>\d+)_protWZ\.csv$", re.IGNORECASE)

DRIVER_SECTION_BY_STATION_KEY = {
    "ug06": "driver_ug6_csv",
    "ug08": "driver_ug8_csv",
    "ug10": "driver_ug10_csv",
    "ug12": "driver_ug12_csv",
    "only_mbus_wz_as_hast": "driver_only_wz_as_hast_csv",
}


@dataclass
class CsvSensorConfig:
    """
    Fachliche Zuordnung eines CSV-Datenpunkts zu einem NAO-Sensor.

    Wichtige Besonderheit bei Schneid:
    `gt`, `lt`, `b1`, `b2` dienen hier nicht zur laufenden Validierung aller
    Messwerte. Sie werden nur genutzt, um bei bisher unbekannten Sensoren zu
    entscheiden, ob der Sensor überhaupt freigeschaltet und synchronisiert
    werden soll. Sobald ein Sensor aktiv ist, werden die Rohdaten übertragen.
    """

    sensor_id: str
    name_dp: str | None = None
    dp: int | None = None
    gt: float | None = None
    lt: float | None = None
    b1: float | None = None
    b2: float | None = None
    sensor_type: str | None = None


@dataclass
class CsvTargetConfig:
    """
    Zielbeschreibung für einen zu synchronisierenden CSV-Datenstrom.

    Ein Ziel kann entweder eine HAST-Instance oder ein lokal konfiguriertes
    Sonderasset sein. Die gleiche Tabelle kann dabei bewusst mehrfach gelesen
    werden, wenn dieselbe CSV-Datei mehrere Zielobjekte bedient.
    """

    target_key: str
    source_type: str
    logical_key: str
    table_name: str
    asset_id: str
    instance_id: str
    sensors: list[CsvSensorConfig]
    driver_section: str | None = None
    mapping_start_time: datetime | None = None


@dataclass
class CsvFileInfo:
    """
    Fachlich relevante Informationen zu einer konkreten CSV-Datei.

    `header_signature` wird bewusst aus Info- und Spaltenheader gebildet, damit
    Regler- oder Stationswechsel in Schneid als Mapping-Wechsel erkannt werden
    koennen.
    """

    table_name: str
    full_path: str
    origin: str
    order_key: tuple[Any, ...]
    info_header: str
    column_header: str
    table_id: str | None
    station_type: str | None
    last_write_time: datetime | None
    interval_seconds: int | None
    columns_by_name: dict[str, dict[str, Any]]
    header_signature: str


@dataclass
class CsvSyncSummary:
    """Kompakte Rueckgabe fuer `main.py` nach dem CSV-Zeitreihensync."""

    checked_targets: int = 0
    skipped_targets: int = 0
    recognized_sensors: int = 0
    synced_sensors: int = 0
    sent_values: int = 0
    ignored_files: int = 0
    migrated_sensors: int = 0
    changed_headers: int = 0
    empty_values_skipped: int = 0
    ignored_tables_without_mapping: int = 0
    ignored_tables: list[str] = field(default_factory=list)


def _parse_optional_float(value: Any) -> float | None:
    """Parst optionale Schwellenwerte aus JSON robust auf `float`."""
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    text = str(value).strip().replace(",", ".")
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_datetime(value: str | None) -> datetime | None:
    """Parst gespeicherte ISO-Zeitstempel tolerant."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _to_iso(value: datetime | None) -> str | None:
    """Formatiert Zeitpunkte fuer den lokalen SQLite-Status einheitlich."""
    if value is None:
        return None
    return value.isoformat(timespec="seconds")


def _to_utc_nanoseconds(value: datetime) -> int:
    """
    Wandelt einen lokalen Schneid-Zeitpunkt in UTC-Nanosekunden um.

    Schneid schreibt die Zeitstempel lokal in Europe/Berlin. Genau diese
    Annahme wurde auch im Altcode verwendet und muss erhalten bleiben.
    """
    localized = pd.Timestamp(value).tz_localize(CSV_TIMEZONE)
    return int(localized.tz_convert("UTC").value)


def _read_csv_header(file_path: str) -> tuple[str, str]:
    """Liest nur die erste CSV-Zeile und trennt Info- und Spaltenheader."""
    with open(file_path, mode="r", newline="", encoding=CSV_ENCODING) as file_handle:
        reader = csv.reader(file_handle, delimiter=CSV_DELIMITER)
        try:
            header = next(reader)
        except StopIteration:
            return "", ""
    if len(header) < 2:
        return header[0] if header else "", ""
    return header[0], header[1]


def _parse_csv_columns(column_header: str) -> dict[str, dict[str, Any]]:
    """
    Zerlegt den Schneid-Spaltenheader.

    Wichtige Besonderheit:
    Der fuer das Mapping benoetigte DP-Wert ist nicht `NR`, sondern die
    Positionsnummer der Spalte in der eigentlichen CSV-Datei. Genau darauf
    basiert bereits der alte Driver.
    """
    columns = COLUMN_COMPILER.findall(column_header)
    result: dict[str, dict[str, Any]] = {}
    position = 0

    for number, content in columns:
        match_name = re.search(COLUMN_SENSOR_NAME_REG, content)
        name = match_name.group(1) if match_name else None
        if name is None:
            position += 1
            continue

        match_n = re.search(COLUMN_N_REG, content)
        match_d = re.search(COLUMN_D_REG, content)
        match_sh = re.search(COLUMN_SH_REG, content)
        result[name] = {
            "database_number": number,
            "datapoint_name": name,
            "N": match_n.group(1) if match_n else None,
            "D": match_d.group(1) if match_d else None,
            "SH": match_sh.group(1) if match_sh else None,
            "position": position,
        }
        position += 1

    return result


def _parse_csv_info(info_header: str) -> tuple[str | None, str | None, datetime | None, int | None]:
    """Liest Tabellenkennung, Stationstyp, Schreibzeitpunkt und Intervall aus."""
    table_id_match = re.search(TABLE_ID_REG, info_header)
    station_type_match = re.search(TABLE_STATION_REG, info_header)
    last_write_match = re.search(TABLE_LAST_WRITE_TIME_REG, info_header)
    interval_match = re.search(TABLE_INTERVAL_REG, info_header)

    table_id = table_id_match.group(1) if table_id_match else None
    station_type = station_type_match.group(1) if station_type_match else None
    last_write_time = None
    if last_write_match:
        try:
            last_write_time = datetime.strptime(
                last_write_match.group(1).split(">")[1],
                CSV_TIME_FORMAT_LAST_TIME,
            )
        except (IndexError, ValueError):
            last_write_time = None

    interval_seconds = None
    if interval_match:
        try:
            interval_seconds = int(interval_match.group(1))
        except ValueError:
            interval_seconds = None

    return table_id, station_type, last_write_time, interval_seconds


def _read_csv_dataframe(file_path: str, lines: int | str = 10) -> pd.DataFrame:
    """
    Liest Schneid-CSV-Dateien mit der gleichen robusten Rueckwaertslogik wie im Altcode.

    Besonderheit bei Schneid:
    Wenn innerhalb derselben Datei der Regler bzw. Stationstyp gewechselt hat,
    kann sich die Struktur mitten in der Datei aendern. Das fuehrt beim Parsen zu
    Fehlern. Die Logik versucht daher bewusst, nur den letzten konsistenten Teil
    der Datei zu lesen. Genau dieses Verhalten wird fuer den neuen Sync erhalten.
    """
    if lines == "all":
        with open(file_path, "r", encoding=CSV_ENCODING) as handle:
            buffer = handle.read().split("\n")
    else:
        multiplier = 1
        while True:
            with open(file_path, "r", encoding=CSV_ENCODING) as handle:
                handle.seek(0, 2)
                file_end = handle.tell()
                position = max(0, file_end - CSV_REVERSED_BUFFER_SIZE * int(lines) * multiplier)
                handle.seek(position)
                buffer = handle.read(file_end - position).split("\n")
            multiplier += 1

            if len(buffer) > int(lines) + 1 or position == 0:
                break

            if buffer and buffer[-1] == "":
                buffer = buffer[-int(lines) - 1 : -1]
            else:
                buffer = buffer[-int(lines) :]

    start_index = 0
    for idx in range(len(buffer)):
        first_cell = buffer[idx].split(CSV_DELIMITER)[0]
        if TIME_COMPILER.match(first_cell):
            start_index = idx
            break

    candidate = "\n".join(buffer[start_index:])
    try:
        dataframe = pd.read_csv(
            StringIO(candidate),
            sep=CSV_DELIMITER,
            header=None,
            encoding=CSV_ENCODING,
        )
    except pd.errors.ParserError as error:
        match = re.search(r"line (\d+)", str(error))
        if not match:
            raise
        line_number = int(match.group(1))
        try:
            dataframe = pd.read_csv(
                StringIO("\n".join(buffer[start_index + line_number :])),
                sep=CSV_DELIMITER,
                header=None,
                encoding=CSV_ENCODING,
            )
        except pd.errors.ParserError:
            dataframe = pd.read_csv(
                StringIO("\n".join(buffer[start_index + line_number + 100 :])),
                sep=CSV_DELIMITER,
                header=None,
                encoding=CSV_ENCODING,
            )

    dataframe[0] = pd.to_datetime(dataframe[0], format=CSV_TIME_FORMAT_TIMESTEPS)
    dataframe.set_index(0, inplace=True)
    dataframe.columns = range(len(dataframe.columns))
    return dataframe


def discover_csv_files(actual_dir: str | None, archive_root_dir: str | None) -> dict[str, list[CsvFileInfo]]:
    """
    Baut ein fachliches Bild aller lesbaren Schneid-CSV-Dateien auf.

    Rueckgabe:
    - Schluessel: Tabellenname, z. B. `1_prot.csv`
    - Wert: chronologisch sortierte Liste aller Vorkommen aus Archiv und aktuellem Bestand
    """
    discovered: list[CsvFileInfo] = []

    def add_file(file_path: str, table_name: str, origin: str, order_key: tuple[Any, ...]) -> None:
        if not path.isfile(file_path) or not table_name.lower().endswith(".csv"):
            return
        try:
            info_header, column_header = _read_csv_header(file_path)
        except (UnicodeDecodeError, OSError, csv.Error):
            return
        if "<PROTHDR" not in info_header:
            return
        table_id, station_type, last_write_time, interval_seconds = _parse_csv_info(info_header)
        columns = _parse_csv_columns(column_header)
        discovered.append(
            CsvFileInfo(
                table_name=table_name,
                full_path=file_path,
                origin=origin,
                order_key=order_key,
                info_header=info_header,
                column_header=column_header,
                table_id=table_id,
                station_type=station_type,
                last_write_time=last_write_time,
                interval_seconds=interval_seconds,
                columns_by_name=columns,
                header_signature=f"{info_header}|{column_header}",
            )
        )

    if archive_root_dir and path.isdir(archive_root_dir):
        for folder_name in sorted(listdir(archive_root_dir)):
            folder_match = ARCHIVE_FOLDER_RE.match(folder_name)
            if not folder_match:
                continue
            folder_path = path.join(archive_root_dir, folder_name)
            if not path.isdir(folder_path):
                continue
            folder_time = datetime.strptime(folder_match.group(1), "%Y%m%d%H%M%S")
            for table_name in sorted(listdir(folder_path)):
                add_file(
                    file_path=path.join(folder_path, table_name),
                    table_name=table_name,
                    origin=folder_name,
                    order_key=(0, folder_time, table_name),
                )

    if actual_dir and path.isdir(actual_dir):
        for table_name in sorted(listdir(actual_dir)):
            add_file(
                file_path=path.join(actual_dir, table_name),
                table_name=table_name,
                origin="current",
                order_key=(1, table_name),
            )

    grouped: dict[str, list[CsvFileInfo]] = {}
    for file_info in sorted(discovered, key=lambda item: item.order_key):
        grouped.setdefault(file_info.table_name, []).append(file_info)
    return grouped


def load_hast_sensor_config(driver_file_path: str) -> dict[str, list[CsvSensorConfig]]:
    """
    Laedt den heutigen HAST-Driver und normalisiert ihn auf eine klare Struktur.

    Der bestehende Driver bleibt damit die fachliche Quelle fuer HAST-Sensoren.
    Die neue CSV-Synchronisierung greift jedoch nicht mehr ueber TinyDB darauf zu,
    sondern ueber ein statisches, transparentes JSON-Mapping.
    """
    with open(driver_file_path, "r", encoding="utf-8") as file_handle:
        raw_driver = json.load(file_handle)

    result: dict[str, list[CsvSensorConfig]] = {}
    for driver_section, entries in raw_driver.items():
        sensor_configs: list[CsvSensorConfig] = []
        for raw_sensor in entries.values():
            sensor_configs.append(
                CsvSensorConfig(
                    sensor_id=str(raw_sensor["id"]),
                    name_dp=raw_sensor.get("name_dp"),
                    dp=int(raw_sensor["dp"]) if raw_sensor.get("dp") is not None else None,
                    gt=_parse_optional_float(raw_sensor.get("gt")),
                    lt=_parse_optional_float(raw_sensor.get("lt")),
                    b1=_parse_optional_float(raw_sensor.get("b1")),
                    b2=_parse_optional_float(raw_sensor.get("b2")),
                    sensor_type=raw_sensor.get("type"),
                )
            )
        result[driver_section] = sensor_configs
    return result


def load_special_asset_config(config_file_path: str | None) -> list[dict[str, Any]]:
    """
    Laedt die lokale Sonderasset-Konfiguration.

    Die Datei darf entweder direkt eine Liste von Targets enthalten oder ein
    Objekt mit dem Schluessel `targets`.
    """
    if not config_file_path or not path.isfile(config_file_path):
        return []
    with open(config_file_path, "r", encoding="utf-8") as file_handle:
        raw = json.load(file_handle)
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        targets = raw.get("targets", [])
        return targets if isinstance(targets, list) else []
    return []


def _detect_hast_driver_section(file_info: CsvFileInfo) -> str | None:
    """Ermittelt anhand des Schneid-Headers den passenden HAST-Driver-Bereich."""
    station_type = file_info.station_type or ""
    table_id = file_info.table_id or ""
    if "UG06" in station_type:
        return DRIVER_SECTION_BY_STATION_KEY["ug06"]
    if "UG08" in station_type:
        return DRIVER_SECTION_BY_STATION_KEY["ug08"]
    if "UG10" in station_type:
        return DRIVER_SECTION_BY_STATION_KEY["ug10"]
    if "UG12" in station_type:
        return DRIVER_SECTION_BY_STATION_KEY["ug12"]
    if "ACT_WMZ" in station_type and "GEN_MBUS" in table_id:
        return DRIVER_SECTION_BY_STATION_KEY["only_mbus_wz_as_hast"]
    return None


def _extract_controller_id_from_table(table_name: str) -> int | None:
    """Leitet die Regler-ID aus dem standardisierten Schneid-Dateinamen ab."""
    for regex in (HAST_TABLE_RE, HAST_WMZ_TABLE_RE):
        match = regex.match(table_name)
        if match:
            return int(match.group("controller_id"))
    return None


def _select_matching_hast_sensors(
    file_info: CsvFileInfo,
    sensor_configs: list[CsvSensorConfig],
) -> list[CsvSensorConfig]:
    """
    Waehlt aus dem Driver nur die Sensoren, die exakt zur aktuellen CSV-Struktur passen.

    Besonderheit:
    Das Matching prueft bewusst sowohl `name_dp` als auch die DP-Position. Damit
    wird verhindert, dass ein Sensor bei Stationswechseln versehentlich auf alte
    Spalten derselben Datei gemappt wird.
    """
    selected: list[CsvSensorConfig] = []
    for sensor in sensor_configs:
        if sensor.name_dp is None or sensor.dp is None:
            continue
        column = file_info.columns_by_name.get(sensor.name_dp)
        if not column:
            continue
        if int(column["position"]) != int(sensor.dp):
            continue
        selected.append(sensor)
    return selected


def _resolve_special_sensor_configs(
    config_entry: dict[str, Any],
    file_info: CsvFileInfo,
) -> list[CsvSensorConfig]:
    """Normalisiert die Sensordefinitionen lokaler Sonderassets."""
    result: list[CsvSensorConfig] = []
    for raw_sensor in config_entry.get("sensors", []):
        name_dp = raw_sensor.get("name_dp")
        dp = raw_sensor.get("dp")
        if dp is None and name_dp and name_dp in file_info.columns_by_name:
            dp = int(file_info.columns_by_name[name_dp]["position"])
        elif dp is not None:
            dp = int(dp)
        if name_dp is None and dp is not None:
            for column_name, column_info in file_info.columns_by_name.items():
                if int(column_info["position"]) == dp:
                    name_dp = column_name
                    break
        if dp is None:
            continue
        result.append(
            CsvSensorConfig(
                sensor_id=str(raw_sensor["sensor_id"]),
                name_dp=name_dp,
                dp=dp,
                gt=_parse_optional_float(raw_sensor.get("gt")),
                lt=_parse_optional_float(raw_sensor.get("lt")),
                b1=_parse_optional_float(raw_sensor.get("b1")),
                b2=_parse_optional_float(raw_sensor.get("b2")),
                sensor_type=raw_sensor.get("type"),
            )
        )
    return result


def _build_mapping_start_time(
    table_sources: list[CsvFileInfo],
    latest_table_id: str,
    default_start_time: datetime,
) -> datetime:
    """
    Bestimmt den fruehesten Zeitpunkt, ab dem das aktuelle Mapping rueckwirkend gilt.

    Die Regel ist bewusst einfach:
    Es wird nur die zusammenhaengende Kette der neuesten Dateien mit gleicher
    Tabellen-ID betrachtet. Sobald eine aeltere Datei eine andere Tabellen-ID
    traegt, endet die Ruecksynchronisierung neuer Sensoren an diesem Punkt. 
    Hinweis: in der Tabellen ID ist der Regler z.B. UG08 hinterlegt, wodurch
    ein Reglerwechsel dadurch abgebildet ist.
    """
    if not table_sources:
        return default_start_time

    boundary = default_start_time
    for index in range(len(table_sources) - 1, -1, -1):
        file_info = table_sources[index]
        if file_info.table_id == latest_table_id:
            continue
        if file_info.last_write_time is not None:
            boundary = max(boundary, file_info.last_write_time)
        break
    return boundary


def build_csv_targets(
    sources_by_table: dict[str, list[CsvFileInfo]],
    stations_nao: dict,
    hast_asset_id: str,
    hast_sensor_config: dict[str, list[CsvSensorConfig]],
    special_asset_config: list[dict[str, Any]],
    default_start_time: datetime,
) -> tuple[list[CsvTargetConfig], list[str]]:
    """
    Baut aus HAST-Driver, Sonderasset-Config und erkannter Dateistruktur die Sync-Ziele.

    Rueckgabe:
    - Liste aller synchronisierbaren Ziele
    - Tabellen, die zwar gelesen wurden, aber keinem Mapping zugeordnet sind
    """
    stations_normalized = {}
    for controller_id, station_data in stations_nao.items():
        try:
            stations_normalized[int(controller_id)] = station_data
        except (TypeError, ValueError):
            continue

    targets: list[CsvTargetConfig] = []
    consumed_tables: set[str] = set()

    for table_name, table_sources in sources_by_table.items():
        latest_file = table_sources[-1]
        controller_id = _extract_controller_id_from_table(table_name)
        driver_section = _detect_hast_driver_section(latest_file)
        if controller_id is None or driver_section is None:
            continue
        station_data = stations_normalized.get(controller_id)
        if not station_data:
            continue
        sensors = _select_matching_hast_sensors(latest_file, hast_sensor_config.get(driver_section, []))
        if not sensors:
            continue
        targets.append(
            CsvTargetConfig(
                target_key=f"hast:{controller_id}:{table_name}",
                source_type="hast",
                logical_key=str(controller_id),
                table_name=table_name,
                asset_id=hast_asset_id,
                instance_id=station_data["Instance-ID"],
                sensors=sensors,
                driver_section=driver_section,
                mapping_start_time=_build_mapping_start_time(
                    table_sources=table_sources,
                    latest_table_id=latest_file.table_id,
                    default_start_time=default_start_time,
                ),
            )
        )
        consumed_tables.add(table_name)

    for raw_target in special_asset_config:
        table_name = raw_target.get("table")
        if not isinstance(table_name, str) or table_name not in sources_by_table:
            continue
        latest_file = sources_by_table[table_name][-1]
        sensors = _resolve_special_sensor_configs(raw_target, latest_file)
        if not sensors:
            continue
        logical_key = str(raw_target.get("key") or f"{table_name}:{raw_target.get('instance_id')}")
        targets.append(
            CsvTargetConfig(
                target_key=f"special:{logical_key}",
                source_type="special",
                logical_key=logical_key,
                table_name=table_name,
                asset_id=str(raw_target["asset_id"]),
                instance_id=str(raw_target["instance_id"]),
                sensors=sensors,
                driver_section=None,
                mapping_start_time=_build_mapping_start_time(
                    table_sources=sources_by_table[table_name],
                    latest_table_id=latest_file.table_id,
                    default_start_time=default_start_time,
                ),
            )
        )
        consumed_tables.add(table_name)

    ignored_tables = sorted(table_name for table_name in sources_by_table if table_name not in consumed_tables)
    return targets, ignored_tables


class CsvSyncStateStore:
    """
    Verwaltet den neuen technischen CSV-Synchronisationsstand in SQLite.

    Warum SQLite:
    - robuster als die alte JSON-Struktur
    - gezielte Updates pro Sensor moeglich
    - besser geeignet fuer spaetere Migrationen oder Auswertungen
    """

    def __init__(self, sqlite_file_path: str) -> None:
        self.sqlite_file_path = sqlite_file_path
        self.connection = sqlite3.connect(sqlite_file_path)
        self.connection.row_factory = sqlite3.Row
        self._initialize()

    def close(self) -> None:
        """Schliesst die geoeffnete SQLite-Verbindung."""
        self.connection.close()

    def _initialize(self) -> None:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_info (
                version INTEGER NOT NULL
            );
            """
        )
        cursor.execute("SELECT COUNT(*) FROM schema_info;")
        if cursor.fetchone()[0] == 0:
            cursor.execute("INSERT INTO schema_info(version) VALUES (?);", (SQLITE_SCHEMA_VERSION,))

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS target_state (
                target_key TEXT PRIMARY KEY,
                source_type TEXT NOT NULL,
                logical_key TEXT NOT NULL,
                table_name TEXT NOT NULL,
                last_header_signature TEXT,
                new_sensor_start_time TEXT,
                updated_at TEXT NOT NULL
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS sensor_state (
                target_key TEXT NOT NULL,
                sensor_id TEXT NOT NULL,
                dp INTEGER,
                name_dp TEXT,
                first_synced_at TEXT,
                last_synced_at TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (target_key, sensor_id)
            );
            """
        )
        self.connection.commit()

    def count_sensor_states(self) -> int:
        """Gibt die Anzahl bereits gespeicherter Sensorsync-Zustaende zurueck."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM sensor_state;")
        return int(cursor.fetchone()[0])

    def migrate_legacy_json(self, legacy_file_path: str | None) -> int:
        """
        Uebernimmt den alten JSON-Sync-Stand einmalig in SQLite.

        Die Migration laeuft nur dann, wenn die neue SQLite noch leer ist. Damit
        wird verhindert, dass vorhandene neue Daten versehentlich ueberschrieben
        werden.
        """
        if not legacy_file_path or not path.isfile(legacy_file_path):
            return 0
        if self.count_sensor_states() > 0:
            return 0

        with open(legacy_file_path, "r", encoding="utf-8") as file_handle:
            legacy = json.load(file_handle)

        migrated = 0
        for workspace_entries in legacy.values():
            if not isinstance(workspace_entries, dict):
                continue
            for controller_id, entry in workspace_entries.items():
                table_name = entry.get("table")
                synced_at = _parse_datetime(entry.get("time_sincronizied"))
                if not table_name or synced_at is None:
                    continue
                target_key = f"hast:{controller_id}:{table_name}"
                self.ensure_target_state(
                    target_key=target_key,
                    source_type="hast",
                    logical_key=str(controller_id),
                    table_name=table_name,
                    latest_header_signature=None,
                    default_new_sensor_start_time=None,
                )
                for sensor_mapping in entry.get("syncronizied", []):
                    if not isinstance(sensor_mapping, dict) or not sensor_mapping:
                        continue
                    dp, sensor_id = next(iter(sensor_mapping.items()))
                    self.upsert_sensor_state(
                        target_key=target_key,
                        sensor_id=str(sensor_id),
                        dp=int(dp),
                        name_dp=None,
                        first_synced_at=synced_at,
                        last_synced_at=synced_at,
                    )
                    migrated += 1
        return migrated

    def ensure_target_state(
        self,
        target_key: str,
        source_type: str,
        logical_key: str,
        table_name: str,
        latest_header_signature: str | None,
        default_new_sensor_start_time: datetime | None,
    ) -> sqlite3.Row:
        """
        Legt einen Target-Zustand an oder aktualisiert ihn auf die aktuelle Header-Signatur.

        Wenn sich die Header-Signatur geaendert hat, wird fuer spaeter neu erkannte
        Sensoren ein neuer Ruecksynchronisierungsstart gesetzt.
        """
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT * FROM target_state WHERE target_key = ?;",
            (target_key,),
        )
        row = cursor.fetchone()
        now = _to_iso(datetime.utcnow()) or ""

        if row is None:
            cursor.execute(
                """
                INSERT INTO target_state(
                    target_key, source_type, logical_key, table_name,
                    last_header_signature, new_sensor_start_time, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    target_key,
                    source_type,
                    logical_key,
                    table_name,
                    latest_header_signature,
                    _to_iso(default_new_sensor_start_time),
                    now,
                ),
            )
            self.connection.commit()
            cursor.execute("SELECT * FROM target_state WHERE target_key = ?;", (target_key,))
            return cursor.fetchone()

        changed_header = (
            latest_header_signature is not None
            and row["last_header_signature"]
            and row["last_header_signature"] != latest_header_signature
        )
        if changed_header:
            max_last_synced = self.get_target_max_last_synced_at(target_key)
            if default_new_sensor_start_time is not None and max_last_synced is not None:
                new_sensor_start_time = max(default_new_sensor_start_time, max_last_synced)
            else:
                new_sensor_start_time = default_new_sensor_start_time or max_last_synced
        else:
            current_new_sensor_start = _parse_datetime(row["new_sensor_start_time"])
            if default_new_sensor_start_time is not None and current_new_sensor_start is not None:
                new_sensor_start_time = max(current_new_sensor_start, default_new_sensor_start_time)
            else:
                new_sensor_start_time = current_new_sensor_start or default_new_sensor_start_time

        cursor.execute(
            """
            UPDATE target_state
            SET source_type = ?,
                logical_key = ?,
                table_name = ?,
                last_header_signature = ?,
                new_sensor_start_time = ?,
                updated_at = ?
            WHERE target_key = ?;
            """,
            (
                source_type,
                logical_key,
                table_name,
                latest_header_signature,
                _to_iso(new_sensor_start_time),
                now,
                target_key,
            ),
        )
        self.connection.commit()
        cursor.execute("SELECT * FROM target_state WHERE target_key = ?;", (target_key,))
        return cursor.fetchone()

    def get_target_max_last_synced_at(self, target_key: str) -> datetime | None:
        """Liefert den neuesten bisher synchronisierten Zeitpunkt eines Targets."""
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT MAX(last_synced_at) AS last_synced_at FROM sensor_state WHERE target_key = ?;",
            (target_key,),
        )
        row = cursor.fetchone()
        return _parse_datetime(row["last_synced_at"]) if row and row["last_synced_at"] else None

    def get_target_new_sensor_start_time(self, target_key: str) -> datetime | None:
        """Liest den aktuell geltenden Ruecksynchronisierungsstart fuer neue Sensoren."""
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT new_sensor_start_time FROM target_state WHERE target_key = ?;",
            (target_key,),
        )
        row = cursor.fetchone()
        return _parse_datetime(row["new_sensor_start_time"]) if row else None

    def get_sensor_state(self, target_key: str, sensor_id: str) -> sqlite3.Row | None:
        """Liest den Zustand eines einzelnen Sensors."""
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT * FROM sensor_state WHERE target_key = ? AND sensor_id = ?;",
            (target_key, sensor_id),
        )
        return cursor.fetchone()

    def upsert_sensor_state(
        self,
        target_key: str,
        sensor_id: str,
        dp: int | None,
        name_dp: str | None,
        first_synced_at: datetime | None,
        last_synced_at: datetime | None,
    ) -> None:
        """Schreibt oder aktualisiert den Zustand eines einzelnen Sensors."""
        existing = self.get_sensor_state(target_key, sensor_id)
        now = _to_iso(datetime.utcnow()) or ""
        if existing is None:
            self.connection.execute(
                """
                INSERT INTO sensor_state(
                    target_key, sensor_id, dp, name_dp, first_synced_at, last_synced_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    target_key,
                    sensor_id,
                    dp,
                    name_dp,
                    _to_iso(first_synced_at),
                    _to_iso(last_synced_at),
                    now,
                ),
            )
        else:
            current_first = _parse_datetime(existing["first_synced_at"])
            current_last = _parse_datetime(existing["last_synced_at"])
            if current_first is not None and first_synced_at is not None:
                first_synced_at = min(current_first, first_synced_at)
            else:
                first_synced_at = current_first or first_synced_at
            if current_last is not None and last_synced_at is not None:
                last_synced_at = max(current_last, last_synced_at)
            else:
                last_synced_at = current_last or last_synced_at
            self.connection.execute(
                """
                UPDATE sensor_state
                SET dp = ?, name_dp = ?, first_synced_at = ?, last_synced_at = ?, updated_at = ?
                WHERE target_key = ? AND sensor_id = ?;
                """,
                (
                    dp,
                    name_dp,
                    _to_iso(first_synced_at),
                    _to_iso(last_synced_at),
                    now,
                    target_key,
                    sensor_id,
                ),
            )
        self.connection.commit()


def _coerce_numeric_value(value: Any) -> float | None:
    """Wandelt einen CSV-Rohwert verlustarm in einen sendbaren numerischen Wert um."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        value_float = float(value)
        if isnan(value_float):
            return None
        return value_float
    text = str(value).strip().replace(",", ".")
    if text == "":
        return None
    try:
        value_float = float(text)
    except ValueError:
        return None
    if isnan(value_float):
        return None
    return value_float


def _sensor_is_syncable(sample_dataframe: pd.DataFrame, sensor_config: CsvSensorConfig) -> bool:
    """
    Entscheidet, ob ein bisher unbekannter Sensor ueberhaupt synchronisiert werden darf.

    Schneid-Besonderheit:
    Diese Pruefung ist bewusst *keine* laufende Messwertvalidierung. Sie dient nur
    der initialen Freischaltung des Sensors anhand des bisherigen Drivers.
    """
    if sensor_config.dp is None or sensor_config.dp not in sample_dataframe.columns:
        return False
    series = pd.to_numeric(sample_dataframe[sensor_config.dp], errors="coerce")
    series = series.mask(series == series.shift(1)).dropna().reset_index(drop=True)
    if series.empty:
        return False

    mask = pd.Series([True] * len(series))
    if sensor_config.lt is not None:
        mask = series.lt(sensor_config.lt) & mask
    if sensor_config.gt is not None:
        mask = series.gt(sensor_config.gt) & mask
    if sensor_config.b1 is not None and sensor_config.b2 is not None:
        mask = (series.lt(sensor_config.b1) | series.gt(sensor_config.b2)) & mask
    return bool(mask.any())


def _build_telegraf_frames(
    dataframe: pd.DataFrame,
    sensor_config: CsvSensorConfig,
    target: CsvTargetConfig,
    wrong_units: dict[str, dict[str, float]],
) -> tuple[list[str], int, datetime | None]:
    """
    Formatiert die Zeitreihe eines Sensors in Telegraf-Zeilen.

    Es werden ausschliesslich Rohdaten uebertragen. Leere Werte werden nicht
    gesendet, weil Telegraf dafuer keinen sinnvollen Zahlenwert hat.
    """
    if sensor_config.dp is None or sensor_config.dp not in dataframe.columns:
        return [], 0, None

    payload: list[str] = []
    empty_values_skipped = 0
    last_seen_time = None
    series = dataframe[sensor_config.dp]

    for timestamp, raw_value in series.items():
        last_seen_time = timestamp
        numeric_value = _coerce_numeric_value(raw_value)
        if numeric_value is None:
            empty_values_skipped += 1
            continue
        if target.instance_id in wrong_units and sensor_config.sensor_id in wrong_units[target.instance_id]:
            numeric_value = numeric_value * wrong_units[target.instance_id][sensor_config.sensor_id]
        payload.append(
            f"{target.asset_id},instance={target.instance_id} "
            f"{sensor_config.sensor_id}={numeric_value} {_to_utc_nanoseconds(timestamp)}"
        )
    return payload, empty_values_skipped, last_seen_time


def _read_target_timeframe(
    file_info: CsvFileInfo,
    data_points: list[int],
    start_time: datetime,
    default_interval_seconds: int,
) -> pd.DataFrame:
    """
    Liest den fuer den aktuellen Sync wirklich noetigen Ausschnitt einer CSV-Datei.

    Wenn der Zeitraum klein ist, wird wie im Altcode nur der hintere Bereich
    gelesen. Bei groesseren Zeitraeumen oder unsicherem Intervall wird die Datei
    vollstaendig geladen.
    """
    if file_info.last_write_time is not None and file_info.last_write_time <= start_time:
        return pd.DataFrame(columns=data_points)

    interval_seconds = file_info.interval_seconds or default_interval_seconds
    lines_to_read: int | str = "all"
    if file_info.last_write_time is not None and interval_seconds > 0:
        delta_seconds = max(0, int((file_info.last_write_time - start_time).total_seconds()))
        line_count = int(delta_seconds / interval_seconds) + 5
        if line_count <= 500:
            lines_to_read = max(line_count, 10)

    dataframe = _read_csv_dataframe(file_info.full_path, lines=lines_to_read)
    missing_columns = [dp for dp in data_points if dp not in dataframe.columns]
    for dp in missing_columns:
        dataframe[dp] = pd.NA
    dataframe = dataframe[data_points]
    return dataframe[dataframe.index > start_time]


def sync_csv_series_from_schneid(
    nao_connect: Any,
    stations_nao: dict,
    hast_asset_id: str,
    driver_file_path: str,
    sqlite_file_path: str,
    actual_csv_dir: str | None,
    archive_root_dir: str | None = None,
    special_asset_config_path: str | None = None,
    legacy_sync_status_path: str | None = None,
    wrong_units: dict[str, dict[str, float]] | None = None,
    default_start_time: datetime = datetime(2010, 1, 1),
) -> CsvSyncSummary:
    """
    Fuehrt den neuen CSV-Zeitreihensync fuer Schneid/Winmiocs11 aus.

    Ablauf in Kurzform:
    1. CSV-Dateien aus aktuellem Bestand und Archiv erkennen
    2. HAST-Targets aus `driver_schneid.json` und Sondertargets aus lokaler Config bauen
    3. SQLite-Sync-Stand initialisieren bzw. einmalig aus altem JSON migrieren
    4. unbekannte Sensoren ueber die alte Grenzlogik freischalten
    5. Rohdaten inkrementell im Telegraf-Format an NAO senden
    """
    summary = CsvSyncSummary()
    wrong_units = wrong_units or {}

    hast_sensor_config = load_hast_sensor_config(driver_file_path)
    special_asset_config = load_special_asset_config(special_asset_config_path)
    sources_by_table = discover_csv_files(actual_dir=actual_csv_dir, archive_root_dir=archive_root_dir)
    targets, ignored_tables = build_csv_targets(
        sources_by_table=sources_by_table,
        stations_nao=stations_nao,
        hast_asset_id=hast_asset_id,
        hast_sensor_config=hast_sensor_config,
        special_asset_config=special_asset_config,
        default_start_time=default_start_time,
    )
    summary.ignored_tables_without_mapping = len(ignored_tables)
    summary.ignored_tables = ignored_tables

    store = CsvSyncStateStore(sqlite_file_path)
    try:
        summary.migrated_sensors = store.migrate_legacy_json(legacy_sync_status_path)

        for target in targets:
            summary.checked_targets += 1
            table_sources = sources_by_table.get(target.table_name, [])
            if not table_sources:
                summary.skipped_targets += 1
                continue

            latest_file = table_sources[-1]
            existing_target_row = store.ensure_target_state(
                target_key=target.target_key,
                source_type=target.source_type,
                logical_key=target.logical_key,
                table_name=target.table_name,
                latest_header_signature=None,
                default_new_sensor_start_time=target.mapping_start_time,
            )
            previous_header_signature = existing_target_row["last_header_signature"]
            store.ensure_target_state(
                target_key=target.target_key,
                source_type=target.source_type,
                logical_key=target.logical_key,
                table_name=target.table_name,
                latest_header_signature=latest_file.header_signature,
                default_new_sensor_start_time=target.mapping_start_time,
            )
            if previous_header_signature and previous_header_signature != latest_file.header_signature:
                summary.changed_headers += 1

            sample_dataframe = None
            # effective_new_sensor_start = store.get_target_new_sensor_start_time(target.target_key) or target.mapping_start_time or default_start_time
            ## ob mappeng start time überhaupt verwendet werden muss, wird noch geprüft, daher erst mal auskommentiert.
            effective_new_sensor_start = store.get_target_new_sensor_start_time(target.target_key) or default_start_time
            active_sensors: list[tuple[CsvSensorConfig, sqlite3.Row | None, datetime]] = []

            for sensor_config in target.sensors:
                sensor_state = store.get_sensor_state(target.target_key, sensor_config.sensor_id)
                if sensor_state is None:
                    if sample_dataframe is None:
                        sample_dataframe = _read_csv_dataframe(latest_file.full_path, lines=DEFAULT_SAMPLE_LINES)
                    if not _sensor_is_syncable(sample_dataframe, sensor_config):
                        continue
                    summary.recognized_sensors += 1
                    start_time = effective_new_sensor_start
                else:
                    start_time = _parse_datetime(sensor_state["last_synced_at"]) or effective_new_sensor_start
                active_sensors.append((sensor_config, sensor_state, start_time))

            if not active_sensors:
                summary.skipped_targets += 1
                continue

            data_points = sorted(
                {
                    sensor_config.dp
                    for sensor_config, _sensor_state, _start_time in active_sensors
                    if sensor_config.dp is not None
                }
            )
            if not data_points:
                summary.skipped_targets += 1
                continue

            for file_info in table_sources:
                earliest_needed_start = min(start_time for _sensor_config, _sensor_state, start_time in active_sensors)
                dataframe = _read_target_timeframe(
                    file_info=file_info,
                    data_points=data_points,
                    start_time=earliest_needed_start,
                    default_interval_seconds=DEFAULT_INTERVAL_SECONDS,
                )
                if dataframe.empty:
                    continue

                payload_for_file: list[str] = []
                sensor_progress: dict[str, tuple[CsvSensorConfig, datetime, datetime | None]] = {}

                for index, (sensor_config, _sensor_state, start_time) in enumerate(active_sensors):
                    sensor_frame = dataframe[dataframe.index > start_time]
                    if sensor_frame.empty:
                        continue

                    sensor_payload, empty_values_skipped, last_seen_time = _build_telegraf_frames(
                        dataframe=sensor_frame,
                        sensor_config=sensor_config,
                        target=target,
                        wrong_units=wrong_units,
                    )
                    summary.empty_values_skipped += empty_values_skipped
                    payload_for_file.extend(sensor_payload)
                    if last_seen_time is not None:
                        sensor_progress[sensor_config.sensor_id] = (sensor_config, start_time, last_seen_time)
                        active_sensors[index] = (sensor_config, None, last_seen_time)

                if payload_for_file:
                    status = nao_connect.sendTelegrafData(
                        payload=payload_for_file,
                        values_count=len(payload_for_file),
                    )
                    if status != 204:
                        continue
                    summary.sent_values += len(payload_for_file)

                for sensor_id, (sensor_config, start_time, last_seen_time) in sensor_progress.items():
                    if last_seen_time is None:
                        continue
                    store.upsert_sensor_state(
                        target_key=target.target_key,
                        sensor_id=sensor_id,
                        dp=sensor_config.dp,
                        name_dp=sensor_config.name_dp,
                        first_synced_at=start_time,
                        last_synced_at=last_seen_time,
                    )
                summary.synced_sensors += len(sensor_progress)

        return summary
    finally:
        store.close()
