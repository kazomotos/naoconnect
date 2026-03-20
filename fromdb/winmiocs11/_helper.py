from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import re
from typing import Any, Iterable
from urllib.request import urlopen

import pandas as pd

from naoconnect.fromdb.SchneidWinmiocs70 import ScheindPostgresWinmiocs70
from naoconnect.naoappV2 import NaoApp


META_SYNC_INFO_DEFAULT = {
    "version": 1,
    "meter_series_sync": {},
    "notes": {},
}

NOTE_DATE_PATTERNS = [
    "%d.%m.%Y %H:%M Uhr",
    "%d.%m.%Y %H:%M",
    "%d.%m.%y %H:%M Uhr",
    "%d.%m.%y %H:%M",
    "%d-%m-%Y %H:%M Uhr",
    "%d-%m-%Y %H:%M",
    "%d-%m-%y %H:%M Uhr",
    "%d-%m-%y %H:%M",
    "%Y.%m.%d %H:%M Uhr",
    "%Y.%m.%d %H:%M",
    "%Y-%m-%d %H:%M",
    "%d.%m.%Y",
    "%d.%m.%y",
    "%d-%m-%Y",
    "%d-%m-%y",
    "%Y.%m.%d",
    "%Y-%m-%d",
]

NOTE_PREFIX_RE = re.compile(
    r"^WinMiocs Notiz:\s*(?P<folder>[^\n\\]+)(?:\\(?P<filename>[^\n]+))?",
    re.IGNORECASE,
)
NOTE_DATETIME_RE = re.compile(
    r"^\s*(?P<date>"
    r"(?:\d{1,4}[.,-]\d{1,2}[.,-]\d{1,4})(?:\s+\d{1,2}:\d{2}(?:\s*Uhr)?)?"
    r")(?P<rest>.*)$"
)


@dataclass
class SyncSummary:
    """Kompakte Rückgabe für die Synchronisierung in `main.py`."""

    checked_controller_ids: int = 0
    changed_items: int = 0
    synced_items: int = 0
    skipped_controller_ids: int = 0


@dataclass
class StationCreationSummary:
    """Rückgabe für das automatische Anlegen fehlender Stationen in NAO."""

    checked_controller_ids: int = 0
    created_instances: int = 0
    skipped_existing_instances: int = 0
    skipped_missing_required_data: int = 0
    created_controller_ids: list[int] | None = None

    def __post_init__(self) -> None:
        if self.created_controller_ids is None:
            self.created_controller_ids = []


def load_meta_sync_info(file_path: str) -> dict:
    """
    Lädt den lokalen Synchronisierungsstand aus `MetaSincInfo.json`.

    Die Datei ist bewusst fehlertolerant gehalten. Falls sie nicht existiert
    oder unvollständig ist, wird eine Standardstruktur zurückgegeben.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            result = json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        result = {}

    meta_sync_info = deepcopy(META_SYNC_INFO_DEFAULT)
    if isinstance(result, dict):
        meta_sync_info.update(result)
    if not isinstance(meta_sync_info.get("meter_series_sync"), dict):
        meta_sync_info["meter_series_sync"] = {}
    if not isinstance(meta_sync_info.get("notes"), dict):
        meta_sync_info["notes"] = {}
    return meta_sync_info


def save_meta_sync_info(file_path: str, meta_sync_info: dict) -> None:
    """Schreibt den lokalen Synchronisierungsstand formatiert in JSON."""
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(meta_sync_info, file, indent=2, ensure_ascii=False, sort_keys=True)


def read_stations_from_api(api_ids: str, hast_asset_attributes: dict) -> dict:
    """
    Liest die aktuelle Stationskonfiguration aus der NAO-Metadaten-API.

    Rückgabeformat:
    - Schlüssel: Regler-ID
    - Wert: Dictionary mit `Instance-ID` sowie den aktuell in NAO vorhandenen
      Metadatenwerten und den zugehörigen `NAO-ID-...`-Referenzen.

    Die Funktion ist bewusst generisch gehalten und bekommt daher die
    Attribut-Beschreibung aus `main.py` übergeben.

    Initiale Sonderlogik für frisch angelegte Stationen:
    Falls das Metadatum `Regler-ID` in NAO noch nicht gesetzt ist, wird einmalig
    geprüft, ob sich der Instanzname als Integer interpretieren lässt. Ist das
    der Fall, wird dieser Wert vorübergehend als Regler-ID verwendet, damit die
    Station direkt nach dem Anlegen schon der Postgres-Zuordnung zugeordnet
    werden kann. Im anschließenden Metadaten-Sync wird `Regler-ID` trotzdem
    regulär nach NAO geschrieben. Sobald dieses Metadatum vorhanden ist, ist es
    wieder die alleinige Quelle der Zuordnung; spätere Namensänderungen sind
    dann ohne Einfluss.
    """
    stations_nao: dict = {}
    with urlopen(api_ids) as response:
        meta_nao_data = json.loads(response.read())["results"]

    for dat in meta_nao_data:
        controller_id = _normalize_controller_id(dat.get("Regler-ID"))
        if controller_id is None:
            controller_id = _normalize_controller_id(dat.get("name"))
        if controller_id is None:
            continue

        result = {"Instance-ID": dat["Instance-ID"]}
        for key in hast_asset_attributes:
            attr_name = hast_asset_attributes[key]["name"]
            nao_id_key = f"NAO-ID-{attr_name}"
            if attr_name in dat or nao_id_key in dat:
                result[attr_name] = {
                    "value": dat.get(attr_name),
                    "id": dat.get(nao_id_key),
                }
        stations_nao[controller_id] = result

    return stations_nao


def _normalize_controller_id(value: Any) -> int | None:
    """Normalisiert Regler-IDs aus API/DB auf `int`."""
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _normalize_stations_nao(stations_nao: dict) -> dict[int, dict]:
    """Normalisiert das Stations-Mapping aus der API auf Integer-Regler-IDs."""
    normalized = {}
    for controller_id, station_data in stations_nao.items():
        controller_id_int = _normalize_controller_id(controller_id)
        if controller_id_int is None:
            continue
        normalized[controller_id_int] = station_data
    return normalized


def _to_iso_z(value: datetime | None) -> str | None:
    """Formatiert Zeitpunkte konsistent wie in den bisherigen NAO-Beispielen."""
    if value is None:
        return None
    return value.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _parse_nao_iso_datetime(value: str | None) -> datetime | None:
    """Parst die von NAO gelieferten ISO-Zeitstempel robust."""
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).replace(tzinfo=None)
    except ValueError:
        return None


def _normalize_scalar(value: Any) -> Any:
    """Normalisiert einfache Metadatenwerte für stabile Vergleiche."""
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        return value if value != "" else None
    return value


def _parse_serial_to_int(value: Any) -> int | None:
    """
    Wandelt Zählernummern verlustfrei in `int` um.

    Die Funktion akzeptiert numerische Strings, echte Zahlen und typische
    PostgreSQL-Rückgabewerte. Wissenschaftliche Notation wird bewusst nicht
    erzeugt, da diese später im Telegraf-Frame vermieden werden muss.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if text == "":
        return None
    if re.fullmatch(r"[+-]?\d+", text):
        return int(text)
    try:
        return int(float(text))
    except ValueError:
        return None


def _get_attr_value(attr_value: Any, key: str) -> Any:
    """Liest einen Schlüssel aus `jsonb`-Attributen der Postgres-Datenbank."""
    if isinstance(attr_value, dict):
        return attr_value.get(key)
    if isinstance(attr_value, str):
        try:
            parsed = json.loads(attr_value)
        except json.JSONDecodeError:
            return None
        if isinstance(parsed, dict):
            return parsed.get(key)
    return None


def _fetch_all(postgres: ScheindPostgresWinmiocs70, query: str, params: Iterable[Any] | None = None) -> list[tuple]:
    """Führt einen lesenden Query gegen Postgres aus und liefert alle Zeilen zurück."""
    postgres.connectToDb()
    try:
        cursor = postgres.conn.cursor()
        cursor.execute(query, tuple(params or ()))
        rows = cursor.fetchall()
        cursor.close()
        return rows
    finally:
        postgres.disconnectToDb()


def _fetch_partner_rows(postgres: ScheindPostgresWinmiocs70) -> list[tuple]:
    """Lädt die Stamm-Metadaten aus `siocs.partner`."""
    return _fetch_all(
        postgres,
        """
        SELECT id, name, address, attr
        FROM siocs.partner
        ORDER BY id;
        """,
    )


def _fetch_partner_creation_rows(postgres: ScheindPostgresWinmiocs70) -> list[tuple]:
    """
    Lädt die minimal nötigen Stammdaten für das automatische Anlegen neuer
    Stationen in NAO.
    """
    return _fetch_all(
        postgres,
        """
        SELECT id, name, attr
        FROM siocs.partner
        ORDER BY id;
        """,
    )


def _fetch_latest_cntswap_rows(postgres: ScheindPostgresWinmiocs70) -> list[tuple]:
    """Lädt je Regler den letzten bekannten Zählerwechsel aus `winmiocs.cntswap`."""
    return _fetch_all(
        postgres,
        """
        SELECT node, time, ser, ser_new
        FROM (
            SELECT
                node,
                time,
                ser,
                ser_new,
                ROW_NUMBER() OVER (
                    PARTITION BY node
                    ORDER BY time DESC, ser_new DESC NULLS LAST, ser DESC NULLS LAST
                ) AS rn
            FROM winmiocs.cntswap
            WHERE node IS NOT NULL
        ) ranked
        WHERE rn = 1
        ORDER BY node;
        """,
    )


def _fetch_lognote_rows(postgres: ScheindPostgresWinmiocs70) -> list[tuple]:
    """
    Lädt die Notizquellen aus `siocs.lognote` samt Gerätezuweisung aus
    `siocs.logbook.param.device`.
    """
    return _fetch_all(
        postgres,
        """
        SELECT
            lb.param ->> 'device' AS controller_id,
            ln.log_id,
            ln.text,
            ln.loginname,
            ln.tst
        FROM siocs.lognote ln
        LEFT JOIN siocs.logbook lb
            ON lb.id = ln.log_id
        WHERE ln.text IS NOT NULL
          AND BTRIM(ln.text) <> ''
        ORDER BY controller_id, ln.log_id, ln.tst;
        """,
    )


def _fetch_meter_series_rows(
    postgres: ScheindPostgresWinmiocs70,
    controller_id: int,
    last_time: datetime,
    last_recid: int,
) -> list[tuple]:
    """Lädt neue Zählernummern-Zeitreihenwerte ab dem letzten Sync-Punkt."""
    return _fetch_all(
        postgres,
        """
        SELECT recid, time, ser
        FROM winmiocs.cnt
        WHERE node = %s
          AND (
              time > %s
              OR (time = %s AND recid > %s)
          )
        ORDER BY time, recid;
        """,
        (controller_id, last_time, last_time, last_recid),
    )


def _extract_note_filename(first_line: str) -> tuple[str | None, str | None]:
    """Extrahiert Dateiname und Regler-Ordner aus der Notiz-Kopfzeile."""
    match = NOTE_PREFIX_RE.match(first_line.strip())
    if not match:
        return None, None
    folder = match.group("folder")
    filename = match.group("filename")
    if filename:
        filename = filename.strip()
    return folder, filename


def _parse_note_datetime(value: str) -> tuple[datetime | None, str]:
    """
    Erkennt Datums-/Zeitpräfixe am Zeilenanfang und trennt sie vom eigentlichen
    Notiztext.
    """
    match = NOTE_DATETIME_RE.match(value)
    if not match:
        return None, value.strip()
    candidate = match.group("date").replace(",", ".").replace("  ", " ").strip()
    candidate = re.sub(r"\s+", " ", candidate)
    rest = match.group("rest").strip(" :-\t")
    for pattern in NOTE_DATE_PATTERNS:
        try:
            return datetime.strptime(candidate, pattern), rest
        except ValueError:
            continue
    return None, value.strip()


def _normalize_note_value(value: str) -> str:
    """Bereitet Notiztexte für Vergleich und Übertragung konsistent auf."""
    value = value.replace("\r\n", "\n").replace("\r", "\n")
    value = re.sub(r"[ \t]+", " ", value)
    return value.strip()


def _parse_float(value: Any) -> float | None:
    """Parst numerische Strings robust und akzeptiert Komma- wie Punktnotation."""
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


def _build_geolocation_from_attr(attr_value: Any) -> list[float]:
    """
    Baut die NAO-Geolocation aus `partner.attr.LON` und `partner.attr.LAT`.

    NAO erwartet hier `[Längengrad, Breitengrad]`.
    """
    lon = _parse_float(_get_attr_value(attr_value, "LON"))
    lat = _parse_float(_get_attr_value(attr_value, "LAT"))
    if lon is None or lat is None:
        return []
    return [lon, lat]


def _build_station_name(controller_id: int) -> str:
    """
    Erzeugt den Instanznamen für neu angelegte Stationen.

    Der Name ist bewusst exakt die Regler-ID, damit die Zuordnung auch direkt
    nach dem Anlegen eindeutig bleibt, bevor die Metadaten nachgezogen wurden.
    """
    return str(controller_id)


def build_station_creation_infos(postgres: ScheindPostgresWinmiocs70) -> dict[int, dict]:
    """
    Baut die Informationen auf, die für das automatische Anlegen fehlender
    Stationen in NAO benötigt werden.
    """
    creation_infos: dict[int, dict] = {}
    for partner_id, _name, attr in _fetch_partner_creation_rows(postgres):
        controller_id = _normalize_controller_id(partner_id)
        if controller_id is None:
            continue
        creation_infos[controller_id] = {
            "name": _build_station_name(controller_id),
            "geolocation": _build_geolocation_from_attr(attr),
        }
    return creation_infos


def _build_note_entries_from_row(text: str, fallback_time: datetime) -> list[dict[str, Any]]:
    """
    Zerlegt einen Schneid-Notizdatensatz in einzelne historische Notizeinträge.

    Regeln:
    - Der Kopf `WinMiocs Notiz: ...\\Datei.txt` wird entfernt.
    - Wenn eine Zeile mit einem Datum beginnt, startet sie einen neuen Eintrag.
    - Zeilen ohne Datumsanfang werden an den vorherigen Eintrag angehängt.
    - Gibt es noch keinen Eintrag, wird die Zeile mit `tst` datiert.
    """
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    if not lines:
        return []

    _, filename = _extract_note_filename(lines[0])
    filename_prefix = f"{filename}: " if filename else ""
    content_lines = lines[1:] if filename or lines[0].startswith("WinMiocs Notiz:") else lines

    entries: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None

    for raw_line in content_lines:
        line = raw_line.strip()
        if line == "":
            continue
        line_datetime, line_text = _parse_note_datetime(line)
        line_text = _normalize_note_value(line_text)
        if line_text == "":
            continue

        if line_datetime is not None:
            if current is not None:
                entries.append(current)
            current = {
                "start": line_datetime,
                "value": _normalize_note_value(f"{filename_prefix}{line_text}"),
            }
            continue

        if current is None:
            current = {
                "start": fallback_time,
                "value": _normalize_note_value(f"{filename_prefix}{line_text}"),
            }
            continue

        current["value"] = _normalize_note_value(f"{current['value']} {line_text}")

    if current is not None:
        entries.append(current)
    return entries


def _canonical_note_history(history: list[dict[str, Any]] | None) -> list[tuple[str | None, str]]:
    """Erzeugt eine stabile Vergleichsrepräsentation für Notiz-Historien."""
    result = []
    for item in history or []:
        start = item.get("start")
        if isinstance(start, datetime):
            start_value = _to_iso_z(start)
        else:
            start_value = _to_iso_z(_parse_nao_iso_datetime(start))
        result.append((start_value, _normalize_note_value(str(item.get("value", "")))))
    result.sort(key=lambda item: ((item[0] or ""), item[1]))
    return result


def build_postgres_metadata(postgres: ScheindPostgresWinmiocs70) -> tuple[dict[int, dict], dict]:
    """
    Baut aus der Postgres-Datenbank das vollständige fachliche Metadatenbild je
    Regler-ID auf.

    Rückgabe:
    - `metadata_by_controller`: die an NAO zu synchronisierenden Metadaten
    - `local_note_state`: lokale Hilfsinformationen für `MetaSincInfo.json`
    """
    metadata_by_controller: dict[int, dict] = {}
    local_note_state: dict[str, dict] = {}

    for partner_id, name, address, attr in _fetch_partner_rows(postgres):
        controller_id = _normalize_controller_id(partner_id)
        if controller_id is None:
            continue
        metadata = metadata_by_controller.setdefault(controller_id, {})
        metadata["Regler-ID"] = controller_id
        metadata["Ansprechperson"] = _normalize_scalar(name)
        metadata["Straße/Hausnr."] = _normalize_scalar(address)
        metadata["Gruppe"] = _normalize_scalar(_get_attr_value(attr, "UNION"))

    for node, switch_time, serial_old, serial_new in _fetch_latest_cntswap_rows(postgres):
        controller_id = _normalize_controller_id(node)
        if controller_id is None:
            continue
        metadata = metadata_by_controller.setdefault(controller_id, {})
        metadata["Wechseldatum"] = _to_iso_z(switch_time)
        metadata["Alte-Zählernummer"] = _parse_serial_to_int(serial_old)
        metadata["Neue-Zählernummer"] = _parse_serial_to_int(serial_new)

    notes_by_controller: dict[int, list[dict[str, Any]]] = {}
    for controller_id_raw, log_id, text, _loginname, tst in _fetch_lognote_rows(postgres):
        controller_id = _normalize_controller_id(controller_id_raw)
        if controller_id is None or text is None or tst is None:
            continue
        entries = _build_note_entries_from_row(str(text), tst)
        if not entries:
            continue
        notes_by_controller.setdefault(controller_id, []).extend(entries)
        local_note_state.setdefault(str(controller_id), {})[str(log_id)] = {
            "tst": _to_iso_z(tst),
            "text_hash": hashlib.sha1(str(text).encode("utf-8")).hexdigest(),
        }

    for controller_id, note_entries in notes_by_controller.items():
        note_entries.sort(key=lambda item: (item["start"], item["value"]))
        metadata_by_controller.setdefault(controller_id, {})["Notiz"] = note_entries

    return metadata_by_controller, local_note_state


def create_missing_stations_from_postgres(
    nao_connect: NaoApp,
    postgres: ScheindPostgresWinmiocs70,
    stations_nao: dict,
    workspace_id: str,
    asset_id: str,
    min_controller_id: int | None = None,
    max_controller_id: int | None = None,
    description: str = "Station aus Schneid",
) -> StationCreationSummary:
    """
    Legt für Regler-IDs aus Postgres fehlende NAO-Instances automatisch an.

    Die Gerätezuordnung basiert auf der aktuell geladenen API-Konfiguration.
    Fehlende Stationen werden aus `siocs.partner` abgeleitet und danach kann die
    API erneut geladen werden.
    """
    summary = StationCreationSummary()
    stations_by_controller = _normalize_stations_nao(stations_nao)
    creation_infos = build_station_creation_infos(postgres)

    for controller_id, station_info in creation_infos.items():
        if min_controller_id is not None and controller_id < min_controller_id:
            continue
        if max_controller_id is not None and controller_id > max_controller_id:
            continue

        summary.checked_controller_ids += 1

        if controller_id in stations_by_controller:
            summary.skipped_existing_instances += 1
            continue

        name = station_info.get("name")
        if not name:
            summary.skipped_missing_required_data += 1
            continue

        result = nao_connect.createInstance(
            name=name,
            description=description,
            asset_id=asset_id,
            workspace_id=workspace_id,
            geolocation=station_info.get("geolocation", []),
            attributevalues=[],
        )

        if isinstance(result, dict) and result.get("_id"):
            summary.created_instances += 1
            summary.created_controller_ids.append(controller_id)

    return summary


def _get_nao_meta(station_data: dict, meta_name: str) -> tuple[Any, str | None]:
    """Liest Wert und NAO-Meta-ID für ein einzelnes Metadatum aus der API-Struktur."""
    meta = station_data.get(meta_name)
    if not isinstance(meta, dict):
        return None, None
    return meta.get("value"), meta.get("id")


def sync_station_metadata_from_postgres(
    nao_connect: NaoApp,
    postgres: ScheindPostgresWinmiocs70,
    stations_nao: dict,
    meta_sync_info: dict,
) -> SyncSummary:
    """
    Synchronisiert die Metadaten aus Postgres nach NAO.

    Die Gerätezuweisung erfolgt immer über die aktuell geladene API-Konfiguration.
    Lokal gespeicherte Informationen werden nur als Merkhilfe verwendet.
    """
    summary = SyncSummary()
    stations_by_controller = _normalize_stations_nao(stations_nao)
    postgres_metadata, local_note_state = build_postgres_metadata(postgres)

    for controller_id, metadata in postgres_metadata.items():
        summary.checked_controller_ids += 1
        station_data = stations_by_controller.get(controller_id)
        if station_data is None:
            summary.skipped_controller_ids += 1
            continue

        instance_id = station_data["Instance-ID"]

        for meta_name, postgres_value in metadata.items():
            nao_value, meta_id = _get_nao_meta(station_data, meta_name)
            if not meta_id:
                continue

            if meta_name == "Notiz":
                postgres_history = postgres_value if isinstance(postgres_value, list) else []
                nao_history = nao_value if isinstance(nao_value, list) else []
                if _canonical_note_history(postgres_history) == _canonical_note_history(nao_history):
                    continue
                summary.changed_items += 1
                nao_connect.patchInstanceMetaHistory(
                    instance_id=instance_id,
                    meta_id=meta_id,
                    history=[{"start": item["start"], "value": item["value"]} for item in postgres_history],
                )
                summary.synced_items += 1
                continue

            postgres_scalar = _normalize_scalar(postgres_value)
            nao_scalar = _normalize_scalar(nao_value)
            if postgres_scalar == nao_scalar:
                continue
            if postgres_scalar is None:
                continue

            summary.changed_items += 1
            nao_connect.patchInstanceMeta(
                instance_id=instance_id,
                meta_id=meta_id,
                value=postgres_scalar,
            )
            summary.synced_items += 1

        if str(controller_id) in local_note_state:
            meta_sync_info["notes"][str(controller_id)] = local_note_state[str(controller_id)]

    return summary


def _timestamp_to_nanoseconds(value: datetime) -> int:
    """Wandelt einen PostgreSQL-Zeitpunkt so in ns um wie die bestehende pandas-Logik."""
    return int(pd.Timestamp(value).value)


def sync_meter_series_from_postgres(
    nao_connect: NaoApp,
    postgres: ScheindPostgresWinmiocs70,
    stations_nao: dict,
    meter_number_id: str,
    hast_asset_id: str,
    meta_sync_info: dict,
    default_start_time: datetime = datetime(2010, 1, 1),
) -> SyncSummary:
    """
    Synchronisiert die Zählernummern-Zeitreihe aus `winmiocs.cnt` nach NAO.

    Der lokale Sync-Punkt wird je Regler-ID in `MetaSincInfo.json` mit
    `last_time` und `last_recid` gespeichert, damit gleiche Zeitstempel sauber
    verarbeitet werden.
    """
    summary = SyncSummary()
    stations_by_controller = _normalize_stations_nao(stations_nao)

    for controller_id, station_data in stations_by_controller.items():
        summary.checked_controller_ids += 1

        sync_state = meta_sync_info["meter_series_sync"].setdefault(
            str(controller_id),
            {
                "last_time": _to_iso_z(default_start_time),
                "last_recid": 0,
                "instance_id": station_data["Instance-ID"],
            },
        )
        sync_state["instance_id"] = station_data["Instance-ID"]

        last_time = _parse_nao_iso_datetime(sync_state.get("last_time")) or default_start_time
        last_recid = int(sync_state.get("last_recid", 0) or 0)

        rows = _fetch_meter_series_rows(postgres, controller_id, last_time, last_recid)
        if not rows:
            continue

        telegraf_payload = []
        last_row_time = last_time
        last_row_recid = last_recid

        for recid, series_time, serial_value in rows:
            serial_int = _parse_serial_to_int(serial_value)
            if series_time is None or serial_int is None:
                continue
            telegraf_payload.append(
                f"{hast_asset_id},instance={station_data['Instance-ID']} "
                f"{meter_number_id}={serial_int} {_timestamp_to_nanoseconds(series_time)}"
            )
            last_row_time = series_time
            last_row_recid = int(recid)

        if not telegraf_payload:
            continue

        summary.changed_items += len(telegraf_payload)
        status = nao_connect.sendTelegrafData(payload=telegraf_payload, values_count=len(telegraf_payload))
        if status != 204:
            continue

        summary.synced_items += len(telegraf_payload)
        sync_state["last_time"] = _to_iso_z(last_row_time)
        sync_state["last_recid"] = last_row_recid

    return summary
