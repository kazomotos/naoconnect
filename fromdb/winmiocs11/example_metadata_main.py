from datetime import datetime
import json

from naoconnect.fromdb.winmiocs11 import (
    create_missing_stations_from_postgres,
    load_meta_sync_info,
    read_stations_from_api,
    save_meta_sync_info,
    sync_meter_series_from_postgres,
    sync_station_metadata_from_postgres,
)
from naoconnect.fromdb.SchneidWinmiocs70 import ScheindPostgresWinmiocs70
from naoconnect.naoappV2 import NaoApp, NaoLoggerMessage

"""
Beispielhafte main.py für den Metadaten- und Zählernummern-Sync von Schneid/Postgres nach NAO.

Diese Datei ist absichtlich anonymisiert:
- keine echten Hosts
- keine echten Tokens
- keine echten Zugangsdaten
- keine echten Asset-/Workspace-/Attribut-IDs

Ziel der Datei:
Sie zeigt die Struktur einer produktiven main.py, ohne projektspezifische Geheimnisse
preiszugeben. Die eigentliche Fachlogik liegt weiterhin im Helper bzw. in den importierten
Sync-Funktionen. Diese main.py orchestriert nur den Ablauf.
"""

# -----------------------------------------------------------------------------
# 1) NAO-Zugänge und Infrastruktur (anonymisiert)
# -----------------------------------------------------------------------------
# Projekt-NAO: dort werden Instances, Metadaten und Zeitreihen geschrieben.
NAO_HOST = "example-project.nao-cloud.invalid"
NAO_USER = "sync-user@example.invalid"
NAO_PASS = "<NAO_PASSWORD>"

# Wartungs-/Messenger-NAO: optional für Restart-/Monitoring-Meldungen.
MESS_HOST = "example-monitoring.nao-cloud.invalid"
MESS_MAIL = "sync-monitor@example.invalid"
MESS_PASSWD = "<MONITORING_PASSWORD>"


# -----------------------------------------------------------------------------
# 2) Feste NAO-Konfiguration für dieses Projekt (anonymisiert)
# -----------------------------------------------------------------------------
# Workspace, in dem die Stationen liegen.
hast_workspace_id = "workspace_example_0001"

# Asset-ID des digitalen Zwillings für die HAST / Station.
hast_asset_id = "asset_example_hast_0001"

# Sensor-ID für die Zählernummern-Zeitreihe.
meter_number_id = "sensor_example_meter_number_0001"


# -----------------------------------------------------------------------------
# 3) Mapping Postgres -> NAO-Metadaten
# -----------------------------------------------------------------------------
# Schlüssel: Herkunft im Schneid/Postgres-Modell
# Werte: Zielmetadatum in NAO inkl. Attribut-ID und erwartetem Typ
hast_asset_attributes = {
    "siocs.lognote.text": {
        "name": "Notiz",
        "description": "Freitext-Notizen aus der Quellseite.",
        "_attribute": "attr_example_note",
        "type": str,
    },
    "siocs.partner.id": {
        "name": "Regler-ID",
        "description": "Eindeutiger fachlicher Schlüssel der Station.",
        "_attribute": "attr_example_controller_id",
        "type": int,
    },
    "siocs.partner.name": {
        "name": "Ansprechperson",
        "description": "Name der verantwortlichen Kontaktperson.",
        "_attribute": "attr_example_contact_name",
        "type": str,
    },
    "siocs.partner.address": {
        "name": "Straße/Hausnr.",
        "description": "Standortadresse der Station.",
        "_attribute": "attr_example_address",
        "type": str,
    },
    "siocs.partner.attr.UNION": {
        "name": "Gruppe",
        "description": "Fachliche oder organisatorische Gruppierung.",
        "_attribute": "attr_example_group",
        "type": str,
    },
    "winmiocs.cntswap.time": {
        "name": "Wechseldatum",
        "description": "Zeitpunkt des dokumentierten Zählerwechsels.",
        "_attribute": "attr_example_exchange_date",
        "type": str,
    },
    "winmiocs.cntswap.ser": {
        "name": "Alte-Zählernummer",
        "description": "Seriennummer des zuvor verbauten Zählers.",
        "_attribute": "attr_example_old_meter_number",
        "type": int,
    },
    "winmiocs.cntswap.ser_new": {
        "name": "Neue-Zählernummer",
        "description": "Seriennummer des aktuell bzw. neu verbauten Zählers.",
        "_attribute": "attr_example_new_meter_number",
        "type": int,
    },
}


# -----------------------------------------------------------------------------
# 4) Read-only API-Endpunkt für vorhandene Stationen / Metadaten in NAO
# -----------------------------------------------------------------------------
# Über diesen Endpunkt wird das aktuelle Stationsbild aus NAO gelesen.
# Die Helper-Logik baut daraus ein Dictionary der Form:
#   Regler-ID -> Instance-ID + vorhandene Meta-Werte + zugehörige Meta-IDs
META_API_TOKEN = "<META_READ_TOKEN>"
api_ids = f"https://example-project.nao-cloud.invalid/api/access/instance/{META_API_TOKEN}"

stations_nao = read_stations_from_api(
    api_ids=api_ids,
    hast_asset_attributes=hast_asset_attributes,
)


# -----------------------------------------------------------------------------
# 5) Monitoring / Betriebslogging (optional)
# -----------------------------------------------------------------------------
# Diese Instanz ist optional, aber in produktiven Setups sinnvoll, um Laufstarts,
# Restart-Ereignisse und Mengenmeldungen auf einem separaten Monitoring-Server
# sichtbar zu machen.
NaoMessager = NaoLoggerMessage(
    host=MESS_HOST,
    email=MESS_MAIL,
    password=MESS_PASSWD,
    asset="asset_example_monitoring",
    instance="instance_example_monitoring",
    count_series="series_example_sent_counter",
    restart_series="series_example_restart_counter",
)
NaoMessager.sendRestart()


# -----------------------------------------------------------------------------
# 6) Schreibender NAO-Connector
# -----------------------------------------------------------------------------
# Über diesen Connector werden später ausgeführt:
# - createInstance(...)              für automatisch neu anzulegende Stationen
# - patchInstanceMeta(...)           für einzelne Metadatenwerte
# - patchInstanceMetaHistory(...)    für Historienfelder wie Notizen
# - sendTelegrafData([...])          für Zeitreihendaten der Zählernummer
NaoConnect = NaoApp(
    host=NAO_HOST,
    email=NAO_USER,
    password=NAO_PASS,
    Messager=NaoMessager,
)


# -----------------------------------------------------------------------------
# 7) Lesender Postgres-Connector
# -----------------------------------------------------------------------------
# Die Quelle ist Schneid/Postgres. Hier werden nur Daten gelesen, nicht geschrieben.
postgres = ScheindPostgresWinmiocs70(
    username="postgres_readonly",
    password="<POSTGRES_PASSWORD>",
)


# -----------------------------------------------------------------------------
# 8) Lokaler technischer Sync-Zustand
# -----------------------------------------------------------------------------
# In dieser Datei werden keine fachlichen Stammdaten gespeichert, sondern nur
# technische Marker, z. B. welche Zählernummern-Zeitreihe bis zu welcher recid
# bereits an NAO übertragen wurde.
META_SYNC_FILE = "MetaSincInfo.example.json"
meta_sync_info = load_meta_sync_info(META_SYNC_FILE)


# -----------------------------------------------------------------------------
# 9) Optionale Auto-Creation fehlender Stationen in NAO
# -----------------------------------------------------------------------------
# Wenn in Postgres Regler-IDs existieren, die in NAO noch keine Instance haben,
# können diese automatisch erzeugt werden.
#
# Wichtige Konvention:
# Neue Instances werden mit dem Namen der Regler-ID erzeugt. Dadurch kann die
# nachfolgende API-Leselogik sie direkt wiederfinden, selbst wenn das Metadatum
# "Regler-ID" unmittelbar nach der Erstellung noch leer ist.
autocreat_stations = True
autocreat_stations_min_controller_id = 1
autocreat_stations_max_controller_id = 999999

if autocreat_stations:
    station_creation_summary = create_missing_stations_from_postgres(
        nao_connect=NaoConnect,
        postgres=postgres,
        stations_nao=stations_nao,
        workspace_id=hast_workspace_id,
        asset_id=hast_asset_id,
        min_controller_id=autocreat_stations_min_controller_id,
        max_controller_id=autocreat_stations_max_controller_id,
    )

    # Falls neue Instances angelegt wurden, muss das Stationsbild aus NAO neu
    # geladen werden, damit die frischen Instance-IDs im weiteren Lauf bekannt sind.
    if station_creation_summary.created_instances > 0:
        stations_nao = read_stations_from_api(
            api_ids=api_ids,
            hast_asset_attributes=hast_asset_attributes,
        )
else:
    station_creation_summary = None


# -----------------------------------------------------------------------------
# 10) Metadaten-Sync
# -----------------------------------------------------------------------------
# Aufgabe dieses Schritts:
# - Postgres-Stammdaten je Regler-ID fachlich aufbereiten
# - gegen das aktuelle Bild in NAO vergleichen
# - nur geänderte Werte schreiben
# - Notiz-Historie als Sonderfall vollständig aktualisieren
metadata_summary = sync_station_metadata_from_postgres(
    nao_connect=NaoConnect,
    postgres=postgres,
    stations_nao=stations_nao,
    meta_sync_info=meta_sync_info,
)


# -----------------------------------------------------------------------------
# 11) Zeitreihen-Sync der Zählernummern
# -----------------------------------------------------------------------------
# Aufgabe dieses Schritts:
# - Zählernummern-Historie aus der Quellseite lesen
# - pro Regler-ID der passenden NAO-Instance zuordnen
# - im Telegraf-Format an NAO senden
# - lokalen Fortschritt in meta_sync_info mitschreiben
series_summary = sync_meter_series_from_postgres(
    nao_connect=NaoConnect,
    postgres=postgres,
    stations_nao=stations_nao,
    meter_number_id=meter_number_id,
    hast_asset_id=hast_asset_id,
    meta_sync_info=meta_sync_info,
    default_start_time=datetime(2010, 1, 1),
)


# -----------------------------------------------------------------------------
# 12) Lokalen Sync-Zustand persistieren
# -----------------------------------------------------------------------------
save_meta_sync_info(META_SYNC_FILE, meta_sync_info)


# -----------------------------------------------------------------------------
# 13) Technische Zusammenfassung für Log, Shell oder Scheduler
# -----------------------------------------------------------------------------
print(
    json.dumps(
        {
            "station_creation": station_creation_summary.__dict__ if station_creation_summary else None,
            "metadata": metadata_summary.__dict__,
            "meter_series": series_summary.__dict__,
        },
        ensure_ascii=False,
        indent=2,
    )
)
