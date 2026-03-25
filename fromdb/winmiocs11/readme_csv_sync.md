# CSV-Zeitreihensynchronisierung Winmiocs11

Diese README beschreibt die neue CSV-Synchronisierung fuer Schneid/Winmiocs11.
Der Fokus liegt auf den fachlichen Besonderheiten der Schneid-CSV-Dateien und auf
der Verantwortung der neuen Hilfsfunktionen unter `naoconnect.fromdb.winmiocs11`.

## Ziel

Die neue Implementierung soll die alte CSV-Logik fachlich erhalten, aber
technisch besser wartbar machen:

- HAST-Ziele werden dynamisch ueber `Regler-ID -> aktuelle Instance-ID in NAO` aufgeloest
- Sonderassets werden lokal ueber JSON konfiguriert
- der technische Synchronisationsstand wird in SQLite gespeichert
- alle Messwerte werden im Telegraf-Format als Rohdaten uebertragen

## Wichtige Schneid-Besonderheiten

### 1. `gt`, `lt`, `b1`, `b2` sind keine laufende Messwertvalidierung

Diese Grenzen stammen aus dem alten Driver und dienen nur dazu, bisher unbekannte
Sensoren erstmalig freizuschalten.

Sobald ein Sensor einmal als synchronisierbar erkannt wurde, werden seine
Messwerte als Rohdaten uebertragen. Es werden also spaeter nicht bei jedem Wert
erneut diese Grenzen geprueft.

### 2. Regler- oder Stationswechsel koennen mitten in der Datei auftreten

Schneid kann CSV-Dateien weiterfuehren, obwohl sich die interne Struktur geaendert
hat. Das fuehrt dazu, dass aeltere Abschnitte einer Datei unter einem anderen
Mapping stehen als neuere Abschnitte.

Die neue Implementierung uebernimmt deshalb die robuste Rueckwaerts-Leselogik aus
dem Altcode:

- bevorzugt wird nur der relevante hintere Bereich gelesen
- bei Parserfehlern wird auf den letzten konsistenten Dateiteil zurueckgefallen

Zusaetzlich gilt fuer neu erkannte Sensoren standardmaessig:

- sie duerfen nur bis zum Zeitpunkt des erkannten Mapping-Wechsels
  ruecksynchronisiert werden
- aeltere CSV-Bereiche mit frueherem Mapping werden nicht automatisch neu
  interpretiert

### 3. Doppelte Sendungen sind fachlich tolerierbar

Die Synchronisierung soll moeglichst inkrementell laufen, um Performance zu
schonen. Wenn es in Grenzfaellen trotzdem zu doppelten Sendungen kommt, ist das
fachlich weniger kritisch als eine unnoetig komplexe Dublettenlogik.

### 4. Leere Werte werden nicht gesendet

Telegraf erwartet hier sendbare Zahlenwerte. Leere oder nicht numerisch
interpretierbare Werte werden daher uebersprungen.

## Neue Bausteine in `_csv_sync.py`

### `discover_csv_files(...)`

Liest den aktuellen Bestand und den Archivbestand ein und baut ein chronologisches
Bild aller fachlich lesbaren CSV-Dateien auf.

### `load_hast_sensor_config(...)`

Normalisiert `driver_schneid.json` auf eine klare Struktur fuer die neue
Synchronisierung.

### `load_special_asset_config(...)`

Laedt die lokale JSON-Konfiguration fuer Sonderassets ausserhalb der HAST-Logik.

### `build_csv_targets(...)`

Fuehrt erkannte CSV-Dateien, HAST-Driver und Sonderasset-Konfiguration zusammen
und erstellt daraus die fachlichen Synchronisationsziele.

### `CsvSyncStateStore`

Verwaltet den technischen CSV-Synchronisationsstand in SQLite.

Gespeichert werden:

- Target-Zustaende, z. B. letzte Header-Signatur
- pro Sensor der erste und letzte synchronisierte Zeitpunkt
- ein Ruecksynchronisierungsstart fuer neu erkannte Sensoren

Zusaetzlich kann der alte JSON-Stand aus `syncronizied_status_old.json` einmalig
in SQLite migriert werden.

### `sync_csv_series_from_schneid(...)`

Das ist der Orchestrator des gesamten CSV-Zeitreihensyncs:

1. Dateibestand erkennen
2. HAST- und Sondertargets bauen
3. SQLite-Status vorbereiten
4. unbekannte Sensoren freischalten
5. Rohdaten an NAO senden
6. den neuen technischen Stand fortschreiben

## Struktur der Sonderasset-Config

Die Datei `csv_special_assets.json` erwartet derzeit folgendes Grundschema:

```json
{
  "targets": [
    {
      "key": "puffer_911",
      "table": "911_puffer.csv",
      "asset_id": "nao-asset-id",
      "instance_id": "nao-instance-id",
      "sensors": [
        {
          "sensor_id": "nao-sensor-id",
          "name_dp": "T.Puffer 1(°C)",
          "dp": 0
        }
      ]
    }
  ]
}
```

Hinweise:

- `table` ist der konkrete Schneid-Dateiname
- dieselbe Tabelle darf mehrfach in unterschiedlichen Targets auftauchen
- `dp` ist die Spaltenposition in der CSV, nicht `NR`
- `name_dp` ist optional, aber fuer Nachvollziehbarkeit empfohlen

## Pfadlogik in `main.py`

Die `main.py` versucht bewusst zuerst Produktivpfade wie
`C:\Winmiocs11\...` zu verwenden. Falls diese lokal nicht existieren, wird auf
die im Projektordner eingecheckten Beispiel-/Debug-Pfade zurueckgefallen.

Das ist absichtlich so, damit dieselbe Datei sowohl im Projekt als auch auf dem
Zielsystem lesbar bleibt.
