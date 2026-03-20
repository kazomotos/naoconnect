# README – Architektur der `main.py` für die Synchronisierung von Schneid/Postgres nach NAO

Diese README beschreibt **nicht die PostgreSQL-Felder im Detail** – dafür gibt es bereits die vorhandene `readme_postgres.md`.

Hier geht es um etwas anderes:

- **welche Rolle `main.py` im Gesamtablauf hat**
- **welche Verantwortung im Helper liegt**
- **wie Schneid/Postgres fachlich mit NAO verbunden wird**
- **welche Reihenfolge für die Synchronisierung wichtig ist**
- **welche Zustände lokal gespeichert werden müssen**
- **wie eine neue `main.py` aufgebaut sein sollte, damit Codex oder ein anderer Entwickler sie wieder sauber nachbauen kann**

Der Fokus ist bewusst auf dem **Postgres-Teil**: also auf der Synchronisierung von **Stammdaten** und der **Zählernummern-Zeitreihe** aus Schneid in Richtung NAO.

---

## 1. Zielbild des Skripts

Die `main.py` ist kein Ort für fachliche Detailverarbeitung der Tabellen, sondern der **Orchestrator** des Syncs.

Sie soll im Kern nur diese Aufgaben haben:

1. **Konfiguration laden**
   - NAO-Zugangsdaten
   - IDs für Workspace, Asset, Attribute und Sensoren
   - API-Endpunkt für den aktuellen NAO-Metastand

2. **NAO-Seite initialisieren**
   - Logger/Messager für Betriebsstatus
   - NAO-Connector für Schreiben nach NAO
   - aktuelles Stationsbild aus NAO einlesen

3. **Postgres-Seite initialisieren**
   - lesender Connector auf Schneid/Postgres

4. **lokalen Sync-Zustand laden**
   - damit bekannte Daten nicht immer wieder neu übertragen werden

5. **optional fehlende Stationen in NAO anlegen**
   - falls in Postgres Regler existieren, die in NAO noch keine Instance haben

6. **Metadaten synchronisieren**
   - nur geänderte Werte schreiben
   - Notiz-Historie als Sonderfall behandeln

7. **Zeitreihe der Zählernummern synchronisieren**
   - inkrementell und robust gegen gleiche Zeitstempel

8. **lokalen Sync-Zustand speichern**
   - damit der nächste Lauf sauber fortsetzen kann

9. **kompakte Zusammenfassung ausgeben**
   - wie viele Stationen geprüft, angelegt, geändert oder synchronisiert wurden

Wichtig: `main.py` soll also **Ablauf steuern**, nicht die PostgreSQL-Details selber auseinandernehmen. Diese Logik gehört in den Helper.

---

## 2. Fachliche Grundidee: Wer ist Quelle, wer ist Ziel?

### Quelle
Schneid/Postgres ist die **fachliche Quellseite** für:

- Regler-/Stationsstammdaten
- Ansprechpartner
- Adresse
- Gruppenzuordnung
- Notizen
- letzter Zählerwechsel
- Zeitreihe der Zählernummern

### Ziel
NAO ist die **Zielseite**, in die diese Informationen übersetzt werden.

In NAO gibt es dabei drei Ebenen, die man auseinanderhalten muss:

- **Workspace**: organisatorischer Container
- **Asset**: der digitale Zwilling / das Modell, also die Vorlage
- **Instance**: die konkrete Station in NAO

Für dieses Projekt gilt fachlich:

- das Asset beschreibt eine **HAST / Station**
- jede Schneid-Regler-ID soll genau einer passenden **NAO-Instance** zugeordnet sein
- Metadaten landen auf der Instance
- die Zählernummern-Zeitreihe wird als **Telegraf-Zeitreihe** an NAO gesendet

---

## 3. Zentrale Zuordnungslogik: Regler-ID ist der Schlüssel

Die komplette Synchronisierung hängt an der **Regler-ID**.

Die Regler-ID ist die gemeinsame fachliche Klammer zwischen beiden Welten:

- in Schneid/Postgres identifiziert sie den Regler bzw. die Station
- in NAO muss sie eine konkrete Instance wiederfindbar machen

### Warum das so wichtig ist
Ohne stabile Regler-ID gibt es keine sichere Antwort auf die Frage:

> Zu welcher NAO-Instance gehört dieser Datensatz aus Postgres?

Deshalb baut die Helper-Logik intern immer auf ein Mapping dieser Form:

```text
Regler-ID -> NAO-Instance-ID
```

Zusätzlich hängen daran die IDs der einzelnen Metadatenfelder in NAO.

---

## 4. Warum zuerst NAO gelesen wird

Der Ablauf startet bewusst nicht mit Postgres, sondern mit einem **Einlesen des aktuellen NAO-Zustands**.

Der Grund ist:

- es muss bekannt sein, **welche Stationen in NAO schon existieren**
- es muss bekannt sein, **welche Metadatenwerte dort bereits liegen**
- es muss bekannt sein, **welche interne NAO-ID ein Metadatum auf einer Instance hat**

Diese Informationen kommen über `read_stations_from_api(...)`.

Die Funktion baut aus der API-Antwort ein kompaktes Arbeitsmodell auf:

```text
stations_nao = {
    <Regler-ID>: {
        "Instance-ID": <instance_id>,
        "Regler-ID": {"value": ..., "id": ...},
        "Ansprechperson": {"value": ..., "id": ...},
        ...
    }
}
```

Dieses Objekt ist das zentrale Bindeglied für alle späteren Sync-Schritte.

### Besonderheit bei frisch angelegten Stationen
Wenn neue Instances eben erst erzeugt wurden, kann es sein, dass das Metadatum `Regler-ID` in NAO noch leer ist.

Damit diese Station trotzdem sofort wieder zugeordnet werden kann, darf die API-Leselogik **einmalig den Instanznamen als Fallback** verwenden – aber nur, wenn sich dieser Name als Integer lesen lässt.

Die Idee dahinter:

- neue Instances werden absichtlich mit dem Namen der Regler-ID angelegt
- dadurch sind sie schon direkt nach dem Erstellen wieder auffindbar
- beim nächsten Metadaten-Sync wird `Regler-ID` regulär in NAO gesetzt
- danach ist nur noch das Metadatum maßgeblich, nicht mehr der Name

Das ist eine **Bootstrap-Logik**, damit Auto-Creation und direkter Folgesync in einem Lauf funktionieren.

---

## 5. Rolle des `hast_asset_attributes`-Mappings

`main.py` definiert ein Mapping zwischen fachlicher Herkunft in Postgres und fachlicher Bedeutung in NAO.

Beispielhaft:

```text
"siocs.partner.name" -> "Ansprechperson"
"siocs.partner.address" -> "Straße/Hausnr."
"winmiocs.cntswap.ser_new" -> "Neue-Zählernummer"
```

Dieses Mapping erfüllt drei Aufgaben gleichzeitig:

1. **Dokumentation**
   - Welche Schneid-Information gehört zu welchem NAO-Metadatum?

2. **Identitätsanker**
   - Welche NAO-Attribut-ID gehört fachlich zu diesem Feld?

3. **Typinformation**
   - Welcher Python-/NAO-Datentyp wird für das Feld erwartet?

Wichtig ist dabei:

- `main.py` beschreibt damit die **fachliche Konfiguration**
- der Helper benutzt diese Informationen, um die API-Antwort korrekt zu lesen und Sync-Operationen zuzuordnen

Die eigentliche Extraktionslogik aus Postgres hängt aber **nicht** an diesem Mapping allein. Sie steckt zusätzlich im Helper, weil manche Felder Sonderlogik brauchen.

---

## 6. Aufgabenverteilung: `main.py` versus `_helper.py`

## `main.py`
`main.py` ist die Ablaufsteuerung.

Sie sollte vor allem:

- Konstanten und IDs definieren
- Verbindungen initialisieren
- den aktuellen NAO-Zustand laden
- optional fehlende Stationen erzeugen
- den Metadaten-Sync auslösen
- den Zeitreihen-Sync auslösen
- Sync-Status speichern
- Ergebnis protokollieren

`main.py` sollte **nicht** selbst:

- SQL-Felder interpretieren
- Notiztexte parsen
- Serienzustände auflösen
- Vergleichslogik zwischen Postgres und NAO implementieren

## `_helper.py`
Der Helper ist die eigentliche fachliche und technische Arbeitslogik.

Dort liegen insbesondere:

- Laden/Speichern von `MetaSincInfo.json`
- Normalisierung von Regler-IDs
- Lesen und Vereinheitlichen der NAO-API-Rückgabe
- SQL-Reads gegen Postgres
- Aufbau des Postgres-Metadatenbilds
- Parsing und Normalisierung von Notizen
- Ermitteln fehlender Stationen
- Abgleich Postgres vs. NAO
- Schreiben geänderter Metadaten nach NAO
- inkrementeller Zeitreihen-Sync der Zählernummern

Die Faustregel lautet:

> `main.py` sagt **wann** etwas passiert, `_helper.py` sagt **wie** es fachlich passiert.

---

## 7. Die drei großen Arbeitsblöcke im Helper

### 7.1 Stationsbild aus NAO lesen
`read_stations_from_api(...)`

Diese Funktion übersetzt die rohe API-Antwort in eine Form, mit der der Rest der Synchronisierung arbeiten kann.

Sie sorgt dafür, dass aus der NAO-Sicht folgende Dinge vorliegen:

- bekannte Stationen
- ihre Instance-IDs
- vorhandene Metadatenwerte
- IDs der jeweiligen NAO-Metadatenfelder

Ohne diese Struktur kann kein Update erfolgen, weil spätere Schreiboperationen die konkrete `instance_id` und `meta_id` brauchen.

---

### 7.2 Fachliches Postgres-Metadatenbild aufbauen
`build_postgres_metadata(...)`

Diese Funktion ist der Kern der fachlichen Übersetzung von Schneid nach NAO.

Sie sammelt Informationen aus mehreren Tabellen und baut daraus pro Regler-ID ein konsolidiertes Bild auf.

Das Ergebnis ist sinngemäß:

```text
controller_id -> {
    "Regler-ID": ...,
    "Ansprechperson": ...,
    "Straße/Hausnr.": ...,
    "Gruppe": ...,
    "Wechseldatum": ...,
    "Alte-Zählernummer": ...,
    "Neue-Zählernummer": ...,
    "Notiz": [...]
}
```

Dabei gilt fachlich:

- `siocs.partner` liefert die allgemeinen Stammdaten
- `winmiocs.cntswap` liefert den letzten bekannten Zählerwechsel je Regler
- `siocs.lognote` + `siocs.logbook.param.device` liefern die Notizen samt Gerätezuordnung

Die Funktion erzeugt zusätzlich einen lokalen Notizzustand für `MetaSincInfo.json`, damit später nachvollziehbar bleibt, welche Rohquellen bereits verarbeitet wurden.

---

### 7.3 Synchronisierung in Richtung NAO
Hier gibt es zwei getrennte Pfade:

#### a) `sync_station_metadata_from_postgres(...)`
Für Stammdaten und Notizen.

#### b) `sync_meter_series_from_postgres(...)`
Für die Zählernummern-Zeitreihe.

Die Trennung ist sehr sinnvoll, weil beide Datenarten in NAO unterschiedlich geschrieben werden:

- Metadaten als Attribute / Historienattribute
- Zeitreihen über Telegraf-Frames

---

## 8. Automatisches Anlegen fehlender Stationen

Ein wichtiger Teil der Logik ist die Frage:

> Was passiert, wenn in Schneid eine Regler-ID existiert, aber in NAO noch keine passende Instance?

Dafür gibt es `create_missing_stations_from_postgres(...)`.

### Was dabei passiert
1. Der Helper liest die dafür nötigen Minimaldaten aus `siocs.partner`.
2. Für jede Regler-ID wird geprüft, ob sie bereits in `stations_nao` existiert.
3. Falls nicht, kann eine neue Instance in NAO angelegt werden.
4. Der Name der neuen Instance wird absichtlich als String der Regler-ID gesetzt.
5. Falls Geodaten vorliegen, werden diese ebenfalls mitgegeben.
6. Nach erfolgreichem Anlegen muss die NAO-Metadaten-API **neu geladen** werden.

### Warum das Nachladen zwingend ist
Die neu erzeugten Instances haben neue NAO-IDs.

Solange `stations_nao` nicht neu geladen wurde, arbeitet der restliche Sync mit einem veralteten Stationsbild. Deshalb ist der Reload direkt nach erfolgreichem Anlegen fachlich notwendig.

### Warum Grenzen für Regler-IDs sinnvoll sind
In `main.py` gibt es Min-/Max-Bereiche für automatisch anzulegende Regler.

Das schützt davor, versehentlich alle historisch oder fehlerhaft vorhandenen IDs ungeprüft nach NAO hochzuziehen. Auto-Creation sollte immer bewusst eingegrenzt bleiben.

---

## 9. Metadaten-Sync: Wie Postgres und NAO verglichen werden

`sync_station_metadata_from_postgres(...)` macht keinen Blindschreibvorgang, sondern arbeitet nach diesem Prinzip:

1. Postgres-Metadatenbild aufbauen
2. Für jede Regler-ID die passende Station in NAO suchen
3. Für jedes Metadatum Postgres-Wert und NAO-Wert vergleichen
4. Nur bei echter Änderung schreiben

Das ist entscheidend, damit:

- keine unnötigen API-Updates erzeugt werden
- bestehende Werte nicht bei jedem Lauf erneut überschrieben werden
- die Synchronisierung idempotent bleibt

### Sonderfall: Notizen
Notizen sind **keine einfachen Einzelwerte**, sondern eine Historie.

Darum werden sie anders behandelt:

- Postgres-Notizen werden in Einträge mit `start` und `value` zerlegt
- die bestehende Historie aus NAO wird ebenfalls betrachtet
- beide Historien werden in eine stabile Vergleichsform überführt
- nur wenn sich diese Historie fachlich unterscheidet, wird geschrieben

Wichtig ist der Schreibmodus in NAO:

- Historien-Metadaten werden nicht punktuell ergänzt
- stattdessen wird die **gesamte Historie neu gesetzt**

Das heißt praktisch:

> Wenn Notizen synchronisiert werden, muss die vollständige gewünschte Historie an NAO übergeben werden.

Darum ist die Vergleichslogik vor dem Schreiben so wichtig.

---

## 10. Warum Notizen eigene Sonderlogik brauchen

Die Notizen aus Schneid sind fachlich deutlich komplexer als normale Stammdaten.

Die Helper-Logik behandelt deshalb mehrere Besonderheiten:

- Präfixe wie `WinMiocs Notiz: ...`
- enthaltene Dateinamen
- Datumsangaben am Anfang einzelner Zeilen
- mehrzeilige Fortsetzungen ohne neues Datum
- Trennung zwischen eigentlichem Notiztext und Kopfbereich

### Wichtigster Fachpunkt
Die Gerätezuweisung einer Notiz erfolgt **nicht** aus der im Text vorkommenden Regler-ID, sondern über die Relation:

```text
lognote.log_id -> logbook.id -> logbook.param.device
```

Das ist zentral, weil der Text zwar ähnlich aussieht, aber fachlich nicht als primäre Zuordnungsquelle gelten soll.

Die Notizlogik im Helper sorgt also dafür, dass aus einem oft unstrukturierten Textblock eine saubere Historie entsteht, die in NAO sinnvoll gespeichert werden kann.

---

## 11. Zeitreihen-Sync der Zählernummern

Der zweite große Block ist `sync_meter_series_from_postgres(...)`.

Hier geht es **nicht** um den letzten Stand als Metadatum, sondern um die vollständige bzw. inkrementelle **Zeitreihe** der Zählernummern.

Die Quelle dafür ist die dafür vorgesehene Schneid-Tabelle, die per Helper in Reihenform gelesen wird.

### Warum diese Logik getrennt von Metadaten laufen muss
Zeitreihen verhalten sich anders als Metadaten:

- es gibt viele Punkte pro Gerät
- sie werden typischerweise inkrementell geladen
- sie werden in NAO nicht per `patchInstanceMeta(...)`, sondern als Telegraf-Frames übertragen

Ein Zeitreihenpunkt wird dabei sinngemäß so an NAO geschickt:

```text
<asset_id>,instance=<instance_id> <sensor_id>=<wert> <timestamp_in_ns>
```

Dabei braucht man:

- die Instance-ID der Zielstation
- die Sensor-ID des Messpunkts in NAO
- den Messwert
- den Messzeitpunkt in Nanosekunden

### Warum `last_time` allein nicht reicht
Der Helper speichert pro Regler-ID lokal:

- `last_time`
- `last_recid`
- `instance_id`

Der zusätzliche `last_recid` ist wichtig, weil gleiche Zeitstempel mehrfach vorkommen können. Wenn man nur „alles größer als letzter Zeitpunkt“ lädt, können bei identischen Zeitpunkten Datensätze verloren gehen.

Deshalb ist die fachlich korrekte Inkrementlogik:

> weiter ab `(last_time, last_recid)` statt nur ab `last_time`

Genau das macht den Zeitreihen-Sync robust.

---

## 12. Rolle von `MetaSincInfo.json`

Die Datei `MetaSincInfo.json` ist kein fachlicher Datenbestand, sondern ein **technischer Merkspeicher**.

Sie dient dazu, dass das Skript bei wiederholten Läufen weiß, wo es weitermachen muss.

### Enthaltene Bereiche
Der Helper arbeitet mit einer Struktur wie:

```json
{
  "version": 1,
  "meter_series_sync": {
    "<Regler-ID>": {
      "last_time": "...",
      "last_recid": 123,
      "instance_id": "..."
    }
  },
  "notes": {
    "<Regler-ID>": {
      "<log_id>": {
        "tst": "...",
        "text_hash": "..."
      }
    }
  }
}
```

### Bedeutung
- `meter_series_sync` merkt sich den Fortschritt je Station für die Zeitreihe
- `notes` hält lokale Hilfsinformationen über verarbeitete Rohquellen bereit
- `instance_id` wird mitgeführt, damit nachvollziehbar bleibt, zu welcher NAO-Instance der letzte Sync gehörte

Diese Datei sorgt dafür, dass der Prozess **inkrementell**, **wiederholbar** und **fehlertolerant** bleibt.

---

## 13. Fachliche Reihenfolge im Gesamtablauf

Die korrekte Reihenfolge in `main.py` ist wichtig und sollte nicht beliebig verändert werden.

### Empfohlene Reihenfolge
1. Konfiguration laden
2. NAO-Connector und Logger initialisieren
3. `stations_nao` über API laden
4. Postgres-Connector initialisieren
5. `MetaSincInfo.json` laden
6. optional fehlende Stationen in NAO anlegen
7. falls neue Stationen angelegt wurden: `stations_nao` neu laden
8. Stammdaten synchronisieren
9. Zeitreihe synchronisieren
10. `MetaSincInfo.json` speichern
11. Zusammenfassung ausgeben

### Warum erst Metadaten und dann Zeitreihe?
Die Reihenfolge ist fachlich sinnvoll, weil:

- eine neue Station eventuell zuerst noch angelegt werden muss
- ihre Regler-ID und andere Stammdaten möglichst früh korrekt in NAO stehen sollen
- erst danach die Zeitreihe auf die finale Instance-Zuordnung geschrieben wird

So bleibt der Gesamtlauf nachvollziehbar und sauber.

---

## 14. Welche Teile einer neuen `main.py` konstant bleiben sollten

Wenn Codex oder ein anderer Entwickler die `main.py` neu aufbauen soll, sollten diese Strukturentscheidungen erhalten bleiben:

### A. Konfiguration oben bündeln
Oben in der Datei gehören:

- Hostnamen
- Logins
- Workspace-ID
- Asset-ID
- Sensor-ID(s)
- Mapping der Metadatenattribute
- URL oder Token für die NAO-Metadaten-API
- Flags für Auto-Creation

### B. Die Datei muss deklarativ lesbar sein
Man sollte beim Lesen sofort erkennen können:

- welche Daten nach NAO gehen
- welche IDs fachlich wofür stehen
- welche Schritte im Ablauf ausgeführt werden

### C. Keine SQL- und Parsing-Details in `main.py`
Die Datei soll orchestrieren, nicht zerlegen.

### D. Ergebnisobjekte zurückgeben und drucken
Die Helper-Funktionen liefern Summary-Objekte zurück. Das ist gut, weil `main.py` dann:

- kompakt protokollieren kann
- bei Bedarf Monitoring anschließen kann
- später leichter testbar bleibt

---

## 15. Was Codex aus dieser Struktur ableiten können soll

Eine gute README für Codex muss nicht jede Programmzeile erklären. Sie muss vor allem die **stabile Denkstruktur** des Skripts erklären.

Codex sollte aus dieser Beschreibung folgende Architektur rekonstruieren können:

### Eingaben
- Postgres als fachliche Quelle
- NAO-API als aktuelles Zielbild
- lokales JSON als technischer Sync-Zustand

### Verarbeitung
- Normalisierung der Regler-IDs
- Aufbau des Postgres-Bildes je Station
- Aufbau des NAO-Bildes je Station
- Vergleich der beiden Bilder
- Schreiben nur bei Änderungen
- getrennte Behandlung von Metadaten und Zeitreihen

### Ausgaben
- aktualisierte Metadaten in NAO
- aktualisierte Zählernummern-Zeitreihe in NAO
- aktualisierter lokaler Sync-Stand
- kompakte Statuszusammenfassung

Wenn diese Architektur verstanden ist, kann Codex eine neue `main.py` bauen, auch wenn einzelne Namen leicht geändert werden.

---

## 16. Minimaler mentaler Bauplan für eine neue `main.py`

Eine neue `main.py` sollte gedanklich ungefähr so aussehen:

```text
Konstanten / IDs / Mapping definieren
↓
NAO-Logger initialisieren
↓
NAO-Connector initialisieren
↓
aktuelles Stationsbild aus NAO laden
↓
Postgres-Connector initialisieren
↓
lokalen Sync-Stand laden
↓
fehlende Stationen optional in NAO anlegen
↓
bei Neuanlagen API-Stationsbild neu laden
↓
Metadaten aus Postgres nach NAO synchronisieren
↓
Zählernummern-Zeitreihe aus Postgres nach NAO synchronisieren
↓
lokalen Sync-Stand speichern
↓
Summary ausgeben
```

Das ist die eigentliche Hauptlogik.

---

## 17. Was bei Änderungen am Helper beachtet werden muss

Wenn der Helper angepasst wird, dürfen folgende Invarianten nicht verloren gehen:

### 1. Regler-ID bleibt Primärschlüssel der Zuordnung
Alle Vergleichs- und Sync-Wege hängen daran.

### 2. `stations_nao` bleibt die zentrale Arbeitsstruktur
Die restliche Logik sollte weiter darauf aufsetzen können.

### 3. Notizen bleiben Historienwerte
Sie dürfen nicht wie ein einfacher String behandelt werden.

### 4. Zeitreihen-Sync bleibt inkrementell
`last_time` allein ist nicht robust genug; `last_recid` muss mitgeführt werden.

### 5. Auto-Creation muss sofort wieder ins Stationsbild einfließen
Neue Instances ohne anschließenden API-Reload sind für den selben Lauf praktisch unsichtbar.

### 6. Nur Änderungen schreiben
Das ist fachlich und technisch der wichtigste Schutz gegen unnötige Überschreibungen.

---

## 18. Abgrenzung: Was diese README bewusst nicht erneut beschreibt

Diese README ersetzt **nicht** die bestehende `readme_postgres.txt`.

Dort stehen bereits die Details zu:

- Tabellen
- Feldern
- Formaten
- Datumsbesonderheiten
- Notizaufbau
- Zählerwechseln
- fachlichen Einzelregeln pro Datenquelle

Diese README ergänzt das um die **Systemlogik**:

- wie aus den Quellen ein Sync-Prozess wird
- wie NAO und Schneid miteinander verknüpft werden
- welche Verantwortung `main.py` und `_helper.py` jeweils tragen

---

## 19. Kurzfassung in einem Satz

Die `main.py` ist der Orchestrator eines idempotenten Postgres→NAO-Synchronisierers: Sie lädt den aktuellen NAO-Zustand, gleicht ihn mit einem im Helper aufgebauten fachlichen Stationsbild aus Schneid ab, legt fehlende Stationen optional an, synchronisiert nur geänderte Metadaten und überträgt die Zählernummern-Zeitreihe inkrementell anhand eines lokal gespeicherten Sync-Zustands.

---

## 20. Empfehlung für die weitere Pflege

Wenn das Projekt weiterentwickelt wird, ist es sinnvoll, diese drei Dinge getrennt zu halten:

- **`readme_postgres.txt`** für Quelltabellen und Feldregeln
- **diese README** für Architektur und Synchronisationslogik
- **Code im Helper** für die konkrete technische Umsetzung

Dann bleibt das Projekt sowohl für Menschen als auch für Codex gut rekonstruierbar.
