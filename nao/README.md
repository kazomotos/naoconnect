# NAO Asset Creator

Dieses Modul kapselt die im Projekt-TODO beschriebenen NAO-Endpunkte, um
Asset-Templates samt Unterstrukturen anzulegen.

Der Fokus liegt bewusst auf:
- Assets auflisten und Details lesen
- neue Assets anlegen
- Bauteile anlegen
- `sensors`, `meters`, `setpoints` und `actors` anlegen
- Eigenschaftsgruppen und Eigenschaften anlegen
- Einheiten prüfen und bei Bedarf automatisch anlegen

Nicht enthalten sind absichtlich alle anderen Bereiche der älteren
`NaoApp`-Implementierungen, damit das Modul für diesen Anwendungsfall klein
und nachvollziehbar bleibt.

## Import

```python
from naoconnect.nao import NaoAssetCreator
```

## Initialisierung

```python
from naoconnect.nao import NaoAssetCreator

nao = NaoAssetCreator(
    host="aura.nao-cloud.de",
    email="user@example.org",
    password="secret",
)
```

Standardmäßig wird HTTPS verwendet. Für lokale Testumgebungen kann mit
`local=True` auf HTTP gewechselt werden.

## Low-Level-Aufrufe

### Assets lesen

```python
assets = nao.list_assets()
asset_detail = nao.get_asset("asset_id")
```

### Asset anlegen

```python
asset = nao.create_asset(
    name="Hausuebergabestation",
    description="Template fuer alle HAST-Instanzen",
    base_interval="2m",
)
asset_id = asset["id"]
```

### Bauteil anlegen

```python
part = nao.create_part(
    asset_id=asset_id,
    name="Waermezaehler",
    description="Gesamtwaermezaehler der Station",
)
part_id = part["_id"]
```

### Sensoren und weitere Reihen anlegen

Die vier unterstützten Reihen-Typen sind:
- `create_sensor(...)`
- `create_meter(...)`
- `create_setpoint(...)`
- `create_actor(...)`

Die Einheit wird über den Namen angegeben. Das Modul prüft automatisch, ob
die Einheit schon vorhanden ist. Falls nicht, wird sie neu angelegt und
direkt verwendet.

```python
sensor = nao.create_sensor(
    asset_id=asset_id,
    name="Vorlauftemperatur",
    description="Momentane Vorlauftemperatur",
    part_id=part_id,
    unit="°C",
    minimum=-20.0,
    maximum=160.0,
    color="#C62828",
)
```

```python
meter = nao.create_meter(
    asset_id=asset_id,
    name="Waermemenge",
    description="Kumulativer Energiezaehler",
    part_id=part_id,
    unit="kWh",
    minimum=0.0,
    color="#7B1FA2",
)
```

### Eigenschaftsgruppen und Eigenschaften

Für Eigenschaften sind aktuell die im TODO geforderten Typen `text` und
`number` umgesetzt.

```python
group = nao.create_attribute_group(
    asset_id=asset_id,
    name="Stammdaten",
    required=True,
    color="#3949ABFF",
)
group_id = group["_id"]
```

```python
nao.create_text_attribute(
    asset_id=asset_id,
    attribute_group_id=group_id,
    name="Ansprechperson",
    description="Vor- und Nachname der verantwortlichen Person",
)
```

```python
nao.create_number_attribute(
    asset_id=asset_id,
    attribute_group_id=group_id,
    name="Regler-ID",
    description="ID aus dem Reglersystem",
    prefix="R-",
    minimum=1,
    maximum=1000000,
)
```

## High-Level-Aufruf für ein komplettes Asset

Wenn ein ganzes Asset-Template in einem Schritt erzeugt werden soll, ist
`create_asset_template(...)` die zentrale Funktion.

```python
result = nao.create_asset_template(
    name="Hausuebergabestation",
    description="Template fuer HAST",
    base_interval="2m",
    parts=[
        {
            "name": "Waermezaehler",
            "description": "Gesamtwaermezaehler",
        },
    ],
    sensors=[
        {
            "name": "Vorlauftemperatur",
            "description": "Momentane Vorlauftemperatur",
            "part": "Waermezaehler",
            "unit": "°C",
            "min": -20.0,
            "max": 160.0,
            "color": "#C62828",
        },
    ],
    meters=[
        {
            "name": "Waermemenge",
            "description": "Kumulativer Energiezaehler",
            "part": "Waermezaehler",
            "unit": "kWh",
            "min": 0.0,
            "color": "#7B1FA2",
        },
    ],
    attribute_groups=[
        {
            "name": "Stammdaten",
            "required": True,
            "color": "#3949ABFF",
        },
    ],
    attributes=[
        {
            "name": "Regler-ID",
            "description": "ID aus dem Reglersystem",
            "attribute_group": "Stammdaten",
            "attribute_type": "number",
            "prefix": "R-",
            "minimum": 1,
            "maximum": 1000000,
        },
        {
            "name": "Ansprechperson",
            "description": "Vor- und Nachname der verantwortlichen Person",
            "attribute_group": "Stammdaten",
            "attribute_type": "text",
        },
    ],
)
```

Der Rückgabewert enthält die Antworten der einzelnen Erstellungsaufrufe:
- `asset`
- `parts`
- `attribute_groups`
- `attributes`
- `sensors`
- `meters`
- `setpoints`
- `actors`

## Datenkonventionen

- Für Serien kann entweder `part` oder `_part` übergeben werden.
- Für Eigenschaftsgruppen kann entweder `attribute_group`, `group`,
  `attributegroup` oder `_attributegroup` verwendet werden.
- Für Einheiten kann entweder `unit` als Name oder `_unit` als bestehende
  Unit-ID übergeben werden.
- Fehlende Einheiten werden automatisch erzeugt.
- Fehlende Bauteile oder Eigenschaftsgruppen können in der High-Level-Funktion
  automatisch nachgezogen werden, wenn sie per Name referenziert werden.

## Bekannte Grenzen

- Das Modul bildet absichtlich nur den in `naocreator_todo.py` beschriebenen
  Umfang ab.
- Für Asset-Eigenschaften sind aktuell nur Text- und Zahlenfelder direkt
  gekapselt. Weitere NAO-Komponenten können bei Bedarf später ergänzt werden.
