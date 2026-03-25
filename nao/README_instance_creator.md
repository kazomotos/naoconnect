# NAO Instance Creator

Dieses Modul ergänzt den Asset-Connector um den zweiten praktischen Teil:
konkrete Instanzen anlegen und Metadaten auf diese Instanzen schreiben.

Die Methoden orientieren sich direkt an den im Projekt bereits verwendeten
Routen aus:
- `main.py`
- `naoconnect/naoappV2.py`
- `naoconnect/fromdb/winmiocs11/_helper.py`

## Import

```python
from naoconnect.nao import NaoInstanceCreator
```

## Initialisierung

```python
from naoconnect.nao import NaoInstanceCreator

nao = NaoInstanceCreator(
    host="aura.nao-cloud.de",
    email="user@example.org",
    password="secret",
)
```

## Unterstützte Routen

- `GET /api/nao/workspace`
- `POST /api/nao/workspace`
- `GET /api/nao/instance`
- `GET /api/nao/instance/more/<instance_id>`
- `GET /api/nao/instance/<instance_id>?select=attributevalues,_id`
- `POST /api/nao/instance`
- `PATCH /api/nao/instance/<instance_id>`
- `PATCH /api/nao/instance/<instance_id>/attributevalues/<meta_id>`
- `DELETE /api/nao/instance/<instance_id>`

## Instanz anlegen

Die Eingaben sind identisch zu dem, was in `main.py` und im Winmiocs11-Helper
bereits genutzt wird.

```python
instance = nao.create_instance(
    name="123",
    description="Station aus Schneid",
    asset_id="asset_id_hast",
    workspace_id="workspace_id_hast",
    geolocation=[11.02, 50.98],
    attributevalues=[],
)
instance_id = instance["_id"]
```

## Metadaten als Einzelwert schreiben

Diese Funktion entspricht fachlich dem bisherigen
`NaoConnect.patchInstanceMeta(...)`.

```python
nao.set_instance_meta(
    instance_id=instance_id,
    meta_id="6991b249d17f698e65b48c4f",
    value=123,
)
```

## Metadaten-Historie schreiben

Diese Funktion ersetzt die komplette Historie des Ziel-Metadatums.

```python
nao.set_instance_meta_history(
    instance_id=instance_id,
    meta_id="6991b57ed17f698e65b48f10",
    history=[
        {
            "start": "2026-03-04T12:00:00.000Z",
            "value": "Beispielnotiz",
        },
        {
            "start": "2026-03-18T12:00:00.000Z",
            "value": "Weitere Notiz",
        },
    ],
)
```

## Mapping wie in `main.py` verwenden

Das Modul kann direkt mit einem Mapping im Stil von `hast_asset_attributes`
arbeiten. Dabei darf der Eingabe-Datensatz entweder über die fachlichen
Schlüssel oder direkt über die NAO-Metadatennamen adressiert sein.

Beispiel für das Attribut-Mapping:

```python
hast_asset_attributes = {
    "siocs.partner.id": {
        "name": "Regler-ID",
        "_attribute": "6991b249d17f698e65b48c4f",
        "type": int,
    },
    "siocs.partner.name": {
        "name": "Ansprechperson",
        "_attribute": "6991b434d17f698e65b48e35",
        "type": str,
    },
    "siocs.lognote.text": {
        "name": "Notiz",
        "_attribute": "6991b57ed17f698e65b48f10",
        "type": str,
    },
}
```

Beispiel für die Werte:

```python
metadata_values = {
    "siocs.partner.id": 123,
    "Ansprechperson": "Max Mustermann",
    "siocs.lognote.text": [
        {
            "start": "2026-03-04T12:00:00.000Z",
            "value": "Erste Notiz",
        },
        {
            "start": "2026-03-18T12:00:00.000Z",
            "value": "Zweite Notiz",
        },
    ],
}
```

Anwenden auf eine bestehende Instanz:

```python
results = nao.apply_metadata_mapping(
    instance_id=instance_id,
    metadata_values=metadata_values,
    attribute_config=hast_asset_attributes,
)
```

## Instanz in einem Schritt anlegen und Metadaten schreiben

```python
result = nao.create_instance_with_metadata(
    name="123",
    description="Station aus Schneid",
    asset_id="asset_id_hast",
    workspace_id="workspace_id_hast",
    geolocation=[11.02, 50.98],
    metadata_values=metadata_values,
    attribute_config=hast_asset_attributes,
)
```

Der Rückgabewert enthält:
- `instance`: Antwort des Erstellungsaufrufs
- `metadata`: Liste der einzelnen Metadaten-Schreiboperationen

## Hinweise

- Listen in `metadata_values` werden als Historienfelder interpretiert.
- Skalare Werte werden als Einzelwert geschrieben.
- Historienaufrufe überschreiben die komplette Historie in NAO.
- Die Typen aus dem Mapping werden so weit wie möglich auf Python-Werte
  normalisiert, bevor sie an NAO gesendet werden.
