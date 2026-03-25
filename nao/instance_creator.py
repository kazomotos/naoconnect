"""
Werkzeuge zum Anlegen von Instanzen und zum Schreiben von Instanz-Metadaten.

Das Modul orientiert sich an den in ``main.py`` und
``naoconnect.fromdb.winmiocs11._helper`` verwendeten Routen und Payloads.
Es deckt damit insbesondere folgende Fälle ab:

- Workspaces lesen und optional anlegen
- einzelne Instanzen anlegen, lesen, anpassen und löschen
- Metadaten als Einzelwert auf eine Instanz schreiben
- Metadaten-Historien vollständig auf eine Instanz schreiben
- vorbereitete Werte über ein Mapping im Stil von ``hast_asset_attributes``
  auf eine Instanz anwenden
"""

from datetime import datetime
from json import dumps
from typing import Any, Dict, List, Optional

from .asset_creator import NaoAssetCreator
from .asset_creator import NaoAssetCreatorError


class NaoInstanceCreator(NaoAssetCreator):
    """
    Connector für konkrete NAO-Instanzen und deren Metadaten.

    Die Authentifizierung und die HTTP-Grundlogik werden vom
    ``NaoAssetCreator`` wiederverwendet. Dieser Connector ergänzt darauf die
    im Projekt für Instanzen relevanten Endpunkte.
    """

    URL_WORKSPACE = "/api/nao/workspace"
    URL_INSTANCE = "/api/nao/instance"
    URL_INSTANCE_MORE = "/api/nao/instance/more/%s"
    URL_INSTANCE_ATTRIBUTEVALUES = "/api/nao/instance/%s?select=attributevalues,_id"
    URL_PATCH_INSTANCE = "/api/nao/instance/%s"
    URL_PATCH_META_INSTANCE = "/api/nao/instance/%s/attributevalues/%s"
    URL_ACTIVATE_DATAPOINT = "/api/nao/instance/%s/datapoints"

    def list_workspaces(self, **filters) -> dict:
        """
        Gibt Workspaces aus NAO zurück.

        Zusätzliche Filter werden im selben ``?query=``-Format wie in der
        vorhandenen Codebasis an die API übergeben.
        """

        return self._request_json(
            self.METHOD_GET,
            self.URL_WORKSPACE + self._build_query(filters),
        )

    def create_workspace(self, name: str, avatar: Optional[str] = None) -> dict:
        """Legt einen neuen Workspace an."""

        payload = {
            "name": name,
            "_avatar": avatar,
        }
        return self._request_json(self.METHOD_POST, self.URL_WORKSPACE, payload)

    def list_instances(self, **filters) -> dict:
        """
        Gibt Instanzen aus NAO zurück.

        Typische Filter sind beispielsweise ``_asset`` oder ``_workspace``.
        """

        return self._request_json(
            self.METHOD_GET,
            self.URL_INSTANCE + self._build_query(filters),
        )

    def get_instance(self, instance_id: str) -> dict:
        """
        Liefert die Detailansicht einer Instanz.

        Die Route entspricht der bereits in ``naoappV2`` verwendeten
        ``/api/nao/instance/more/<instance_id>``-Abfrage.
        """

        return self._request_json(
            self.METHOD_GET,
            self.URL_INSTANCE_MORE % instance_id,
        )

    def get_instance_attribute_values(self, instance_id: str) -> dict:
        """
        Liefert die Attributwerte einer Instanz inklusive der einzelnen
        Attributwert-IDs zurück.
        """

        return self._request_json(
            self.METHOD_GET,
            self.URL_INSTANCE_ATTRIBUTEVALUES % instance_id,
        )

    def create_instance(
        self,
        name: str,
        description: str,
        asset_id: str,
        workspace_id: str,
        geolocation: Optional[List[float]] = None,
        attributevalues: Optional[List[dict]] = None,
    ) -> dict:
        """
        Legt eine konkrete Instanz eines bestehenden Asset-Templates an.

        Die Eingaben entsprechen dem bisherigen Aufruf in ``main.py`` und
        ``winmiocs11/_helper.py``:
        - ``name``: Instanzname, bei Winmiocs11 typischerweise die Regler-ID
        - ``description``: sprechende Beschreibung
        - ``asset_id``: ID des zugehörigen Assets
        - ``workspace_id``: Ziel-Workspace
        - ``geolocation``: optional ``[Längengrad, Breitengrad]``
        - ``attributevalues``: wird aus Kompatibilitätsgründen weitergereicht,
          fachlich sollen Instanz-Metadaten aber anschließend separat per
          History-Patch auf die instanzspezifischen Attributwert-IDs gesetzt
          werden
        """

        payload = {
            "name": name,
            "description": description,
            "_asset": asset_id,
            "_workspace": workspace_id,
            "geolocation": geolocation or [],
            "attributevalues": attributevalues or [],
        }
        return self._request_json(self.METHOD_POST, self.URL_INSTANCE, payload)

    def patch_instance(self, instance_id: str, payload: dict) -> dict:
        """Patcht eine bestehende Instanz mit einem frei definierten Payload."""

        return self._request_json(
            "PATCH",
            self.URL_PATCH_INSTANCE % instance_id,
            payload,
        )

    def delete_instance(self, instance_id: str) -> dict:
        """Löscht eine Instanz."""

        return self._request_json(
            "DELETE",
            "%s/%s" % (self.URL_INSTANCE, instance_id),
        )

    def activate_datapoint(
        self,
        instance_id: str,
        sensor_id: str,
        point_model: str,
        config: Optional[Dict[str, Any]] = None,
    ) -> dict:
        """
        Aktiviert einen im Asset definierten Datenpunkt auf einer Instanz.

        Die NAO-API erwartet dabei:
        - ``pointModel`` als fachlichen Typ, z. B. ``"Sensor"``,
          ``"Meter"``, ``"Setpoint"`` oder ``"Actor"``
        - ``point`` als ID des im Asset angelegten Sensors / Zählers /
          Sollwerts / Aktors
        - ``config`` als JSON-String
        """

        payload = {
            "_point": sensor_id,
            "pointModel": point_model,
            "config": "{}" if config is None else dumps(config),
        }
        return self._request_json(
            self.METHOD_POST,
            self.URL_ACTIVATE_DATAPOINT % instance_id,
            payload,
        )

    def set_instance_meta(
        self,
        instance_id: str,
        meta_id: str,
        value: Any,
        start: Optional[datetime] = None,
    ) -> dict:
        """
        Schreibt einen einzelnen Metadatenwert auf eine Instanz.

        Die API erwartet dafür ebenfalls ein ``history``-Array, selbst wenn nur
        ein einzelner Eintrag geschrieben wird.
        """

        history_item = {
            "value": value,
            "start": self._format_history_start(start or datetime.utcnow()),
        }
        payload = {"history": [history_item]}
        return self._request_json(
            "PATCH",
            self.URL_PATCH_META_INSTANCE % (instance_id, meta_id),
            payload,
        )

    def set_instance_meta_history(
        self,
        instance_id: str,
        meta_id: str,
        history: List[dict],
    ) -> dict:
        """
        Überschreibt die komplette Historie eines Metadatums.

        Wichtig:
        Die NAO-API behandelt diese Operation als Vollersetzung. Wenn ältere
        Einträge erhalten bleiben sollen, müssen sie in ``history`` erneut
        mitgegeben werden.
        """

        payload = {
            "history": [self._normalize_history_item(item) for item in history],
        }
        return self._request_json(
            "PATCH",
            self.URL_PATCH_META_INSTANCE % (instance_id, meta_id),
            payload,
        )

    def build_attributevalue_entry(
        self,
        attribute_id: str,
        value: Any,
        name: str = "",
        description: str = "",
    ) -> dict:
        """
        Baut einen Eintrag für ``attributevalues`` beim Anlegen einer Instanz.

        Hinweis:
        In diesem Projekt sollen Metadaten bevorzugt *nicht* direkt beim
        Anlegen gesetzt werden, sondern anschließend separat über die
        instanzspezifischen Attributwert-IDs per History-Patch.
        """

        return {
            "_attribute": attribute_id,
            "name": name,
            "description": description,
            "value": value,
        }

    def apply_metadata_mapping(
        self,
        instance_id: str,
        metadata_values: Dict[str, Any],
        attribute_config: Dict[str, dict],
        default_start: Optional[datetime] = None,
        skip_none_values: bool = True,
    ) -> List[dict]:
        """
        Wendet vorbereitete Metadaten auf eine bestehende Instanz an.

        Das ``attribute_config`` ist für das in ``main.py`` verwendete Mapping
        gedacht, also beispielsweise:

        ``{
            "siocs.partner.id": {
                "name": "Regler-ID",
                "_attribute": "...",
                "type": int,
            }
        }``

        Die ``metadata_values`` dürfen entweder über die fachlichen
        Config-Schlüssel oder direkt über die NAO-Metadatennamen adressiert
        werden.

        Regeln:
        - skalare Werte werden mit ``set_instance_meta(...)`` geschrieben
        - Listen werden als Historie interpretiert und vollständig mit
          ``set_instance_meta_history(...)`` geschrieben
        - Werte werden, soweit möglich, auf den in ``type`` hinterlegten Typ
          normalisiert
        """

        results = []
        fallback_start = default_start or datetime.utcnow()

        for config_key, config in attribute_config.items():
            meta_id = config.get("_attribute")
            if not meta_id:
                continue

            meta_name = config.get("name")
            expected_type = config.get("type")
            has_value = False
            raw_value = None

            if config_key in metadata_values:
                raw_value = metadata_values[config_key]
                has_value = True
            elif meta_name in metadata_values:
                raw_value = metadata_values[meta_name]
                has_value = True

            if not has_value:
                continue
            if raw_value is None and skip_none_values:
                continue

            if isinstance(raw_value, list):
                history = self._coerce_history(raw_value, expected_type, fallback_start)
                result = self.set_instance_meta_history(
                    instance_id=instance_id,
                    meta_id=meta_id,
                    history=history,
                )
            else:
                coerced_value = self._coerce_meta_value(raw_value, expected_type)
                if coerced_value is None and skip_none_values:
                    continue
                result = self.set_instance_meta(
                    instance_id=instance_id,
                    meta_id=meta_id,
                    value=coerced_value,
                    start=fallback_start,
                )

            results.append(
                {
                    "config_key": config_key,
                    "meta_name": meta_name,
                    "meta_id": meta_id,
                    "result": result,
                }
            )

        return results

    def apply_metadata_mapping_from_instance(
        self,
        instance_data: Dict[str, Any],
        metadata_values: Dict[str, Any],
        attribute_config: Dict[str, dict],
        default_start: Optional[datetime] = None,
        skip_none_values: bool = True,
    ) -> List[dict]:
        """
        Wendet Metadaten auf Basis der instanzspezifischen Attributwert-IDs an.

        Wichtig:
        Beim Erzeugen einer Instanz liefert NAO die konkreten Attributwerte der
        Instanz zurück. Für spätere History-Patches muss die ID dieses
        Instanz-Attributwerts verwendet werden und nicht die Asset-Attribut-ID.
        Genau diesen Fall kapselt diese Methode.
        """

        instance_id = instance_data.get("_id") or instance_data.get("id")
        if not instance_id:
            raise NaoAssetCreatorError(
                "Die Instanzdaten enthalten keine Instanz-ID: %r" % (instance_data,)
            )

        attributevalues = instance_data.get("attributevalues")
        if not isinstance(attributevalues, list) or not attributevalues:
            attributevalues_response = self.get_instance_attribute_values(instance_id)
            attributevalues = attributevalues_response.get("attributevalues", [])

        instance_value_ids_by_attribute: Dict[str, str] = {}
        instance_value_ids_by_name: Dict[str, str] = {}
        for attributevalue in attributevalues:
            if not isinstance(attributevalue, dict):
                continue
            instance_value_id = attributevalue.get("_id") or attributevalue.get("id")
            asset_attribute_id = attributevalue.get("_attribute")
            attribute_name = attributevalue.get("name")
            if isinstance(asset_attribute_id, str) and isinstance(instance_value_id, str):
                instance_value_ids_by_attribute[asset_attribute_id] = instance_value_id
            if isinstance(attribute_name, str) and isinstance(instance_value_id, str):
                instance_value_ids_by_name[attribute_name] = instance_value_id

        results = []
        fallback_start = default_start or datetime.utcnow()

        for config_key, config in attribute_config.items():
            asset_attribute_id = config.get("_attribute")
            meta_name = config.get("name")
            expected_type = config.get("type")
            has_value = False
            raw_value = None

            if config_key in metadata_values:
                raw_value = metadata_values[config_key]
                has_value = True
            elif meta_name in metadata_values:
                raw_value = metadata_values[meta_name]
                has_value = True

            if not has_value:
                continue
            if raw_value is None and skip_none_values:
                continue

            instance_meta_id = None
            if isinstance(asset_attribute_id, str):
                instance_meta_id = instance_value_ids_by_attribute.get(asset_attribute_id)
            if instance_meta_id is None and isinstance(meta_name, str):
                instance_meta_id = instance_value_ids_by_name.get(meta_name)
            if not instance_meta_id:
                continue

            if isinstance(raw_value, list):
                history = self._coerce_history(raw_value, expected_type, fallback_start)
                result = self.set_instance_meta_history(
                    instance_id=instance_id,
                    meta_id=instance_meta_id,
                    history=history,
                )
            else:
                coerced_value = self._coerce_meta_value(raw_value, expected_type)
                if coerced_value is None and skip_none_values:
                    continue
                result = self.set_instance_meta(
                    instance_id=instance_id,
                    meta_id=instance_meta_id,
                    value=coerced_value,
                    start=fallback_start,
                )

            results.append(
                {
                    "config_key": config_key,
                    "meta_name": meta_name,
                    "meta_id": instance_meta_id,
                    "result": result,
                }
            )

        return results

    def create_instance_with_metadata(
        self,
        name: str,
        description: str,
        asset_id: str,
        workspace_id: str,
        geolocation: Optional[List[float]] = None,
        attributevalues: Optional[List[dict]] = None,
        metadata_values: Optional[Dict[str, Any]] = None,
        attribute_config: Optional[Dict[str, dict]] = None,
        default_meta_start: Optional[datetime] = None,
    ) -> dict:
        """
        Legt eine Instanz an und schreibt anschließend optional Metadaten.

        Diese Funktion bündelt den Ablauf aus dem Winmiocs11-Kontext:
        1. Instanz erzeugen
        2. Instanz-ID aus der Antwort lesen
        3. fachlich vorbereitete Metadaten mit dem Attribut-Mapping auf die
           Instanz schreiben
        """

        created_instance = self.create_instance(
            name=name,
            description=description,
            asset_id=asset_id,
            workspace_id=workspace_id,
            geolocation=geolocation,
            attributevalues=[],
        )

        instance_id = created_instance.get("_id") or created_instance.get("id")
        if not instance_id:
            raise NaoAssetCreatorError(
                "Die Instanz-Erstellung hat keine Instanz-ID geliefert: %r"
                % (created_instance,)
            )

        metadata_results = []
        if metadata_values and attribute_config:
            metadata_results = self.apply_metadata_mapping_from_instance(
                instance_data=created_instance,
                metadata_values=metadata_values,
                attribute_config=attribute_config,
                default_start=default_meta_start,
            )

        return {
            "instance": created_instance,
            "metadata": metadata_results,
        }

    def _coerce_history(
        self,
        history: List[Any],
        expected_type: Any,
        fallback_start: datetime,
    ) -> List[dict]:
        normalized = []
        for item in history:
            if isinstance(item, dict):
                value = self._coerce_meta_value(item.get("value"), expected_type)
                start = item.get("start", fallback_start)
            else:
                value = self._coerce_meta_value(item, expected_type)
                start = fallback_start
            normalized.append(
                {
                    "value": value,
                    "start": start,
                }
            )
        return normalized

    def _coerce_meta_value(self, value: Any, expected_type: Any) -> Any:
        if value is None or expected_type is None:
            return value

        if expected_type is str:
            return str(value)

        if expected_type is int:
            if isinstance(value, bool):
                raise ValueError("Bool kann nicht verlustfrei in int-Metadaten übertragen werden.")
            if isinstance(value, int):
                return value
            text = str(value).strip()
            if text == "":
                return None
            return int(float(text))

        if expected_type is float:
            if isinstance(value, bool):
                raise ValueError("Bool kann nicht verlustfrei in float-Metadaten übertragen werden.")
            if isinstance(value, (int, float)):
                return float(value)
            text = str(value).strip().replace(",", ".")
            if text == "":
                return None
            return float(text)

        if expected_type is bool:
            if isinstance(value, bool):
                return value
            text = str(value).strip().lower()
            if text in ("1", "true", "ja", "yes", "y"):
                return True
            if text in ("0", "false", "nein", "no", "n"):
                return False
            raise ValueError("Wert '%s' kann nicht als bool interpretiert werden." % value)

        try:
            return expected_type(value)
        except Exception:
            return value

    def _normalize_history_item(self, item: dict) -> dict:
        if "value" not in item:
            raise ValueError("History-Eintrag ohne 'value' ist nicht zulässig.")

        start = item.get("start")
        if start is None:
            raise ValueError("History-Eintrag ohne 'start' ist nicht zulässig.")

        return {
            "value": item["value"],
            "start": self._format_history_start(start),
        }

    def _format_history_start(self, start: Any) -> str:
        if isinstance(start, datetime):
            return start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        return str(start)
