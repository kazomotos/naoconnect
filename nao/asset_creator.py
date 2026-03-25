"""
Werkzeuge zum Anlegen von Asset-Templates in NAO.

Das Modul kapselt genau die im Projekt-TODO beschriebenen NAO-Endpunkte
für Assets, Bauteile, Sensoren, Eigenschaftsgruppen, Eigenschaften und
globale Einheiten. Die Methoden sind bewusst klein gehalten, damit sie
einzeln oder über die High-Level-Funktion ``create_asset_template`` genutzt
werden können.
"""

import http.client
from copy import copy
from json import dumps, loads
from time import sleep
from typing import Dict, List, Optional
from urllib.parse import quote


class NaoAssetCreatorError(RuntimeError):
    """Fehler beim Zugriff auf die NAO-API."""


class NaoAssetCreator(object):
    """
    Schlanker Connector für das Anlegen von Asset-Templates in NAO.

    Der Connector konzentriert sich auf die im TODO beschriebenen
    Erstellungsfälle:
    - Assets auflisten und Detailinformationen lesen
    - neue Assets anlegen
    - Bauteile innerhalb eines Assets anlegen
    - Sensoren, Zähler, Sollwerte und Aktoren anlegen
    - Eigenschaftsgruppen und Eigenschaften anlegen
    - globale Einheiten auflösen oder bei Bedarf neu anlegen

    Parameter:
        host:
            Hostname der NAO-Instanz, zum Beispiel ``"aura.nao-cloud.de"``.
        email:
            Benutzername für den Login.
        password:
            Passwort für den Login.
        local:
            Wenn ``True``, wird eine unverschlüsselte HTTP-Verbindung
            verwendet. Standardmäßig wird HTTPS genutzt.
        timeout:
            Timeout pro HTTP-Anfrage in Sekunden.
    """

    URL_LOGIN = "/api/user/auth/login"
    URL_ASSET = "/api/nao/asset"
    URL_ASSET_MORE = "/api/nao/asset/more/%s"
    URL_UNITS_LIST = "/api/nao/units/"
    URL_UNITS_CREATE = "/api/nao/units"
    QUERY_GET = "?query="
    QUERY_BEARER = "Bearer "
    NAME_ACCESS_TOKEN = "accessToken"
    NAME_AUTHORIZATION = "Authorization"
    NAME_CONTENT_TYPE = "Content-Type"
    NAME_UTF8 = "utf-8"
    METHOD_GET = "GET"
    METHOD_POST = "POST"
    TRANSFER_HEADERS = {
        "Authorization": "",
        "Content-Type": "text/plain",
        "Cookie": "",
    }
    LOGIN_HEADERS = {
        "Content-Type": "application/x-www-form-urlencoded",
    }
    JSON_CONTENT_TYPE = "application/json"
    DEFAULT_UNIT = "-"
    DEFAULT_ASSET_COLOR = "#02C1DE"
    DEFAULT_ATTRIBUTE_GROUP_COLOR = "#FF0000FF"

    SERIES_ENDPOINTS = {
        "sensor": "sensors",
        "sensors": "sensors",
        "meter": "meters",
        "meters": "meters",
        "setpoint": "setpoints",
        "setpoints": "setpoints",
        "actor": "actors",
        "actors": "actors",
    }

    ATTRIBUTE_COMPONENTS = {
        "text": "input-text-field",
        "number": "input-number-field",
    }

    def __init__(
        self,
        host: str,
        email: str,
        password: str,
        local: bool = False,
        timeout: int = 120,
    ) -> None:
        self.host = host
        self.local = local
        self.timeout = timeout
        self.headers = copy(self.TRANSFER_HEADERS)
        self._login_payload = "email=%s&password=%s" % (
            quote(email),
            quote(password),
        )

    def list_assets(self, **filters) -> dict:
        """
        Gibt die Asset-Liste von NAO zurück.

        Zusätzliche Keyword-Argumente werden in den von der bestehenden
        Codebasis genutzten ``?query=``-String umgewandelt.
        """

        return self._request_json(
            self.METHOD_GET,
            self.URL_ASSET + self._build_query(filters),
        )

    def get_asset(self, asset_id: str) -> dict:
        """
        Liefert die Detailansicht eines einzelnen Assets inklusive
        Bauteilen, Sensoren, Attributgruppen und Eigenschaften.
        """

        return self._request_json(
            self.METHOD_GET,
            self.URL_ASSET_MORE % asset_id,
        )

    def create_asset(
        self,
        name: str,
        description: str = "",
        base_interval: str = "2m",
    ) -> dict:
        """
        Legt ein neues leeres Asset-Template an.

        ``base_interval`` entspricht dem in NAO erwarteten Wert wie
        ``"2m"``, ``"15m"`` oder ``"1h"``.
        """

        payload = {
            "name": name,
            "description": description,
            "baseInterval": base_interval,
        }
        return self._request_json(self.METHOD_POST, self.URL_ASSET, payload)

    def create_part(
        self,
        asset_id: str,
        name: str,
        description: str = "",
    ) -> dict:
        """
        Legt ein Bauteil innerhalb eines Assets an.

        Bauteile dienen in NAO primär der Strukturierung des
        Asset-Templates.
        """

        payload = {
            "name": name,
            "description": description,
        }
        return self._request_json(
            self.METHOD_POST,
            "%s/%s/parts" % (self.URL_ASSET, asset_id),
            payload,
        )

    def list_units(self, **filters) -> dict:
        """
        Gibt die globalen Einheiten aus NAO zurück.

        In der Regel ist es sinnvoll, Einheiten lokal anhand des Namens zu
        vergleichen, weil die Anzahl meist klein ist.
        """

        return self._request_json(
            self.METHOD_GET,
            self.URL_UNITS_LIST + self._build_query(filters),
        )

    def create_unit(self, name: str) -> dict:
        """
        Legt eine globale Einheit in NAO an.

        Der verwendete POST-Endpunkt entspricht der bereits vorhandenen
        ``naoconnect.NaoApp.createUnit``-Implementierung.
        """

        payload = {"name": name}
        return self._request_json(self.METHOD_POST, self.URL_UNITS_CREATE, payload)

    def ensure_unit(self, name: Optional[str]) -> dict:
        """
        Gibt eine vorhandene Einheit zurück oder legt sie neu an.

        Ist ``name`` leer oder ``None``, wird ``"-"`` verwendet.
        """

        unit_name = name or self.DEFAULT_UNIT
        units = self.list_units().get("results", [])
        for unit in units:
            if unit.get("name") == unit_name:
                return unit
        return self.create_unit(unit_name)

    def create_sensor(
        self,
        asset_id: str,
        name: str,
        description: str,
        part_id: str,
        unit: Optional[str] = None,
        minimum: Optional[float] = None,
        maximum: Optional[float] = None,
        color: str = DEFAULT_ASSET_COLOR,
    ) -> dict:
        """Legt einen Standard-Sensor im Asset an."""

        return self._create_series(
            asset_id=asset_id,
            series_type="sensor",
            name=name,
            description=description,
            part_id=part_id,
            unit=unit,
            minimum=minimum,
            maximum=maximum,
            color=color,
        )

    def create_meter(
        self,
        asset_id: str,
        name: str,
        description: str,
        part_id: str,
        unit: Optional[str] = None,
        minimum: Optional[float] = None,
        maximum: Optional[float] = None,
        color: str = DEFAULT_ASSET_COLOR,
    ) -> dict:
        """Legt einen kumulativen Zählerwert im Asset an."""

        return self._create_series(
            asset_id=asset_id,
            series_type="meter",
            name=name,
            description=description,
            part_id=part_id,
            unit=unit,
            minimum=minimum,
            maximum=maximum,
            color=color,
        )

    def create_setpoint(
        self,
        asset_id: str,
        name: str,
        description: str,
        part_id: str,
        unit: Optional[str] = None,
        minimum: Optional[float] = None,
        maximum: Optional[float] = None,
        color: str = DEFAULT_ASSET_COLOR,
    ) -> dict:
        """Legt einen Sollwert im Asset an."""

        return self._create_series(
            asset_id=asset_id,
            series_type="setpoint",
            name=name,
            description=description,
            part_id=part_id,
            unit=unit,
            minimum=minimum,
            maximum=maximum,
            color=color,
        )

    def create_actor(
        self,
        asset_id: str,
        name: str,
        description: str,
        part_id: str,
        unit: Optional[str] = None,
        minimum: Optional[float] = None,
        maximum: Optional[float] = None,
        color: str = DEFAULT_ASSET_COLOR,
    ) -> dict:
        """Legt einen Aktor beziehungsweise Stellwert im Asset an."""

        return self._create_series(
            asset_id=asset_id,
            series_type="actor",
            name=name,
            description=description,
            part_id=part_id,
            unit=unit,
            minimum=minimum,
            maximum=maximum,
            color=color,
        )

    def create_attribute_group(
        self,
        asset_id: str,
        name: str,
        required: bool = True,
        color: str = DEFAULT_ATTRIBUTE_GROUP_COLOR,
    ) -> dict:
        """
        Legt eine Eigenschaftsgruppe für Stammdaten innerhalb eines Assets an.
        """

        payload = {
            "name": name,
            "required": required,
            "color": color,
        }
        return self._request_json(
            self.METHOD_POST,
            "%s/%s/attributegroups" % (self.URL_ASSET, asset_id),
            payload,
        )

    def create_text_attribute(
        self,
        asset_id: str,
        attribute_group_id: str,
        name: str,
        description: str,
        required: bool = False,
        value=None,
        prefix: str = "",
        suffix: str = "",
        clearable: bool = False,
        placeholder: str = "",
        is_email: bool = False,
        max_chars: Optional[int] = None,
        min_chars: Optional[int] = None,
    ) -> dict:
        """Legt eine Text-Eigenschaft an."""

        return self.create_attribute(
            asset_id=asset_id,
            attribute_group_id=attribute_group_id,
            name=name,
            description=description,
            attribute_type="text",
            required=required,
            value=value,
            prefix=prefix,
            suffix=suffix,
            clearable=clearable,
            placeholder=placeholder,
            is_email=is_email,
            max_chars=max_chars,
            min_chars=min_chars,
        )

    def create_number_attribute(
        self,
        asset_id: str,
        attribute_group_id: str,
        name: str,
        description: str,
        required: bool = False,
        value=None,
        prefix: str = "",
        suffix: str = "",
        clearable: bool = False,
        placeholder: str = "",
        minimum: Optional[float] = None,
        maximum: Optional[float] = None,
    ) -> dict:
        """Legt eine numerische Eigenschaft an."""

        return self.create_attribute(
            asset_id=asset_id,
            attribute_group_id=attribute_group_id,
            name=name,
            description=description,
            attribute_type="number",
            required=required,
            value=value,
            prefix=prefix,
            suffix=suffix,
            clearable=clearable,
            placeholder=placeholder,
            minimum=minimum,
            maximum=maximum,
        )

    def create_attribute(
        self,
        asset_id: str,
        attribute_group_id: str,
        name: str,
        description: str,
        attribute_type: str = "text",
        required: bool = False,
        value=None,
        prefix: str = "",
        suffix: str = "",
        clearable: bool = False,
        placeholder: str = "",
        is_email: bool = False,
        max_chars: Optional[int] = None,
        min_chars: Optional[int] = None,
        minimum: Optional[float] = None,
        maximum: Optional[float] = None,
    ) -> dict:
        """
        Legt eine Eigenschaft im Asset an.

        Unterstützt aktuell die im TODO genannten Typen:
        - ``text`` für ``input-text-field``
        - ``number`` für ``input-number-field``
        """

        if attribute_type not in self.ATTRIBUTE_COMPONENTS:
            raise ValueError(
                "Unbekannter attribute_type '%s'. Erlaubt sind: %s"
                % (attribute_type, ", ".join(sorted(self.ATTRIBUTE_COMPONENTS)))
            )

        payload = {
            "_attributegroup": attribute_group_id,
            "value": value,
            "field": "",
            "component": self.ATTRIBUTE_COMPONENTS[attribute_type],
            "name": name,
            "description": description,
        }

        if attribute_type == "text":
            payload["rules"] = {
                "required": required,
                "isEmail": is_email,
                "maxChars": max_chars,
                "minChars": min_chars,
            }
            payload["props"] = {
                "prefix": prefix,
                "suffix": suffix,
                "clearable": clearable,
                "placeholder": placeholder,
            }
        else:
            payload["rules"] = {
                "required": required,
                "isNumber": True,
            }
            payload["props"] = {
                "prefix": prefix,
                "suffix": suffix,
                "clearable": clearable,
                "min": minimum,
                "max": maximum,
                "placeholder": placeholder,
            }

        return self._request_json(
            self.METHOD_POST,
            "%s/%s/attributes" % (self.URL_ASSET, asset_id),
            payload,
        )

    def create_asset_template(
        self,
        name: str,
        description: str = "",
        base_interval: str = "2m",
        parts: Optional[List[dict]] = None,
        sensors: Optional[List[dict]] = None,
        meters: Optional[List[dict]] = None,
        setpoints: Optional[List[dict]] = None,
        actors: Optional[List[dict]] = None,
        attribute_groups: Optional[List[dict]] = None,
        attributes: Optional[List[dict]] = None,
    ) -> dict:
        """
        Legt ein komplettes Asset-Template inklusive Unterstrukturen an.

        Erwartete Eingaben:
        - ``parts``: Liste mit ``{"name": ..., "description": ...}``
        - ``attribute_groups``: Liste mit ``{"name": ..., "required": ...}``
        - ``attributes``: Liste mit mindestens ``name``, ``description`` und
          entweder ``attribute_group`` oder ``_attributegroup``
        - ``sensors``, ``meters``, ``setpoints``, ``actors``: Listen mit
          mindestens ``name``, ``description`` und entweder ``part`` oder
          ``_part``. Für die Einheit kann ``unit`` oder ``_unit`` verwendet
          werden.

        Die Methode erstellt fehlende Einheiten automatisch. Wenn eine Serie
        oder Eigenschaft per Namen auf ein noch nicht angelegtes Bauteil oder
        eine noch nicht angelegte Eigenschaftsgruppe verweist, wird diese
        Struktur ebenfalls zuerst erzeugt.
        """

        parts = parts or []
        sensors = sensors or []
        meters = meters or []
        setpoints = setpoints or []
        actors = actors or []
        attribute_groups = attribute_groups or []
        attributes = attributes or []

        created_asset = self.create_asset(
            name=name,
            description=description,
            base_interval=base_interval,
        )
        asset_id = created_asset.get("id") or created_asset.get("_id")
        if not asset_id:
            raise NaoAssetCreatorError(
                "Die Asset-Erstellung hat keine Asset-ID geliefert: %r"
                % (created_asset,)
            )

        part_ids = {}
        created_parts = []
        for part in parts:
            created_part = self.create_part(
                asset_id=asset_id,
                name=part["name"],
                description=part.get("description", ""),
            )
            created_parts.append(created_part)
            if created_part.get("_id"):
                part_ids[part["name"]] = created_part["_id"]

        attribute_group_ids = {}
        created_attribute_groups = []
        for group in attribute_groups:
            created_group = self.create_attribute_group(
                asset_id=asset_id,
                name=group["name"],
                required=group.get("required", True),
                color=group.get("color", self.DEFAULT_ATTRIBUTE_GROUP_COLOR),
            )
            created_attribute_groups.append(created_group)
            if created_group.get("_id"):
                attribute_group_ids[group["name"]] = created_group["_id"]

        created_attributes = []
        for attribute in attributes:
            group_id = attribute.get("_attributegroup")
            group_name = (
                attribute.get("attribute_group")
                or attribute.get("group")
                or attribute.get("attributegroup")
            )
            if not group_id:
                group_id = attribute_group_ids.get(group_name)
            if not group_id and group_name:
                created_group = self.create_attribute_group(asset_id, group_name)
                created_attribute_groups.append(created_group)
                group_id = created_group.get("_id")
                if group_id:
                    attribute_group_ids[group_name] = group_id
            if not group_id:
                raise ValueError(
                    "Für die Eigenschaft '%s' fehlt eine Eigenschaftsgruppe."
                    % attribute.get("name", "<unbekannt>")
                )

            created_attribute = self.create_attribute(
                asset_id=asset_id,
                attribute_group_id=group_id,
                name=attribute["name"],
                description=attribute.get("description", ""),
                attribute_type=attribute.get("attribute_type", "text"),
                required=attribute.get("required", False),
                value=attribute.get("value"),
                prefix=attribute.get("prefix", ""),
                suffix=attribute.get("suffix", ""),
                clearable=attribute.get("clearable", False),
                placeholder=attribute.get("placeholder", ""),
                is_email=attribute.get("is_email", False),
                max_chars=attribute.get("max_chars"),
                min_chars=attribute.get("min_chars"),
                minimum=attribute.get("minimum"),
                maximum=attribute.get("maximum"),
            )
            created_attributes.append(created_attribute)

        created_sensors = self._create_series_collection(
            asset_id=asset_id,
            series_type="sensor",
            items=sensors,
            known_part_ids=part_ids,
            created_parts=created_parts,
        )
        created_meters = self._create_series_collection(
            asset_id=asset_id,
            series_type="meter",
            items=meters,
            known_part_ids=part_ids,
            created_parts=created_parts,
        )
        created_setpoints = self._create_series_collection(
            asset_id=asset_id,
            series_type="setpoint",
            items=setpoints,
            known_part_ids=part_ids,
            created_parts=created_parts,
        )
        created_actors = self._create_series_collection(
            asset_id=asset_id,
            series_type="actor",
            items=actors,
            known_part_ids=part_ids,
            created_parts=created_parts,
        )

        return {
            "asset": created_asset,
            "parts": created_parts,
            "attribute_groups": created_attribute_groups,
            "attributes": created_attributes,
            "sensors": created_sensors,
            "meters": created_meters,
            "setpoints": created_setpoints,
            "actors": created_actors,
        }

    def _create_series_collection(
        self,
        asset_id: str,
        series_type: str,
        items: List[dict],
        known_part_ids: Dict[str, str],
        created_parts: List[dict],
    ) -> List[dict]:
        created_items = []
        for item in items:
            part_id = item.get("_part")
            part_name = item.get("part") or item.get("part_name")
            if not part_id and part_name:
                part_id = known_part_ids.get(part_name)
            if not part_id and part_name:
                created_part = self.create_part(
                    asset_id=asset_id,
                    name=part_name,
                    description=item.get("part_description", ""),
                )
                created_parts.append(created_part)
                part_id = created_part.get("_id")
                if part_id:
                    known_part_ids[part_name] = part_id
            if not part_id:
                raise ValueError(
                    "Für %s '%s' fehlt ein Bauteil."
                    % (series_type, item.get("name", "<unbekannt>"))
                )

            created_item = self._create_series(
                asset_id=asset_id,
                series_type=series_type,
                name=item["name"],
                description=item.get("description", ""),
                part_id=part_id,
                unit=item.get("unit"),
                unit_id=item.get("_unit"),
                minimum=item.get("min", item.get("minimum")),
                maximum=item.get("max", item.get("maximum")),
                color=item.get("color", self.DEFAULT_ASSET_COLOR),
            )
            created_items.append(created_item)
        return created_items

    def _create_series(
        self,
        asset_id: str,
        series_type: str,
        name: str,
        description: str,
        part_id: str,
        unit: Optional[str] = None,
        unit_id: Optional[str] = None,
        minimum: Optional[float] = None,
        maximum: Optional[float] = None,
        color: str = DEFAULT_ASSET_COLOR,
    ) -> dict:
        endpoint = self.SERIES_ENDPOINTS.get(series_type)
        if not endpoint:
            raise ValueError(
                "Unbekannter series_type '%s'. Erlaubt sind: %s"
                % (series_type, ", ".join(sorted(self.SERIES_ENDPOINTS)))
            )

        resolved_unit_id = unit_id or self._resolve_unit_id(unit)
        payload = {
            "name": name,
            "description": description,
            "_part": part_id,
            "_unit": resolved_unit_id,
            "color": color,
        }
        if minimum is not None:
            payload["min"] = minimum
        if maximum is not None:
            payload["max"] = maximum

        return self._request_json(
            self.METHOD_POST,
            "%s/%s/%s" % (self.URL_ASSET, asset_id, endpoint),
            payload,
        )

    def _resolve_unit_id(self, unit: Optional[str]) -> str:
        ensured_unit = self.ensure_unit(unit)
        unit_id = ensured_unit.get("_id") or ensured_unit.get("id")
        if not unit_id:
            raise NaoAssetCreatorError(
                "Für die Einheit '%s' konnte keine Unit-ID ermittelt werden."
                % (unit or self.DEFAULT_UNIT)
            )
        return unit_id

    def _build_query(self, filters: dict) -> str:
        if not filters:
            return ""
        query_parts = []
        for key, value in filters.items():
            query_parts.append("%s=%s" % (key, value))
        return self.QUERY_GET + ",".join(query_parts)

    def _connect(self):
        if self.local:
            return http.client.HTTPConnection(self.host, timeout=self.timeout)
        return http.client.HTTPSConnection(self.host, timeout=self.timeout)

    def _login(self) -> None:
        connection = self._connect()
        try:
            connection.request(
                self.METHOD_POST,
                self.URL_LOGIN,
                self._login_payload,
                self.LOGIN_HEADERS,
            )
            response = connection.getresponse()
            raw_data = response.read()
        except Exception as exc:
            connection.close()
            raise NaoAssetCreatorError("Login zu NAO fehlgeschlagen: %s" % exc)
        finally:
            try:
                connection.close()
            except Exception:
                pass

        if response.status >= 400:
            raise NaoAssetCreatorError(
                "Login zu NAO fehlgeschlagen: HTTP %s - %s"
                % (response.status, raw_data.decode(self.NAME_UTF8, errors="replace"))
            )

        try:
            data = loads(raw_data.decode(self.NAME_UTF8))
        except Exception as exc:
            raise NaoAssetCreatorError(
                "Login-Antwort konnte nicht gelesen werden: %s" % exc
            )

        token = data.get(self.NAME_ACCESS_TOKEN)
        if not token:
            raise NaoAssetCreatorError(
                "Login erfolgreich, aber kein Zugriffstoken erhalten: %r" % (data,)
            )
        self.headers[self.NAME_AUTHORIZATION] = self.QUERY_BEARER + token

    def _request_json(self, method: str, url: str, payload: Optional[dict] = None):
        if not self.headers.get(self.NAME_AUTHORIZATION):
            self._login()

        encoded_payload = dumps(payload) if payload is not None else None
        last_error = None

        for attempt in range(2):
            connection = self._connect()
            try:
                headers = copy(self.headers)
                headers[self.NAME_CONTENT_TYPE] = self.JSON_CONTENT_TYPE
                connection.request(method, url, encoded_payload, headers)
                response = connection.getresponse()
                raw_data = response.read()
            except Exception as exc:
                last_error = exc
                try:
                    connection.close()
                except Exception:
                    pass
                if attempt == 0:
                    sleep(0.2)
                    self._login()
                    continue
                raise NaoAssetCreatorError(
                    "NAO-Anfrage %s %s fehlgeschlagen: %s" % (method, url, exc)
                )
            finally:
                try:
                    connection.close()
                except Exception:
                    pass

            if response.status in (401, 403) and attempt == 0:
                self._login()
                continue

            if response.status >= 400:
                raise NaoAssetCreatorError(
                    "NAO-Anfrage %s %s fehlgeschlagen: HTTP %s - %s"
                    % (
                        method,
                        url,
                        response.status,
                        raw_data.decode(self.NAME_UTF8, errors="replace"),
                    )
                )

            if raw_data == b"":
                return {}

            try:
                return loads(raw_data.decode(self.NAME_UTF8))
            except Exception as exc:
                raise NaoAssetCreatorError(
                    "Antwort von %s %s ist kein gültiges JSON: %s"
                    % (method, url, exc)
                )

        raise NaoAssetCreatorError(
            "NAO-Anfrage %s %s fehlgeschlagen: %s" % (method, url, last_error)
        )
