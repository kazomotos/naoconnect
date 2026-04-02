from __future__ import annotations

from typing import Dict, List, Optional, TypedDict, Union

import requests


_JsonPrimitive = Union[str, int, float, bool, None]
_JsonValue = Union[_JsonPrimitive, List["_JsonValue"], Dict[str, "_JsonValue"]]


class _TagOut(TypedDict):
    """Beschreibt die reduzierte Darstellung eines Tags."""

    name: _JsonValue
    slug: _JsonValue
    inserted_at: _JsonValue
    updated_at: _JsonValue
    mandate_id: _JsonValue


class _DeviceOut(TypedDict):
    """Beschreibt die reduzierte Darstellung eines Geräts."""

    name: _JsonValue
    slug: _JsonValue
    device_eui: str
    inserted_at: _JsonValue
    updated_at: _JsonValue
    lon: float | None
    lat: float | None
    fields: _JsonValue
    stats: _JsonValue
    tags: list[_TagOut]


def _fetch_all(
    session: requests.Session,
    base_url: str,
    api_key: str,
    endpoint: str,
    extra_params: dict[str, _JsonValue] | None = None,
) -> list[dict[str, _JsonValue]]:
    """Lädt alle Seiten eines Element-IoT-Endpunkts und gibt die gesammelten Datensätze zurück."""
    items: list[dict[str, _JsonValue]] = []
    retrieve_after: str | None = None

    while True:
        params: dict[str, _JsonValue] = {
            "auth": api_key,
            "limit": 1500,
        }

        if extra_params:
            params.update(extra_params)

        if retrieve_after:
            params["retrieve_after"] = retrieve_after

        response = session.get(f"{base_url.rstrip('/')}{endpoint}", params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        if not data.get("ok", False):
            raise RuntimeError(f"API-Fehler auf {endpoint}: {data}")

        batch = data.get("body", [])
        if not isinstance(batch, list) or not batch:
            break

        items.extend(item for item in batch if isinstance(item, dict))

        next_id = data.get("retrieve_after_id")
        if not isinstance(next_id, str) or next_id == retrieve_after:
            break

        retrieve_after = next_id

    return items


def _extract_device_eui(device: dict[str, _JsonValue]) -> str:
    """Extrahiert alle eindeutigen Device-EUIs eines Geräts als kommaseparierten String."""
    euis: list[str] = []

    for interface in device.get("interfaces", []) or []:
        if not isinstance(interface, dict):
            continue
        options = interface.get("opts") or {}
        if not isinstance(options, dict):
            continue
        device_eui = options.get("device_eui")
        if device_eui is None:
            continue

        device_eui_str = str(device_eui).strip().upper()
        if device_eui_str and device_eui_str not in euis:
            euis.append(device_eui_str)

    return ",".join(euis)


def _extract_lon_lat(device: dict[str, _JsonValue]) -> tuple[float | None, float | None]:
    """Liest Längen- und Breitengrad aus der GeoJSON-Location eines Geräts aus."""
    location = device.get("location")
    if isinstance(location, dict) and location.get("type") == "Point":
        coordinates = location.get("coordinates")
        if isinstance(coordinates, list) and len(coordinates) >= 2:
            try:
                return float(coordinates[0]), float(coordinates[1])
            except (TypeError, ValueError):
                return None, None
    return None, None


def _reduce_tags(device: dict[str, _JsonValue]) -> list[_TagOut]:
    """Reduziert die Tag-Struktur auf die für den Export relevanten Felder."""
    output: list[_TagOut] = []

    for tag in device.get("tags", []) or []:
        if not isinstance(tag, dict):
            continue
        output.append(
            {
                "name": tag.get("name"),
                "slug": tag.get("slug"),
                "inserted_at": tag.get("inserted_at"),
                "updated_at": tag.get("updated_at"),
                "mandate_id": tag.get("mandate_id"),
            }
        )

    return output


def _reduce_device(device: dict[str, _JsonValue]) -> _DeviceOut:
    """Formt ein Rohgerät in die reduzierte Ausgabestruktur um."""
    longitude, latitude = _extract_lon_lat(device)
    return {
        "name": device.get("name"),
        "slug": device.get("slug"),
        "device_eui": _extract_device_eui(device),
        "inserted_at": device.get("inserted_at"),
        "updated_at": device.get("updated_at"),
        "lon": longitude,
        "lat": latitude,
        "fields": device.get("fields"),
        "stats": device.get("stats"),
        "tags": _reduce_tags(device),
    }


def get_devices_out(base_url: str, api_key: str) -> list[_DeviceOut]:
    """Lädt alle Geräte von Element IoT und gibt die reduzierte `out`-Struktur zurück.

    Beispiel für die Rückgabe:
        [
            {
                "name": "Wasserzaehler Keller",
                "slug": "wasserzaehler-keller",
                "device_eui": "ABCDEF1234567890,1234567890ABCDEF",
                "inserted_at": "2026-03-23T08:15:00Z",
                "updated_at": "2026-03-23T09:30:00Z",
                "lon": 11.0767,
                "lat": 49.4521,
                "fields": {
                    "seriennummer": "12345678",
                    "typ": "zaehler",
                },
                "stats": {
                    "transceived_at": "2026-03-30T16:06:08.219593Z",
                },
                "tags": [
                    {
                        "name": "Geraet",
                        "slug": "geraet",
                        "inserted_at": "2026-03-20T10:00:00Z",
                        "updated_at": "2026-03-21T10:00:00Z",
                        "mandate_id": 42,
                    }
                ],
            }
        ]
    """
    with requests.Session() as session:
        devices = _fetch_all(
            session=session,
            base_url=base_url,
            api_key=api_key,
            endpoint="/devices/",
            extra_params={"with_profile": 1},
        )

    return [_reduce_device(device) for device in devices]
