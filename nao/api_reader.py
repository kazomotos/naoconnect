import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen


def read_nao_api(
    api_url: str,
    api_token: str | None = None,
    filter_meta: list[dict[str, Any]] | None = None,
) -> dict:
    """
    Liest Stationsdaten von der API.

    Neuer Endpoint:
        Der Token wird über den Authorization-Header übergeben.

    Alter Endpoint als Fallback:
        Der Token wird in den URL-Pfad eingefügt:

        https://example.com/stations/<token>?filter=...

    Beispiel für filter_meta:
        [
            {"country": "DE"},
            {"active": True},
            {"station_type": "weather"},
        ]
    """

    request_url = api_url

    # Mehrere Filter-Dictionaries zu einem Filter zusammenführen
    if filter_meta:
        combined_filter: dict[str, Any] = {}

        for filter_item in filter_meta:
            combined_filter.update(filter_item)

        filter_json = json.dumps(
            combined_filter,
            separators=(",", ":"),
        )

        # Vorhandene Query-Parameter der URL beibehalten
        url_parts = urlsplit(api_url)
        query_params = dict(
            parse_qsl(
                url_parts.query,
                keep_blank_values=True,
            )
        )

        query_params["filter"] = filter_json

        request_url = urlunsplit(
            (
                url_parts.scheme,
                url_parts.netloc,
                url_parts.path,
                urlencode(query_params),
                url_parts.fragment,
            )
        )

    # Anfrage für den neuen Endpoint erstellen
    request = Request(request_url)

    if api_token:
        auth_value = api_token.strip()

        if not auth_value.lower().startswith("bearer "):
            auth_value = f"Bearer {auth_value}"

        request.add_header("Authorization", auth_value)

    try:
        # Neuer Endpoint: Token im Authorization-Header
        with urlopen(request) as response:
            response_data = json.loads(
                response.read().decode("utf-8")
            )

    except (HTTPError, URLError):
        # Alter Endpoint: Token im URL-Pfad
        if not api_token:
            raise

        # Für den URL-Pfad darf kein "Bearer " enthalten sein
        fallback_token = api_token.strip()

        if fallback_token.lower().startswith("bearer "):
            fallback_token = fallback_token[7:].strip()

        url_parts = urlsplit(request_url)

        fallback_path = (
            f"{url_parts.path.rstrip('/')}/{fallback_token}"
        )

        fallback_url = urlunsplit(
            (
                url_parts.scheme,
                url_parts.netloc,
                fallback_path,
                url_parts.query,
                url_parts.fragment,
            )
        )

        with urlopen(fallback_url) as response:
            response_data = json.loads(
                response.read().decode("utf-8")
            )

    return response_data["results"]




