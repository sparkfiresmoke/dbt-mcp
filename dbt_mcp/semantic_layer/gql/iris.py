from dataclasses import dataclass

import requests


@dataclass
class ConnAttr:
    host: str  # "grpc+tls:semantic-layer.cloud.getdbt.com:443"
    params: dict  # {"environmentId": 42}
    auth_header: str  # "Bearer dbts_thisismyprivateservicetoken"


def submit_request(
    _conn_attr: ConnAttr,
    payload: dict,
    source: str | None = None,
    host_override: str | None = None,
    path: str = "/api/graphql",
) -> dict:
    # TODO: This should take into account multi-region and single-tenant
    url = f"{host_override or _conn_attr.host}{path}"
    if "variables" not in payload:
        payload["variables"] = {}
    payload["variables"]["environmentId"] = _conn_attr.params["environmentid"]
    r = requests.post(
        url,
        json=payload,
        headers={
            "Authorization": _conn_attr.auth_header,
            "x-dbt-partner-source": "dbt-mcp",
        },
    )
    return r.json()
