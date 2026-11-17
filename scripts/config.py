"""Provider configuration for report generation."""

import json
import os
from pathlib import Path

SERVER_HOSTNAME = os.environ.get(
    "DATABRICKS_SERVER_HOSTNAME", "bolt-incentives.cloud.databricks.com"
)
# Live Unity Catalog warehouse. The old hive_metastore cluster
# 0221-081903-9ag4bh69 can no longer be autostarted.
HTTP_PATH = os.environ.get(
    "DATABRICKS_HTTP_PATH",
    "sql/protocolv1/o/2472566184436351/0505-112942-d3yviznw",
)
CATALOG = os.environ.get("DATABRICKS_CATALOG", "main") or None

# Legacy hive_metastore clusters expose these schemas with a `_spark` suffix;
# Unity Catalog clusters expose the same tables without it.
_UC_SCHEMA_MAP = {
    "ng_delivery_store_spark": "ng_delivery_store",
    "ng_delivery_spark": "ng_delivery",
    "ng_public_spark": "ng_public",
    "core_models_spark": "core_models",
}

_OAUTH_CACHE = Path.home() / ".databricks" / "partner-reports-oauth.json"
_CURSOR_OAUTH_CACHE = Path.home() / ".databricks" / "cursor-sql-oauth.json"
_OAUTH_CLIENT_ID = "databricks-sql-python"


def resolve_sql(query):
    """Translate legacy schema names when running against a Unity Catalog catalog."""
    if not CATALOG:
        return query
    for legacy, unity in _UC_SCHEMA_MAP.items():
        query = query.replace(legacy + ".", unity + ".")
    return query


def _oauth_hostname():
    host = SERVER_HOSTNAME.strip()
    if host.startswith("https://"):
        host = host[len("https://") :]
    elif host.startswith("http://"):
        host = host[len("http://") :]
    return f"https://{host.rstrip('/')}/"


def _is_oauth_refresh(token):
    t = (token or "").strip().lower()
    return t.startswith("doau") or t.startswith("dau")


def _is_pat(token):
    t = (token or "").strip().lower()
    return t.startswith("dapi")


def _write_oauth_cache(payload):
    _OAUTH_CACHE.parent.mkdir(parents=True, exist_ok=True)
    _OAUTH_CACHE.write_text(json.dumps(payload))
    _OAUTH_CACHE.chmod(0o600)
    return _OAUTH_CACHE


def _seed_oauth_cache(refresh_token=None):
    """Write an OAuth persistence file for the incentives warehouse."""
    host = _oauth_hostname()
    existing = {}
    if _OAUTH_CACHE.exists():
        try:
            existing = json.loads(_OAUTH_CACHE.read_text())
        except Exception:
            existing = {}

    if refresh_token:
        payload = {"refresh_token": refresh_token.strip(), "hostname": host}
        if (
            existing.get("refresh_token") == payload["refresh_token"]
            and existing.get("access_token")
        ):
            payload["access_token"] = existing["access_token"]
        return _write_oauth_cache(payload)

    if existing.get("refresh_token") and existing.get("hostname", "").rstrip("/") + "/" == host:
        return _OAUTH_CACHE

    if _CURSOR_OAUTH_CACHE.exists():
        try:
            data = json.loads(_CURSOR_OAUTH_CACHE.read_text())
        except Exception:
            data = {}
        cached_host = (data.get("hostname") or "").rstrip("/") + "/"
        if cached_host == host and data.get("refresh_token"):
            payload = {
                "refresh_token": data["refresh_token"],
                "hostname": host,
            }
            if data.get("access_token"):
                payload["access_token"] = data["access_token"]
            return _write_oauth_cache(payload)

    raise RuntimeError(
        "Databricks PAT expired and no OAuth refresh is available. "
        "Put a doau… refresh token in DATABRICKS_TOKEN (GitHub secret) "
        "or sign in locally with dbx.py against bolt-incentives."
    )


def _refresh_access_token(refresh_token):
    """Exchange a doau… refresh token for a 1-hour JWT. No browser.

    The SQL connector's auth_type=databricks-oauth opens localhost:8020
    when persistence is missing — that hangs forever on GitHub Actions.
    """
    import urllib.parse
    import urllib.request

    host = _oauth_hostname().rstrip("/")
    well_known = f"{host}/oidc/.well-known/oauth-authorization-server"
    with urllib.request.urlopen(well_known, timeout=30) as resp:
        token_url = json.loads(resp.read().decode())["token_endpoint"]
    body = urllib.parse.urlencode(
        {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token.strip(),
            "client_id": _OAUTH_CLIENT_ID,
        }
    ).encode()
    req = urllib.request.Request(
        token_url,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode())
    access = data.get("access_token")
    if not access:
        raise RuntimeError("OAuth refresh returned no access_token")
    return access


def _oauth_connect(refresh_token=None):
    from databricks import sql

    token = refresh_token
    if not token:
        cache = _seed_oauth_cache()
        data = json.loads(Path(cache).read_text())
        token = data.get("refresh_token")
    if not token:
        raise RuntimeError("No OAuth refresh token available")

    access = _refresh_access_token(token)
    kwargs = {"catalog": CATALOG} if CATALOG else {}
    print(f"  Databricks auth: OAuth refresh ({SERVER_HOSTNAME})")
    return sql.connect(
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH,
        access_token=access,
        **kwargs,
    )


def db_connect(token=None):
    """Connect to the live warehouse with Unity Catalog.

    Auth order:
    1. OAuth refresh token (doau… in DATABRICKS_TOKEN) — used by GitHub Actions
    2. Databricks PAT (dapi…) — falls back to OAuth if the PAT is dead
    3. Local OAuth cache from dbx.py (same incentives hostname)
    """
    from databricks import sql

    token = (token or os.environ.get("DATABRICKS_TOKEN") or "").strip()
    if _is_oauth_refresh(token):
        return _oauth_connect(token)

    if _is_pat(token):
        kwargs = {"catalog": CATALOG} if CATALOG else {}
        try:
            conn = sql.connect(
                server_hostname=SERVER_HOSTNAME,
                http_path=HTTP_PATH,
                access_token=token,
                **kwargs,
            )
            print(f"  Databricks auth: PAT ({SERVER_HOSTNAME})")
            return conn
        except Exception as e:
            msg = str(e).lower()
            if "invalid access token" in msg or "invalid access to org" in msg:
                print(f"  WARN: DATABRICKS_TOKEN PAT rejected; falling back to OAuth")
                return _oauth_connect()
            raise

    if token:
        # JWT access token from OAuth also works as a SQL access_token.
        kwargs = {"catalog": CATALOG} if CATALOG else {}
        try:
            return sql.connect(
                server_hostname=SERVER_HOSTNAME,
                http_path=HTTP_PATH,
                access_token=token,
                **kwargs,
            )
        except Exception:
            return _oauth_connect()

    return _oauth_connect()


def db_query(conn, q):
    """Run SQL, rewriting legacy `_spark` schema names when CATALOG is set."""
    import pandas as pd

    sql_text = resolve_sql(q)
    with conn.cursor() as cur:
        cur.execute(sql_text)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)

PROVIDERS = {
    31504: {"name": "Epic Cheeseburger", "slug": "epic-cheeseburger", "city": "Львів"},
    187635: {"name": "EPIC CHEESEBURGER CAFE DL", "slug": "epic-cheeseburger-cafe-dl", "city": "Львів"},
    31506: {"name": "TEDDY", "slug": "teddy", "city": "Львів"},
    31505: {"name": "МОРЕ РИБИ", "slug": "more-ryby", "city": "Львів"},
    31502: {"name": "Cukor Red", "slug": "cukor-red", "city": "Львів"},
    31503: {"name": "Cukor Black", "slug": "cukor-black", "city": "Львів"},
    199279: {"name": "Epic Cheeseburger (Київ)", "slug": "epic-cheeseburger-kyiv", "city": "Київ"},
    682702: {"name": "EPIC LEM", "slug": "epic-lem", "city": "Львів"},
}

WEEKS_BACK = 8
