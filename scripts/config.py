"""Provider configuration for report generation."""

import os

SERVER_HOSTNAME = os.environ.get(
    "DATABRICKS_SERVER_HOSTNAME", "bolt-incentives.cloud.databricks.com"
)
# Shared cluster ids change when the compute is rotated; override without editing code.
HTTP_PATH = os.environ.get(
    "DATABRICKS_HTTP_PATH", "sql/protocolv1/o/2472566184436351/0221-081903-9ag4bh69"
)
CATALOG = os.environ.get("DATABRICKS_CATALOG") or None

# Legacy hive_metastore clusters expose these schemas with a `_spark` suffix;
# Unity Catalog clusters expose the same tables without it.
_UC_SCHEMA_MAP = {
    "ng_delivery_store_spark": "ng_delivery_store",
    "ng_delivery_spark": "ng_delivery",
    "ng_public_spark": "ng_public",
    "core_models_spark": "core_models",
}


def resolve_sql(query):
    """Translate legacy schema names when running against a Unity Catalog catalog."""
    if not CATALOG:
        return query
    for legacy, unity in _UC_SCHEMA_MAP.items():
        query = query.replace(legacy + ".", unity + ".")
    return query

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
