"""
Generate daily JSON data for the Smart Promo Topup Traits interactive dashboard.
Queries Databricks for providers, traits, SP offers, enrollments, and DD orders.
Outputs smart-promo-traits/data.json consumed by the client-side dashboard.
"""

import os
import json
import math
from pathlib import Path
from datetime import datetime
from decimal import Decimal

from databricks import sql
import pandas as pd

from config import SERVER_HOSTNAME, HTTP_PATH

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO_ROOT / "smart-promo-traits"
OUTPUT_FILE = OUTPUT_DIR / "data.json"

TRAIT_IDS = [258321, 258322, 258323]
TRAIT_IDS_STR = ",".join(str(t) for t in TRAIT_IDS)


def connect():
    token = os.environ.get("DATABRICKS_TOKEN")
    if not token:
        raise RuntimeError("DATABRICKS_TOKEN env var is required")
    return sql.connect(
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH,
        access_token=token,
    )


def query(conn, q):
    with conn.cursor() as cur:
        cur.execute(q)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


ID_COLUMNS = {
    "trait_id", "provider_id", "vendor_id", "smart_promo_offer_id",
    "campaign_id", "order_id", "smart_promo_bundle_id", "city_id",
    "smart_promo_regular_campaign_id", "bundle_id",
}


def to_native(val, default=None, key=None):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return default
    if isinstance(val, Decimal):
        v = float(val)
        if key and key in ID_COLUMNS:
            return int(v)
        return v
    if hasattr(val, "item"):
        v = val.item()
        if key and key in ID_COLUMNS and isinstance(v, float):
            return int(v)
        return v
    try:
        f = float(val)
        if math.isnan(f):
            return default
        if key and key in ID_COLUMNS:
            return int(f)
        return f
    except (TypeError, ValueError):
        return str(val)


def df_to_records(df):
    records = []
    for _, row in df.iterrows():
        records.append({k: to_native(v, key=k) for k, v in row.items()})
    return records


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

def fetch_traits(conn):
    print("  [1/5] Fetching trait definitions...")
    df = query(conn, f"""
        SELECT id AS trait_id, name, slug, status, global, type, created
        FROM ng_delivery_spark.delivery_provider_trait
        WHERE id IN ({TRAIT_IDS_STR})
    """)
    return df


def fetch_providers(conn):
    print("  [2/5] Fetching providers in traits...")
    df = query(conn, f"""
        SELECT
            pt.trait_id,
            pt.provider_id,
            p.vendor_id,
            p.provider_name,
            p.brand_name,
            p.city_name,
            p.country_code,
            p.provider_status
        FROM ng_delivery_spark.delivery_provider_provider_trait pt
        LEFT JOIN ng_delivery_spark.dim_provider_v2 p
            ON pt.provider_id = p.provider_id
        WHERE pt.trait_id IN ({TRAIT_IDS_STR})
          AND pt.deleted = false
          AND pt.created_date >= '2026-01-01'
        ORDER BY pt.trait_id, p.brand_name, pt.provider_id
    """)
    return df


def fetch_enrollments(conn):
    print("  [3/5] Fetching SP enrollments...")
    df = query(conn, f"""
        SELECT
            sp.provider_id,
            sp.smart_promo_offer_id,
            sp.smart_promo_offer_type,
            sp.smart_promo_type,
            sp.smart_promo_offer_mode,
            sp.campaign_spend_objective,
            sp.smart_promo_enrollment_state,
            sp.campaign_id,
            sp.smart_promo_bundle_id,
            sp.smart_promo_regular_campaign_id,
            sp.smart_promo_offer_start_ts,
            sp.smart_promo_offer_end_ts
        FROM core_models_spark.fact_provider_smart_promo_offer_campaign_enrollment sp
        WHERE sp.provider_id IN (
            SELECT DISTINCT pt.provider_id
            FROM ng_delivery_spark.delivery_provider_provider_trait pt
            WHERE pt.trait_id IN ({TRAIT_IDS_STR})
              AND pt.deleted = false
              AND pt.created_date >= '2026-01-01'
        )
        AND sp.smart_promo_offer_provider_enrollment_start_date >= DATE_SUB(CURRENT_DATE(), 28)
        AND sp.smart_promo_enrollment_state = 'active'
        ORDER BY sp.provider_id, sp.smart_promo_offer_start_ts DESC
    """)
    return df


def fetch_offers(conn, enrollment_df):
    print("  [4/5] Fetching campaign/offer details...")
    if enrollment_df.empty:
        return pd.DataFrame()

    campaign_ids = enrollment_df["campaign_id"].dropna().unique().tolist()
    if not campaign_ids:
        return pd.DataFrame()

    ids_str = ",".join(str(int(c)) for c in campaign_ids)
    df = query(conn, f"""
        SELECT
            c.id AS campaign_id,
            c.name AS campaign_name,
            c.bonus_type,
            c.bonus_data_percentage,
            c.bonus_data_max_value,
            c.cost_share_percentage,
            c.spend_objective,
            c.city_id,
            c.start,
            c.end,
            c.bundle_id
        FROM ng_delivery_spark.delivery_eater_campaign_targeted_campaign c
        WHERE c.id IN ({ids_str})
          AND c.created_date >= DATE_SUB(CURRENT_DATE(), 35)
        ORDER BY c.id
    """)
    return df


def fetch_dd_orders(conn):
    print("  [5/5] Fetching DD orders (last 28 days)...")
    enrollment_campaigns = query(conn, f"""
        SELECT DISTINCT sp.campaign_id
        FROM core_models_spark.fact_provider_smart_promo_offer_campaign_enrollment sp
        WHERE sp.provider_id IN (
            SELECT DISTINCT pt.provider_id
            FROM ng_delivery_spark.delivery_provider_provider_trait pt
            WHERE pt.trait_id IN ({TRAIT_IDS_STR})
              AND pt.deleted = false
              AND pt.created_date >= '2026-01-01'
        )
        AND sp.smart_promo_offer_type = 'double_deal'
        AND sp.smart_promo_enrollment_state = 'active'
        AND sp.smart_promo_offer_provider_enrollment_start_date >= '2026-01-01'
    """)

    if enrollment_campaigns.empty:
        return pd.DataFrame()

    campaign_ids = enrollment_campaigns["campaign_id"].dropna().unique().tolist()
    if not campaign_ids:
        return pd.DataFrame()

    ids_str = ",".join(str(int(c)) for c in campaign_ids)
    df = query(conn, f"""
        SELECT
            co.order_id,
            co.provider_id,
            co.order_created_date,
            co.campaign_id,
            co.spend_objective,
            co.discount_value_local AS discount_uah,
            co.bolt_spend_local AS bolt_uah,
            co.provider_spend_local AS provider_uah
        FROM ng_public_spark.etl_delivery_campaign_order_metrics co
        WHERE co.campaign_id IN ({ids_str})
          AND co.provider_id IN (
              SELECT DISTINCT pt.provider_id
              FROM ng_delivery_spark.delivery_provider_provider_trait pt
              WHERE pt.trait_id IN ({TRAIT_IDS_STR})
                AND pt.deleted = false
                AND pt.created_date >= '2026-01-01'
          )
          AND co.order_created_date >= DATE_SUB(CURRENT_DATE(), 28)
          AND co.country = 'ua'
        ORDER BY co.order_created_date DESC, co.order_id DESC
    """)
    return df


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------

def build_provider_list(providers_df):
    """Group by provider, collect trait_ids as list of ints."""
    grouped = (
        providers_df.groupby(
            ["provider_id", "vendor_id", "provider_name", "brand_name",
             "city_name", "country_code", "provider_status"]
        )["trait_id"]
        .apply(lambda x: sorted(set(int(v) for v in x if pd.notna(v))))
        .reset_index()
        .rename(columns={"trait_id": "trait_ids"})
    )
    records = []
    for _, row in grouped.iterrows():
        rec = {k: to_native(v, key=k) for k, v in row.items() if k != "trait_ids"}
        rec["trait_ids"] = row["trait_ids"]
        records.append(rec)
    return records


def build_offers(enrollment_df, offers_df):
    """Deduplicate offers and merge campaign details."""
    if enrollment_df.empty:
        return []

    offer_cols = [
        "smart_promo_offer_id", "smart_promo_offer_type",
        "smart_promo_offer_mode", "campaign_spend_objective",
        "campaign_id", "smart_promo_bundle_id",
        "smart_promo_offer_start_ts", "smart_promo_offer_end_ts"
    ]
    offers_dedup = enrollment_df[offer_cols].drop_duplicates()

    if not offers_df.empty:
        merged = offers_dedup.merge(offers_df, on="campaign_id", how="left")
    else:
        merged = offers_dedup

    return df_to_records(merged)


def build_enrollments(enrollment_df):
    """Deduplicate to provider + offer level."""
    if enrollment_df.empty:
        return []

    cols = [
        "provider_id", "smart_promo_offer_id", "smart_promo_offer_type",
        "smart_promo_offer_mode", "campaign_spend_objective",
        "smart_promo_enrollment_state",
        "smart_promo_offer_start_ts", "smart_promo_offer_end_ts"
    ]
    dedup = enrollment_df[cols].drop_duplicates()
    return df_to_records(dedup)


def main():
    print("Smart Promo Traits — generating dashboard data...")
    conn = connect()

    try:
        traits_df = fetch_traits(conn)
        providers_df = fetch_providers(conn)
        enrollment_df = fetch_enrollments(conn)
        offers_df = fetch_offers(conn, enrollment_df)
        dd_orders_df = fetch_dd_orders(conn)
    finally:
        conn.close()

    trait_provider_counts = (
        providers_df.groupby("trait_id")["provider_id"]
        .nunique()
        .to_dict()
    )
    traits_list = df_to_records(traits_df)
    for t in traits_list:
        tid = t.get("trait_id")
        t["provider_count"] = trait_provider_counts.get(tid, 0)

    data = {
        "meta": {
            "last_updated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "trait_ids": TRAIT_IDS,
        },
        "traits": traits_list,
        "providers": build_provider_list(providers_df),
        "offers": build_offers(enrollment_df, offers_df),
        "enrollments": build_enrollments(enrollment_df),
        "dd_orders": df_to_records(dd_orders_df) if not dd_orders_df.empty else [],
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, default=str)

    size_kb = OUTPUT_FILE.stat().st_size / 1024
    print(f"  Written {OUTPUT_FILE} ({size_kb:.1f} KB)")
    print(f"  Providers: {len(data['providers'])}")
    print(f"  Offers: {len(data['offers'])}")
    print(f"  Enrollments: {len(data['enrollments'])}")
    print(f"  DD Orders: {len(data['dd_orders'])}")
    print("Done.")


if __name__ == "__main__":
    main()
