#!/usr/bin/env python3
"""
Галицька перепічка / Кулиничі / ВЖАР — multi-brand partner report (Muza-style).

Grains: week (Mon–Sun), calendar month, calendar quarter, calendar year.
History from partnership start. Writes halytska/index.html.

CI: DATABRICKS_TOKEN (incentives warehouse).
Local: same token from the workspace .env if the env var is empty.
"""

from __future__ import annotations

import json
import math
import os
import re
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

sys.stdout.reconfigure(line_buffering=True)

import pandas as pd

from config import db_connect, resolve_sql

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = REPO_ROOT / "halytska"
TEMPLATE = Path(__file__).resolve().parent / "halytska_template.html"
START = "2022-01-01"

PROVIDER_IDS = [
    153492, 153504, 196611, 202522, 202527, 202538, 202539, 202543,
    202548, 202552, 202553, 202554, 202555, 202556, 202557, 202558,
    202559, 5512683,
]

FALLBACK = {
    153492: {"name": "Галицька перепічка вул. Івана Кавалерідзе", "city": "Lviv"},
    153504: {"name": "Галицька перепічка вул. Володимира Великого", "city": "Lviv"},
    196611: {"name": "ВЖАР вул. Симона Петлюри", "city": "Lviv"},
    202522: {"name": "Галицька перепічка вул. Грінченка", "city": "Lviv"},
    202527: {"name": "Галицька перепічка вул. Левицького", "city": "Lviv"},
    202538: {"name": "Кулиничі пекарня,піца / Kulinichi pizza,bakery вул. Липинського", "city": "Lviv"},
    202539: {"name": "Галицька перепічка вул. Чорновола", "city": "Lviv"},
    202543: {"name": "Галицька перепічка вул. Шевченка", "city": "Lviv"},
    202548: {"name": "Кулиничі пекарня,піца / Kulinichi pizza,bakery вул. Полуботка", "city": "Rivne"},
    202552: {"name": "Кулиничі пекарня,піца / Kulinichi pizza,bakery вул. Богоявленська", "city": "Rivne"},
    202553: {"name": "Галицька перепічка вул. Бандери", "city": "Lviv"},
    202554: {"name": "Галицька перепічка вул. Личаківська", "city": "Lviv"},
    202555: {"name": "Галицька перепічка вул. Івана Мазепи", "city": "Lviv"},
    202556: {"name": "Галицька перепічка вул. Наукова", "city": "Lviv"},
    202557: {"name": "Галицька перепічка вул. Пекарська", "city": "Lviv"},
    202558: {"name": "Галицька перепічка вул. Стрийська", "city": "Lviv"},
    202559: {"name": "Галицька перепічка Червоної калини", "city": "Lviv"},
    5512683: {"name": "ВЖАР просп. Шевченка", "city": "Lviv"},
}

BRANDS = [
    {"id": "all", "name": "Усі точки", "short": "Усі точки"},
    {"id": "halytska", "name": "Галицька перепічка", "short": "Перепічка"},
    {"id": "kulinichi", "name": "Кулиничі", "short": "Кулиничі"},
    {"id": "vzhar", "name": "ВЖАР", "short": "ВЖАР"},
]

CITY_UA = {
    "Lviv": "Львів",
    "Drohobych": "Дрогобич",
    "Ivano-Frankivsk": "Івано-Франківськ",
    "Kyiv": "Київ",
    "Rivne": "Рівне",
}

STATUS_UA = {
    "active": "активний",
    "hidden": "прихований",
    "deleted": "закритий",
}

GRAIN_SQL = {
    "week": "DATE_FORMAT(DATE_TRUNC('WEEK', {c}), 'yyyy-MM-dd')",
    "month": "DATE_FORMAT(DATE_TRUNC('MONTH', {c}), 'yyyy-MM')",
    "quarter": "CONCAT(CAST(YEAR({c}) AS STRING), '-Q', CAST(QUARTER({c}) AS STRING))",
    "year": "DATE_FORMAT({c}, 'yyyy')",
}

NAME_PREFIXES = (
    "Кулиничі пекарня,піца / Kulinichi pizza,bakery вул. ",
    "Кулиничі пекарня,піца / Kulinichi pizza,bakery ",
    "Галицька перепічка вул. ",
    "Галицька перепічка ",
    "ВЖАР просп. ",
    "ВЖАР вул. ",
    "ВЖАР ",
)


def load_token():
    if os.environ.get("DATABRICKS_TOKEN"):
        return
    env_file = Path(
        "/Users/vadymbardysh/Library/CloudStorage/GoogleDrive-vadim.bardysh@bolt.eu/"
        "My Drive/Vadym_Cursor/.env"
    )
    if env_file.exists():
        try:
            from dotenv import dotenv_values
        except ImportError:
            vals = {}
            for line in env_file.read_text().splitlines():
                if line.strip() and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    vals[k.strip()] = v.strip().strip('"').strip("'")
        else:
            vals = dotenv_values(env_file)
        token = (vals or {}).get("DATABRICKS_TOKEN")
        if token:
            os.environ["DATABRICKS_TOKEN"] = token


def connect():
    load_token()
    return db_connect()


def query(conn, q):
    sql_text = resolve_sql(q)
    with conn.cursor() as cur:
        cur.execute(sql_text)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def qtry(conn, name, sql):
    print(f"→ {name}", flush=True)
    try:
        df = query(conn, sql)
        print(f"  {name}: {len(df)} rows", flush=True)
        return df
    except Exception as e:
        print(f"  FAIL {name}: {e}", flush=True)
        return pd.DataFrame()


def to_native(val, default=None):
    if val is None:
        return default
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, (datetime, date, pd.Timestamp)):
        return str(val)[:10]
    if hasattr(val, "item"):
        try:
            return val.item()
        except Exception:
            pass
    try:
        f = float(val)
        if math.isnan(f):
            return default
        if f == int(f) and abs(f) < 1e15:
            return int(f)
        return f
    except (TypeError, ValueError):
        return str(val)


def brand_of(name, group_name=None):
    n = (name or "").lower()
    if "вжар" in n or "vzhar" in n:
        return "vzhar"
    if "kulinichi" in n or "кулинич" in n:
        return "kulinichi"
    return "halytska"


def short_name(name):
    n = re.sub(r"\s+", " ", name or "").strip()
    for p in NAME_PREFIXES:
        if n.lower().startswith(p.lower()):
            n = n[len(p) :]
            break
    n = n.replace("вул. ", "").replace("просп. ", "")
    return n.strip() or (name or "")


def brand_case(ids_brands, col="provider_id"):
    parts = " ".join(f"WHEN {pid} THEN '{br}'" for pid, br in ids_brands.items())
    return f"CASE {col} {parts} ELSE 'halytska' END"


def nda_clean(text):
    if not text:
        return ""
    s = re.sub(r"Admin:\s*[\w\-]+(?:\s+[\w\-]+)*\s*Reason:\s*", "", str(text))
    s = re.sub(r"h?t?tps?://beehive\.bolt\.eu/\S*", "", s)
    return s.strip()


def period_end(grain, key):
    if grain == "week":
        return datetime.strptime(key, "%Y-%m-%d").date() + timedelta(days=6)
    if grain == "month":
        y, m = map(int, key.split("-"))
        if m == 12:
            return date(y, 12, 31)
        return date(y, m + 1, 1) - timedelta(days=1)
    if grain == "quarter":
        y, q = key.split("-Q")
        y, q = int(y), int(q)
        last_m = q * 3
        nxt = date(y + 1, 1, 1) if last_m == 12 else date(y, last_m + 1, 1)
        return nxt - timedelta(days=1)
    return date(int(key), 12, 31)


def period_label(grain, key):
    months = [
        "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
        "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень",
    ]
    if grain == "week":
        start = datetime.strptime(key, "%Y-%m-%d").date()
        end = start + timedelta(days=6)
        return f"{start.strftime('%d.%m')}–{end.strftime('%d.%m.%Y')}"
    if grain == "month":
        y, m = map(int, key.split("-"))
        return f"{months[m - 1]} {y}"
    if grain == "quarter":
        y, q = key.split("-Q")
        return f"Q{q} {y}"
    return str(key)


def df_records(df, cols=None):
    if df is None or df.empty:
        return []
    use = cols or list(df.columns)
    out = []
    for _, row in df.iterrows():
        rec = {}
        for c in use:
            if c in df.columns:
                rec[c] = to_native(
                    row[c],
                    0 if c not in ("item", "reason", "text", "b", "g", "k", "actor") else None,
                )
        out.append(rec)
    return out


def fetch_providers(conn):
    ids = ",".join(str(i) for i in PROVIDER_IDS)
    df = query(conn, f"""
        SELECT
          provider_id,
          provider_name,
          brand_name,
          group_name,
          city_name,
          provider_status
        FROM main.ng_delivery.dim_provider_v2
        WHERE provider_id IN ({ids})
        ORDER BY provider_name
    """)
    providers = {}
    for pid, fb in FALLBACK.items():
        providers[pid] = {
            "id": pid,
            "name": fb["name"],
            "short": short_name(fb["name"]),
            "city": fb["city"],
            "city_ua": CITY_UA.get(fb["city"], fb["city"]),
            "brand": brand_of(fb["name"]),
            "status": "active",
            "status_ua": STATUS_UA["active"],
        }
    for _, r in df.iterrows():
        pid = int(r["provider_id"])
        name = str(r["provider_name"] or providers.get(pid, {}).get("name") or pid)
        city = str(r["city_name"] or providers.get(pid, {}).get("city") or "")
        brand = brand_of(name)
        providers[pid] = {
            "id": pid,
            "name": name,
            "short": short_name(name),
            "city": city,
            "city_ua": CITY_UA.get(city, city),
            "brand": brand,
            "status": str(r["provider_status"] or "active"),
            "status_ua": STATUS_UA.get(str(r["provider_status"] or "active"), str(r["provider_status"])),
        }
    missing = [i for i in PROVIDER_IDS if i not in providers]
    if missing:
        print(f"WARN missing providers in dim: {missing}", flush=True)
    return providers


def ids_sql(providers):
    return ",".join(str(i) for i in providers)


def facts_sql(grain, ids, bcase):
    gexpr = GRAIN_SQL[grain].format(c="order_created_date")
    return f"""
    WITH base AS (
      SELECT
        provider_id,
        user_id,
        order_id,
        order_created_date,
        {bcase} AS brand,
        CAST(provider_price_before_discount_local AS DOUBLE) AS rev,
        CAST(total_order_item_discount_local AS DOUBLE) AS disc,
        CAST(provider_preparation_actual_duration_seconds AS DOUBLE) AS cook_s,
        CAST(order_provider_acceptance_duration_seconds AS DOUBLE) AS acc_s,
        CASE WHEN is_bolt_plus_order THEN 1 ELSE 0 END AS bp,
        CASE WHEN is_first_provider_order THEN 1 ELSE 0 END AS newp
      FROM main.ng_delivery.dim_order_delivery
      WHERE provider_id IN ({ids})
        AND order_created_date >= DATE '{START}'
        AND order_created_date < DATE_TRUNC('WEEK', CURRENT_DATE())
        AND order_state = 'delivered'
    ),
    flagged AS (
      SELECT
        *,
        MIN(order_created_date) OVER (PARTITION BY user_id) AS first_hold,
        MIN(order_created_date) OVER (PARTITION BY user_id, brand) AS first_brand
      FROM base
    )
    SELECT
      '{grain}' AS g,
      {gexpr} AS k,
      provider_id AS p,
      brand AS b,
      COUNT(DISTINCT order_id) AS o,
      ROUND(SUM(rev), 0) AS r,
      ROUND(SUM(rev) / NULLIF(COUNT(DISTINCT order_id), 0), 0) AS aov,
      COUNT(DISTINCT CASE WHEN bp = 1 THEN order_id END) AS bp,
      COUNT(DISTINCT CASE WHEN COALESCE(disc, 0) > 0 THEN order_id END) AS promo,
      ROUND(SUM(cook_s), 0) AS cook_s,
      SUM(CASE WHEN cook_s IS NOT NULL THEN 1 ELSE 0 END) AS cook_n,
      ROUND(SUM(acc_s), 0) AS acc_s,
      SUM(CASE WHEN acc_s IS NOT NULL THEN 1 ELSE 0 END) AS acc_n,
      COUNT(DISTINCT user_id) AS cust,
      COUNT(DISTINCT CASE WHEN newp = 1 THEN user_id END) AS newp,
      COUNT(DISTINCT CASE WHEN order_created_date = first_brand THEN user_id END) AS newb,
      COUNT(DISTINCT CASE WHEN order_created_date = first_hold THEN user_id END) AS newh
    FROM flagged
    GROUP BY 1, 2, 3, 4
    """


def uniq_sql(grain, ids, bcase, level):
    gexpr = GRAIN_SQL[grain].format(c="order_created_date")
    group_b = "brand" if level == "brand" else "'all'"
    return f"""
    WITH base AS (
      SELECT
        user_id,
        order_created_date,
        {bcase} AS brand
      FROM main.ng_delivery.dim_order_delivery
      WHERE provider_id IN ({ids})
        AND order_created_date >= DATE '{START}'
        AND order_created_date < DATE_TRUNC('WEEK', CURRENT_DATE())
        AND order_state = 'delivered'
    ),
    flagged AS (
      SELECT
        *,
        MIN(order_created_date) OVER (PARTITION BY user_id) AS first_hold,
        MIN(order_created_date) OVER (PARTITION BY user_id, brand) AS first_brand
      FROM base
    )
    SELECT
      '{grain}' AS g,
      {gexpr} AS k,
      {group_b} AS b,
      COUNT(DISTINCT user_id) AS cust,
      COUNT(DISTINCT CASE WHEN order_created_date = first_brand THEN user_id END) AS newb,
      COUNT(DISTINCT CASE WHEN order_created_date = first_hold THEN user_id END) AS newh
    FROM flagged
    GROUP BY 1, 2, 3
    """


def campaign_sql(grain, ids):
    # order_created_date is a string on this table
    gexpr = GRAIN_SQL[grain].format(c="TO_DATE(order_created_date)")
    return f"""
    SELECT
      '{grain}' AS g,
      {gexpr} AS k,
      provider_id AS p,
      COUNT(DISTINCT order_id) AS promo_o,
      ROUND(SUM(CAST(bolt_spend_local AS DOUBLE)), 0) AS bolt,
      ROUND(SUM(CAST(provider_spend_local AS DOUBLE)), 0) AS prov
    FROM main.ng_public.etl_delivery_campaign_order_metrics
    WHERE provider_id IN ({ids})
      AND order_created_date >= '{START}'
      AND order_created_date < DATE_FORMAT(DATE_TRUNC('WEEK', CURRENT_DATE()), 'yyyy-MM-dd')
    GROUP BY 1, 2, 3
    """


def dishes_sql(ids, bcase):
    return f"""
    WITH d AS (
      SELECT
        DATE_FORMAT(DATE_TRUNC('MONTH', o.order_created_date), 'yyyy-MM') AS k,
        {bcase.replace('provider_id', 'o.provider_id')} AS b,
        COALESCE(
          GET_JSON_OBJECT(b.basket_item_name_translation, '$.uk-UA'),
          b.basket_item_name
        ) AS item,
        COUNT(*) AS qty
      FROM main.ng_delivery.dim_basket_item_delivery b
      JOIN main.ng_delivery.dim_order_delivery o
        ON b.order_id = o.order_id
      WHERE o.provider_id IN ({ids})
        AND o.order_created_date >= DATE '{START}'
        AND o.order_created_date < DATE_TRUNC('WEEK', CURRENT_DATE())
        AND o.order_state = 'delivered'
        AND b.basket_item_created_date >= '{START}'
        AND COALESCE(b.item_price_before_discount_with_vat_local, 0) > 0
        AND LOWER(COALESCE(GET_JSON_OBJECT(b.basket_item_name_translation, '$.uk-UA'), b.basket_item_name, ''))
            NOT RLIKE 'прибор|пакет|упаков|столов|сервет'
      GROUP BY 1, 2, 3
    ),
    ranked AS (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY k, b ORDER BY qty DESC) AS rn
      FROM d
      WHERE item IS NOT NULL AND LENGTH(item) > 1
    )
    SELECT k, b, item, qty FROM ranked WHERE rn <= 15
    """


def cancels_sql(grain, ids):
    gexpr = GRAIN_SQL[grain].format(c="order_created_date")
    return f"""
    SELECT
      '{grain}' AS g,
      {gexpr} AS k,
      provider_id AS p,
      CASE
        WHEN LOWER(CONCAT(
               COALESCE(failed_order_reason,''), ' ',
               COALESCE(failed_order_parent_reason,''), ' ',
               COALESCE(failed_order_comment,'')))
             RLIKE 'тривог|air.?alert|air raid|алерт' THEN 'air'
        ELSE LOWER(TRIM(COALESCE(
          NULLIF(failed_order_parent_reason, ''),
          NULLIF(failed_order_reason, ''),
          CASE
            WHEN is_not_responded_by_provider = true THEN 'did_not_respond'
            WHEN is_rejected_by_provider = true THEN 'rejected_by_provider'
            WHEN failed_order_actor_at_fault = 'provider' THEN 'provider'
            WHEN failed_order_actor_at_fault = 'courier' THEN 'courier'
            WHEN failed_order_actor_at_fault = 'eater' THEN 'customer'
            ELSE 'other'
          END
        )))
      END AS reason,
      CASE
        WHEN LOWER(CONCAT(
               COALESCE(failed_order_reason,''), ' ',
               COALESCE(failed_order_parent_reason,''), ' ',
               COALESCE(failed_order_comment,'')))
             RLIKE 'тривог|air.?alert|air raid|алерт' THEN 'air'
        WHEN is_not_responded_by_provider = true
          OR is_rejected_by_provider = true
          OR failed_order_actor_at_fault = 'provider' THEN 'provider'
        WHEN failed_order_actor_at_fault = 'courier' THEN 'courier'
        WHEN failed_order_actor_at_fault = 'eater' THEN 'customer'
        ELSE 'other'
      END AS actor,
      COUNT(DISTINCT order_id) AS n
    FROM main.ng_delivery.dim_order_delivery
    WHERE provider_id IN ({ids})
      AND order_created_date >= DATE '{START}'
      AND order_created_date < DATE_TRUNC('WEEK', CURRENT_DATE())
      AND (
        order_state IN ('cancelled', 'rejected', 'failed')
        OR is_failed_order = true
      )
    GROUP BY 1, 2, 3, 4, 5
    """


def created_sql(grain, ids):
    gexpr = GRAIN_SQL[grain].format(c="order_created_date")
    return f"""
    SELECT
      '{grain}' AS g,
      {gexpr} AS k,
      provider_id AS p,
      COUNT(DISTINCT order_id) AS created
    FROM main.ng_delivery.dim_order_delivery
    WHERE provider_id IN ({ids})
      AND order_created_date >= DATE '{START}'
      AND order_created_date < DATE_TRUNC('WEEK', CURRENT_DATE())
    GROUP BY 1, 2, 3
    """


def ops_sql(ids):
    return f"""
    SELECT
      provider_id AS p,
      DATE_FORMAT(DATE_TRUNC('WEEK', date), 'yyyy-MM-dd') AS k,
      ROUND(AVG(availability_rate_last_7d) * 100, 1) AS avail,
      ROUND(AVG(acceptance_rate_last_7d) * 100, 1) AS accept,
      ROUND(AVG(COALESCE(avg_rating_last_7d, avg_rating_last_14d, avg_rating_last_30d)), 2) AS rating
    FROM main.ng_public.etl_incentives_provider_targeting_features
    WHERE provider_id IN ({ids})
      AND date >= DATE '{START}'
      AND date < DATE_TRUNC('WEEK', CURRENT_DATE())
    GROUP BY 1, 2
    """


def daypart_sql(ids, bcase):
    return f"""
    SELECT
      DATE_FORMAT(DATE_TRUNC('MONTH', order_created_date), 'yyyy-MM') AS k,
      {bcase} AS b,
      CASE
        WHEN HOUR(order_created_ts_local) < 11 THEN 'breakfast'
        WHEN HOUR(order_created_ts_local) < 16 THEN 'lunch'
        WHEN HOUR(order_created_ts_local) < 20 THEN 'afternoon'
        ELSE 'evening'
      END AS slot,
      COUNT(DISTINCT order_id) AS n
    FROM main.ng_delivery.dim_order_delivery
    WHERE provider_id IN ({ids})
      AND order_created_date >= DATE '{START}'
      AND order_created_date < DATE_TRUNC('WEEK', CURRENT_DATE())
      AND order_state = 'delivered'
    GROUP BY 1, 2, 3
    """


def ratings_sql(ids):
    return f"""
    SELECT
      DATE_FORMAT(DATE_TRUNC('MONTH', order_created_date), 'yyyy-MM') AS k,
      provider_id AS p,
      ROUND(AVG(CAST(provider_rating_value AS DOUBLE)), 2) AS avg,
      COUNT(provider_rating_value) AS n,
      SUM(CASE WHEN CAST(provider_rating_value AS DOUBLE) >= 4 THEN 1 ELSE 0 END) AS pos,
      SUM(CASE WHEN CAST(provider_rating_value AS DOUBLE) > 0 AND CAST(provider_rating_value AS DOUBLE) < 3 THEN 1 ELSE 0 END) AS neg
    FROM main.ng_delivery.dim_order_delivery
    WHERE provider_id IN ({ids})
      AND order_created_date >= DATE '{START}'
      AND order_created_date < DATE_TRUNC('WEEK', CURRENT_DATE())
      AND provider_rating_value IS NOT NULL
    GROUP BY 1, 2
    """


def reviews_sql(ids):
    return f"""
    SELECT
      provider_id AS p,
      DATE(order_created_date) AS dt,
      CAST(provider_rating_value AS DOUBLE) AS rating,
      provider_rating_comment AS text
    FROM main.ng_delivery.dim_order_delivery
    WHERE provider_id IN ({ids})
      AND order_created_date >= DATE_SUB(CURRENT_DATE(), 180)
      AND order_created_date < DATE_TRUNC('WEEK', CURRENT_DATE())
      AND provider_rating_comment IS NOT NULL
      AND LENGTH(provider_rating_comment) > 8
    ORDER BY order_created_date DESC
    LIMIT 400
    """


def sl_sql(ids):
    return f"""
    SELECT
      provider_id AS p,
      CAST(sponsored_listing_start_ts AS DATE) AS start_d,
      CAST(COALESCE(sponsored_listing_actual_end_ts, sponsored_listing_default_end_ts) AS DATE) AS end_d
    FROM main.core_models.fact_provider_sponsored_listing_signup
    WHERE provider_id IN ({ids})
      AND sponsored_listing_start_ts IS NOT NULL
    """


def build_periods(facts, today):
    periods = {g: [] for g in GRAIN_SQL}
    seen = {g: set() for g in GRAIN_SQL}
    for rec in facts:
        g, k = rec["g"], str(rec["k"])
        if not k or k in seen[g]:
            continue
        seen[g].add(k)
        end = period_end(g, k)
        periods[g].append({
            "id": k,
            "label": period_label(g, k),
            "partial": end >= today,
        })
    for g in periods:
        periods[g].sort(key=lambda x: x["id"])
    defaults = {}
    for g, rows in periods.items():
        complete = [r for r in rows if not r["partial"]]
        defaults[g] = (complete[-1]["id"] if complete else (rows[-1]["id"] if rows else None))
    return periods, defaults


def main():
    global START
    print("Halytska / Kulinichi / VZHAR report", flush=True)
    conn = connect()
    providers = fetch_providers(conn)
    print(f"providers {len(providers)}", flush=True)
    if not providers:
        raise SystemExit("No providers found for Halytska list")
    ids_preview = ",".join(str(i) for i in providers)
    first = qtry(conn, "first_order", f"""
        SELECT MIN(order_created_date) AS d
        FROM main.ng_delivery.dim_order_delivery
        WHERE provider_id IN ({ids_preview})
          AND order_state = 'delivered'
    """)
    if first is not None and not first.empty and first.iloc[0]["d"] is not None:
        START = str(to_native(first.iloc[0]["d"]))[:10]
        print(f"partnership start {START}", flush=True)

    ids = ids_sql(providers)
    ids_brands = {pid: m["brand"] for pid, m in providers.items()}
    bcase = brand_case(ids_brands)

    facts = []
    for grain in GRAIN_SQL:
        df = qtry(conn, f"facts:{grain}", facts_sql(grain, ids, bcase))
        if df.empty:
            continue
        for rec in df_records(df):
            rec["g"] = grain
            rec["k"] = str(rec.get("k") or "")
            rec["p"] = int(rec.get("p") or 0)
            rec["b"] = rec.get("b") or ids_brands.get(rec["p"], "halytska")
            facts.append(rec)

    uniq = []
    for grain in GRAIN_SQL:
        for level in ("brand", "all"):
            df = qtry(conn, f"uniq:{grain}:{level}", uniq_sql(grain, ids, bcase, level))
            for rec in df_records(df):
                rec["g"] = grain
                rec["k"] = str(rec.get("k") or "")
                rec["b"] = rec.get("b") or "all"
                uniq.append(rec)

    camps = []
    for grain in ("week", "month"):
        df = qtry(conn, f"campaigns:{grain}", campaign_sql(grain, ids))
        for rec in df_records(df):
            rec["g"] = grain
            rec["k"] = str(rec.get("k") or "")
            rec["p"] = int(rec.get("p") or 0)
            camps.append(rec)

    dishes = []
    df = qtry(conn, "dishes", dishes_sql(ids, bcase))
    for rec in df_records(df, ["k", "b", "item", "qty"]):
        rec["k"] = str(rec.get("k") or "")
        dishes.append(rec)

    cancels = []
    for grain in ("week", "month"):
        df = qtry(conn, f"cancels:{grain}", cancels_sql(grain, ids))
        for rec in df_records(df):
            rec["g"] = grain
            rec["k"] = str(rec.get("k") or "")
            rec["p"] = int(rec.get("p") or 0)
            rec["reason"] = str(rec.get("reason") or "other")[:80]
            rec["actor"] = str(rec.get("actor") or "other")
            rec["n"] = int(rec.get("n") or 0)
            cancels.append(rec)

    created = []
    for grain in GRAIN_SQL:
        df = qtry(conn, f"created:{grain}", created_sql(grain, ids))
        for rec in df_records(df):
            rec["g"] = grain
            rec["k"] = str(rec.get("k") or "")
            rec["p"] = int(rec.get("p") or 0)
            created.append(rec)

    ops = df_records(qtry(conn, "ops", ops_sql(ids)))
    for rec in ops:
        rec["p"] = int(rec.get("p") or 0)
        rec["k"] = str(rec.get("k") or "")

    daypart = df_records(qtry(conn, "daypart", daypart_sql(ids, bcase)))
    ratings = df_records(qtry(conn, "ratings", ratings_sql(ids)))
    for rec in ratings:
        rec["p"] = int(rec.get("p") or 0)
        rec["k"] = str(rec.get("k") or "")

    reviews = []
    df = qtry(conn, "reviews", reviews_sql(ids))
    for rec in df_records(df, ["p", "dt", "rating", "text"]):
        rec["p"] = int(rec.get("p") or 0)
        rec["text"] = nda_clean(rec.get("text"))
        if rec["text"]:
            reviews.append(rec)

    sl = []

    try:
        conn.close()
    except Exception:
        pass

    today = date.today()
    periods, defaults = build_periods(facts, today)

    payload = {
        "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "start": START,
        "start_label": f"з {START[5:7]}.{START[:4]}" if START else "від старту співпраці",
        "holding": "Галицька перепічка",
        "brands": BRANDS,
        "providers": {str(k): v for k, v in providers.items()},
        "periods": periods,
        "defaults": defaults,
        "facts": facts,
        "uniq": uniq,
        "camps": camps,
        "dishes": dishes,
        "cancels": cancels,
        "created": created,
        "ops": ops,
        "daypart": daypart,
        "ratings": ratings,
        "reviews": reviews[:80],
        "sl": sl,
    }

    html = TEMPLATE.read_text(encoding="utf-8")
    html = html.replace("__DATA__", json.dumps(payload, ensure_ascii=False, default=str))
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUT_DIR / "index.html"
    out.write_text(html, encoding="utf-8")
    print(f"Wrote {out} ({out.stat().st_size / 1024:.0f} KB)", flush=True)
    print("defaults", defaults, flush=True)
    print("venues", len(providers), "facts", len(facts), flush=True)


if __name__ == "__main__":
    main()
