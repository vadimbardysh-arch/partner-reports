"""
Generate a multi-store weekly HTML report for Мʼясоруб by querying Databricks.
Produces miasorub/index.html — identical structure to the Мʼясоруб dashboard,
with added Smart Promo & Sponsored Listing analytics.
"""

import os
import sys
import json
import math
from pathlib import Path
from datetime import datetime
from decimal import Decimal

sys.stdout.reconfigure(line_buffering=True)

from databricks import sql
import pandas as pd

from config import SERVER_HOSTNAME, HTTP_PATH

REPO_ROOT = Path(__file__).resolve().parent.parent
WEEKS_BACK = 52

MIASORUB_PROVIDERS = {
    161580: {"name": "Мʼясоруб Тернопіль", "short": "Купчинського", "city": "Ternopil"},
    159803: {"name": "Мʼясоруб Хмельницький", "short": "Проскурівського Підпілля", "city": "Khmelnytskyi"},
    203549: {"name": "Мʼясоруб Львів", "short": "Наукова", "city": "Lviv"},
    161698: {"name": "Мʼясоруб Чернівці Фастівська", "short": "Фастівська", "city": "Chernivtsi"},
    161701: {"name": "Мʼясоруб Чернівці Кільцева", "short": "Кільцева", "city": "Chernivtsi"},
    161582: {"name": "Мʼясоруб Вовчинецька", "short": "Вовчинецька", "city": "Ivano-Frankivsk"},
}

PROVIDER_IDS = ",".join(str(k) for k in MIASORUB_PROVIDERS)

CITY_UA = {
    "Lviv": "Львів",
    "Zhytomyr": "Житомир",
    "Khmelnytskyi": "Хмельницький",
    "Rivne": "Рівне",
    "Ternopil": "Тернопіль",
    "Kyiv": "Київ",
    "Kharkiv": "Харків",
    "Vyshhorod": "Вишгород",
    "Kolomyia": "Коломия",
    "Chernivtsi": "Чернівці",
    "Ivano-Frankivsk": "Івано-Франківськ",
}

BAD_ORDER_TYPE_UA = {
    "late_delivery_order_15min": "Пізня доставка (>15 хв)",
    "timing_quality_cs_ticket": "Скарга на час доставки",
    "delivery_quality_cs_ticket": "Скарга на доставку",
    "order_quality_cs_ticket": "Скарга на якість замовлення",
    "missing_or_wrong_items_cs_ticket": "Відсутні/неправильні товари",
    "failed_order_after_provider_accepted": "Невдале після прийняття",
    "bad_rating_order": "Поганий рейтинг",
}
FAULT_UA = {"provider": "Заклад", "courier": "Кур'єр", "bolt": "Bolt", "eater": "Клієнт", "unknown": "Невідомо"}
ORDER_STATE_UA = {"delivered": "Доставлено", "cancelled": "Скасовано", "rejected": "Відхилено", "failed": "Помилка"}


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


def to_native(val, default=None):
    if val is None:
        return default
    if isinstance(val, Decimal):
        return float(val)
    if hasattr(val, "item"):
        return val.item()
    try:
        f = float(val)
        return default if math.isnan(f) else f
    except (TypeError, ValueError):
        return str(val)


def safe_json(df):
    # Newer pandas serializes Decimal cells as JSON strings, which breaks
    # numeric comparisons/formatting downstream. Coerce Decimals to float first.
    df = df.map(lambda v: float(v) if isinstance(v, Decimal) else v)
    return json.loads(df.to_json(orient="records", date_format="iso"))


def week_sort_key(w):
    parts = w.split("-W")
    return (int(parts[0]), int(parts[1]))

def month_sort_key(m):
    parts = m.split("-")
    return (int(parts[0]), int(parts[1]))

def week_to_month(w):
    """Convert '2026-W17' to '2026-04' using ISO 8601 Thursday rule."""
    year, wk = w.split("-W")
    from datetime import datetime, timedelta
    jan4 = datetime(int(year), 1, 4)
    start_of_w1 = jan4 - timedelta(days=jan4.weekday())
    monday = start_of_w1 + timedelta(weeks=int(wk) - 1)
    thursday = monday + timedelta(days=3)
    return thursday.strftime("%Y-%m")


# ── Queries ──────────────────────────────────────────────────────────────

def fetch_weekly_per_store(conn):
    return query(conn, f"""
    SELECT
      f.provider_id,
      f.provider_name,
      f.city_name,
      f.order_week,
      COUNT(*) AS orders,
      ROUND(AVG(f.order_gmv), 0) AS avg_check,
      ROUND(AVG(f.order_actual_cooking_time_minutes), 1) AS avg_cooking,
      SUM(CASE WHEN f.is_bad_order = true THEN 1 ELSE 0 END) AS bad_orders
    FROM ng_delivery_spark.fact_order_delivery f
    WHERE f.provider_id IN ({PROVIDER_IDS})
      AND f.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
      AND f.order_created_date < DATE_TRUNC('WEEK', CURRENT_DATE())
      AND f.order_state = 'delivered'
    GROUP BY f.provider_id, f.provider_name, f.city_name, f.order_week
    ORDER BY f.order_week, f.provider_id
    """)


def fetch_monthly_per_store(conn):
    return query(conn, f"""
    SELECT
      f.provider_id,
      f.provider_name,
      f.city_name,
      DATE_FORMAT(f.order_created_date, 'yyyy-MM') AS order_month,
      COUNT(*) AS orders,
      ROUND(AVG(f.order_gmv), 0) AS avg_check,
      ROUND(AVG(f.order_actual_cooking_time_minutes), 1) AS avg_cooking,
      SUM(CASE WHEN f.is_bad_order = true THEN 1 ELSE 0 END) AS bad_orders
    FROM ng_delivery_spark.fact_order_delivery f
    WHERE f.provider_id IN ({PROVIDER_IDS})
      AND f.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
      AND f.order_state = 'delivered'
    GROUP BY f.provider_id, f.provider_name, f.city_name, DATE_FORMAT(f.order_created_date, 'yyyy-MM')
    ORDER BY order_month, f.provider_id
    """)


def fetch_ops_metrics(conn):
    return query(conn, f"""
    SELECT
      provider_id,
      date,
      ROUND(availability_rate_last_7d * 100, 1) AS availability,
      ROUND(acceptance_rate_last_7d * 100, 1) AS acceptance,
      ROUND(image_coverage_rate * 100, 1) AS photo_coverage,
      ROUND(COALESCE(avg_rating_last_7d, avg_rating_last_14d, avg_rating_last_30d), 2) AS rating
    FROM ng_public_spark.etl_incentives_provider_targeting_features
    WHERE provider_id IN ({PROVIDER_IDS})
      AND date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
    ORDER BY date, provider_id
    """)


def fetch_top_items(conn):
    return query(conn, f"""
    WITH ranked AS (
      SELECT
        f.provider_id,
        f.order_week,
        COALESCE(GET_JSON_OBJECT(b.basket_item_name_translation, '$.uk-UA'), b.basket_item_name) AS item_name,
        COUNT(*) AS qty,
        ROUND(SUM(b.item_price_before_discount_with_vat_local), 0) AS revenue,
        ROW_NUMBER() OVER (PARTITION BY f.provider_id, f.order_week ORDER BY COUNT(*) DESC) AS rn
      FROM ng_delivery_spark.dim_basket_item_delivery b
      JOIN ng_delivery_spark.fact_order_delivery f ON b.order_id = f.order_id
      WHERE f.provider_id IN ({PROVIDER_IDS})
        AND f.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
        AND b.basket_item_created_date >= DATE_FORMAT(DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7}), 'yyyy-MM-dd')
        AND f.order_state = 'delivered'
        AND b.item_price_before_discount_with_vat_local IS NOT NULL
        AND b.item_price_before_discount_with_vat_local > 0
      GROUP BY f.provider_id, f.order_week, COALESCE(GET_JSON_OBJECT(b.basket_item_name_translation, '$.uk-UA'), b.basket_item_name)
    )
    SELECT provider_id, order_week, item_name, qty, revenue
    FROM ranked WHERE rn <= 10
    ORDER BY provider_id, order_week, qty DESC
    """)


def fetch_orders_detail(conn):
    return query(conn, f"""
    SELECT
      f.order_id, f.order_reference_id, f.order_created_date, f.order_week,
      f.provider_id, f.provider_name, f.order_state,
      CASE WHEN f.is_bolt_plus_order THEN 'Bolt Plus' ELSE 'Ні' END AS bolt_plus,
      f.is_bolt_plus_order,
      ROUND(COALESCE(f.provider_price_before_discount, 0), 2) AS food_before_discount,
      ROUND(COALESCE(f.total_order_item_discount, 0), 2) AS total_discount,
      ROUND(COALESCE((COALESCE(m.bolt_delivery_campaign_cost_eur, 0) + COALESCE(m.bolt_menu_campaign_cost_eur, 0)) * m.currency_rate, 0), 2) AS bolt_discount,
      ROUND(COALESCE((COALESCE(m.provider_delivery_campaign_cost_eur, 0) + COALESCE(m.provider_menu_campaign_cost_eur, 0)) * m.currency_rate, 0), 2) AS provider_discount,
      ROUND(COALESCE(f.provider_price_after_discount, 0), 2) AS food_revenue,
      ROUND(COALESCE(f.inv_bolt_cmp_spent_local, 0), 2) AS bolt_compensation,
      ROUND(COALESCE(m.provider_commission_net_eur * m.currency_rate, 0), 2) AS fee_net,
      ROUND(COALESCE(m.provider_commission_gross_eur * m.currency_rate, 0), 2) AS fee_gross,
      ROUND(COALESCE(f.commission_local - COALESCE(m.provider_commission_net_eur * m.currency_rate, 0), 0), 2) AS bp_fee_net,
      ROUND(COALESCE((f.commission_local - COALESCE(m.provider_commission_net_eur * m.currency_rate, 0)) * 1.2, 0), 2) AS bp_fee_gross,
      ROUND(COALESCE(f.commission_local * 1.2, 0), 2) AS total_fee_gross,
      ROUND(COALESCE(f.total_refunded_amount, 0), 2) AS refund,
      ROUND(COALESCE(f.provider_price_after_discount, 0) + COALESCE(f.inv_bolt_cmp_spent_local, 0) - COALESCE(f.commission_local * 1.2, 0) - COALESCE(f.total_refunded_amount, 0), 2) AS net_income,
      CASE
        WHEN f.order_state = 'delivered' THEN NULL
        WHEN f.is_rejected_by_provider = true THEN 'Відхилено закладом'
        WHEN f.is_not_responded_by_provider = true THEN 'Без відповіді від закладу'
        WHEN f.order_state = 'failed' THEN 'Помилка системи'
        WHEN f.order_state = 'cancelled' THEN 'Скасовано клієнтом'
        ELSE 'Інше'
      END AS fail_reason
    FROM ng_delivery_spark.fact_order_delivery f
    LEFT JOIN ng_public_spark.etl_delivery_order_monetary_metrics m
      ON f.order_id = m.order_id
      AND m.order_created_date >= DATE_FORMAT(DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7}), 'yyyy-MM-dd')
    WHERE f.provider_id IN ({PROVIDER_IDS})
      AND f.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
    ORDER BY f.order_created_date DESC, f.order_id DESC
    """)


def fetch_complaints(conn):
    return query(conn, f"""
    SELECT
      d.order_id, f.order_reference_id, f.order_created_date, f.order_week,
      f.provider_id, f.provider_name,
      ROUND(f.order_gmv, 0) AS sum_uah,
      d.bad_order_type, d.bad_order_actor_at_fault AS fault,
      d.provider_rating_value AS rating,
      d.provider_rating_comment
    FROM ng_delivery_spark.dim_order_delivery d
    JOIN ng_delivery_spark.fact_order_delivery f ON d.order_id = f.order_id
    WHERE f.provider_id IN ({PROVIDER_IDS})
      AND f.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
      AND (d.is_bad_order = true OR d.is_cs_ticket_order = true)
    ORDER BY f.order_created_date DESC
    """)


def fetch_cancelled(conn):
    return query(conn, f"""
    SELECT
      f.order_id, f.order_reference_id, f.order_created_date, f.order_week,
      f.provider_id, f.provider_name, f.order_state,
      CASE
        WHEN f.is_rejected_by_provider = true THEN 'Відхилено закладом'
        WHEN f.is_not_responded_by_provider = true THEN 'Без відповіді від закладу'
        WHEN f.order_state = 'failed' THEN 'Помилка'
        ELSE 'Скасовано'
      END AS reason,
      d.failed_order_comment AS comment
    FROM ng_delivery_spark.fact_order_delivery f
    LEFT JOIN ng_delivery_spark.dim_order_delivery d ON f.order_id = d.order_id
    WHERE f.provider_id IN ({PROVIDER_IDS})
      AND f.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
      AND f.order_state IN ('rejected', 'cancelled', 'failed')
    ORDER BY f.order_created_date DESC
    """)


def fetch_revenue_weekly(conn):
    return query(conn, f"""
    SELECT
      f.order_week, f.provider_id,
      COUNT(*) AS orders,
      ROUND(SUM(f.provider_price_after_discount), 0) AS food_revenue,
      ROUND(SUM(COALESCE(f.inv_bolt_cmp_spent_local, 0)), 0) AS bolt_compensation,
      ROUND(SUM(f.commission_local * 1.2), 0) AS total_fee_gross,
      ROUND(SUM(COALESCE(f.total_refunded_amount, 0)), 0) AS refund,
      ROUND(SUM(f.provider_price_after_discount) + SUM(COALESCE(f.inv_bolt_cmp_spent_local, 0)) - SUM(f.commission_local * 1.2) - SUM(COALESCE(f.total_refunded_amount, 0)), 0) AS net_income
    FROM ng_delivery_spark.fact_order_delivery f
    WHERE f.provider_id IN ({PROVIDER_IDS})
      AND f.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
      AND f.order_created_date < DATE_TRUNC('WEEK', CURRENT_DATE())
      AND f.order_state = 'delivered'
    GROUP BY f.order_week, f.provider_id
    ORDER BY f.order_week, f.provider_id
    """)


def fetch_campaigns(conn):
    return query(conn, f"""
    SELECT
      c.campaign_id,
      c.name AS campaign_name,
      c.spend_objective,
      c.target,
      ROUND(c.discount_level, 0) AS discount_pct,
      c.cost_share_v2,
      DATE(c.campaign_start) AS start_date,
      DATE(c.campaign_end) AS end_date,
      c.provider_id,
      CONCAT(YEAR(c.order_created_date), '-W', WEEKOFYEAR(c.order_created_date)) AS order_week,
      COUNT(*) AS orders,
      ROUND(SUM(c.discount_value_local), 0) AS total_discount_uah,
      ROUND(SUM(c.bolt_spend_local), 0) AS bolt_spend_uah,
      ROUND(SUM(c.provider_spend_local), 0) AS provider_spend_uah
    FROM ng_public_spark.etl_delivery_campaign_order_metrics c
    WHERE c.provider_id IN ({PROVIDER_IDS})
      AND c.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
    GROUP BY c.campaign_id, c.name, c.spend_objective, c.target,
             c.discount_level, c.cost_share_v2,
             DATE(c.campaign_start), DATE(c.campaign_end),
             c.provider_id,
             CONCAT(YEAR(c.order_created_date), '-W', WEEKOFYEAR(c.order_created_date))
    ORDER BY order_week DESC, orders DESC
    """)


def fetch_smart_promo(conn):
    return query(conn, f"""
    SELECT
      provider_id,
      smart_promo_offer_type,
      smart_promo_type,
      smart_promo_enrollment_state,
      smart_promo_offer_mode,
      DATE(smart_promo_provider_enrollment_start_ts) AS enrollment_start,
      DATE(smart_promo_provider_enrollment_end_ts) AS enrollment_end,
      is_valid_promotion,
      campaign_id,
      campaign_spend_objective
    FROM core_models_spark.fact_provider_smart_promo_offer_campaign_enrollment
    WHERE provider_id IN ({PROVIDER_IDS})
    ORDER BY provider_id, smart_promo_provider_enrollment_start_ts DESC
    """)


def fetch_smart_promo_orders(conn):
    """Fetch orders that came through Smart Promo campaigns."""
    return query(conn, f"""
    SELECT
      c.provider_id,
      CONCAT(YEAR(c.order_created_date), '-W', WEEKOFYEAR(c.order_created_date)) AS order_week,
      COUNT(DISTINCT c.order_id) AS orders,
      ROUND(SUM(c.discount_value_local), 0) AS discount_uah,
      ROUND(SUM(c.provider_spend_local), 0) AS provider_spend_uah,
      ROUND(SUM(c.bolt_spend_local), 0) AS bolt_spend_uah
    FROM ng_public_spark.etl_delivery_campaign_order_metrics c
    WHERE c.provider_id IN ({PROVIDER_IDS})
      AND c.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
      AND LOWER(c.spend_objective) LIKE '%smart_promo%'
    GROUP BY c.provider_id,
             CONCAT(YEAR(c.order_created_date), '-W', WEEKOFYEAR(c.order_created_date))
    ORDER BY order_week DESC, provider_id
    """)


def fetch_sponsored_listings(conn):
    """Fetch Sponsored Listing campaign status and spend."""
    return query(conn, f"""
    SELECT
      sl.provider_id,
      sl.offer_name AS campaign_name,
      DATE(sl.sponsored_listing_start_ts_local) AS start_date,
      DATE(COALESCE(sl.sponsored_listing_actual_end_ts_local, sl.sponsored_listing_default_end_ts_local)) AS end_date,
      CONCAT(YEAR(sl.sponsored_listing_start_ts_local), '-W', WEEKOFYEAR(sl.sponsored_listing_start_ts_local)) AS order_week,
      0 AS orders,
      ROUND(SUM(sl.offer_price_per_day_local * (
        DATEDIFF(
          DATE(COALESCE(sl.sponsored_listing_actual_end_ts_local, sl.sponsored_listing_default_end_ts_local)),
          DATE(sl.sponsored_listing_start_ts_local)
        ) + 1
      )), 2) AS total_spend_uah,
      ROUND(SUM(sl.offer_price_per_day_local * (
        DATEDIFF(
          DATE(COALESCE(sl.sponsored_listing_actual_end_ts_local, sl.sponsored_listing_default_end_ts_local)),
          DATE(sl.sponsored_listing_start_ts_local)
        ) + 1
      )), 2) AS provider_spend_uah
    FROM ng_delivery_spark.dim_sponsored_listing sl
    WHERE sl.provider_id IN ({PROVIDER_IDS})
      AND sl.sponsored_listing_start_ts_local >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
      AND sl.sponsored_listing_state IN ('finished', 'active')
    GROUP BY sl.provider_id, sl.offer_name,
             DATE(sl.sponsored_listing_start_ts_local),
             DATE(COALESCE(sl.sponsored_listing_actual_end_ts_local, sl.sponsored_listing_default_end_ts_local)),
             CONCAT(YEAR(sl.sponsored_listing_start_ts_local), '-W', WEEKOFYEAR(sl.sponsored_listing_start_ts_local))
    ORDER BY start_date DESC, sl.provider_id
    """)


SPEND_OBJ_UA = {
    "provider_campaign_obligations_commitments": "Зобов'язання",
    "provider_campaign_portal": "Портал провайдера",
    "provider_campaign_marketing": "Маркетинг",
    "provider_campaign_locations": "Локації",
    "bolt_plus_campaign": "Bolt Plus",
    "new_city_launch": "Запуск міста",
    "activation": "Активація",
    "marketing_3rd_party_partnership": "Партнерство",
    "retention": "Утримання",
}
TARGET_UA = {"delivery_price": "Доставка", "item_price": "Знижка на товар"}


# ── Build data for HTML ──────────────────────────────────────────────────

def build_data(weekly_df, ops_df, items_df, orders_df, complaints_df, cancelled_df, revenue_df, campaigns_df,
               smart_promo_df=None, smart_promo_orders_df=None, sponsored_df=None, monthly_df=None):
    data = {}

    stores_map = {}
    for _, r in weekly_df.iterrows():
        pid = to_native(r["provider_id"])
        info = MIASORUB_PROVIDERS.get(int(pid), {})
        stores_map[int(pid)] = {
            "name": info.get("name", r["provider_name"]),
            "short": info.get("short", r["provider_name"]),
            "city": CITY_UA.get(r["city_name"], r["city_name"]),
            "city_en": r["city_name"],
        }
    data["stores"] = stores_map

    weekly = {}
    for _, r in weekly_df.iterrows():
        pid = int(to_native(r["provider_id"]))
        week = str(r["order_week"])
        if week not in weekly:
            weekly[week] = {}
        weekly[week][pid] = {
            "orders": to_native(r["orders"]),
            "avg_check": to_native(r["avg_check"]),
            "avg_cooking": to_native(r["avg_cooking"]),
            "bad_orders": to_native(r["bad_orders"]),
        }
    weekly = dict(sorted(weekly.items(), key=lambda x: week_sort_key(x[0])))
    data["weekly"] = weekly

    # --- Monthly aggregation from direct query (calendar months) ---
    monthly = {}
    if monthly_df is not None and len(monthly_df):
        for _, r in monthly_df.iterrows():
            pid = int(to_native(r["provider_id"]))
            mk = str(r["order_month"])
            if mk not in monthly:
                monthly[mk] = {}
            monthly[mk][pid] = {
                "orders": to_native(r["orders"]),
                "avg_check": to_native(r["avg_check"]),
                "avg_cooking": to_native(r["avg_cooking"]),
                "bad_orders": to_native(r["bad_orders"]),
            }
    else:
        for wk, stores_data in weekly.items():
            mk = week_to_month(wk)
            if mk not in monthly:
                monthly[mk] = {}
            for pid, vals in stores_data.items():
                if pid not in monthly[mk]:
                    monthly[mk][pid] = {"orders": 0, "_check_sum": 0, "_cook_sum": 0, "_cook_cnt": 0, "bad_orders": 0}
                monthly[mk][pid]["orders"] += vals["orders"]
                monthly[mk][pid]["_check_sum"] += vals["avg_check"] * vals["orders"]
                if vals["avg_cooking"] and vals["avg_cooking"] > 0:
                    monthly[mk][pid]["_cook_sum"] += vals["avg_cooking"] * vals["orders"]
                    monthly[mk][pid]["_cook_cnt"] += vals["orders"]
                monthly[mk][pid]["bad_orders"] += vals["bad_orders"]
        for mk in monthly:
            for pid in monthly[mk]:
                d = monthly[mk][pid]
                d["avg_check"] = round(d["_check_sum"] / d["orders"]) if d["orders"] else 0
                d["avg_cooking"] = round(d["_cook_sum"] / d["_cook_cnt"], 1) if d["_cook_cnt"] else 0
                del d["_check_sum"]
                del d["_cook_sum"]
                del d["_cook_cnt"]
    monthly = dict(sorted(monthly.items(), key=lambda x: month_sort_key(x[0])))
    data["monthly"] = monthly

    ops_weekly = {}
    if len(ops_df):
        ops_df["date_str"] = ops_df["date"].astype(str)
        ops_df_sorted = ops_df.sort_values("date")
        for _, r in ops_df_sorted.iterrows():
            pid = int(to_native(r["provider_id"]))
            ds = str(r["date_str"])
            ts = pd.Timestamp(ds)
            iso_cal = ts.isocalendar()
            year = int(iso_cal.year)
            iso_week = int(iso_cal.week)
            week_key = f"{year}-W{iso_week}"
            if week_key not in ops_weekly:
                ops_weekly[week_key] = {}
            ops_weekly[week_key][pid] = {
                "availability": to_native(r["availability"], default=None),
                "acceptance": to_native(r["acceptance"], default=None),
                "photo_coverage": to_native(r["photo_coverage"], default=None),
                "rating": to_native(r["rating"], default=None),
            }

    latest_ops = {}
    if len(ops_df):
        latest = ops_df.sort_values("date").drop_duplicates(subset=["provider_id"], keep="last")
        for _, r in latest.iterrows():
            pid = int(to_native(r["provider_id"]))
            latest_ops[pid] = {
                "availability": to_native(r["availability"], default=None),
                "acceptance": to_native(r["acceptance"], default=None),
                "photo_coverage": to_native(r["photo_coverage"], default=None),
                "rating": to_native(r["rating"], default=None),
            }
    data["ops_weekly"] = ops_weekly
    data["latest_ops"] = latest_ops

    # --- Monthly ops aggregation ---
    ops_monthly = {}
    for wk, stores_data in ops_weekly.items():
        mk = week_to_month(wk)
        if mk not in ops_monthly:
            ops_monthly[mk] = {}
        for pid, vals in stores_data.items():
            if pid not in ops_monthly[mk]:
                ops_monthly[mk][pid] = {
                    "_avail_sum": 0, "_avail_cnt": 0,
                    "_accept_sum": 0, "_accept_cnt": 0,
                    "_photo_sum": 0, "_photo_cnt": 0,
                    "_rating_sum": 0, "_rating_cnt": 0,
                }
            if vals.get("availability") is not None:
                ops_monthly[mk][pid]["_avail_sum"] += vals["availability"]
                ops_monthly[mk][pid]["_avail_cnt"] += 1
            if vals.get("acceptance") is not None:
                ops_monthly[mk][pid]["_accept_sum"] += vals["acceptance"]
                ops_monthly[mk][pid]["_accept_cnt"] += 1
            if vals.get("photo_coverage") is not None:
                ops_monthly[mk][pid]["_photo_sum"] += vals["photo_coverage"]
                ops_monthly[mk][pid]["_photo_cnt"] += 1
            if vals.get("rating") is not None:
                ops_monthly[mk][pid]["_rating_sum"] += vals["rating"]
                ops_monthly[mk][pid]["_rating_cnt"] += 1
    for mk in ops_monthly:
        for pid in ops_monthly[mk]:
            d = ops_monthly[mk][pid]
            ops_monthly[mk][pid] = {
                "availability": round(d["_avail_sum"] / d["_avail_cnt"], 1) if d["_avail_cnt"] else None,
                "acceptance": round(d["_accept_sum"] / d["_accept_cnt"], 1) if d["_accept_cnt"] else None,
                "photo_coverage": round(d["_photo_sum"] / d["_photo_cnt"], 1) if d["_photo_cnt"] else None,
                "rating": round(d["_rating_sum"] / d["_rating_cnt"], 2) if d["_rating_cnt"] else None,
            }
    data["ops_monthly"] = ops_monthly

    top_items = {}
    for _, r in items_df.iterrows():
        pid = int(to_native(r["provider_id"]))
        week = str(r["order_week"])
        if week not in top_items:
            top_items[week] = {}
        if pid not in top_items[week]:
            top_items[week][pid] = []
        top_items[week][pid].append({
            "name": str(r["item_name"]),
            "qty": to_native(r["qty"]),
            "revenue": to_native(r["revenue"]),
        })
    data["top_items"] = top_items

    # --- Monthly items aggregation ---
    monthly_items = {}
    for wk, stores_data in top_items.items():
        mk = week_to_month(wk)
        if mk not in monthly_items:
            monthly_items[mk] = {}
        for pid, items_list in stores_data.items():
            if pid not in monthly_items[mk]:
                monthly_items[mk][pid] = {}
            for it in items_list:
                nm = it["name"]
                if nm not in monthly_items[mk][pid]:
                    monthly_items[mk][pid][nm] = {"name": nm, "qty": 0, "revenue": 0}
                monthly_items[mk][pid][nm]["qty"] += it["qty"]
                monthly_items[mk][pid][nm]["revenue"] += it["revenue"]
    for mk in monthly_items:
        for pid in monthly_items[mk]:
            monthly_items[mk][pid] = sorted(monthly_items[mk][pid].values(), key=lambda x: -x["qty"])[:10]
    data["monthly_items"] = monthly_items

    orders_list = safe_json(orders_df)
    for row in orders_list:
        raw_state = row.get("order_state", "") or ""
        row["order_state_raw"] = raw_state
        row["order_state"] = ORDER_STATE_UA.get(raw_state, raw_state)
        pid = row.get("provider_id")
        if pid and int(pid) in MIASORUB_PROVIDERS:
            row["provider_short"] = MIASORUB_PROVIDERS[int(pid)]["short"]
        else:
            row["provider_short"] = row.get("provider_name", "")
        ow = row.get("order_week", "")
        if ow:
            row["order_month"] = week_to_month(str(ow))
    data["orders"] = orders_list

    comp_list = safe_json(complaints_df)
    for row in comp_list:
        raw_type = row.get("bad_order_type", "") or ""
        row["bad_order_type"] = BAD_ORDER_TYPE_UA.get(raw_type, raw_type)
        raw_fault = row.get("fault", "") or ""
        row["fault"] = FAULT_UA.get(str(raw_fault).lower(), raw_fault)
        pid = row.get("provider_id")
        if pid and int(pid) in MIASORUB_PROVIDERS:
            row["provider_short"] = MIASORUB_PROVIDERS[int(pid)]["short"]
        else:
            row["provider_short"] = row.get("provider_name", "")
        ow = row.get("order_week", "")
        if ow:
            row["order_month"] = week_to_month(str(ow))
    data["complaints"] = comp_list

    canc_list = safe_json(cancelled_df)
    for row in canc_list:
        raw_state = row.get("order_state", "") or ""
        row["order_state"] = ORDER_STATE_UA.get(raw_state, raw_state)
        pid = row.get("provider_id")
        if pid and int(pid) in MIASORUB_PROVIDERS:
            row["provider_short"] = MIASORUB_PROVIDERS[int(pid)]["short"]
        else:
            row["provider_short"] = row.get("provider_name", "")
        ow = row.get("order_week", "")
        if ow:
            row["order_month"] = week_to_month(str(ow))
    data["cancelled"] = canc_list

    revenue = {}
    for _, r in revenue_df.iterrows():
        week = str(r["order_week"])
        pid = int(to_native(r["provider_id"]))
        if week not in revenue:
            revenue[week] = {}
        revenue[week][pid] = {
            "orders": to_native(r["orders"]),
            "food_revenue": to_native(r["food_revenue"]),
            "bolt_compensation": to_native(r["bolt_compensation"]),
            "total_fee_gross": to_native(r["total_fee_gross"]),
            "refund": to_native(r["refund"]),
            "net_income": to_native(r["net_income"]),
        }
    revenue = dict(sorted(revenue.items(), key=lambda x: week_sort_key(x[0])))
    data["revenue"] = revenue

    # --- Monthly revenue ---
    monthly_revenue = {}
    for wk, stores_data in revenue.items():
        mk = week_to_month(wk)
        if mk not in monthly_revenue:
            monthly_revenue[mk] = {}
        for pid, vals in stores_data.items():
            if pid not in monthly_revenue[mk]:
                monthly_revenue[mk][pid] = {"orders": 0, "food_revenue": 0, "bolt_compensation": 0, "total_fee_gross": 0, "refund": 0, "net_income": 0}
            for k in ("orders", "food_revenue", "bolt_compensation", "total_fee_gross", "refund", "net_income"):
                monthly_revenue[mk][pid][k] += vals.get(k, 0)
    monthly_revenue = dict(sorted(monthly_revenue.items(), key=lambda x: month_sort_key(x[0])))
    data["monthly_revenue"] = monthly_revenue

    campaigns = []
    for _, r in campaigns_df.iterrows():
        pid = int(to_native(r["provider_id"]))
        raw_obj = str(r["spend_objective"] or "")
        raw_target = str(r["target"] or "")
        cname = str(r["campaign_name"] or "")
        disc_pct = to_native(r["discount_pct"])
        target_ua = TARGET_UA.get(raw_target, raw_target)
        obj_ua = SPEND_OBJ_UA.get(raw_obj, raw_obj)
        if raw_target == "delivery_price":
            friendly = f"Безк. доставка — {obj_ua}"
        else:
            friendly = f"{int(disc_pct)}% на товар — {obj_ua}"
        campaigns.append({
            "campaign_id": to_native(r["campaign_id"]),
            "name": friendly,
            "full_name": cname[:120],
            "objective": SPEND_OBJ_UA.get(raw_obj, raw_obj),
            "target": TARGET_UA.get(raw_target, raw_target),
            "discount_pct": to_native(r["discount_pct"]),
            "cost_share": to_native(r["cost_share_v2"]),
            "start_date": str(r["start_date"]),
            "end_date": str(r["end_date"]),
            "provider_id": pid,
            "provider_short": MIASORUB_PROVIDERS.get(pid, {}).get("short", str(pid)),
            "order_week": str(r["order_week"]),
            "order_month": week_to_month(str(r["order_week"])),
            "orders": to_native(r["orders"]),
            "total_discount": to_native(r["total_discount_uah"]),
            "bolt_spend": to_native(r["bolt_spend_uah"]),
            "provider_spend": to_native(r["provider_spend_uah"]),
        })
    data["campaigns"] = campaigns

    # Smart Promo enrollments
    sp_enrollments = []
    if smart_promo_df is not None and len(smart_promo_df):
        for _, r in smart_promo_df.iterrows():
            pid = int(to_native(r["provider_id"]))
            sp_enrollments.append({
                "provider_id": pid,
                "provider_short": MIASORUB_PROVIDERS.get(pid, {}).get("short", str(pid)),
                "offer_type": str(r.get("smart_promo_offer_type", "") or ""),
                "promo_type": str(r.get("smart_promo_type", "") or ""),
                "state": str(r.get("smart_promo_enrollment_state", "") or ""),
                "mode": str(r.get("smart_promo_offer_mode", "") or ""),
                "start": str(r.get("enrollment_start", "") or ""),
                "end": str(r.get("enrollment_end", "") or ""),
                "is_valid": bool(r.get("is_valid_promotion", False)),
            })
    data["smart_promo_enrollments"] = sp_enrollments

    sp_status = {}
    for pid in MIASORUB_PROVIDERS:
        enrollments = [e for e in sp_enrollments if e["provider_id"] == pid]
        active = [e for e in enrollments if e["state"] in ("active", "enrolled")]
        sp_status[pid] = {
            "has_ever_enrolled": len(enrollments) > 0,
            "is_active": len(active) > 0,
            "active_count": len(active),
            "total_enrollments": len(enrollments),
        }
    data["smart_promo_status"] = sp_status

    # Smart Promo orders by week
    sp_orders = {}
    if smart_promo_orders_df is not None and len(smart_promo_orders_df):
        for _, r in smart_promo_orders_df.iterrows():
            pid = int(to_native(r["provider_id"]))
            week = str(r["order_week"])
            if week not in sp_orders:
                sp_orders[week] = {}
            sp_orders[week][pid] = {
                "orders": to_native(r["orders"]),
                "discount_uah": to_native(r["discount_uah"]),
                "provider_spend": to_native(r["provider_spend_uah"]),
                "bolt_spend": to_native(r["bolt_spend_uah"]),
            }
    data["smart_promo_orders"] = sp_orders

    # Sponsored Listings
    sl_data = []
    if sponsored_df is not None and len(sponsored_df):
        for _, r in sponsored_df.iterrows():
            pid = int(to_native(r["provider_id"]))
            sl_data.append({
                "provider_id": pid,
                "provider_short": MIASORUB_PROVIDERS.get(pid, {}).get("short", str(pid)),
                "campaign_name": str(r.get("campaign_name", "") or ""),
                "start_date": str(r.get("start_date", "") or ""),
                "end_date": str(r.get("end_date", "") or ""),
                "order_week": str(r.get("order_week", "") or ""),
                "orders": to_native(r.get("orders", 0)),
                "total_spend": to_native(r.get("total_spend_uah", 0)),
                "provider_spend": to_native(r.get("provider_spend_uah", 0)),
            })
    data["sponsored_listings"] = sl_data

    sl_status = {}
    for pid in MIASORUB_PROVIDERS:
        provider_sl = [s for s in sl_data if s["provider_id"] == pid]
        sl_status[pid] = {
            "has_ever_used": len(provider_sl) > 0,
            "total_orders": sum(s["orders"] for s in provider_sl),
            "total_spend": sum(s["total_spend"] for s in provider_sl),
        }
    data["sponsored_status"] = sl_status

    return data


# ── HTML ─────────────────────────────────────────────────────────────────

CITY_UA_JSON = json.dumps(CITY_UA, ensure_ascii=False)

def generate_html(data, generated_at):
    return f"""<!DOCTYPE html>
<html lang="uk">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>МʼЯСОРУБ | тижневий звіт</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
:root{{
  --green:#34D186;--green-bg:rgba(52,209,134,.08);--dark:#1A1D21;
  --bg:#F3F4F6;--card:#FFF;--text:#111827;--text2:#6B7280;--border:#E5E7EB;
  --pos:#10B981;--neg:#EF4444;--warn:#F59E0B;--blue:#3B82F6;--orange:#F97316;
  --r:12px;--shadow:0 1px 3px rgba(0,0,0,.06),0 1px 2px rgba(0,0,0,.04);
}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Inter',system-ui,sans-serif;background:var(--bg);color:var(--text);line-height:1.5}}
a{{text-decoration:none;color:inherit}}

.header{{position:sticky;top:0;z-index:102;background:var(--card);border-bottom:1px solid var(--border);padding:12px 24px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px}}
.header-left{{display:flex;align-items:center;gap:12px}}
.header-left h1{{font-size:20px;font-weight:800;letter-spacing:-.3px}}
.brand-dot{{width:10px;height:10px;border-radius:50%;background:var(--orange);display:inline-block}}
.header-right{{display:flex;align-items:center;gap:10px;flex-wrap:wrap}}
.ms-wrap{{position:relative;min-width:180px}}
.ms-btn{{padding:8px 32px 8px 14px;border:1px solid var(--border);border-radius:8px;font-size:13px;font-family:inherit;background:var(--card);cursor:pointer;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:260px;display:block;width:100%;text-align:left;color:var(--text);position:relative}}
.ms-btn::after{{content:'▾';position:absolute;right:10px;top:50%;transform:translateY(-50%);font-size:11px;color:var(--text2)}}
.ms-btn:hover,.ms-btn.open{{border-color:var(--orange)}}
.ms-panel{{display:none;position:absolute;top:calc(100% + 4px);left:0;min-width:100%;max-height:320px;overflow-y:auto;background:var(--card);border:1px solid var(--border);border-radius:8px;box-shadow:0 8px 24px rgba(0,0,0,.12);z-index:1000;padding:4px 0}}
.ms-panel.open{{display:block}}
.ms-item{{display:flex;align-items:center;gap:8px;padding:6px 14px;font-size:13px;cursor:pointer;white-space:nowrap}}
.ms-item:hover{{background:var(--bg)}}
.ms-item input{{accent-color:var(--orange);width:15px;height:15px;cursor:pointer;flex-shrink:0}}
.ms-item.all-item{{border-bottom:1px solid var(--border);padding-bottom:8px;margin-bottom:2px;font-weight:600}}
.ms-count{{display:inline-block;background:var(--orange);color:#fff;font-size:10px;font-weight:700;border-radius:10px;padding:1px 6px;margin-left:4px}}
.reset-btn{{background:transparent;border:1px solid var(--border);color:var(--text2);border-radius:8px;padding:7px 11px;font-size:14px;cursor:pointer;transition:all .15s;line-height:1}}
.reset-btn:hover{{background:var(--neg);color:#fff;border-color:var(--neg)}}
.calc-card{{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:20px 24px;margin-top:20px}}
.calc-title{{font-size:15px;font-weight:700;color:var(--text);margin:0 0 16px}}
.calc-controls{{display:flex;gap:16px;flex-wrap:wrap;margin-bottom:16px}}
.calc-field label{{display:block;font-size:11px;font-weight:600;color:var(--text2);margin-bottom:4px;text-transform:uppercase;letter-spacing:.3px}}
.calc-field input,.calc-field select{{padding:8px 12px;border:1px solid var(--border);border-radius:8px;font-size:14px;font-family:inherit;background:var(--bg);color:var(--text);min-width:160px}}
.calc-field input:focus,.calc-field select:focus{{outline:none;border-color:var(--orange);box-shadow:0 0 0 3px rgba(249,115,22,.12)}}
.calc-store-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:12px}}
.calc-store{{background:var(--bg);border:1px solid var(--border);border-radius:10px;padding:14px 16px}}
.calc-store-name{{font-size:13px;font-weight:700;color:var(--text);margin-bottom:4px}}
.calc-store-city{{font-size:11px;color:var(--text2);margin-bottom:10px}}
.calc-bar-wrap{{height:8px;background:rgba(0,0,0,.06);border-radius:4px;overflow:hidden;margin-bottom:8px}}
.calc-bar{{height:100%;border-radius:4px;transition:width .3s}}
.calc-metrics{{display:flex;justify-content:space-between;font-size:12px}}
.calc-spent{{font-weight:700}}
.calc-left{{font-weight:600}}
.calc-total-row{{margin-top:16px;padding:14px 16px;background:var(--card);border:2px solid var(--orange);border-radius:10px;display:flex;flex-wrap:wrap;gap:24px;align-items:center}}
.calc-total-label{{font-size:13px;font-weight:700;color:var(--text)}}
.calc-total-val{{font-size:18px;font-weight:800}}
.period-toggle-wrap{{display:flex;align-items:center;padding:0 20px;margin-top:-4px}}
.period-select{{padding:6px 32px 6px 14px;font-size:13px;font-weight:600;border:1px solid var(--border);background:var(--card);color:var(--text);cursor:pointer;border-radius:8px;font-family:inherit;appearance:none;-webkit-appearance:none;background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6'%3E%3Cpath d='M0 0l5 6 5-6z' fill='%23666'/%3E%3C/svg%3E");background-repeat:no-repeat;background-position:right 10px center;transition:all .15s}}
.period-select:hover{{border-color:var(--orange)}}
.period-select:focus{{outline:none;border-color:var(--orange);box-shadow:0 0 0 3px rgba(249,115,22,.15)}}
.theme-toggle{{background:transparent;border:1px solid var(--border);color:var(--text2);border-radius:8px;padding:7px 12px;font-size:16px;cursor:pointer;transition:all .15s;line-height:1}}
.theme-toggle:hover{{background:var(--bg);color:var(--text)}}
.last-update{{font-size:12px;color:var(--text2)}}

.main-nav{{position:sticky;top:52px;z-index:100;background:var(--card);border-bottom:1px solid var(--border);display:flex;gap:0;overflow-x:auto;scrollbar-width:none;-webkit-overflow-scrolling:touch;padding:0 16px}}
.main-nav::-webkit-scrollbar{{display:none}}
.nav-link{{padding:12px 16px;font-size:13px;font-weight:500;color:var(--text2);white-space:nowrap;border-bottom:2px solid transparent;transition:all .15s}}
.nav-link:hover{{color:var(--text);background:var(--bg)}}
.nav-link.active{{color:var(--orange);border-bottom-color:var(--orange)}}

.week-bar{{position:sticky;top:94px;z-index:99;background:var(--card);border-bottom:1px solid var(--border);display:flex;align-items:center;gap:6px;padding:8px 16px;overflow-x:auto;scrollbar-width:none;-webkit-overflow-scrolling:touch}}
.week-bar::-webkit-scrollbar{{display:none}}
.week-bar-label{{font-size:12px;font-weight:600;color:var(--text2);white-space:nowrap;margin-right:4px}}
.week-pill{{padding:5px 14px;border-radius:20px;font-size:12px;font-weight:500;background:var(--bg);color:var(--text2);cursor:pointer;white-space:nowrap;border:1px solid transparent;transition:all .15s;user-select:none}}
.week-pill:hover{{background:rgba(249,115,22,.08);color:var(--orange)}}
.week-pill.active{{background:var(--orange);color:#fff;border-color:var(--orange)}}

.main-content{{max-width:1360px;margin:0 auto;padding:20px}}
.section{{margin-bottom:32px}}
.section-title{{font-size:18px;font-weight:700;margin-bottom:16px;display:flex;align-items:center;gap:8px}}
.section-icon{{font-size:20px}}

.kpi-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:14px;margin-bottom:28px}}
.kpi-card{{background:var(--card);border-radius:var(--r);padding:18px 20px;box-shadow:var(--shadow);border:1px solid var(--border)}}
.kpi-label{{font-size:12px;color:var(--text2);font-weight:500;text-transform:uppercase;letter-spacing:.3px;margin-bottom:8px}}
.kpi-value{{font-size:26px;font-weight:700;letter-spacing:-.5px;line-height:1.1}}
.kpi-change{{display:inline-flex;align-items:center;gap:3px;font-size:12px;font-weight:600;margin-top:8px;padding:2px 8px;border-radius:20px}}
.kpi-change.up{{color:var(--pos);background:rgba(16,185,129,.1)}}
.kpi-change.down{{color:var(--neg);background:rgba(239,68,68,.1)}}
.kpi-change.neutral{{color:var(--text2);background:var(--bg)}}

.charts-grid{{display:grid;grid-template-columns:repeat(2,1fr);gap:16px}}
.chart-card{{background:var(--card);border-radius:var(--r);box-shadow:var(--shadow);border:1px solid var(--border);padding:20px;min-height:320px;display:flex;flex-direction:column}}
.chart-card h3{{font-size:14px;font-weight:600;margin-bottom:14px;color:var(--text)}}
.chart-card .chart-wrap{{flex:1;position:relative;min-height:240px}}
.chart-card canvas{{width:100%!important}}

.table-wrap{{overflow-x:auto;border-radius:var(--r);border:1px solid var(--border);background:var(--card)}}
.data-table{{width:100%;border-collapse:collapse;font-size:13px}}
.data-table th{{background:var(--bg);font-weight:600;text-align:left;padding:10px 14px;white-space:nowrap;border-bottom:1px solid var(--border);position:sticky;top:0;z-index:1}}
.data-table td{{padding:9px 14px;border-bottom:1px solid var(--border);white-space:nowrap}}
.data-table tr:last-child td{{border-bottom:none}}
.data-table tr:hover td{{background:rgba(249,115,22,.04)}}
.data-table tr.total-row td{{background:var(--bg);font-weight:700}}
.data-table tr.total-row:hover td{{background:var(--bg)}}
.cell-best{{background:rgba(16,185,129,.1)!important;font-weight:600}}
.cell-worst{{background:rgba(239,68,68,.08)!important}}
.text-right{{text-align:right}}
.text-center{{text-align:center}}

.section-insight{{background:linear-gradient(135deg,rgba(249,115,22,.06),rgba(59,130,246,.04));border:1px solid rgba(249,115,22,.15);border-radius:10px;padding:14px 18px;margin-bottom:16px;font-size:13px;line-height:1.6;color:var(--text)}}
.section-insight b{{font-weight:600}}
.insight-good{{color:var(--pos)}}
.insight-bad{{color:var(--neg)}}

.badge{{display:inline-flex;align-items:center;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:600}}
.badge-orange{{background:rgba(249,115,22,.1);color:var(--orange)}}
.bp{{color:var(--blue);font-weight:600}}

.store-filter-wrap{{margin-bottom:12px;display:flex;align-items:center;gap:10px;flex-wrap:wrap}}
.store-filter-wrap label{{font-size:12px;font-weight:600;color:var(--text2)}}
.store-filter-wrap select{{padding:6px 12px;border:1px solid var(--border);border-radius:8px;font-size:12px;font-family:inherit;background:var(--card);cursor:pointer;min-width:200px}}

.items-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:16px}}
.items-card{{background:var(--card);border-radius:var(--r);box-shadow:var(--shadow);border:1px solid var(--border);padding:16px;overflow:hidden}}
.items-card h4{{font-size:14px;font-weight:700;margin-bottom:4px}}
.items-card .items-city{{font-size:11px;color:var(--text2);margin-bottom:10px}}
.items-card ol{{margin:0;padding-left:20px;font-size:12px}}
.items-card li{{padding:3px 0;border-bottom:1px solid var(--border)}}
.items-card li:last-child{{border-bottom:none}}
.items-card .item-qty{{color:var(--orange);font-weight:600;float:right}}
.items-card .item-rev{{color:var(--text2);font-size:11px;float:right;margin-right:8px}}

.scroll-table{{max-height:600px;overflow-y:auto}}
.comment-cell{{max-width:250px;white-space:normal;word-break:break-word}}
.revenue-summary-table{{width:100%;border-collapse:collapse;font-size:12px;margin-top:4px}}
.revenue-summary-table th{{background:var(--bg);font-weight:600;text-align:left;padding:8px 10px;border-bottom:1px solid var(--border)}}
.revenue-summary-table td{{padding:7px 10px;border-bottom:1px solid var(--border)}}

body.dark{{--bg:#111827;--card:#1F2937;--text:#F9FAFB;--text2:#9CA3AF;--border:#374151;--shadow:0 1px 3px rgba(0,0,0,.3)}}
body.dark .header{{background:var(--card)}}
body.dark .main-nav{{background:var(--card)}}
body.dark .week-bar{{background:var(--card)}}
body.dark .data-table th{{background:#111827}}
body.dark .ms-btn{{background:var(--card);color:var(--text);border-color:var(--border)}}
body.dark .ms-panel{{background:var(--card);border-color:var(--border);box-shadow:0 8px 24px rgba(0,0,0,.4)}}
body.dark .ms-item:hover{{background:var(--bg)}}
body.dark .section-insight{{background:linear-gradient(135deg,rgba(249,115,22,.08),rgba(59,130,246,.06));border-color:rgba(249,115,22,.2)}}
body.dark .week-pill{{background:#374151;color:var(--text2)}}
body.dark .chart-card{{background:var(--card)}}
body.dark .kpi-card{{background:var(--card)}}
body.dark .items-card{{background:var(--card)}}
body.dark .store-filter-wrap select{{background:var(--card);color:var(--text);border-color:var(--border)}}
body.dark .revenue-summary-table th{{background:#111827}}

@media(max-width:900px){{
  .charts-grid{{grid-template-columns:1fr}}
  .kpi-grid{{grid-template-columns:repeat(auto-fill,minmax(150px,1fr))}}
  .header{{padding:12px 16px}}
  .main-content{{padding:14px}}
  .items-grid{{grid-template-columns:1fr}}
}}
@media(max-width:600px){{
  .kpi-grid{{grid-template-columns:repeat(2,1fr)}}
  .kpi-value{{font-size:20px}}
  .header-left h1{{font-size:16px}}
}}
</style>
</head>
<body>

<header class="header">
  <div class="header-left">
    <span class="brand-dot"></span>
    <h1>МʼЯСОРУБ | тижневий звіт</h1>
  </div>
  <div class="header-right">
    <div class="ms-wrap" id="city-ms"><button class="ms-btn" id="city-btn">Всі міста</button><div class="ms-panel" id="city-panel"></div></div>
    <div class="ms-wrap" id="store-ms"><button class="ms-btn" id="store-btn">Всі заклади</button><div class="ms-panel" id="store-panel"></div></div>
    <button class="reset-btn" id="reset-btn" onclick="resetAllFilters()" title="Скинути всі фільтри">✕</button>
    <button class="theme-toggle" id="theme-toggle" onclick="toggleDark()">🌙</button>
    <span class="last-update">Оновлено: {generated_at}</span>
  </div>
</header>

<nav class="main-nav" id="main-nav">
  <a href="#kpis" class="nav-link active">Огляд</a>
  <a href="#orders-section" class="nav-link">Замовлення</a>
  <a href="#ops-section" class="nav-link">Операції</a>
  <a href="#stores-section" class="nav-link">Деталі закладів</a>
  <a href="#revenue-section" class="nav-link">Дохідність</a>
  <a href="#campaigns-section" class="nav-link">Кампанії</a>
  <a href="#orders-detail-section" class="nav-link">Деталі замовлень</a>
  <a href="#complaints-section" class="nav-link">Скарги</a>
  <a href="#cancelled-section" class="nav-link">Скасовані</a>
  <a href="#items-section" class="nav-link">Топ позиції</a>
  <a href="#smart-promo-section" class="nav-link">Smart Promo</a>
  <a href="#sponsored-section" class="nav-link">Sponsored Listing</a>
</nav>

<div class="period-toggle-wrap">
  <select class="period-select" id="period-select">
    <option value="week" selected>Тижні</option>
    <option value="month">Місяці</option>
  </select>
</div>
<div class="week-bar" id="week-bar">
  <div class="week-bar-label">Тиждень:</div>
</div>

<main class="main-content">
  <section id="kpis" class="section">
    <div class="kpi-grid" id="kpi-grid"></div>
  </section>

  <section id="orders-section" class="section">
    <div class="section-title"><span class="section-icon">📦</span> Замовлення</div>
    <div class="section-insight" id="insight-orders"></div>
    <div class="charts-grid">
      <div class="chart-card"><h3>Замовлення по тижнях</h3><div class="chart-wrap"><canvas id="chart-orders"></canvas></div></div>
      <div class="chart-card"><h3>Середній чек (₴) по тижнях</h3><div class="chart-wrap"><canvas id="chart-avg-check"></canvas></div></div>
    </div>
  </section>

  <section id="ops-section" class="section">
    <div class="section-title"><span class="section-icon">⚙️</span> Операційні показники</div>
    <div class="section-insight" id="insight-ops"></div>
    <div class="charts-grid">
      <div class="chart-card"><h3>Доступність та Прийняття (%)</h3><div class="chart-wrap"><canvas id="chart-ops-rates"></canvas></div></div>
      <div class="chart-card"><h3>Рівень поганих замовлень (%)</h3><div class="chart-wrap"><canvas id="chart-bad-orders"></canvas></div></div>
    </div>
  </section>

  <section id="stores-section" class="section">
    <div class="section-title"><span class="section-icon">🏪</span> Деталі по закладах <span id="stores-week-label" style="font-size:13px;font-weight:500;color:var(--text2);margin-left:8px"></span></div>
    <div class="table-wrap" id="stores-table-wrap"></div>
  </section>

  <section id="revenue-section" class="section">
    <div class="section-title"><span class="section-icon">💰</span> Дохідність по тижнях</div>
    <div class="charts-grid">
      <div class="chart-card"><h3>Дохід по тижнях (₴)</h3><div class="chart-wrap"><canvas id="chart-revenue"></canvas></div></div>
      <div class="chart-card"><h3>Замовлення — деталі по закладах</h3><div class="chart-wrap" style="min-height:auto"><div id="revenue-summary"></div></div></div>
    </div>
  </section>

  <section id="campaigns-section" class="section">
    <div class="section-title"><span class="section-icon">🎯</span> Кампанії <span id="campaigns-week-label" style="font-size:13px;font-weight:500;color:var(--text2);margin-left:8px"></span></div>
    <div class="section-insight" id="campaigns-summary"></div>
    <div class="charts-grid">
      <div class="chart-card"><h3>Витрати закладів на кампанії по тижнях (₴)</h3><div class="chart-wrap"><canvas id="chart-campaign-spend"></canvas></div></div>
      <div class="chart-card"><h3>Витрати Bolt на кампанії по тижнях (₴)</h3><div class="chart-wrap"><canvas id="chart-campaign-bolt"></canvas></div></div>
    </div>
    <div class="table-wrap scroll-table" id="campaigns-wrap"></div>

  </section>

  <section id="orders-detail-section" class="section">
    <div class="section-title"><span class="section-icon">🧾</span> Дохідність по замовленнях <span id="orders-detail-week-label" style="font-size:13px;font-weight:500;color:var(--text2);margin-left:8px"></span></div>
    <div class="store-filter-wrap">
      <label>Bolt Plus:</label>
      <select id="bp-filter"><option value="__all__">Всі</option><option value="yes">Bolt Plus</option><option value="no">Без Bolt Plus</option></select>
      <label style="margin-left:12px">Статус:</label>
      <select id="state-filter"><option value="__all__">Всі</option><option value="delivered">Доставлені</option><option value="failed">Невдалі / Скасовані</option></select>
    </div>
    <div class="table-wrap scroll-table" id="orders-detail-wrap"></div>
  </section>

  <section id="complaints-section" class="section">
    <div class="section-title"><span class="section-icon">⚠️</span> Замовлення зі скаргами <span id="comp-count" style="font-size:13px;font-weight:500;color:var(--text2);margin-left:8px"></span></div>
    <div class="table-wrap scroll-table" id="complaints-wrap"></div>
  </section>

  <section id="cancelled-section" class="section">
    <div class="section-title"><span class="section-icon">❌</span> Скасовані замовлення <span id="canc-count" style="font-size:13px;font-weight:500;color:var(--text2);margin-left:8px"></span></div>
    <div class="table-wrap scroll-table" id="cancelled-wrap"></div>
  </section>

  <section id="items-section" class="section">
    <div class="section-title"><span class="section-icon">🍽️</span> Топ-10 позицій по закладах <span id="items-week-label" style="font-size:13px;font-weight:500;color:var(--text2);margin-left:8px"></span></div>
    <div class="section-insight" id="items-insight">Найпопулярніші позиції за обраний період. Кількість замовлених одиниць та виручка (₴).</div>
    <div class="items-grid" id="items-grid"></div>
  </section>

  <section id="smart-promo-section" class="section">
    <div class="section-title"><span class="section-icon">🎯</span> Smart Promo — статус та аналітика</div>
    <div class="section-insight" id="sp-insight"></div>
    <div class="kpi-grid" id="sp-kpi-grid"></div>
    <div style="margin-bottom:20px"><h3 style="font-size:15px;font-weight:700;margin-bottom:12px">Статус Smart Promo по закладах</h3></div>
    <div class="table-wrap" id="sp-status-wrap"></div>
    <div style="margin-top:20px">
      <h3 style="font-size:15px;font-weight:700;margin-bottom:12px">Замовлення через Smart Promo по тижнях</h3>
      <div class="charts-grid">
        <div class="chart-card"><h3>Smart Promo замовлення</h3><div class="chart-wrap"><canvas id="chart-sp-orders"></canvas></div></div>
        <div class="chart-card"><h3>Smart Promo витрати (₴)</h3><div class="chart-wrap"><canvas id="chart-sp-spend"></canvas></div></div>
      </div>
    </div>
    <div class="table-wrap scroll-table" style="margin-top:16px" id="sp-enrollments-wrap"></div>
  </section>

  <section id="sponsored-section" class="section">
    <div class="section-title"><span class="section-icon">📢</span> Sponsored Listing — статус та аналітика</div>
    <div class="section-insight" id="sl-insight"></div>
    <div class="kpi-grid" id="sl-kpi-grid"></div>
    <div style="margin-bottom:20px"><h3 style="font-size:15px;font-weight:700;margin-bottom:12px">Статус Sponsored Listing по закладах</h3></div>
    <div class="table-wrap" id="sl-status-wrap"></div>
    <div class="table-wrap scroll-table" style="margin-top:16px" id="sl-detail-wrap"></div>
  </section>
</main>

<script>
const D = {json.dumps(data, ensure_ascii=False)};

const CITY_UA = {CITY_UA_JSON};
let allWeeks = Object.keys(D.weekly).sort((a,b) => {{
  const [ay,aw] = a.split('-W').map(Number);
  const [by,bw] = b.split('-W').map(Number);
  return ay !== by ? ay - by : aw - bw;
}});
let allMonths = Object.keys(D.monthly || {{}}).sort();
let periodMode = 'week';
let selectedWeekIdx = allWeeks.length - 1;
let selectedMonthIdx = allMonths.length - 1;
let selectedCities = new Set();
let selectedStores = new Set();
let selectedBP = '__all__';
let selectedState = '__all__';
let chartInstances = {{}};

function weekSortCmp(a, b) {{
  const [ay,aw] = a.split('-W').map(Number);
  const [by,bw] = b.split('-W').map(Number);
  return ay !== by ? ay - by : aw - bw;
}}

function getPeriodKeys() {{ return periodMode === 'month' ? allMonths : allWeeks; }}
function getSelectedPeriodIdx() {{ return periodMode === 'month' ? selectedMonthIdx : selectedWeekIdx; }}
function setSelectedPeriodIdx(i) {{ if (periodMode === 'month') selectedMonthIdx = i; else selectedWeekIdx = i; }}
function getSelectedPeriodKey() {{ const keys = getPeriodKeys(); return keys[getSelectedPeriodIdx()] || keys[keys.length - 1]; }}
function getPeriodLabel(key) {{
  if (periodMode === 'month') {{
    const [y, m] = key.split('-');
    const months = ['Січ','Лют','Бер','Кві','Тра','Чер','Лип','Сер','Вер','Жов','Лис','Гру'];
    return months[parseInt(m) - 1] + ' ' + y;
  }}
  return key;
}}
function getPerStoreData() {{ return periodMode === 'month' ? (D.monthly || {{}}) : D.weekly; }}
function getRevenueData() {{ return periodMode === 'month' ? (D.monthly_revenue || {{}}) : D.revenue; }}
function getItemsData() {{ return periodMode === 'month' ? (D.monthly_items || {{}}) : D.top_items; }}
function getCampaignPeriodKey(c) {{ return periodMode === 'month' ? c.order_month : c.order_week; }}
function getOpsData() {{ return periodMode === 'month' ? (D.ops_monthly || {{}}) : D.ops_weekly; }}

function getSelectedWeek() {{ return allWeeks.length ? allWeeks[selectedWeekIdx >= 0 ? selectedWeekIdx : allWeeks.length - 1] : null; }}
function getSelectedPeriod() {{ return getSelectedPeriodKey(); }}
function cityUA(c) {{ return CITY_UA[c] || c; }}

function buildMsPanel(panelEl, items, selected, allLabel, onChange) {{
  let html = '<label class="ms-item all-item"><input type="checkbox" data-val="__all__" ' + (selected.size === 0 ? 'checked' : '') + '> ' + allLabel + '</label>';
  items.forEach(it => {{
    html += '<label class="ms-item"><input type="checkbox" data-val="' + it.value + '" ' + (selected.has(it.value) ? 'checked' : '') + '> ' + it.label + '</label>';
  }});
  panelEl.innerHTML = html;
  panelEl.querySelectorAll('input[type=checkbox]').forEach(cb => {{
    cb.addEventListener('change', function() {{
      const val = this.dataset.val;
      if (val === '__all__') {{
        selected.clear();
        panelEl.querySelectorAll('input[data-val]').forEach(x => {{ x.checked = (x.dataset.val === '__all__'); }});
      }} else {{
        if (this.checked) selected.add(val); else selected.delete(val);
        const allCb = panelEl.querySelector('input[data-val="__all__"]');
        if (selected.size === 0) {{ allCb.checked = true; }}
        else {{ allCb.checked = false; }}
      }}
      onChange();
    }});
  }});
}}

function updateMsLabel(btnEl, selected, allLabel, getLabel) {{
  if (selected.size === 0) {{ btnEl.textContent = allLabel; return; }}
  if (selected.size === 1) {{ btnEl.textContent = getLabel([...selected][0]); return; }}
  btnEl.innerHTML = getLabel([...selected][0]) + ' <span class="ms-count">+' + (selected.size - 1) + '</span>';
}}

function initMsToggle(btnId, panelId) {{
  const btn = document.getElementById(btnId);
  const panel = document.getElementById(panelId);
  btn.addEventListener('click', function(e) {{
    e.stopPropagation();
    const wasOpen = panel.classList.contains('open');
    closeAllMs();
    if (!wasOpen) {{ panel.classList.add('open'); btn.classList.add('open'); }}
  }});
  panel.addEventListener('click', e => e.stopPropagation());
}}
function closeAllMs() {{
  document.querySelectorAll('.ms-panel.open').forEach(p => p.classList.remove('open'));
  document.querySelectorAll('.ms-btn.open').forEach(b => b.classList.remove('open'));
}}
document.addEventListener('click', closeAllMs);

function populateCityFilter() {{
  const panel = document.getElementById('city-panel');
  const btn = document.getElementById('city-btn');
  const cities = [...new Set(Object.values(D.stores).map(s => s.city_en))].sort();
  const items = cities.map(c => ({{ value: c, label: cityUA(c) }}));
  buildMsPanel(panel, items, selectedCities, 'Всі міста', function() {{
    updateMsLabel(btn, selectedCities, 'Всі міста', v => cityUA(v));
    selectedStores.clear();
    populateStoreFilter();
    populateWeekBar();
    renderAll();
  }});
  updateMsLabel(btn, selectedCities, 'Всі міста', v => cityUA(v));
}}

function populateStoreFilter() {{
  const panel = document.getElementById('store-panel');
  const btn = document.getElementById('store-btn');
  const cityIds = selectedCities.size === 0
    ? Object.keys(D.stores).map(Number)
    : Object.entries(D.stores).filter(([_, s]) => selectedCities.has(s.city_en)).map(([id]) => Number(id));
  const items = cityIds
    .sort((a, b) => (D.stores[a].short || '').localeCompare(D.stores[b].short || '', 'uk'))
    .filter(id => D.stores[id])
    .map(id => ({{ value: String(id), label: D.stores[id].short + ' (' + D.stores[id].city + ')' }}));
  buildMsPanel(panel, items, selectedStores, 'Всі заклади', function() {{
    updateMsLabel(btn, selectedStores, 'Всі заклади', v => D.stores[v] ? D.stores[v].short : v);
    populateWeekBar();
    renderAll();
  }});
  updateMsLabel(btn, selectedStores, 'Всі заклади', v => D.stores[v] ? D.stores[v].short : v);
}}

function resetAllFilters() {{
  selectedCities.clear();
  selectedStores.clear();
  populateCityFilter();
  populateStoreFilter();
  selectedWeekIdx = allWeeks.length - 1;
  selectedMonthIdx = allMonths.length - 1;
  populateWeekBar();
  renderAll();
}}

function getFilteredStoreIds() {{
  if (selectedStores.size > 0) return [...selectedStores].map(Number);
  if (selectedCities.size === 0) return Object.keys(D.stores).map(Number);
  return Object.entries(D.stores).filter(([_, s]) => selectedCities.has(s.city_en)).map(([id]) => Number(id));
}}

function getFilteredPeriodKeys() {{
  const ids = getFilteredStoreIds();
  const keys = getPeriodKeys();
  const store = getPerStoreData();
  return keys.filter(k => {{
    const kd = store[k] || {{}};
    return ids.some(id => kd[id]);
  }});
}}

function populateWeekBar() {{
  const bar = document.getElementById('week-bar');
  const label = periodMode === 'month' ? 'Місяць:' : 'Тиждень:';
  let html = '<div class="week-bar-label">' + label + '</div>';
  const keys = getFilteredPeriodKeys();
  const allKeys = getPeriodKeys();
  const selIdx = getSelectedPeriodIdx();
  keys.forEach(k => {{
    const idx = allKeys.indexOf(k);
    const lbl = getPeriodLabel(k);
    html += '<div class="week-pill' + (idx === selIdx ? ' active' : '') + '" data-idx="' + idx + '">' + lbl + '</div>';
  }});
  bar.innerHTML = html;
  bar.querySelectorAll('.week-pill').forEach(pill => {{
    pill.addEventListener('click', () => {{
      setSelectedPeriodIdx(parseInt(pill.dataset.idx));
      bar.querySelectorAll('.week-pill').forEach(p => p.classList.remove('active'));
      pill.classList.add('active');
      renderAll();
    }});
  }});
  const active = bar.querySelector('.week-pill.active');
  if (active) active.scrollIntoView({{ behavior: 'smooth', block: 'nearest', inline: 'center' }});
}}

function destroyChart(id) {{ if (chartInstances[id]) {{ chartInstances[id].destroy(); delete chartInstances[id]; }} }}

function wow(cur, prev, dir) {{
  const lbl = periodMode === 'month' ? 'MoM' : 'WoW';
  if (!prev || prev === 0) return {{ cls: 'neutral', text: '— ' + lbl }};
  const chg = ((cur - prev) / Math.abs(prev)) * 100;
  const good = (dir === 'up' && chg > 0) || (dir === 'down' && chg < 0);
  const bad = (dir === 'up' && chg < 0) || (dir === 'down' && chg > 0);
  const cls = good ? 'up' : bad ? 'down' : 'neutral';
  const arrow = chg > 0 ? '↑' : chg < 0 ? '↓' : '';
  return {{ cls, text: arrow + ' ' + Math.abs(chg).toFixed(1) + '% ' + lbl }};
}}

function renderKPIs() {{
  const ids = getFilteredStoreIds();
  const store = getPerStoreData();
  const keys = getPeriodKeys();
  const selK = getSelectedPeriod();
  const prevIdx = keys.indexOf(selK) - 1;
  const prevK = prevIdx >= 0 ? keys[prevIdx] : null;
  const wd = store[selK] || {{}};
  const pd = prevK ? (store[prevK] || {{}}) : {{}};

  let curOrders = 0, curCheck = 0, curCooking = 0, curBad = 0, cnt = 0;
  let prevOrders = 0, prevCheck = 0, prevCooking = 0, prevBad = 0, pcnt = 0;
  ids.forEach(id => {{
    if (wd[id]) {{ curOrders += wd[id].orders; curCheck += wd[id].avg_check * wd[id].orders; curCooking += wd[id].avg_cooking * wd[id].orders; curBad += wd[id].bad_orders; cnt += wd[id].orders; }}
    if (pd[id]) {{ prevOrders += pd[id].orders; prevCheck += pd[id].avg_check * pd[id].orders; prevCooking += pd[id].avg_cooking * pd[id].orders; prevBad += pd[id].bad_orders; pcnt += pd[id].orders; }}
  }});
  const avgChk = cnt > 0 ? curCheck / cnt : 0;
  const avgCook = cnt > 0 ? curCooking / cnt : 0;
  const badRate = curOrders > 0 ? (curBad / curOrders * 100) : 0;
  const prevAvgChk = pcnt > 0 ? prevCheck / pcnt : 0;
  const prevAvgCook = pcnt > 0 ? prevCooking / pcnt : 0;
  const prevBadRate = prevOrders > 0 ? (prevBad / prevOrders * 100) : 0;
  const storeCount = ids.filter(id => wd[id]).length;

  let avgAvail = 0, avgAccept = 0, avgRating = 0, availCnt = 0, acceptCnt = 0, ratingCnt = 0;
  ids.forEach(id => {{
    const lo = D.latest_ops[id];
    if (lo) {{
      if (lo.availability != null) {{ avgAvail += lo.availability; availCnt++; }}
      if (lo.acceptance != null) {{ avgAccept += lo.acceptance; acceptCnt++; }}
      if (lo.rating != null) {{ avgRating += lo.rating; ratingCnt++; }}
    }}
  }});
  if (availCnt > 0) avgAvail /= availCnt;
  if (acceptCnt > 0) avgAccept /= acceptCnt;
  if (ratingCnt > 0) avgRating /= ratingCnt;

  const periodLabel = periodMode === 'month' ? 'за обраний місяць' : 'за обраний тиждень';
  const changeLabel = periodMode === 'month' ? 'MoM' : 'WoW';
  const kpis = [
    {{ label: 'Замовлення', value: curOrders.toLocaleString('uk-UA'), ...wow(curOrders, prevOrders, 'up') }},
    {{ label: 'Середній чек', value: '₴' + avgChk.toFixed(0), ...wow(avgChk, prevAvgChk, 'up') }},
    {{ label: 'Час приготування', value: avgCook.toFixed(1) + ' хв', ...wow(avgCook, prevAvgCook, 'down') }},
    {{ label: 'Доступність', value: avgAvail.toFixed(1) + '%', cls: avgAvail >= 90 ? 'up' : 'down', text: 'середнє по закладах' }},
    {{ label: 'Прийняття', value: avgAccept.toFixed(1) + '%', cls: avgAccept >= 90 ? 'up' : 'down', text: 'середнє по закладах' }},
    {{ label: 'Погані замовлення', value: badRate.toFixed(1) + '%', ...wow(badRate, prevBadRate, 'down') }},
    {{ label: 'Рейтинг', value: ratingCnt ? avgRating.toFixed(2) : '—', cls: avgRating >= 4.5 ? 'up' : 'neutral', text: 'середнє по закладах' }},
    {{ label: 'Активних закладів', value: storeCount, cls: 'neutral', text: periodLabel }},
  ];

  document.getElementById('kpi-grid').innerHTML = kpis.map(k =>
    '<div class="kpi-card"><div class="kpi-label">' + k.label + '</div><div class="kpi-value">' + k.value + '</div>' +
    '<span class="kpi-change ' + k.cls + '">' + k.text + '</span></div>'
  ).join('');
}}

function renderOrdersCharts() {{
  const ids = getFilteredStoreIds();
  const pkeys = getFilteredPeriodKeys();
  const labels = pkeys.map(k => getPeriodLabel(k));
  const store = getPerStoreData();

  destroyChart('chart-orders');
  const ordersData = pkeys.map(k => {{
    const kd = store[k] || {{}};
    return ids.reduce((s, id) => s + (kd[id] ? kd[id].orders : 0), 0);
  }});
  chartInstances['chart-orders'] = new Chart(document.getElementById('chart-orders'), {{
    type: 'bar',
    data: {{ labels, datasets: [{{ label: 'Замовлення', data: ordersData, backgroundColor: 'rgba(249,115,22,.7)', borderColor: '#F97316', borderWidth: 1, borderRadius: 6, barPercentage: .6 }}] }},
    options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }}, scales: {{ y: {{ beginAtZero: true, grid: {{ color: 'rgba(0,0,0,.05)' }} }}, x: {{ grid: {{ display: false }} }} }} }}
  }});

  destroyChart('chart-avg-check');
  const checkData = pkeys.map(k => {{
    const kd = store[k] || {{}};
    let sum = 0, cnt = 0;
    ids.forEach(id => {{ if (kd[id]) {{ sum += kd[id].avg_check * kd[id].orders; cnt += kd[id].orders; }} }});
    return cnt > 0 ? Math.round(sum / cnt) : 0;
  }});
  chartInstances['chart-avg-check'] = new Chart(document.getElementById('chart-avg-check'), {{
    type: 'line',
    data: {{ labels, datasets: [{{ label: 'Середній чек', data: checkData, borderColor: '#3B82F6', backgroundColor: 'rgba(59,130,246,.08)', fill: true, tension: .35, pointRadius: 4, pointBackgroundColor: '#3B82F6', borderWidth: 2.5 }}] }},
    options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }}, scales: {{ y: {{ beginAtZero: false, grid: {{ color: 'rgba(0,0,0,.05)' }}, ticks: {{ callback: v => '₴' + v }} }}, x: {{ grid: {{ display: false }} }} }} }}
  }});
}}

function renderOpsCharts() {{
  const ids = getFilteredStoreIds();
  const pkeys = getFilteredPeriodKeys();
  const labels = pkeys.map(k => getPeriodLabel(k));
  const opsStore = getOpsData();
  const store = getPerStoreData();

  const avgByPeriod = (field) => pkeys.map(k => {{
    const ow = opsStore[k] || {{}};
    let sum = 0, cnt = 0;
    ids.forEach(id => {{ if (ow[id] && ow[id][field] != null) {{ sum += ow[id][field]; cnt++; }} }});
    return cnt > 0 ? +(sum / cnt).toFixed(1) : null;
  }});

  const badRates = pkeys.map(k => {{
    const kd = store[k] || {{}};
    let ord = 0, bad = 0;
    ids.forEach(id => {{ if (kd[id]) {{ ord += kd[id].orders; bad += kd[id].bad_orders; }} }});
    return ord > 0 ? +(bad / ord * 100).toFixed(1) : 0;
  }});

  destroyChart('chart-ops-rates');
  chartInstances['chart-ops-rates'] = new Chart(document.getElementById('chart-ops-rates'), {{
    type: 'line',
    data: {{ labels, datasets: [
      {{ label: 'Доступність', data: avgByPeriod('availability'), borderColor: '#F97316', backgroundColor: 'rgba(249,115,22,.08)', fill: true, tension: .35, pointRadius: 4, pointBackgroundColor: '#F97316', borderWidth: 2.5 }},
      {{ label: 'Прийняття', data: avgByPeriod('acceptance'), borderColor: '#3B82F6', backgroundColor: 'rgba(59,130,246,.06)', fill: true, tension: .35, pointRadius: 4, pointBackgroundColor: '#3B82F6', borderWidth: 2.5 }}
    ] }},
    options: {{ responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ position: 'bottom', labels: {{ usePointStyle: true, padding: 12, font: {{ size: 11 }} }} }} }},
      scales: {{ y: {{ beginAtZero: true, max: 100, grid: {{ color: 'rgba(0,0,0,.05)' }}, ticks: {{ callback: v => v + '%' }} }}, x: {{ grid: {{ display: false }} }} }} }}
  }});

  destroyChart('chart-bad-orders');
  chartInstances['chart-bad-orders'] = new Chart(document.getElementById('chart-bad-orders'), {{
    type: 'line',
    data: {{ labels, datasets: [
      {{ label: 'Погані замовлення', data: badRates, borderColor: '#EF4444', backgroundColor: 'rgba(239,68,68,.06)', fill: true, tension: .35, pointRadius: 4, pointBackgroundColor: '#EF4444', borderWidth: 2.5 }}
    ] }},
    options: {{ responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ display: false }} }},
      scales: {{ y: {{ beginAtZero: true, grid: {{ color: 'rgba(0,0,0,.05)' }}, ticks: {{ callback: v => v + '%' }} }}, x: {{ grid: {{ display: false }} }} }} }}
  }});
}}

function renderInsights() {{
  const ids = getFilteredStoreIds();
  const store = getPerStoreData();
  const keys = getPeriodKeys();
  const selK = getSelectedPeriod();
  const prevIdx = keys.indexOf(selK) - 1;
  const prevK = prevIdx >= 0 ? keys[prevIdx] : null;
  const wd = store[selK] || {{}};
  const pd = prevK ? (store[prevK] || {{}}) : {{}};
  const lbl = periodMode === 'month' ? 'MoM' : 'WoW';

  let curOrd = 0, prevOrd = 0;
  ids.forEach(id => {{
    if (wd[id]) curOrd += wd[id].orders;
    if (pd[id]) prevOrd += pd[id].orders;
  }});
  const ordChg = prevOrd > 0 ? ((curOrd - prevOrd) / prevOrd * 100) : null;

  function cs(chg, dir) {{
    if (chg == null) return '';
    const good = (dir === 'up' && chg > 0) || (dir === 'down' && chg < 0);
    const cls = good ? 'insight-good' : 'insight-bad';
    return '<b class="' + cls + '">' + (chg > 0 ? '+' : '') + chg.toFixed(1) + '%</b>';
  }}

  let o = '<b>' + getPeriodLabel(selK) + '</b>. ';
  o += 'Доставлено <b>' + curOrd + '</b> замовлень (' + cs(ordChg, 'up') + ' ' + lbl + '). ';
  if (ordChg != null && ordChg < -10) o += '<span class="insight-bad">Суттєве падіння замовлень.</span>';
  else if (ordChg != null && ordChg > 10) o += '<span class="insight-good">Гарне зростання!</span>';
  document.getElementById('insight-orders').innerHTML = o;

  let avgAvail = 0, avgAccept = 0, availCnt2 = 0, acceptCnt2 = 0, bad = 0, ord = 0;
  ids.forEach(id => {{
    const lo = D.latest_ops[id];
    if (lo) {{
      if (lo.availability != null) {{ avgAvail += lo.availability; availCnt2++; }}
      if (lo.acceptance != null) {{ avgAccept += lo.acceptance; acceptCnt2++; }}
    }}
    if (wd[id]) {{ bad += wd[id].bad_orders; ord += wd[id].orders; }}
  }});
  if (availCnt2 > 0) avgAvail /= availCnt2;
  if (acceptCnt2 > 0) avgAccept /= acceptCnt2;
  const badRate = ord > 0 ? (bad / ord * 100) : 0;

  let ops = '<b>' + getPeriodLabel(selK) + '</b>. ';
  ops += 'Доступність — <b>' + avgAvail.toFixed(1) + '%</b>. Прийняття — <b>' + avgAccept.toFixed(1) + '%</b>. Погані замовлення — <b>' + badRate.toFixed(1) + '%</b>. ';
  if (avgAvail < 80) ops += '<span class="insight-bad">Доступність критично низька!</span> ';
  else if (avgAvail >= 95) ops += '<span class="insight-good">Відмінна доступність!</span> ';
  if (badRate > 15) ops += '<span class="insight-bad">Високий рівень поганих замовлень.</span>';
  document.getElementById('insight-ops').innerHTML = ops;
}}

function renderStoresTable() {{
  const ids = getFilteredStoreIds();
  const store = getPerStoreData();
  const keys = getPeriodKeys();
  const selK = getSelectedPeriod();
  const prevIdx = keys.indexOf(selK) - 1;
  const prevK = prevIdx >= 0 ? keys[prevIdx] : null;
  const chgLbl = periodMode === 'month' ? 'MoM' : 'WoW';
  document.getElementById('stores-week-label').textContent = '— ' + getPeriodLabel(selK) + (prevK ? ' (' + chgLbl + ' до ' + getPeriodLabel(prevK) + ')' : '');
  const wd = store[selK] || {{}};
  const pd = prevK ? (store[prevK] || {{}}) : {{}};

  const rows = ids.filter(id => wd[id]).map(id => ({{
    id, ...D.stores[id], ...wd[id],
    ops: D.latest_ops[id] || {{}},
    prev: pd[id] || null
  }})).sort((a, b) => b.orders - a.orders);

  function wBadge(cur, prev, dir) {{
    if (!prev || prev === 0) return '';
    const chg = ((cur - prev) / Math.abs(prev)) * 100;
    const good = (dir === 'up' && chg > 0) || (dir === 'down' && chg < 0);
    const color = good ? 'var(--pos)' : 'var(--neg)';
    const bg = good ? 'rgba(16,185,129,.1)' : 'rgba(239,68,68,.08)';
    const arrow = chg > 0 ? '↑' : '↓';
    return ' <span style="font-size:10px;font-weight:600;color:' + color + ';background:' + bg + ';padding:1px 5px;border-radius:10px">' + arrow + Math.abs(chg).toFixed(0) + '%</span>';
  }}

  let t = '<table class="data-table"><thead><tr><th>#</th><th>Заклад</th><th>Місто</th><th class="text-right">Замовлення</th><th class="text-right">Сер. чек</th><th class="text-right">Час приг.</th><th class="text-right">Доступність</th><th class="text-right">Прийняття</th><th class="text-right">Фото</th><th class="text-right">Погані зам.</th></tr></thead><tbody>';
  let totOrd = 0, totBad = 0;
  rows.forEach((d, i) => {{
    const badRate = d.orders > 0 ? (d.bad_orders / d.orders * 100) : 0;
    totOrd += d.orders; totBad += d.bad_orders;
    t += '<tr><td>' + (i + 1) + '</td><td>' + d.short + '</td><td>' + d.city + '</td>';
    t += '<td class="text-right">' + d.orders + (d.prev ? wBadge(d.orders, d.prev.orders, 'up') : '') + '</td>';
    t += '<td class="text-right">₴' + d.avg_check + '</td>';
    t += '<td class="text-right">' + d.avg_cooking + ' хв</td>';
    t += '<td class="text-right">' + (d.ops.availability != null ? d.ops.availability.toFixed(1) + '%' : '—') + '</td>';
    t += '<td class="text-right">' + (d.ops.acceptance != null ? d.ops.acceptance.toFixed(1) + '%' : '—') + '</td>';
    t += '<td class="text-right">' + (d.ops.photo_coverage != null ? d.ops.photo_coverage.toFixed(1) + '%' : '—') + '</td>';
    t += '<td class="text-right">' + badRate.toFixed(1) + '%</td></tr>';
  }});
  const totalBadRate = totOrd > 0 ? (totBad / totOrd * 100) : 0;
  t += '<tr class="total-row"><td></td><td colspan="2">Всього</td><td class="text-right">' + totOrd + '</td><td colspan="5"></td><td class="text-right">' + totalBadRate.toFixed(1) + '%</td></tr>';
  t += '</tbody></table>';
  document.getElementById('stores-table-wrap').innerHTML = t;
}}

function renderRevenueChart() {{
  const ids = getFilteredStoreIds();
  const pkeys = getFilteredPeriodKeys();
  const labels = pkeys.map(k => getPeriodLabel(k));
  const revStore = getRevenueData();
  const selK = getSelectedPeriod();

  destroyChart('chart-revenue');
  const foodData = pkeys.map(k => {{
    const rw = revStore[k] || {{}};
    return ids.reduce((s, id) => s + ((rw[id] && rw[id].food_revenue) || 0), 0);
  }});
  const feeData = pkeys.map(k => {{
    const rw = revStore[k] || {{}};
    return ids.reduce((s, id) => s + ((rw[id] && rw[id].total_fee_gross) || 0), 0);
  }});
  const netData = pkeys.map(k => {{
    const rw = revStore[k] || {{}};
    return ids.reduce((s, id) => s + ((rw[id] && rw[id].net_income) || 0), 0);
  }});

  chartInstances['chart-revenue'] = new Chart(document.getElementById('chart-revenue'), {{
    type: 'bar',
    data: {{
      labels,
      datasets: [
        {{ label: 'Дохід від їжі', data: foodData, backgroundColor: 'rgba(59,130,246,.7)', borderRadius: 4, barPercentage: .7 }},
        {{ label: 'Комісія (брутто)', data: feeData, backgroundColor: 'rgba(239,68,68,.6)', borderRadius: 4, barPercentage: .7 }},
        {{ label: 'Чистий дохід', data: netData, backgroundColor: 'rgba(16,185,129,.7)', borderRadius: 4, barPercentage: .7 }}
      ]
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ position: 'bottom', labels: {{ usePointStyle: true, padding: 12, font: {{ size: 11 }} }} }} }},
      scales: {{
        x: {{ stacked: false, grid: {{ display: false }} }},
        y: {{ beginAtZero: true, grid: {{ color: 'rgba(0,0,0,.05)' }}, ticks: {{ callback: v => '₴' + v.toLocaleString('uk-UA') }} }}
      }}
    }}
  }});

  let sumHtml = '<table class="revenue-summary-table"><thead><tr><th>Заклад</th><th class="text-right">Замовлення</th><th class="text-right">Дохід їжі</th><th class="text-right">Комісія</th><th class="text-right">Повернення</th><th class="text-right">Чистий дохід</th></tr></thead><tbody>';
  const rw = revStore[selK] || {{}};
  let tOrd = 0, tFood = 0, tFee = 0, tRef = 0, tNet = 0;
  ids.filter(id => rw[id]).sort((a, b) => (rw[b].net_income || 0) - (rw[a].net_income || 0)).forEach(id => {{
    const r = rw[id];
    const s = D.stores[id];
    tOrd += r.orders || 0; tFood += r.food_revenue || 0; tFee += r.total_fee_gross || 0; tRef += r.refund || 0; tNet += r.net_income || 0;
    sumHtml += '<tr><td>' + (s ? s.short : id) + '</td><td class="text-right">' + (r.orders || 0) + '</td><td class="text-right">₴' + (r.food_revenue || 0).toLocaleString('uk-UA') + '</td><td class="text-right" style="color:var(--neg)">₴' + (r.total_fee_gross || 0).toLocaleString('uk-UA') + '</td><td class="text-right">₴' + (r.refund || 0).toLocaleString('uk-UA') + '</td><td class="text-right" style="color:var(--pos)">₴' + (r.net_income || 0).toLocaleString('uk-UA') + '</td></tr>';
  }});
  sumHtml += '<tr class="total-row"><td>Всього</td><td class="text-right">' + tOrd + '</td><td class="text-right">₴' + tFood.toLocaleString('uk-UA') + '</td><td class="text-right" style="color:var(--neg)">₴' + tFee.toLocaleString('uk-UA') + '</td><td class="text-right">₴' + tRef.toLocaleString('uk-UA') + '</td><td class="text-right" style="color:var(--pos)">₴' + tNet.toLocaleString('uk-UA') + '</td></tr>';
  sumHtml += '</tbody></table>';
  document.getElementById('revenue-summary').innerHTML = sumHtml;
}}

function renderCampaignsChart() {{
  const ids = getFilteredStoreIds();
  const pkeys = getFilteredPeriodKeys();
  const labels = pkeys.map(k => getPeriodLabel(k));
  const camps = D.campaigns || [];

  destroyChart('chart-campaign-spend');
  destroyChart('chart-campaign-bolt');

  const provSpend = pkeys.map(k =>
    camps.filter(r => getCampaignPeriodKey(r) === k && ids.includes(r.provider_id))
         .reduce((s, r) => s + (r.provider_spend || 0), 0)
  );
  const boltSpend = pkeys.map(k =>
    camps.filter(r => getCampaignPeriodKey(r) === k && ids.includes(r.provider_id))
         .reduce((s, r) => s + (r.bolt_spend || 0), 0)
  );
  const campOrders = pkeys.map(k =>
    camps.filter(r => getCampaignPeriodKey(r) === k && ids.includes(r.provider_id))
         .reduce((s, r) => s + (r.orders || 0), 0)
  );

  chartInstances['chart-campaign-spend'] = new Chart(document.getElementById('chart-campaign-spend'), {{
    type: 'bar',
    data: {{
      labels,
      datasets: [
        {{ label: 'Витрати закладу ₴', data: provSpend, backgroundColor: 'rgba(239,68,68,.7)', borderRadius: 4, barPercentage: .6, yAxisID: 'y' }},
        {{ label: 'Промо-замовлення', data: campOrders, type: 'line', borderColor: '#F97316', backgroundColor: 'rgba(249,115,22,.08)', pointRadius: 3, pointBackgroundColor: '#F97316', borderWidth: 2, tension: .3, fill: false, yAxisID: 'y1' }}
      ]
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ position: 'bottom', labels: {{ usePointStyle: true, padding: 12, font: {{ size: 11 }} }} }} }},
      scales: {{
        x: {{ grid: {{ display: false }} }},
        y: {{ beginAtZero: true, position: 'left', grid: {{ color: 'rgba(0,0,0,.05)' }}, ticks: {{ callback: v => '₴' + v.toLocaleString('uk-UA') }} }},
        y1: {{ beginAtZero: true, position: 'right', grid: {{ display: false }}, ticks: {{ font: {{ size: 10 }} }} }}
      }}
    }}
  }});

  chartInstances['chart-campaign-bolt'] = new Chart(document.getElementById('chart-campaign-bolt'), {{
    type: 'bar',
    data: {{
      labels,
      datasets: [
        {{ label: 'Витрати Bolt ₴', data: boltSpend, backgroundColor: 'rgba(59,130,246,.7)', borderRadius: 4, barPercentage: .6, yAxisID: 'y' }},
        {{ label: 'Промо-замовлення', data: campOrders, type: 'line', borderColor: '#F97316', backgroundColor: 'rgba(249,115,22,.08)', pointRadius: 3, pointBackgroundColor: '#F97316', borderWidth: 2, tension: .3, fill: false, yAxisID: 'y1' }}
      ]
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ position: 'bottom', labels: {{ usePointStyle: true, padding: 12, font: {{ size: 11 }} }} }} }},
      scales: {{
        x: {{ grid: {{ display: false }} }},
        y: {{ beginAtZero: true, position: 'left', grid: {{ color: 'rgba(0,0,0,.05)' }}, ticks: {{ callback: v => '₴' + v.toLocaleString('uk-UA') }} }},
        y1: {{ beginAtZero: true, position: 'right', grid: {{ display: false }}, ticks: {{ font: {{ size: 10 }} }} }}
      }}
    }}
  }});
}}

function renderCampaigns() {{
  const ids = getFilteredStoreIds();
  const selK = getSelectedPeriod();
  document.getElementById('campaigns-week-label').textContent = '— ' + getPeriodLabel(selK);

  const rows = (D.campaigns || []).filter(r => getCampaignPeriodKey(r) === selK && ids.includes(r.provider_id));

  const bycamp = {{}};
  rows.forEach(r => {{
    const key = r.campaign_id;
    if (!bycamp[key]) bycamp[key] = {{ ...r, orders: 0, total_discount: 0, bolt_spend: 0, provider_spend: 0, providers: new Set() }};
    bycamp[key].orders += r.orders;
    bycamp[key].total_discount += r.total_discount;
    bycamp[key].bolt_spend += r.bolt_spend;
    bycamp[key].provider_spend += r.provider_spend;
    bycamp[key].providers.add(r.provider_short);
  }});
  const campList = Object.values(bycamp).sort((a, b) => b.orders - a.orders);

  let totOrd = 0, totDisc = 0, totBolt = 0, totProv = 0;
  campList.forEach(c => {{ totOrd += c.orders; totDisc += c.total_discount; totBolt += c.bolt_spend; totProv += c.provider_spend; }});

  const summaryEl = document.getElementById('campaigns-summary');
  if (campList.length === 0) {{
    summaryEl.innerHTML = '<b>' + getPeriodLabel(selK) + '</b>. Немає активних кампаній для обраних закладів.';
  }} else {{
    summaryEl.innerHTML = '<b>' + getPeriodLabel(selK) + '</b>. Активних кампаній: <b>' + campList.length + '</b>. '
      + 'Замовлень з кампаніями: <b>' + totOrd + '</b>. '
      + 'Загальна знижка: <b>₴' + totDisc.toLocaleString('uk-UA') + '</b> '
      + '(Bolt: ₴' + totBolt.toLocaleString('uk-UA') + ', Заклад: ₴' + totProv.toLocaleString('uk-UA') + ').';
  }}

  let t = '<table class="data-table"><thead><tr>'
    + '<th>Кампанія</th><th>Хто платить</th>'
    + '<th>Дати</th><th>Заклади</th>'
    + '<th class="text-right">Зам.</th><th class="text-right">Знижка ₴</th>'
    + '<th class="text-right">Bolt ₴</th><th class="text-right">Заклад ₴</th>'
    + '</tr></thead><tbody>';

  if (campList.length === 0) {{
    t += '<tr><td colspan="8" style="text-align:center;color:var(--text2);padding:24px">Немає кампаній за цей період</td></tr>';
  }} else {{
    campList.forEach(c => {{
      const provArr = [...c.providers];
      const provText = provArr.length > 3 ? provArr.slice(0, 3).join(', ') + ' +' + (provArr.length - 3) : provArr.join(', ');
      const payer = c.provider_spend > 0 && c.bolt_spend > 0 ? 'Спільно'
        : c.provider_spend > 0 ? 'Заклад'
        : c.bolt_spend > 0 ? 'Bolt' : '—';
      const payerCls = payer === 'Bolt' ? 'color:var(--blue);font-weight:600'
        : payer === 'Заклад' ? 'color:var(--neg);font-weight:600'
        : payer === 'Спільно' ? 'color:var(--warn);font-weight:600' : '';
      t += '<tr>';
      t += '<td style="white-space:normal;min-width:180px;max-width:280px" title="' + (c.full_name || '').replace(/"/g,'&quot;') + '">' + c.name + '</td>';
      t += '<td style="' + payerCls + ';white-space:nowrap">' + payer + '</td>';
      t += '<td style="font-size:11px;white-space:nowrap">' + c.start_date + ' → ' + c.end_date + '</td>';
      t += '<td style="font-size:12px">' + provText + '</td>';
      t += '<td class="text-right">' + c.orders + '</td>';
      t += '<td class="text-right">₴' + c.total_discount.toLocaleString('uk-UA') + '</td>';
      t += '<td class="text-right" style="color:var(--blue)">₴' + c.bolt_spend.toLocaleString('uk-UA') + '</td>';
      t += '<td class="text-right" style="color:var(--neg)">₴' + c.provider_spend.toLocaleString('uk-UA') + '</td>';
      t += '</tr>';
    }});
    t += '<tr class="total-row"><td colspan="4">Всього</td>';
    t += '<td class="text-right">' + totOrd + '</td>';
    t += '<td class="text-right">₴' + totDisc.toLocaleString('uk-UA') + '</td>';
    t += '<td class="text-right" style="color:var(--blue)">₴' + totBolt.toLocaleString('uk-UA') + '</td>';
    t += '<td class="text-right" style="color:var(--neg)">₴' + totProv.toLocaleString('uk-UA') + '</td>';
    t += '</tr>';
  }}
  t += '</tbody></table>';
  document.getElementById('campaigns-wrap').innerHTML = t;
}}

function fmtDiscount(r) {{
  const bolt = r.bolt_discount || 0;
  const prov = r.provider_discount || 0;
  const total = r.total_discount || 0;
  if (total <= 0) return '—';
  const parts = [];
  if (bolt > 0) parts.push('Bolt: ' + Math.round(bolt));
  if (prov > 0) parts.push('Заклад: ' + Math.round(prov));
  if (!parts.length) parts.push(Math.round(total));
  return parts.join(' / ');
}}

function fmtFee(net, gross) {{
  const vat = Math.round(gross - net);
  return Math.round(net) + ' + ' + vat + ' = ' + Math.round(gross);
}}

function renderOrdersDetail() {{
  const ids = getFilteredStoreIds();
  const selK = getSelectedPeriod();
  document.getElementById('orders-detail-week-label').textContent = '— ' + getPeriodLabel(selK);
  const periodField = periodMode === 'month' ? 'order_month' : 'order_week';

  let rows = (D.orders || []).filter(r => r[periodField] === selK && ids.includes(r.provider_id));
  if (selectedBP === 'yes') rows = rows.filter(r => r.bolt_plus === 'Bolt Plus');
  else if (selectedBP === 'no') rows = rows.filter(r => r.bolt_plus !== 'Bolt Plus');
  if (selectedState === 'delivered') rows = rows.filter(r => r.order_state_raw === 'delivered');
  else if (selectedState === 'failed') rows = rows.filter(r => r.order_state_raw !== 'delivered');

  let t = '<table class="data-table"><thead><tr>';
  t += '<th>Дата</th><th>Order Ref</th><th>Заклад</th><th>Статус</th><th>Bolt+</th>';
  t += '<th class="text-right">Ціна до знижки</th><th class="text-right">Знижка (за чий рахунок)</th>';
  t += '<th class="text-right">Дохід від їжі</th><th class="text-right">Комісія (нетто+ПДВ=брутто)</th>';
  t += '<th class="text-right">Bolt Plus комісія</th><th class="text-right">Всього комісія</th>';
  t += '<th class="text-right">Повернення</th><th class="text-right">Чистий дохід</th>';
  t += '<th>Причина</th>';
  t += '</tr></thead><tbody>';

  let totFood = 0, totRev = 0, totFee = 0, totBpFee = 0, totRef = 0, totNet = 0;
  rows.forEach(r => {{
    const date = r.order_created_date ? String(r.order_created_date).substring(0, 10) : '';
    totFood += r.food_before_discount || 0;
    totRev += r.food_revenue || 0;
    totFee += r.total_fee_gross || 0;
    totBpFee += r.bp_fee_gross || 0;
    totRef += r.refund || 0;
    totNet += r.net_income || 0;

    const isBp = (r.bp_fee_net || 0) > 0.5;
    const bpLabel = r.bolt_plus || 'Ні';
    const bpClass = isBp ? ' class="bp"' : '';
    const feeNet = r.fee_net || 0;
    const feeGross = r.fee_gross || 0;
    const bpFeeNet = r.bp_fee_net || 0;
    const bpFeeGross = r.bp_fee_gross || 0;
    const bpText = (isBp && bpFeeNet > 0.5) ? fmtFee(bpFeeNet, bpFeeGross) : '—';
    const isFailed = r.order_state_raw !== 'delivered';
    const stateColor = isFailed ? ' style="color:var(--neg);font-weight:600"' : '';
    const failReason = r.fail_reason || '';

    const nc = (r.net_income || 0) < 0 ? ' style="color:var(--neg)"' : '';
    t += '<tr' + (isFailed ? ' style="background:rgba(239,68,68,.04)"' : '') + '><td>' + date + '</td>';
    t += '<td>' + (r.order_reference_id || '') + '</td>';
    t += '<td>' + (r.provider_short || '') + '</td>';
    t += '<td' + stateColor + '>' + (r.order_state || '') + '</td>';
    t += '<td' + bpClass + '>' + bpLabel + '</td>';
    t += '<td class="text-right">' + (r.food_before_discount || 0).toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) + '</td>';
    t += '<td class="text-right">' + fmtDiscount(r) + '</td>';
    t += '<td class="text-right">' + (r.food_revenue || 0).toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) + '</td>';
    t += '<td class="text-right" style="font-size:11px">' + (isFailed ? '—' : fmtFee(feeNet, feeGross)) + '</td>';
    t += '<td class="text-right" style="font-size:11px">' + (isFailed ? '—' : bpText) + '</td>';
    t += '<td class="text-right" style="color:var(--neg)">' + (r.total_fee_gross || 0).toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) + '</td>';
    t += '<td class="text-right">' + (r.refund || 0).toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) + '</td>';
    t += '<td class="text-right"' + nc + '>' + (r.net_income || 0).toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) + '</td>';
    t += '<td class="comment-cell"' + (isFailed ? ' style="color:var(--neg)"' : '') + '>' + failReason + '</td></tr>';
  }});

  const failedCount = rows.filter(r => r.order_state_raw !== 'delivered').length;
  const deliveredCount = rows.length - failedCount;
  t += '<tr class="total-row"><td colspan="5">Всього (' + rows.length + ' зам., ' + deliveredCount + ' дост., ' + failedCount + ' невдал.)</td>';
  t += '<td class="text-right">' + totFood.toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) + '</td>';
  t += '<td></td>';
  t += '<td class="text-right">' + totRev.toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) + '</td>';
  t += '<td></td>';
  t += '<td class="text-right">' + totBpFee.toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) + '</td>';
  t += '<td class="text-right" style="color:var(--neg)">' + totFee.toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) + '</td>';
  t += '<td class="text-right">' + totRef.toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) + '</td>';
  t += '<td class="text-right">' + totNet.toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) + '</td>';
  t += '<td></td></tr>';
  t += '</tbody></table>';
  document.getElementById('orders-detail-wrap').innerHTML = t;
}}

function renderComplaints() {{
  const ids = getFilteredStoreIds();
  const selK = getSelectedPeriod();
  const periodField = periodMode === 'month' ? 'order_month' : 'order_week';
  const rows = (D.complaints || []).filter(r => r[periodField] === selK && ids.includes(r.provider_id));
  document.getElementById('comp-count').textContent = '(' + rows.length + ' за ' + getPeriodLabel(selK) + ')';

  let t = '<table class="data-table"><thead><tr><th>Дата</th><th>Order Ref</th><th>Заклад</th><th class="text-right">Сума</th><th>Тип проблеми</th><th>Винний</th><th class="text-center">Рейтинг</th><th>Коментар</th></tr></thead><tbody>';
  if (rows.length === 0) {{
    t += '<tr><td colspan="8" style="text-align:center;color:var(--text2);padding:24px">Немає скарг за цей період</td></tr>';
  }} else {{
    rows.forEach(r => {{
      const date = r.order_created_date ? String(r.order_created_date).substring(0, 10) : '';
      const comment = (r.provider_rating_comment || '').substring(0, 120);
      t += '<tr><td>' + date + '</td><td>' + (r.order_reference_id || '') + '</td><td>' + (r.provider_short || '') + '</td>';
      t += '<td class="text-right">₴' + (r.sum_uah || 0) + '</td>';
      t += '<td>' + (r.bad_order_type || '') + '</td>';
      t += '<td>' + (r.fault || '') + '</td>';
      t += '<td class="text-center">' + (r.rating != null ? r.rating : '—') + '</td>';
      t += '<td class="comment-cell">' + comment + '</td></tr>';
    }});
  }}
  t += '</tbody></table>';
  document.getElementById('complaints-wrap').innerHTML = t;
}}

function renderCancelled() {{
  const ids = getFilteredStoreIds();
  const selK = getSelectedPeriod();
  const periodField = periodMode === 'month' ? 'order_month' : 'order_week';
  const rows = (D.cancelled || []).filter(r => r[periodField] === selK && ids.includes(r.provider_id));
  document.getElementById('canc-count').textContent = '(' + rows.length + ' за ' + getPeriodLabel(selK) + ')';

  let t = '<table class="data-table"><thead><tr><th>Дата</th><th>Order Ref</th><th>Заклад</th><th>Статус</th><th>Причина</th><th>Коментар</th></tr></thead><tbody>';
  if (rows.length === 0) {{
    t += '<tr><td colspan="6" style="text-align:center;color:var(--text2);padding:24px">Немає скасованих за цей період</td></tr>';
  }} else {{
    rows.forEach(r => {{
      const date = r.order_created_date ? String(r.order_created_date).substring(0, 10) : '';
      const comment = (r.comment || '').substring(0, 150);
      t += '<tr><td>' + date + '</td><td>' + (r.order_reference_id || '') + '</td><td>' + (r.provider_short || '') + '</td>';
      t += '<td>' + (r.order_state || '') + '</td>';
      t += '<td>' + (r.reason || '') + '</td>';
      t += '<td class="comment-cell">' + comment + '</td></tr>';
    }});
  }}
  t += '</tbody></table>';
  document.getElementById('cancelled-wrap').innerHTML = t;
}}

function renderTopItems() {{
  const ids = getFilteredStoreIds();
  const selK = getSelectedPeriod();
  const itemsStore = getItemsData();
  const weekItems = itemsStore[selK] || {{}};
  document.getElementById('items-week-label').textContent = '— ' + getPeriodLabel(selK);
  let html = '';
  ids.forEach(id => {{
    const items = weekItems[id];
    if (!items || !items.length) return;
    const s = D.stores[id];
    html += '<div class="items-card"><h4>' + s.short + '</h4><div class="items-city">' + s.city + '</div><ol>';
    items.forEach(it => {{
      html += '<li>' + it.name + '<span class="item-qty">' + it.qty + ' шт</span><span class="item-rev">₴' + it.revenue.toLocaleString('uk-UA') + '</span></li>';
    }});
    html += '</ol></div>';
  }});
  document.getElementById('items-grid').innerHTML = html || '<p style="color:var(--text2);padding:24px;text-align:center">Немає даних.</p>';
}}

function renderSmartPromo() {{
  const ids = getFilteredStoreIds();
  const spStatus = D.smart_promo_status || {{}};
  const spEnrollments = D.smart_promo_enrollments || [];
  const spOrders = D.smart_promo_orders || {{}};

  let activeCount = 0, enrolledEver = 0, totalStores = ids.length;
  ids.forEach(id => {{
    const s = spStatus[id];
    if (s) {{
      if (s.is_active) activeCount++;
      if (s.has_ever_enrolled) enrolledEver++;
    }}
  }});

  const insightEl = document.getElementById('sp-insight');
  if (activeCount === 0 && enrolledEver === 0) {{
    insightEl.innerHTML = '<b>Smart Promo не використовується жодним закладом.</b> Жоден заклад ніколи не був підключений до Smart Promo. Це означає, що партнер втрачає потенційний апліфт +20–30% замовлень. Рекомендуємо обговорити підключення.';
  }} else if (activeCount === 0) {{
    insightEl.innerHTML = '<b>Smart Promo неактивний.</b> ' + enrolledEver + ' із ' + totalStores + ' закладів були підключені раніше, але зараз жоден не має активної кампанії.';
  }} else {{
    insightEl.innerHTML = '<b>Smart Promo активний на ' + activeCount + ' із ' + totalStores + ' закладів.</b> ' + (totalStores - activeCount) + ' закладів без Smart Promo — потенціал для зростання.';
  }}

  document.getElementById('sp-kpi-grid').innerHTML =
    '<div class="kpi-card"><div class="kpi-label">Активних Smart Promo</div><div class="kpi-value" style="color:' + (activeCount > 0 ? 'var(--pos)' : 'var(--neg)') + '">' + activeCount + ' / ' + totalStores + '</div><div class="kpi-change ' + (activeCount > 0 ? 'up' : 'down') + '">' + (activeCount > 0 ? 'Активні' : 'Жодного') + '</div></div>' +
    '<div class="kpi-card"><div class="kpi-label">Коли-небудь підключались</div><div class="kpi-value">' + enrolledEver + '</div><div class="kpi-change neutral">з ' + totalStores + ' закладів</div></div>' +
    '<div class="kpi-card"><div class="kpi-label">Без Smart Promo</div><div class="kpi-value" style="color:var(--warn)">' + (totalStores - activeCount) + '</div><div class="kpi-change down">Потенціал для зростання</div></div>';

  let t = '<table class="data-table"><thead><tr><th>#</th><th>Заклад</th><th class="text-center">Smart Promo</th><th class="text-center">Sponsored Listing</th><th>Статус</th><th>Рекомендація</th></tr></thead><tbody>';
  const slStatus = D.sponsored_status || {{}};
  ids.sort((a,b) => a - b).forEach((id, i) => {{
    const s = D.stores[id];
    const sp = spStatus[id] || {{}};
    const sl = slStatus[id] || {{}};
    const spActive = sp.is_active;
    const slActive = sl.has_ever_used;
    const spBadge = spActive ? '<span style="color:var(--pos);font-weight:700">✓ Активний</span>' : '<span style="color:var(--neg);font-weight:700">✗ Ні</span>';
    const slBadge = slActive ? '<span style="color:var(--pos);font-weight:700">✓ Так</span>' : '<span style="color:var(--neg);font-weight:700">✗ Ні</span>';
    let status = '', reco = '';
    if (!spActive && !slActive) {{
      status = '<span style="color:var(--neg);font-weight:600">Без промо</span>';
      reco = 'Підключити Smart Promo + Sponsored Listing';
    }} else if (spActive && !slActive) {{
      status = '<span style="color:var(--warn);font-weight:600">Частково</span>';
      reco = 'Додати Sponsored Listing';
    }} else if (!spActive && slActive) {{
      status = '<span style="color:var(--warn);font-weight:600">Частково</span>';
      reco = 'Додати Smart Promo';
    }} else {{
      status = '<span style="color:var(--pos);font-weight:600">Повний набір</span>';
      reco = 'Моніторити ефективність';
    }}
    t += '<tr><td>' + (i+1) + '</td><td>' + (s ? s.short : id) + '</td><td class="text-center">' + spBadge + '</td><td class="text-center">' + slBadge + '</td><td class="text-center">' + status + '</td><td>' + reco + '</td></tr>';
  }});
  t += '</tbody></table>';
  document.getElementById('sp-status-wrap').innerHTML = t;

  const pkeys = getFilteredPeriodKeys();
  const labels = pkeys.map(k => getPeriodLabel(k));

  destroyChart('chart-sp-orders');
  destroyChart('chart-sp-spend');
  const spOrdData = pkeys.map(k => {{
    const wk = spOrders[k] || {{}};
    return ids.reduce((s, id) => s + ((wk[id] && wk[id].orders) || 0), 0);
  }});
  const spSpendData = pkeys.map(k => {{
    const wk = spOrders[k] || {{}};
    return ids.reduce((s, id) => s + ((wk[id] && wk[id].provider_spend) || 0), 0);
  }});
  const hasData = spOrdData.some(v => v > 0);
  if (hasData) {{
    chartInstances['chart-sp-orders'] = new Chart(document.getElementById('chart-sp-orders'), {{
      type: 'bar',
      data: {{ labels, datasets: [{{ label: 'Smart Promo замовлення', data: spOrdData, backgroundColor: 'rgba(16,185,129,.7)', borderRadius: 4, barPercentage: .6 }}] }},
      options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }}, scales: {{ y: {{ beginAtZero: true }}, x: {{ grid: {{ display: false }} }} }} }}
    }});
    chartInstances['chart-sp-spend'] = new Chart(document.getElementById('chart-sp-spend'), {{
      type: 'bar',
      data: {{ labels, datasets: [{{ label: 'Витрати ₴', data: spSpendData, backgroundColor: 'rgba(239,68,68,.7)', borderRadius: 4, barPercentage: .6 }}] }},
      options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }}, scales: {{ y: {{ beginAtZero: true, ticks: {{ callback: v => '₴' + v }} }}, x: {{ grid: {{ display: false }} }} }} }}
    }});
  }} else {{
    document.getElementById('chart-sp-orders').parentElement.innerHTML = '<p style="color:var(--text2);text-align:center;padding:60px 20px">Немає замовлень через Smart Promo. Жоден заклад не підключений.</p>';
    document.getElementById('chart-sp-spend').parentElement.innerHTML = '<p style="color:var(--text2);text-align:center;padding:60px 20px">Немає даних про витрати Smart Promo.</p>';
  }}

  if (spEnrollments.length > 0) {{
    let et = '<table class="data-table"><thead><tr><th>Заклад</th><th>Тип</th><th>Режим</th><th>Стан</th><th>Початок</th><th>Кінець</th><th>Валідна</th></tr></thead><tbody>';
    spEnrollments.filter(e => ids.includes(e.provider_id)).forEach(e => {{
      et += '<tr><td>' + e.provider_short + '</td><td>' + e.offer_type + '</td><td>' + e.mode + '</td><td>' + e.state + '</td><td>' + e.start + '</td><td>' + e.end + '</td><td>' + (e.is_valid ? '✓' : '✗') + '</td></tr>';
    }});
    et += '</tbody></table>';
    document.getElementById('sp-enrollments-wrap').innerHTML = et;
  }} else {{
    document.getElementById('sp-enrollments-wrap').innerHTML = '<p style="color:var(--text2);text-align:center;padding:20px">Історія підключень Smart Promo відсутня.</p>';
  }}
}}

function renderSponsoredListings() {{
  const ids = getFilteredStoreIds();
  const slStatus = D.sponsored_status || {{}};
  const slData = D.sponsored_listings || [];
  const totalStores = ids.length;

  let usedEver = 0, totalOrders = 0, totalSpend = 0;
  ids.forEach(id => {{
    const s = slStatus[id];
    if (s) {{
      if (s.has_ever_used) usedEver++;
      totalOrders += s.total_orders;
      totalSpend += s.total_spend;
    }}
  }});

  const insightEl = document.getElementById('sl-insight');
  if (usedEver === 0) {{
    insightEl.innerHTML = '<b>Sponsored Listing ніколи не використовувався.</b> Жоден заклад Мʼясоруб не мав спонсорованих оголошень. Це означає, що конкуренти можуть з\\\'являтись вище у пошуку. Рекомендуємо тестовий запуск з бюджетом 400–600 ₴/тиждень.';
  }} else {{
    insightEl.innerHTML = '<b>Sponsored Listing використовується ' + usedEver + ' із ' + totalStores + ' закладів.</b> Всього ' + totalOrders + ' замовлень через спонсоровані оголошення. Загальні витрати: ₴' + totalSpend.toLocaleString('uk-UA') + '.';
  }}

  document.getElementById('sl-kpi-grid').innerHTML =
    '<div class="kpi-card"><div class="kpi-label">Закладів зі Sponsored</div><div class="kpi-value" style="color:' + (usedEver > 0 ? 'var(--pos)' : 'var(--neg)') + '">' + usedEver + ' / ' + totalStores + '</div></div>' +
    '<div class="kpi-card"><div class="kpi-label">Замовлень через SL</div><div class="kpi-value">' + totalOrders + '</div></div>' +
    '<div class="kpi-card"><div class="kpi-label">Витрати на SL</div><div class="kpi-value">₴' + totalSpend.toLocaleString('uk-UA') + '</div></div>';

  let t = '<table class="data-table"><thead><tr><th>#</th><th>Заклад</th><th class="text-center">Sponsored Listing</th><th class="text-right">Замовлень</th><th class="text-right">Витрати ₴</th></tr></thead><tbody>';
  ids.sort((a,b) => a - b).forEach((id, i) => {{
    const s = D.stores[id];
    const sl = slStatus[id] || {{}};
    const badge = sl.has_ever_used ? '<span style="color:var(--pos);font-weight:700">✓ Використовувався</span>' : '<span style="color:var(--neg);font-weight:700">✗ Ніколи</span>';
    t += '<tr><td>' + (i+1) + '</td><td>' + (s ? s.short : id) + '</td><td class="text-center">' + badge + '</td><td class="text-right">' + (sl.total_orders || 0) + '</td><td class="text-right">₴' + (sl.total_spend || 0).toLocaleString('uk-UA') + '</td></tr>';
  }});
  t += '</tbody></table>';
  document.getElementById('sl-status-wrap').innerHTML = t;

  if (slData.length > 0) {{
    let dt = '<table class="data-table"><thead><tr><th>Заклад</th><th>Кампанія</th><th>Тиждень</th><th class="text-right">Замовлення</th><th class="text-right">Витрати ₴</th></tr></thead><tbody>';
    slData.filter(r => ids.includes(r.provider_id)).forEach(r => {{
      dt += '<tr><td>' + r.provider_short + '</td><td style="max-width:250px;white-space:normal">' + r.campaign_name + '</td><td>' + r.order_week + '</td><td class="text-right">' + r.orders + '</td><td class="text-right">₴' + r.total_spend.toLocaleString('uk-UA') + '</td></tr>';
    }});
    dt += '</tbody></table>';
    document.getElementById('sl-detail-wrap').innerHTML = dt;
  }} else {{
    document.getElementById('sl-detail-wrap').innerHTML = '<p style="color:var(--text2);text-align:center;padding:20px">Немає даних по Sponsored Listing кампаніях.</p>';
  }}
}}

function renderAll() {{
  renderKPIs();
  renderInsights();
  renderOrdersCharts();
  renderOpsCharts();
  renderStoresTable();
  renderRevenueChart();
  renderCampaignsChart();
  renderCampaigns();
  renderOrdersDetail();
  renderComplaints();
  renderCancelled();
  renderTopItems();
  renderSmartPromo();
  renderSponsoredListings();
}}

function setupNav() {{
  const links = document.querySelectorAll('.nav-link');
  links.forEach(a => {{
    a.addEventListener('click', e => {{
      e.preventDefault();
      const id = a.getAttribute('href').substring(1);
      const el = document.getElementById(id);
      if (el) el.scrollIntoView({{ behavior: 'smooth', block: 'start' }});
      links.forEach(l => l.classList.remove('active'));
      a.classList.add('active');
    }});
  }});
  const observer = new IntersectionObserver(entries => {{
    entries.forEach(en => {{
      if (en.isIntersecting) {{
        const id = en.target.id;
        links.forEach(l => l.classList.toggle('active', l.getAttribute('href') === '#' + id));
      }}
    }});
  }}, {{ rootMargin: '-140px 0px -70% 0px' }});
  document.querySelectorAll('.section').forEach(s => observer.observe(s));
}}

document.getElementById('bp-filter').addEventListener('change', function() {{
  selectedBP = this.value;
  renderOrdersDetail();
}});

document.getElementById('state-filter').addEventListener('change', function() {{
  selectedState = this.value;
  renderOrdersDetail();
}});

window.toggleDark = function() {{
  document.body.classList.toggle('dark');
  const isDark = document.body.classList.contains('dark');
  document.getElementById('theme-toggle').textContent = isDark ? '☀️' : '🌙';
  try {{ localStorage.setItem('miasorub-dark', isDark ? '1' : '0') }} catch(e) {{}}
  Chart.defaults.color = isDark ? '#D1D5DB' : '#374151';
  renderAll();
}};
(function() {{ try {{ if (localStorage.getItem('miasorub-dark') === '1') {{ document.body.classList.add('dark'); document.getElementById('theme-toggle').textContent = '☀️'; Chart.defaults.color = '#D1D5DB'; }} }} catch(e) {{}} }})();

document.getElementById('period-select').addEventListener('change', function() {{
  periodMode = this.value;
  populateWeekBar();
  renderAll();
}});

initMsToggle('city-btn', 'city-panel');
initMsToggle('store-btn', 'store-panel');
populateCityFilter();
populateStoreFilter();
populateWeekBar();
setupNav();
renderAll();
</script>
</body>
</html>"""


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    generated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    print(f"Starting Мʼясоруб report generation at {generated_at}")

    conn = connect()
    try:
        print("  Fetching weekly per-store data…")
        weekly_df = fetch_weekly_per_store(conn)
        print(f"  → {len(weekly_df)} rows")

        print("  Fetching operational metrics…")
        ops_df = fetch_ops_metrics(conn)
        print(f"  → {len(ops_df)} rows")

        print("  Fetching top items…")
        items_df = fetch_top_items(conn)
        print(f"  → {len(items_df)} rows")

        print("  Fetching orders detail…")
        orders_df = fetch_orders_detail(conn)
        print(f"  → {len(orders_df)} rows")

        print("  Fetching complaints…")
        complaints_df = fetch_complaints(conn)
        print(f"  → {len(complaints_df)} rows")

        print("  Fetching cancelled orders…")
        cancelled_df = fetch_cancelled(conn)
        print(f"  → {len(cancelled_df)} rows")

        print("  Fetching revenue weekly…")
        revenue_df = fetch_revenue_weekly(conn)
        print(f"  → {len(revenue_df)} rows")

        print("  Fetching campaigns…")
        campaigns_df = fetch_campaigns(conn)
        print(f"  → {len(campaigns_df)} rows")

        print("  Fetching monthly per-store data…")
        monthly_df = fetch_monthly_per_store(conn)
        print(f"  → {len(monthly_df)} rows")

        print("  Fetching Smart Promo enrollments…")
        smart_promo_df = fetch_smart_promo(conn)
        print(f"  → {len(smart_promo_df)} rows")

        print("  Fetching Smart Promo orders…")
        smart_promo_orders_df = fetch_smart_promo_orders(conn)
        print(f"  → {len(smart_promo_orders_df)} rows")

        print("  Fetching Sponsored Listings…")
        sponsored_df = fetch_sponsored_listings(conn)
        print(f"  → {len(sponsored_df)} rows")
    finally:
        conn.close()

    data = build_data(
        weekly_df, ops_df, items_df, orders_df, complaints_df, cancelled_df,
        revenue_df, campaigns_df, smart_promo_df, smart_promo_orders_df, sponsored_df,
        monthly_df=monthly_df
    )
    html = generate_html(data, generated_at)

    out_dir = REPO_ROOT / "miasorub"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "index.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"  Saved: {out_path}")
    print("\nDone!")


if __name__ == "__main__":
    main()
