"""
Generate a multi-store weekly HTML report for Кулиничі by querying Databricks.
Produces kulinichi/index.html — a dashboard with availability tracking by day/hour.
"""

import os
import sys
import json
import math
import re
from pathlib import Path
from datetime import datetime
from decimal import Decimal

sys.stdout.reconfigure(line_buffering=True)

from databricks import sql
import pandas as pd

from config import SERVER_HOSTNAME, HTTP_PATH

REPO_ROOT = Path(__file__).resolve().parent.parent
WEEKS_BACK = 8

KULINICHI_PROVIDERS = {
    160247: {"name": "Кулиничі просп. Червоної Калини", "short": "Червоної Калини", "city": "Lviv"},
    153492: {"name": "Галицька перепічка вул. Кавалерідзе", "short": "ГП Кавалерідзе", "city": "Lviv"},
    153466: {"name": "Кулиничі вул. Стрийська", "short": "Стрийська", "city": "Lviv"},
    153462: {"name": "Кулиничі вул. Грінченка", "short": "Грінченка", "city": "Lviv"},
    160241: {"name": "Кулиничі просп. Чорновола", "short": "Чорновола", "city": "Lviv"},
    153505: {"name": "Кулиничі вул. Полуботка", "short": "Полуботка", "city": "Rivne"},
    153507: {"name": "Кулиничі вул. Личаківська", "short": "Личаківська", "city": "Lviv"},
    153431: {"name": "Кулиничі вул. Наукова", "short": "Наукова", "city": "Lviv"},
    153502: {"name": "Кулиничі вул. Винна Гора", "short": "Винна Гора", "city": "Lviv"},
    153499: {"name": "Кулиничі вул. Левицького", "short": "Левицького", "city": "Lviv"},
    153483: {"name": "Кулиничі вул. Липинського", "short": "Липинського", "city": "Lviv"},
    153509: {"name": "Кулиничі вул. Шевченка", "short": "Шевченка", "city": "Lviv"},
    160249: {"name": "Кулиничі вул. Пекарська", "short": "Пекарська", "city": "Lviv"},
    153440: {"name": "Кулиничі вул. Богоявленська", "short": "Богоявленська", "city": "Rivne"},
    153504: {"name": "Галицька перепічка вул. Вол. Великого", "short": "ГП Вол. Великого", "city": "Lviv"},
    153484: {"name": "Кулиничі вул. Мазепи", "short": "Мазепи", "city": "Lviv"},
}

PROVIDER_IDS = ",".join(str(k) for k in KULINICHI_PROVIDERS)

CITY_UA = {"Lviv": "Львів", "Rivne": "Рівне"}

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

OFFLINE_REASON_UA = {
    "dropped_polling": "Втрата з'єднання",
    "working_hours_ended": "Кінець робочого часу",
    "working_hours_started": "Початок робочого часу",
    "provider_app_change": "Ручне вимкнення",
    "did_not_respond_to_order": "Не відповів на замовлення",
    "resumed_polling": "Відновлення з'єднання",
    "Closing for today": "Закриття на сьогодні",
    "Closing for renovation": "Закриття на ремонт",
    "Device problems": "Проблеми з пристроєм",
}


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


def to_native(val, default=0):
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
    return json.loads(df.to_json(orient="records", date_format="iso"))


def week_sort_key(w):
    parts = w.split("-W")
    return (int(parts[0]), int(parts[1]))


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


def fetch_ops_metrics(conn):
    return query(conn, f"""
    SELECT
    provider_id,
    date,
    ROUND(availability_rate_last_7d * 100, 1) AS availability,
    ROUND(acceptance_rate_last_7d * 100, 1) AS acceptance,
    ROUND(image_coverage_rate * 100, 1) AS photo_coverage
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
    COALESCE(GET_JSON_OBJECT(b.name_translations, '$.uk-UA'), b.name) AS item_name,
    COUNT(*) AS qty,
    ROUND(SUM(b.total_price), 0) AS revenue,
    ROW_NUMBER() OVER (PARTITION BY f.provider_id, f.order_week ORDER BY COUNT(*) DESC) AS rn
    FROM ng_delivery_spark.etl_delivery_order_user_basket_item_v2 b
    JOIN ng_delivery_spark.fact_order_delivery f ON b.order_id = f.order_id
    WHERE f.provider_id IN ({PROVIDER_IDS})
    AND f.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
    AND f.order_created_date < DATE_TRUNC('WEEK', CURRENT_DATE())
    AND b.created_date >= DATE_FORMAT(DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7}), 'yyyy-MM-dd')
    AND f.order_state = 'delivered'
    AND b.total_price IS NOT NULL
    AND b.total_price > 0
    GROUP BY f.provider_id, f.order_week, COALESCE(GET_JSON_OBJECT(b.name_translations, '$.uk-UA'), b.name)
    )
    SELECT provider_id, order_week, item_name, qty, revenue
    FROM ranked WHERE rn <= 10
    ORDER BY provider_id, order_week, qty DESC
    """)


def fetch_orders_detail(conn):
    return query(conn, f"""
    WITH order_promos AS (
        SELECT
            c.order_id,
            CONCAT_WS('; ', COLLECT_SET(c.name)) AS promo_names,
            ROUND(SUM(c.bolt_spend_local), 2) AS promo_bolt_spend,
            ROUND(SUM(c.provider_spend_local), 2) AS promo_provider_spend,
            ROUND(SUM(c.discount_value_local), 2) AS promo_total_discount
        FROM ng_public_spark.etl_delivery_campaign_order_metrics c
        WHERE c.provider_id IN ({PROVIDER_IDS})
        AND c.order_created_date >= DATE_FORMAT(DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7}), 'yyyy-MM-dd')
        AND c.order_created_date < DATE_FORMAT(DATE_TRUNC('WEEK', CURRENT_DATE()), 'yyyy-MM-dd')
        GROUP BY c.order_id
    )
    SELECT
    f.order_id, f.order_reference_id, f.order_created_date, f.order_week,
    f.provider_id, f.provider_name, f.order_state,
    'Ні' AS bolt_plus,
    false AS is_bolt_plus_order,
    ROUND(f.provider_price_before_discount, 2) AS food_before_discount,
    ROUND(f.total_order_item_discount, 2) AS total_discount,
    ROUND((COALESCE(m.bolt_delivery_campaign_cost_eur, 0) + COALESCE(m.bolt_menu_campaign_cost_eur, 0)) * m.currency_rate, 2) AS bolt_discount,
    ROUND((COALESCE(m.provider_delivery_campaign_cost_eur, 0) + COALESCE(m.provider_menu_campaign_cost_eur, 0)) * m.currency_rate, 2) AS provider_discount,
    ROUND(f.provider_price_after_discount, 2) AS food_revenue,
    ROUND(m.provider_commission_net_eur * m.currency_rate, 2) AS fee_net,
    ROUND(m.provider_commission_gross_eur * m.currency_rate, 2) AS fee_gross,
    ROUND(f.commission_local - m.provider_commission_net_eur * m.currency_rate, 2) AS campaign_fee_net,
    ROUND((f.commission_local - m.provider_commission_net_eur * m.currency_rate) * 1.2, 2) AS campaign_fee_gross,
    ROUND(f.commission_local * 1.2, 2) AS total_fee_gross,
    ROUND(COALESCE(f.total_refunded_amount, 0), 2) AS refund,
    ROUND(
        f.provider_price_after_discount
        + (COALESCE(m.bolt_delivery_campaign_cost_eur, 0) + COALESCE(m.bolt_menu_campaign_cost_eur, 0)) * m.currency_rate
        - f.commission_local * 1.2
        - COALESCE(f.total_refunded_amount, 0)
    , 2) AS net_income,
    p.promo_names,
    COALESCE(p.promo_bolt_spend, 0) AS promo_bolt_spend,
    COALESCE(p.promo_provider_spend, 0) AS promo_provider_spend,
    COALESCE(p.promo_total_discount, 0) AS promo_total_discount
    FROM ng_delivery_spark.fact_order_delivery f
    JOIN ng_public_spark.etl_delivery_order_monetary_metrics m ON f.order_id = m.order_id
    LEFT JOIN order_promos p ON f.order_id = p.order_id
    WHERE f.provider_id IN ({PROVIDER_IDS})
    AND f.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
    AND f.order_created_date < DATE_TRUNC('WEEK', CURRENT_DATE())
    AND m.order_created_date >= DATE_FORMAT(DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7}), 'yyyy-MM-dd')
    AND f.order_state = 'delivered'
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
    AND f.order_created_date < DATE_TRUNC('WEEK', CURRENT_DATE())
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
    AND f.order_created_date < DATE_TRUNC('WEEK', CURRENT_DATE())
    AND f.order_state IN ('rejected', 'cancelled', 'failed')
    ORDER BY f.order_created_date DESC
    """)


def fetch_revenue_weekly(conn):
    return query(conn, f"""
    SELECT
    f.order_week, f.provider_id,
    COUNT(*) AS orders,
    ROUND(SUM(f.provider_price_after_discount), 0) AS food_revenue,
    ROUND(SUM(f.commission_local * 1.2), 0) AS total_fee_gross,
    ROUND(SUM(COALESCE(f.total_refunded_amount, 0)), 0) AS refund,
    ROUND(SUM((COALESCE(m.bolt_delivery_campaign_cost_eur, 0) + COALESCE(m.bolt_menu_campaign_cost_eur, 0)) * m.currency_rate), 0) AS bolt_compensation,
    ROUND(
        SUM(f.provider_price_after_discount)
        + SUM((COALESCE(m.bolt_delivery_campaign_cost_eur, 0) + COALESCE(m.bolt_menu_campaign_cost_eur, 0)) * m.currency_rate)
        - SUM(f.commission_local * 1.2)
        - SUM(COALESCE(f.total_refunded_amount, 0))
    , 0) AS net_income
    FROM ng_delivery_spark.fact_order_delivery f
    JOIN ng_public_spark.etl_delivery_order_monetary_metrics m ON f.order_id = m.order_id
    WHERE f.provider_id IN ({PROVIDER_IDS})
    AND f.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
    AND f.order_created_date < DATE_TRUNC('WEEK', CURRENT_DATE())
    AND m.order_created_date >= DATE_FORMAT(DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7}), 'yyyy-MM-dd')
    AND f.order_state = 'delivered'
    GROUP BY f.order_week, f.provider_id
    ORDER BY f.order_week, f.provider_id
    """)


def fetch_daily_availability(conn):
    return query(conn, f"""
    SELECT
    provider_id,
    created_date,
    ROUND(availability * 100, 1) AS availability_pct,
    ROUND(active_time / 60.0, 0) AS active_minutes,
    ROUND(working_time / 60.0, 0) AS working_minutes,
    ROUND(inactive_time / 60.0, 0) AS inactive_minutes
    FROM ng_delivery_spark.etl_delivery_provider_daily_availability
    WHERE provider_id IN ({PROVIDER_IDS})
    AND created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
    ORDER BY created_date DESC, provider_id
    """)


def fetch_availability_log(conn):
    return query(conn, f"""
    SELECT
    provider_id,
    created,
    availability,
    availability_change_reason,
    offline_availability_reason
    FROM ng_delivery_spark.provider_availability_log
    WHERE provider_id IN ({PROVIDER_IDS})
    AND created_date >= DATE_FORMAT(DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7}), 'yyyy-MM-dd')
    ORDER BY created DESC
    """)


def fetch_promo_data(conn):
    return query(conn, f"""
    SELECT
    f.order_week,
    f.provider_id,
    c.name AS campaign_name,
    c.campaign_type,
    c.spend_objective,
    COUNT(DISTINCT c.order_id) AS promo_orders,
    ROUND(SUM(c.discount_value_local), 0) AS total_discount,
    ROUND(SUM(c.bolt_spend_local), 0) AS bolt_spend,
    ROUND(SUM(c.provider_spend_local), 0) AS provider_spend
    FROM ng_public_spark.etl_delivery_campaign_order_metrics c
    JOIN ng_delivery_spark.fact_order_delivery f ON c.order_id = f.order_id
    WHERE c.provider_id IN ({PROVIDER_IDS})
    AND c.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
    AND c.order_created_date < DATE_TRUNC('WEEK', CURRENT_DATE())
    AND f.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
    AND f.order_created_date < DATE_TRUNC('WEEK', CURRENT_DATE())
    AND f.order_state = 'delivered'
    GROUP BY f.order_week, f.provider_id, c.name, c.campaign_type, c.spend_objective
    ORDER BY f.order_week DESC, promo_orders DESC
    """)


def fetch_promo_unique_orders(conn):
    return query(conn, f"""
    SELECT
    f.order_week,
    COUNT(DISTINCT c.order_id) AS unique_promo_orders
    FROM ng_public_spark.etl_delivery_campaign_order_metrics c
    JOIN ng_delivery_spark.fact_order_delivery f ON c.order_id = f.order_id
    WHERE c.provider_id IN ({PROVIDER_IDS})
    AND c.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
    AND c.order_created_date < DATE_TRUNC('WEEK', CURRENT_DATE())
    AND f.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
    AND f.order_created_date < DATE_TRUNC('WEEK', CURRENT_DATE())
    AND f.order_state = 'delivered'
    GROUP BY f.order_week
    """)


# ── Build data for HTML ──────────────────────────────────────────────────

def strip_nda(text):
    """Remove admin names, beehive URLs, and 'Reason:' prefixes from comments."""
    if not text:
        return ""
    text = re.sub(r'Admin:\s*[A-Za-zА-Яа-яІіЇїЄєҐґ\s\-]+?\s*Reason:\s*', '', text)
    text = re.sub(r'h?t?tps?://beehive[^\s]*', '', text)
    text = re.sub(r'beehive\.bolt\.eu\S*', '', text)
    text = re.sub(r'https?://\S*beehive\S*', '', text)
    return text.strip()


def build_data(weekly_df, ops_df, items_df, orders_df, complaints_df, cancelled_df, revenue_df, daily_avail_df, avail_log_df, promo_df, promo_unique_df):
    data = {}

    stores_map = {}
    for _, r in weekly_df.iterrows():
        pid = to_native(r["provider_id"])
        info = KULINICHI_PROVIDERS.get(int(pid), {})
        stores_map[int(pid)] = {
            "name": info.get("name", r["provider_name"]),
            "short": info.get("short", r["provider_name"]),
            "city": CITY_UA.get(r["city_name"], r["city_name"]),
            "city_en": r["city_name"],
        }
    for pid, info in KULINICHI_PROVIDERS.items():
        if pid not in stores_map:
            stores_map[pid] = {
                "name": info["name"],
                "short": info["short"],
                "city": CITY_UA.get(info["city"], info["city"]),
                "city_en": info["city"],
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

    ops_weekly = {}
    if len(ops_df):
        ops_df["date_str"] = ops_df["date"].astype(str)
        ops_df["dow"] = pd.to_datetime(ops_df["date"]).dt.dayofweek
        mondays = ops_df[ops_df["dow"] == 0].copy()
        if len(mondays) == 0:
            mondays = ops_df.sort_values("date").drop_duplicates(subset=["provider_id"], keep="last")

        for _, r in mondays.iterrows():
            pid = int(to_native(r["provider_id"]))
            ds = str(r["date_str"])
            year = int(ds[:4])
            iso_week = pd.Timestamp(ds).isocalendar().week
            week_key = f"{year}-W{iso_week}"
            if week_key not in ops_weekly:
                ops_weekly[week_key] = {}
            ops_weekly[week_key][pid] = {
                "availability": to_native(r["availability"]),
                "acceptance": to_native(r["acceptance"]),
                "photo_coverage": to_native(r["photo_coverage"]),
            }

    latest_ops = {}
    if len(ops_df):
        latest = ops_df.sort_values("date").drop_duplicates(subset=["provider_id"], keep="last")
        for _, r in latest.iterrows():
            pid = int(to_native(r["provider_id"]))
            latest_ops[pid] = {
                "availability": to_native(r["availability"]),
                "acceptance": to_native(r["acceptance"]),
                "photo_coverage": to_native(r["photo_coverage"]),
            }
    data["ops_weekly"] = ops_weekly
    data["latest_ops"] = latest_ops

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

    orders_list = safe_json(orders_df)
    for row in orders_list:
        row["order_state"] = ORDER_STATE_UA.get(row.get("order_state", ""), row.get("order_state", ""))
        pid = row.get("provider_id")
        if pid and int(pid) in KULINICHI_PROVIDERS:
            row["provider_short"] = KULINICHI_PROVIDERS[int(pid)]["short"]
        else:
            row["provider_short"] = row.get("provider_name", "")
    data["orders"] = orders_list

    comp_list = safe_json(complaints_df)
    for row in comp_list:
        raw_type = row.get("bad_order_type", "") or ""
        row["bad_order_type"] = BAD_ORDER_TYPE_UA.get(raw_type, raw_type)
        raw_fault = row.get("fault", "") or ""
        row["fault"] = FAULT_UA.get(str(raw_fault).lower(), raw_fault)
        pid = row.get("provider_id")
        if pid and int(pid) in KULINICHI_PROVIDERS:
            row["provider_short"] = KULINICHI_PROVIDERS[int(pid)]["short"]
        else:
            row["provider_short"] = row.get("provider_name", "")
    data["complaints"] = comp_list

    canc_list = safe_json(cancelled_df)
    for row in canc_list:
        raw_state = row.get("order_state", "") or ""
        row["order_state"] = ORDER_STATE_UA.get(raw_state, raw_state)
        pid = row.get("provider_id")
        if pid and int(pid) in KULINICHI_PROVIDERS:
            row["provider_short"] = KULINICHI_PROVIDERS[int(pid)]["short"]
        else:
            row["provider_short"] = row.get("provider_name", "")
        row["comment"] = strip_nda(row.get("comment", ""))
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
            "total_fee_gross": to_native(r["total_fee_gross"]),
            "refund": to_native(r["refund"]),
            "bolt_compensation": to_native(r["bolt_compensation"]),
            "net_income": to_native(r["net_income"]),
        }
    revenue = dict(sorted(revenue.items(), key=lambda x: week_sort_key(x[0])))
    data["revenue"] = revenue

    # Daily availability: { "2026-03-25": { provider_id: { availability_pct, active_minutes, working_minutes, inactive_minutes } } }
    daily_avail = {}
    for _, r in daily_avail_df.iterrows():
        ds = str(r["created_date"])
        pid = int(to_native(r["provider_id"]))
        if ds not in daily_avail:
            daily_avail[ds] = {}
        daily_avail[ds][pid] = {
            "pct": to_native(r["availability_pct"]),
            "active": to_native(r["active_minutes"]),
            "working": to_native(r["working_minutes"]),
            "inactive": to_native(r["inactive_minutes"]),
        }
    daily_avail = dict(sorted(daily_avail.items()))
    data["daily_avail"] = daily_avail

    # Availability log: list of offline events
    avail_events = []
    for _, r in avail_log_df.iterrows():
        pid = int(to_native(r["provider_id"]))
        avail_status = str(r["availability"]) if r["availability"] else ""
        reason = str(r["availability_change_reason"]) if r["availability_change_reason"] else ""
        offline_reason = str(r["offline_availability_reason"]) if r["offline_availability_reason"] else ""

        reason_ua = OFFLINE_REASON_UA.get(reason, reason)
        offline_reason_ua = OFFLINE_REASON_UA.get(offline_reason, offline_reason) if offline_reason and offline_reason != "None" else ""

        short = KULINICHI_PROVIDERS.get(pid, {}).get("short", str(pid))
        ts = str(r["created"])

        avail_events.append({
            "ts": ts,
            "pid": pid,
            "short": short,
            "status": avail_status,
            "reason": reason_ua,
            "offline_reason": offline_reason_ua,
        })
    data["avail_events"] = avail_events

    # Promo data: { week: [ { campaign_name, promo_orders, total_discount, bolt_spend, provider_spend, provider_id } ] }
    promo = {}
    for _, r in promo_df.iterrows():
        week = str(r["order_week"])
        pid = int(to_native(r["provider_id"]))
        if week not in promo:
            promo[week] = []
        short = KULINICHI_PROVIDERS.get(pid, {}).get("short", str(pid))
        promo[week].append({
            "pid": pid,
            "short": short,
            "name": str(r["campaign_name"]),
            "type": str(r["spend_objective"] or r["campaign_type"]),
            "orders": to_native(r["promo_orders"]),
            "discount": to_native(r["total_discount"]),
            "bolt_spend": to_native(r["bolt_spend"]),
            "provider_spend": to_native(r["provider_spend"]),
        })
    promo = dict(sorted(promo.items(), key=lambda x: week_sort_key(x[0])))
    data["promo"] = promo

    promo_unique = {}
    for _, r in promo_unique_df.iterrows():
        week = str(r["order_week"])
        promo_unique[week] = to_native(r["unique_promo_orders"])
    data["promo_unique"] = promo_unique

    return data


# ── HTML ─────────────────────────────────────────────────────────────────

def generate_html(data, generated_at):
    return f"""\
<!DOCTYPE html>
<html lang="uk">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Кулиничі | тижневий звіт</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
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
#city-filter{{padding:8px 14px;border:1px solid var(--border);border-radius:8px;font-size:13px;font-family:inherit;background:var(--card);cursor:pointer;min-width:180px}}
#city-filter:focus{{outline:none;border-color:var(--orange)}}
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

.avail-cell{{text-align:center;font-size:12px;font-weight:600;min-width:56px}}
.avail-high{{background:rgba(16,185,129,.12);color:#059669}}
.avail-med{{background:rgba(245,158,11,.12);color:#D97706}}
.avail-low{{background:rgba(239,68,68,.12);color:#DC2626}}
.avail-na{{color:var(--text2);font-size:11px}}
.avail-legend{{display:flex;gap:16px;margin-bottom:12px;font-size:12px;align-items:center}}
.avail-legend span{{display:inline-flex;align-items:center;gap:4px}}
.avail-legend .dot{{width:10px;height:10px;border-radius:3px}}
.offline-status{{display:inline-flex;align-items:center;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:600}}
.offline-status.active{{background:rgba(16,185,129,.1);color:#059669}}
.offline-status.inactive{{background:rgba(239,68,68,.1);color:#DC2626}}

body.dark{{--bg:#111827;--card:#1F2937;--text:#F9FAFB;--text2:#9CA3AF;--border:#374151;--shadow:0 1px 3px rgba(0,0,0,.3)}}
body.dark .header{{background:var(--card)}}
body.dark .main-nav{{background:var(--card)}}
body.dark .week-bar{{background:var(--card)}}
body.dark .data-table th{{background:#111827}}
body.dark #city-filter{{background:var(--card);color:var(--text);border-color:var(--border)}}
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

<div class="header">
 <div class="header-left">
 <span class="brand-dot"></span>
 <h1>Кулиничі | тижневий звіт</h1>
 </div>
 <div class="header-right">
 <select id="city-filter"><option value="__all__">Всі міста</option></select>
 <button class="theme-toggle" id="theme-toggle" onclick="toggleDark()">🌙</button>
 <span class="last-update">Оновлено: {generated_at}</span>
 </div>
</div>

<nav class="main-nav">
 <a href="#sec-overview" class="nav-link active">Огляд</a>
 <a href="#sec-orders" class="nav-link">Замовлення</a>
 <a href="#sec-ops" class="nav-link">Операції</a>
 <a href="#sec-stores" class="nav-link">Деталі закладів</a>
 <a href="#sec-availability" class="nav-link">Доступність</a>
 <a href="#sec-revenue" class="nav-link">Дохідність</a>
 <a href="#sec-orders-detail" class="nav-link">Деталі замовлень</a>
 <a href="#sec-complaints" class="nav-link">Скарги</a>
 <a href="#sec-cancelled" class="nav-link">Скасовані</a>
 <a href="#sec-promo" class="nav-link">Промо</a>
 <a href="#sec-items" class="nav-link">Топ позиції</a>
</nav>

<div id="week-bar" class="week-bar">
 <span class="week-bar-label">Тиждень:</span>
</div>

<div class="main-content">
 <div class="section" id="sec-overview">
 <div id="kpi-grid" class="kpi-grid"></div>
 <div id="insight-orders" class="section-insight"></div>
 </div>

 <div class="section" id="sec-orders">
 <div class="section-title"><span class="section-icon">📦</span> Замовлення</div>
 <div class="charts-grid">
 <div class="chart-card"><h3>Замовлення по тижнях</h3><div class="chart-wrap"><canvas id="chart-orders"></canvas></div></div>
 <div class="chart-card"><h3>Середній чек (₴) по тижнях</h3><div class="chart-wrap"><canvas id="chart-avg-check"></canvas></div></div>
 </div>
 </div>

 <div class="section" id="sec-ops">
 <div class="section-title"><span class="section-icon">⚙️</span> Операційні показники</div>
 <div id="insight-ops" class="section-insight"></div>
 <div class="charts-grid">
 <div class="chart-card"><h3>Доступність та Прийняття (%)</h3><div class="chart-wrap"><canvas id="chart-ops-rates"></canvas></div></div>
 <div class="chart-card"><h3>Рівень поганих замовлень (%)</h3><div class="chart-wrap"><canvas id="chart-bad-orders"></canvas></div></div>
 </div>
 </div>

 <div class="section" id="sec-stores">
 <div class="section-title"><span class="section-icon">🏪</span> Деталі по закладах <span id="stores-week-label" style="font-size:13px;font-weight:400;color:var(--text2)"></span></div>
 <div id="stores-table-wrap"></div>
 </div>

 <div class="section" id="sec-availability">
 <div class="section-title"><span class="section-icon">📡</span> Доступність по днях <span id="avail-week-label" style="font-size:13px;font-weight:400;color:var(--text2)"></span></div>
 <div class="avail-legend">
  <span><span class="dot" style="background:#059669"></span> ≥90%</span>
  <span><span class="dot" style="background:#D97706"></span> 70–89%</span>
  <span><span class="dot" style="background:#DC2626"></span> &lt;70%</span>
  <span style="color:var(--text2);font-size:11px;margin-left:8px">Активних год / Робочих год • (Неактивних год)</span>
 </div>
 <div id="avail-daily-wrap" class="table-wrap" style="margin-bottom:24px"></div>
 <div class="section-title" style="margin-top:16px"><span class="section-icon">🔌</span> Лог офлайн подій <span id="offline-log-count" style="font-size:13px;font-weight:400;color:var(--text2)"></span></div>
 <div class="store-filter-wrap">
  <label>Заклад:</label>
  <select id="avail-store-filter"><option value="__all__">Всі заклади</option></select>
 </div>
 <div id="avail-log-wrap" class="table-wrap"><div class="scroll-table" id="avail-log-scroll"></div></div>
 </div>

 <div class="section" id="sec-revenue">
 <div class="section-title"><span class="section-icon">💰</span> Дохідність по тижнях</div>
 <div class="charts-grid">
 <div class="chart-card"><h3>Дохід по тижнях (₴)</h3><div class="chart-wrap"><canvas id="chart-revenue"></canvas></div></div>
 <div class="chart-card"><h3>Замовлення — деталі по закладах</h3><div id="revenue-summary" style="overflow:auto;max-height:340px"></div></div>
 </div>
 </div>

 <div class="section" id="sec-orders-detail">
 <div class="section-title"><span class="section-icon">🧾</span> Дохідність по замовленнях <span id="orders-detail-week-label" style="font-size:13px;font-weight:400;color:var(--text2)"></span></div>
 <div class="store-filter-wrap">
 <label>Заклад:</label>
 <select id="orders-store-filter"><option value="__all__">Всі заклади</option></select>
 </div>
 <div id="orders-detail-wrap" class="table-wrap"><div class="scroll-table"></div></div>
 </div>

 <div class="section" id="sec-complaints">
 <div class="section-title"><span class="section-icon">⚠️</span> Замовлення зі скаргами <span id="comp-count" style="font-size:13px;font-weight:400;color:var(--text2)"></span></div>
 <div id="complaints-wrap" class="table-wrap"><div class="scroll-table"></div></div>
 </div>

 <div class="section" id="sec-cancelled">
 <div class="section-title"><span class="section-icon">❌</span> Скасовані замовлення <span id="canc-count" style="font-size:13px;font-weight:400;color:var(--text2)"></span></div>
 <div id="cancelled-wrap" class="table-wrap"><div class="scroll-table"></div></div>
 </div>

 <div class="section" id="sec-promo">
 <div class="section-title"><span class="section-icon">🎯</span> Промо-аналітика <span id="promo-week-label" style="font-size:13px;font-weight:400;color:var(--text2)"></span></div>
 <div id="promo-kpi" class="kpi-grid" style="margin-bottom:16px"></div>
 <div id="promo-table-wrap" class="table-wrap"></div>
 </div>

 <div class="section" id="sec-items">
 <div class="section-title"><span class="section-icon">🍽️</span> Топ-10 позицій по закладах</div>
 <div class="section-insight">Найпопулярніші позиції за обраний тиждень. Кількість замовлених одиниць та виручка (₴).</div>
 <div id="items-grid" class="items-grid"></div>
 <span id="items-week-label" style="display:none"></span>
 </div>
</div>

<script>
const D = {json.dumps(data, ensure_ascii=False)};

const CITY_UA = {{"Lviv":"Львів","Rivne":"Рівне"}};
let allWeeks = Object.keys(D.weekly).sort((a,b) => {{
 const [ay,aw] = a.split('-W').map(Number);
 const [by,bw] = b.split('-W').map(Number);
 return ay !== by ? ay - by : aw - bw;
}});
let selectedWeekIdx = allWeeks.length - 1;
let selectedCity = '__all__';
let selectedOrdersStore = '__all__';
let selectedAvailStore = '__all__';
let chartInstances = {{}};

function weekSortCmp(a, b) {{
 const [ay,aw] = a.split('-W').map(Number);
 const [by,bw] = b.split('-W').map(Number);
 return ay !== by ? ay - by : aw - bw;
}}

function getSelectedWeek() {{ return allWeeks.length ? allWeeks[selectedWeekIdx >= 0 ? selectedWeekIdx : allWeeks.length - 1] : null; }}
function cityUA(c) {{ return CITY_UA[c] || c; }}

function populateCityFilter() {{
 const sel = document.getElementById('city-filter');
 sel.innerHTML = '<option value="__all__">Всі міста</option>';
 const cities = new Set(Object.values(D.stores).map(s => s.city_en));
 [...cities].sort().forEach(c => {{
 const o = document.createElement('option');
 o.value = c; o.textContent = cityUA(c);
 sel.appendChild(o);
 }});
}}

function populateOrdersStoreFilter() {{
 const sel = document.getElementById('orders-store-filter');
 sel.innerHTML = '<option value="__all__">Всі заклади</option>';
 const ids = getFilteredStoreIds();
 ids.forEach(id => {{
 const s = D.stores[id];
 if (s) {{
 const o = document.createElement('option');
 o.value = id; o.textContent = s.short + ' (' + s.city + ')';
 sel.appendChild(o);
 }}
 }});
}}

function populateAvailStoreFilter() {{
 const sel = document.getElementById('avail-store-filter');
 sel.innerHTML = '<option value="__all__">Всі заклади</option>';
 const ids = getFilteredStoreIds();
 ids.forEach(id => {{
 const s = D.stores[id];
 if (s) {{
 const o = document.createElement('option');
 o.value = id; o.textContent = s.short + ' (' + s.city + ')';
 sel.appendChild(o);
 }}
 }});
}}

function getFilteredStoreIds() {{
 if (selectedCity === '__all__') return Object.keys(D.stores).map(Number);
 return Object.entries(D.stores).filter(([_, s]) => s.city_en === selectedCity).map(([id]) => Number(id));
}}

function getFilteredWeeks() {{
 const ids = getFilteredStoreIds();
 return allWeeks.filter(w => {{
 const wd = D.weekly[w] || {{}};
 return ids.some(id => wd[id]);
 }});
}}

function getWeekDates(weekStr) {{
 const [y, w] = weekStr.split('-W').map(Number);
 const jan1 = new Date(Date.UTC(y, 0, 1));
 const jan1dow = jan1.getUTCDay() || 7;
 const mondayOfW1 = new Date(Date.UTC(y, 0, 1 + (1 - jan1dow)));
 if (jan1dow > 4) mondayOfW1.setUTCDate(mondayOfW1.getUTCDate() + 7);
 const monday = new Date(mondayOfW1);
 monday.setUTCDate(mondayOfW1.getUTCDate() + (w - 1) * 7);
 const dates = [];
 for (let i = 0; i < 7; i++) {{
  const dt = new Date(monday);
  dt.setUTCDate(monday.getUTCDate() + i);
  dates.push(dt.toISOString().slice(0, 10));
 }}
 return dates;
}}

function populateWeekBar() {{
 const bar = document.getElementById('week-bar');
 let html = '<span class="week-bar-label">Тиждень:</span>';
 const weeks = getFilteredWeeks();
 weeks.forEach(w => {{
 const idx = allWeeks.indexOf(w);
 html += '<span class="week-pill' + (idx === selectedWeekIdx ? ' active' : '') + '" data-idx="' + idx + '">' + w + '</span>';
 }});
 bar.innerHTML = html;
 bar.querySelectorAll('.week-pill').forEach(pill => {{
 pill.addEventListener('click', () => {{
 selectedWeekIdx = parseInt(pill.dataset.idx);
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
 if (!prev || prev === 0) return {{ cls: 'neutral', text: '— WoW' }};
 const chg = ((cur - prev) / Math.abs(prev)) * 100;
 const good = (dir === 'up' && chg > 0) || (dir === 'down' && chg < 0);
 const bad = (dir === 'up' && chg < 0) || (dir === 'down' && chg > 0);
 const cls = good ? 'up' : bad ? 'down' : 'neutral';
 const arrow = chg > 0 ? '↑' : chg < 0 ? '↓' : '';
 return {{ cls, text: arrow + ' ' + Math.abs(chg).toFixed(1) + '% WoW' }};
}}

function renderKPIs() {{
 const ids = getFilteredStoreIds();
 const selW = getSelectedWeek();
 const prevIdx = allWeeks.indexOf(selW) - 1;
 const prevW = prevIdx >= 0 ? allWeeks[prevIdx] : null;
 const wd = D.weekly[selW] || {{}};
 const pd = prevW ? (D.weekly[prevW] || {{}}) : {{}};

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

 let avgAvail = 0, avgAccept = 0, aCnt = 0;
 ids.forEach(id => {{
 const lo = D.latest_ops[id];
 if (lo) {{ avgAvail += lo.availability; avgAccept += lo.acceptance; aCnt++; }}
 }});
 if (aCnt > 0) {{ avgAvail /= aCnt; avgAccept /= aCnt; }}

 const kpis = [
 {{ label: 'Замовлення', value: curOrders.toLocaleString('uk-UA'), ...wow(curOrders, prevOrders, 'up') }},
 {{ label: 'Середній чек', value: '₴' + avgChk.toFixed(0), ...wow(avgChk, prevAvgChk, 'up') }},
 {{ label: 'Час приготування', value: avgCook.toFixed(1) + ' хв', ...wow(avgCook, prevAvgCook, 'down') }},
 {{ label: 'Доступність', value: avgAvail.toFixed(1) + '%', cls: avgAvail >= 90 ? 'up' : 'down', text: 'середнє по закладах' }},
 {{ label: 'Прийняття', value: avgAccept.toFixed(1) + '%', cls: avgAccept >= 90 ? 'up' : 'down', text: 'середнє по закладах' }},
 {{ label: 'Погані замовлення', value: badRate.toFixed(1) + '%', ...wow(badRate, prevBadRate, 'down') }},
 {{ label: 'Активних закладів', value: storeCount, cls: 'neutral', text: 'за обраний тиждень' }},
 ];

 document.getElementById('kpi-grid').innerHTML = kpis.map(k =>
 '<div class="kpi-card"><div class="kpi-label">' + k.label + '</div><div class="kpi-value">' + k.value + '</div>' +
 '<div class="kpi-change ' + k.cls + '">' + k.text + '</div></div>'
 ).join('');
}}

function renderOrdersCharts() {{
 const ids = getFilteredStoreIds();
 const weeks = getFilteredWeeks();

 destroyChart('chart-orders');
 const ordersData = weeks.map(w => {{
 const wd = D.weekly[w] || {{}};
 return ids.reduce((s, id) => s + (wd[id] ? wd[id].orders : 0), 0);
 }});
 chartInstances['chart-orders'] = new Chart(document.getElementById('chart-orders'), {{
 type: 'bar',
 data: {{ labels: weeks, datasets: [{{ label: 'Замовлення', data: ordersData, backgroundColor: 'rgba(249,115,22,.7)', borderColor: '#F97316', borderWidth: 1, borderRadius: 6, barPercentage: .6 }}] }},
 options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }}, scales: {{ y: {{ beginAtZero: true, grid: {{ color: 'rgba(0,0,0,.05)' }} }}, x: {{ grid: {{ display: false }} }} }} }}
 }});

 destroyChart('chart-avg-check');
 const checkData = weeks.map(w => {{
 const wd = D.weekly[w] || {{}};
 let sum = 0, cnt = 0;
 ids.forEach(id => {{ if (wd[id]) {{ sum += wd[id].avg_check * wd[id].orders; cnt += wd[id].orders; }} }});
 return cnt > 0 ? Math.round(sum / cnt) : 0;
 }});
 chartInstances['chart-avg-check'] = new Chart(document.getElementById('chart-avg-check'), {{
 type: 'line',
 data: {{ labels: weeks, datasets: [{{ label: 'Середній чек', data: checkData, borderColor: '#3B82F6', backgroundColor: 'rgba(59,130,246,.08)', fill: true, tension: .35, pointRadius: 4, pointBackgroundColor: '#3B82F6', borderWidth: 2.5 }}] }},
 options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }}, scales: {{ y: {{ beginAtZero: false, grid: {{ color: 'rgba(0,0,0,.05)' }}, ticks: {{ callback: v => '₴' + v }} }}, x: {{ grid: {{ display: false }} }} }} }}
 }});
}}

function renderOpsCharts() {{
 const ids = getFilteredStoreIds();
 const weeks = getFilteredWeeks();

 const avgByWeek = (field) => weeks.map(w => {{
 const ow = D.ops_weekly[w] || {{}};
 let sum = 0, cnt = 0;
 ids.forEach(id => {{ if (ow[id]) {{ sum += ow[id][field]; cnt++; }} }});
 return cnt > 0 ? +(sum / cnt).toFixed(1) : null;
 }});

 const badRates = weeks.map(w => {{
 const wd = D.weekly[w] || {{}};
 let ord = 0, bad = 0;
 ids.forEach(id => {{ if (wd[id]) {{ ord += wd[id].orders; bad += wd[id].bad_orders; }} }});
 return ord > 0 ? +(bad / ord * 100).toFixed(1) : 0;
 }});

 destroyChart('chart-ops-rates');
 chartInstances['chart-ops-rates'] = new Chart(document.getElementById('chart-ops-rates'), {{
 type: 'line',
 data: {{ labels: weeks, datasets: [
 {{ label: 'Доступність', data: avgByWeek('availability'), borderColor: '#F97316', backgroundColor: 'rgba(249,115,22,.08)', fill: true, tension: .35, pointRadius: 4, pointBackgroundColor: '#F97316', borderWidth: 2.5 }},
 {{ label: 'Прийняття', data: avgByWeek('acceptance'), borderColor: '#3B82F6', backgroundColor: 'rgba(59,130,246,.06)', fill: true, tension: .35, pointRadius: 4, pointBackgroundColor: '#3B82F6', borderWidth: 2.5 }}
 ] }},
 options: {{ responsive: true, maintainAspectRatio: false,
 plugins: {{ legend: {{ position: 'bottom', labels: {{ usePointStyle: true, padding: 12, font: {{ size: 11 }} }} }} }},
 scales: {{ y: {{ beginAtZero: true, max: 100, grid: {{ color: 'rgba(0,0,0,.05)' }}, ticks: {{ callback: v => v + '%' }} }}, x: {{ grid: {{ display: false }} }} }} }}
 }});

 destroyChart('chart-bad-orders');
 chartInstances['chart-bad-orders'] = new Chart(document.getElementById('chart-bad-orders'), {{
 type: 'line',
 data: {{ labels: weeks, datasets: [
 {{ label: 'Погані замовлення', data: badRates, borderColor: '#EF4444', backgroundColor: 'rgba(239,68,68,.06)', fill: true, tension: .35, pointRadius: 4, pointBackgroundColor: '#EF4444', borderWidth: 2.5 }}
 ] }},
 options: {{ responsive: true, maintainAspectRatio: false,
 plugins: {{ legend: {{ display: false }} }},
 scales: {{ y: {{ beginAtZero: true, grid: {{ color: 'rgba(0,0,0,.05)' }}, ticks: {{ callback: v => v + '%' }} }}, x: {{ grid: {{ display: false }} }} }} }}
 }});
}}

function renderInsights() {{
 const ids = getFilteredStoreIds();
 const selW = getSelectedWeek();
 const prevIdx = allWeeks.indexOf(selW) - 1;
 const prevW = prevIdx >= 0 ? allWeeks[prevIdx] : null;
 const wd = D.weekly[selW] || {{}};
 const pd = prevW ? (D.weekly[prevW] || {{}}) : {{}};

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

 let o = '<b>' + selW + '.</b> ';
 o += 'Доставлено <b>' + curOrd + '</b> замовлень (' + cs(ordChg, 'up') + ' WoW). ';
 if (ordChg != null && ordChg < -10) o += '<span class="insight-bad">Суттєве падіння замовлень.</span>';
 else if (ordChg != null && ordChg > 10) o += '<span class="insight-good">Гарне зростання!</span>';
 document.getElementById('insight-orders').innerHTML = o;

 let avgAvail = 0, avgAccept = 0, aCnt = 0, bad = 0, ord = 0;
 ids.forEach(id => {{
 const lo = D.latest_ops[id];
 if (lo) {{ avgAvail += lo.availability; avgAccept += lo.acceptance; aCnt++; }}
 if (wd[id]) {{ bad += wd[id].bad_orders; ord += wd[id].orders; }}
 }});
 if (aCnt > 0) {{ avgAvail /= aCnt; avgAccept /= aCnt; }}
 const badRate = ord > 0 ? (bad / ord * 100) : 0;

 let ops = '<b>' + selW + '.</b> ';
 ops += 'Доступність — <b>' + avgAvail.toFixed(1) + '%</b>. Прийняття — <b>' + avgAccept.toFixed(1) + '%</b>. Погані замовлення — <b>' + badRate.toFixed(1) + '%</b>. ';
 if (avgAvail < 80) ops += '<span class="insight-bad">Доступність критично низька!</span>';
 else if (avgAvail >= 95) ops += '<span class="insight-good">Відмінна доступність!</span>';
 if (badRate > 15) ops += '<span class="insight-bad">Високий рівень поганих замовлень.</span>';
 document.getElementById('insight-ops').innerHTML = ops;
}}

function renderStoresTable() {{
 const ids = getFilteredStoreIds();
 const selW = getSelectedWeek();
 const prevIdx = allWeeks.indexOf(selW) - 1;
 const prevW = prevIdx >= 0 ? allWeeks[prevIdx] : null;
 document.getElementById('stores-week-label').textContent = '— ' + selW + (prevW ? ' (WoW до ' + prevW + ')' : '');
 const wd = D.weekly[selW] || {{}};
 const pd = prevW ? (D.weekly[prevW] || {{}}) : {{}};

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
 return ' <span style="font-size:10px;padding:1px 5px;border-radius:8px;background:' + bg + ';color:' + color + '">' + arrow + Math.abs(chg).toFixed(0) + '%</span>';
 }}

 let t = '<div class="table-wrap"><table class="data-table"><thead><tr><th>#</th><th>Заклад</th><th>Місто</th><th class="text-right">Замовлення</th><th class="text-right">Сер. чек</th><th class="text-right">Час приг.</th><th class="text-right">Доступність</th><th class="text-right">Прийняття</th><th class="text-right">Фото</th><th class="text-right">Погані зам.</th></tr></thead><tbody>';
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
 t += '<td class="text-right">' + badRate.toFixed(1) + '%</td>';
 }});
 const totalBadRate = totOrd > 0 ? (totBad / totOrd * 100) : 0;
 t += '<tr class="total-row"><td colspan="3">Всього</td><td class="text-right">' + totOrd + '</td><td colspan="5"></td><td class="text-right">' + totalBadRate.toFixed(1) + '%</td></tr>';
 t += '</tbody></table></div>';
 document.getElementById('stores-table-wrap').innerHTML = t;
}}

function renderDailyAvailability() {{
 const ids = getFilteredStoreIds();
 const selW = getSelectedWeek();
 document.getElementById('avail-week-label').textContent = '— ' + selW;
 const dates = getWeekDates(selW);
 const dayNames = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Нд'];

 let t = '<table class="data-table"><thead><tr><th>Заклад</th><th>Місто</th>';
 dates.forEach((d, i) => {{
  t += '<th class="text-center" style="min-width:90px">' + dayNames[i] + '<br><span style="font-size:10px;color:var(--text2)">' + d.slice(5) + '</span></th>';
 }});
 t += '<th class="text-center" style="min-width:70px">Середнє</th></tr></thead><tbody>';

 ids.forEach(id => {{
  const s = D.stores[id];
  if (!s) return;
  t += '<tr><td>' + s.short + '</td><td>' + s.city + '</td>';
  let totalPct = 0, totalDays = 0;
  dates.forEach(d => {{
   const da = D.daily_avail[d];
   const v = da ? da[id] : null;
   if (v) {{
    const pct = v.pct;
    const cls = pct >= 90 ? 'avail-high' : pct >= 70 ? 'avail-med' : 'avail-low';
    const activeH = Math.round(v.active / 60 * 10) / 10;
    const workH = Math.round(v.working / 60 * 10) / 10;
    const inactiveH = Math.round(v.inactive / 60 * 10) / 10;
    t += '<td class="avail-cell ' + cls + '">' + pct + '%<br><span style="font-size:10px;font-weight:400">' + activeH + '/' + workH + 'г';
    if (inactiveH > 0) t += ' <span style="color:var(--neg)">(' + inactiveH + 'г)</span>';
    t += '</span></td>';
    totalPct += pct; totalDays++;
   }} else {{
    t += '<td class="avail-cell avail-na">—</td>';
   }}
  }});
  const avg = totalDays > 0 ? (totalPct / totalDays) : 0;
  const avgCls = avg >= 90 ? 'avail-high' : avg >= 70 ? 'avail-med' : 'avail-low';
  t += '<td class="avail-cell ' + (totalDays > 0 ? avgCls : 'avail-na') + '">' + (totalDays > 0 ? avg.toFixed(1) + '%' : '—') + '</td>';
  t += '</tr>';
 }});

 t += '</tbody></table>';
 document.getElementById('avail-daily-wrap').innerHTML = t;
}}

function renderAvailLog() {{
 const ids = getFilteredStoreIds();
 const selW = getSelectedWeek();
 const dates = getWeekDates(selW);
 const startDate = dates[0];
 const endDate = dates[6];

 let rows = (D.avail_events || []).filter(r => {{
  const d = r.ts.slice(0, 10);
  return d >= startDate && d <= endDate && ids.includes(r.pid);
 }});
 if (selectedAvailStore !== '__all__') {{
  rows = rows.filter(r => r.pid === Number(selectedAvailStore));
 }}

 document.getElementById('offline-log-count').textContent = '(' + rows.length + ' подій за ' + selW + ')';

 let t = '<table class="data-table"><thead><tr><th>Час</th><th>Заклад</th><th>Статус</th><th>Причина</th><th>Деталі офлайну</th></tr></thead><tbody>';
 if (rows.length === 0) {{
  t += '<tr><td colspan="5" class="text-center" style="padding:24px;color:var(--text2)">Немає подій за цей тиждень</td></tr>';
 }} else {{
  rows.forEach(r => {{
   const time = r.ts.slice(0, 16).replace('T', ' ');
   const statusCls = r.status === 'active' ? 'active' : 'inactive';
   const statusLabel = r.status === 'active' ? 'Онлайн' : 'Офлайн';
   t += '<tr><td>' + time + '</td><td>' + r.short + '</td>';
   t += '<td><span class="offline-status ' + statusCls + '">' + statusLabel + '</span></td>';
   t += '<td>' + (r.reason || '') + '</td>';
   t += '<td>' + (r.offline_reason || '—') + '</td></tr>';
  }});
 }}
 t += '</tbody></table>';
 document.getElementById('avail-log-scroll').innerHTML = t;
}}

function renderRevenueChart() {{
 const ids = getFilteredStoreIds();
 const weeks = getFilteredWeeks();
 const selW = getSelectedWeek();

 destroyChart('chart-revenue');
 const foodData = weeks.map(w => {{
 const rw = D.revenue[w] || {{}};
 return ids.reduce((s, id) => s + ((rw[id] && rw[id].food_revenue) || 0), 0);
 }});
 const feeData = weeks.map(w => {{
 const rw = D.revenue[w] || {{}};
 return ids.reduce((s, id) => s + ((rw[id] && rw[id].total_fee_gross) || 0), 0);
 }});
 const netData = weeks.map(w => {{
 const rw = D.revenue[w] || {{}};
 return ids.reduce((s, id) => s + ((rw[id] && rw[id].net_income) || 0), 0);
 }});

 chartInstances['chart-revenue'] = new Chart(document.getElementById('chart-revenue'), {{
 type: 'bar',
 data: {{
 labels: weeks,
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
 const rw = D.revenue[selW] || {{}};
 let tOrd = 0, tFood = 0, tFee = 0, tRef = 0, tNet = 0;
 ids.filter(id => rw[id]).sort((a, b) => (rw[b].net_income || 0) - (rw[a].net_income || 0)).forEach(id => {{
 const r = rw[id];
 const s = D.stores[id];
 tOrd += r.orders || 0; tFood += r.food_revenue || 0; tFee += r.total_fee_gross || 0; tRef += r.refund || 0; tNet += r.net_income || 0;
 sumHtml += '<tr><td>' + (s ? s.short : id) + '</td><td class="text-right">' + (r.orders || 0) + '</td><td class="text-right">₴' + (r.food_revenue || 0).toLocaleString('uk-UA') + '</td><td class="text-right">₴' + (r.total_fee_gross || 0).toLocaleString('uk-UA') + '</td><td class="text-right">₴' + (r.refund || 0).toLocaleString('uk-UA') + '</td><td class="text-right">₴' + (r.net_income || 0).toLocaleString('uk-UA') + '</td></tr>';
 }});
 sumHtml += '<tr class="total-row"><td>Всього</td><td class="text-right">' + tOrd + '</td><td class="text-right">₴' + tFood.toLocaleString('uk-UA') + '</td><td class="text-right">₴' + tFee.toLocaleString('uk-UA') + '</td><td class="text-right">₴' + tRef.toLocaleString('uk-UA') + '</td><td class="text-right">₴' + tNet.toLocaleString('uk-UA') + '</td></tr>';
 sumHtml += '</tbody></table>';
 document.getElementById('revenue-summary').innerHTML = sumHtml;
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

function shortenPromo(raw) {{
 if (!raw) return '';
 return raw.split('; ').map(function(s) {{
  const m1 = s.match(/(\\d+)%\\s*%?\\s*(Menu|Item)\\s*Discount.*?(\\d+)%\\s*On Provider/i);
  if (m1) {{
   const provPct = parseInt(m1[3]);
   const boltPct = 100 - provPct;
   const type = m1[2].toLowerCase() === 'menu' ? 'меню' : 'товар';
   return m1[1] + '% ' + type + ' (' + boltPct + '/' + provPct + ')';
  }}
  if (/Free Delivery/i.test(s)) {{
   const up = s.match(/Up to (\\d+)/i);
   return 'Безк. доставка' + (up ? ' до ' + up[1] : '');
  }}
  if (/Provider Targeting/i.test(s)) {{
   if (/NEW_CITY/i.test(s)) return 'Таргетинг (нове місто)';
   if (/Signup/i.test(s)) return 'Таргетинг (signup)';
   if (/Never Activated/i.test(s)) return 'Таргетинг (реактивація)';
   if (/Engagement/i.test(s)) return 'Таргетинг (engagement)';
   return 'Таргетинг';
  }}
  return s.length > 40 ? s.substring(0, 37) + '…' : s;
 }}).join('; ');
}}

function renderOrdersDetail() {{
 const ids = getFilteredStoreIds();
 const selW = getSelectedWeek();
 document.getElementById('orders-detail-week-label').textContent = '— ' + selW;

 let rows = (D.orders || []).filter(r => r.order_week === selW && ids.includes(r.provider_id));
 if (selectedOrdersStore !== '__all__') {{
 rows = rows.filter(r => r.provider_id === Number(selectedOrdersStore));
 }}

 let t = '<div class="scroll-table"><table class="data-table"><thead><tr>';
 t += '<th>Дата</th><th>Order Ref</th><th>Заклад</th><th>Bolt+</th>';
 t += '<th class="text-right">Ціна до знижки</th><th>Знижка (за чий рахунок)</th>';
 t += '<th class="text-right">Дохід від їжі</th><th>Комісія (нетто+ПДВ=брутто)</th>';
 t += '<th class="text-right">Компенсація Bolt</th><th class="text-right">Всього комісія</th>';
 t += '<th class="text-right">Повернення</th><th class="text-right">Чистий дохід</th>';
 t += '<th>Промо</th><th class="text-right">Знижка промо</th><th class="text-right">Bolt оплатив</th><th class="text-right">Заклад оплатив</th>';
 t += '</tr></thead><tbody>';

 let totFood = 0, totRev = 0, totFee = 0, totBoltComp = 0, totRef = 0, totNet = 0;
 rows.forEach(r => {{
 const date = r.order_created_date ? String(r.order_created_date).substring(0, 10) : '';
 totFood += r.food_before_discount || 0;
 totRev += r.food_revenue || 0;
 totFee += r.total_fee_gross || 0;
 totBoltComp += r.bolt_discount || 0;
 totRef += r.refund || 0;
 totNet += r.net_income || 0;

 const feeNet = r.fee_net || 0;
 const feeGross = r.fee_gross || 0;
 const boltComp = r.bolt_discount || 0;
 const boltCompText = boltComp > 0.5 ? '+' + boltComp.toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) : '—';

 const nc = (r.net_income || 0) < 0 ? ' style="color:var(--neg)"' : '';
 t += '<tr><td>' + date + '</td>';
 t += '<td>' + (r.order_reference_id || '') + '</td>';
 t += '<td>' + (r.provider_short || '') + '</td>';
 t += '<td>Ні</td>';
 t += '<td class="text-right">' + (r.food_before_discount || 0).toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) + '</td>';
 t += '<td>' + fmtDiscount(r) + '</td>';
 t += '<td class="text-right">' + (r.food_revenue || 0).toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) + '</td>';
 t += '<td>' + fmtFee(feeNet, feeGross) + '</td>';
 t += '<td class="text-right" style="color:var(--pos)">' + boltCompText + '</td>';
 t += '<td class="text-right">' + (r.total_fee_gross || 0).toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) + '</td>';
 t += '<td class="text-right">' + (r.refund || 0).toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) + '</td>';
 t += '<td class="text-right"' + nc + '>' + (r.net_income || 0).toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) + '</td>';
 const promoName = r.promo_names || '';
 const promoShort = shortenPromo(promoName);
 t += '<td style="max-width:220px;white-space:normal;font-size:0.82em" title="' + promoName.replace(/"/g, '&quot;') + '">' + (promoShort || '—') + '</td>';
 const pDisc = r.promo_total_discount || 0;
 const pBolt = r.promo_bolt_spend || 0;
 const pProv = r.promo_provider_spend || 0;
 if (pDisc > 0) {{
   t += '<td class="text-right">' + pDisc.toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) + '</td>';
   t += '<td class="text-right">' + pBolt.toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) + '</td>';
   t += '<td class="text-right">' + pProv.toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) + '</td>';
 }} else {{
   t += '<td class="text-right">—</td><td class="text-right">—</td><td class="text-right">—</td>';
 }}
 }});

 t += '<tr class="total-row"><td colspan="3">Всього (' + rows.length + ' зам.)</td><td></td>';
 t += '<td class="text-right">' + totFood.toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) + '</td>';
 t += '<td></td>';
 t += '<td class="text-right">' + totRev.toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) + '</td>';
 t += '<td></td>';
 t += '<td class="text-right" style="color:var(--pos)">+' + totBoltComp.toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) + '</td>';
 t += '<td class="text-right">' + totFee.toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) + '</td>';
 t += '<td class="text-right">' + totRef.toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) + '</td>';
 t += '<td class="text-right">' + totNet.toLocaleString('uk-UA', {{minimumFractionDigits:2, maximumFractionDigits:2}}) + '</td>';
 t += '<td></td><td></td><td></td><td></td>';
 t += '</tr></tbody></table></div>';
 document.getElementById('orders-detail-wrap').innerHTML = t;
}}

function renderComplaints() {{
 const ids = getFilteredStoreIds();
 const selW = getSelectedWeek();
 const rows = (D.complaints || []).filter(r => r.order_week === selW && ids.includes(r.provider_id));
 document.getElementById('comp-count').textContent = '(' + rows.length + ' за ' + selW + ')';

 let t = '<div class="scroll-table"><table class="data-table"><thead><tr><th>Дата</th><th>Order Ref</th><th>Заклад</th><th class="text-right">Сума</th><th>Тип проблеми</th><th>Винний</th><th>Рейтинг</th><th>Коментар</th></tr></thead><tbody>';
 if (rows.length === 0) {{
 t += '<tr><td colspan="8" class="text-center" style="padding:24px;color:var(--text2)">Немає скарг за цей тиждень</td></tr>';
 }} else {{
 rows.forEach(r => {{
 const date = r.order_created_date ? String(r.order_created_date).substring(0, 10) : '';
 const comment = (r.provider_rating_comment || '').substring(0, 120);
 t += '<tr><td>' + date + '</td><td>' + (r.order_reference_id || '') + '</td><td>' + (r.provider_short || '') + '</td>';
 t += '<td class="text-right">₴' + (r.sum_uah || 0) + '</td>';
 t += '<td>' + (r.bad_order_type || '') + '</td>';
 t += '<td>' + (r.fault || '') + '</td>';
 t += '<td class="text-center">' + (r.rating != null ? r.rating : '—') + '</td>';
 t += '<td class="comment-cell">' + comment + '</td>';
 }});
 }}
 t += '</tbody></table></div>';
 document.getElementById('complaints-wrap').innerHTML = t;
}}

function renderCancelled() {{
 const ids = getFilteredStoreIds();
 const selW = getSelectedWeek();
 const rows = (D.cancelled || []).filter(r => r.order_week === selW && ids.includes(r.provider_id));
 document.getElementById('canc-count').textContent = '(' + rows.length + ' за ' + selW + ')';

 let t = '<div class="scroll-table"><table class="data-table"><thead><tr><th>Дата</th><th>Order Ref</th><th>Заклад</th><th>Статус</th><th>Причина</th><th>Коментар</th></tr></thead><tbody>';
 if (rows.length === 0) {{
 t += '<tr><td colspan="6" class="text-center" style="padding:24px;color:var(--text2)">Немає скасованих за цей тиждень</td></tr>';
 }} else {{
 rows.forEach(r => {{
 const date = r.order_created_date ? String(r.order_created_date).substring(0, 10) : '';
 const comment = (r.comment || '').substring(0, 150);
 t += '<tr><td>' + date + '</td><td>' + (r.order_reference_id || '') + '</td><td>' + (r.provider_short || '') + '</td>';
 t += '<td>' + (r.order_state || '') + '</td>';
 t += '<td>' + (r.reason || '') + '</td>';
 t += '<td class="comment-cell">' + comment + '</td>';
 }});
 }}
 t += '</tbody></table></div>';
 document.getElementById('cancelled-wrap').innerHTML = t;
}}

function renderPromo() {{
 const ids = getFilteredStoreIds();
 const selW = getSelectedWeek();
 document.getElementById('promo-week-label').textContent = '— ' + selW;

 const promoWeek = D.promo[selW] || [];
 const filteredPromo = promoWeek.filter(p => ids.includes(p.pid));

 const wd = D.weekly[selW] || {{}};
 let totalOrders = 0;
 ids.forEach(id => {{ if (wd[id]) totalOrders += wd[id].orders; }});

 const uniquePromoOrders = D.promo_unique[selW] || 0;
 const promoRate = totalOrders > 0 ? (uniquePromoOrders / totalOrders * 100) : 0;
 let totalDiscount = 0, totalBolt = 0, totalProv = 0;
 filteredPromo.forEach(p => {{
  totalDiscount += p.discount || 0;
  totalBolt += p.bolt_spend || 0;
  totalProv += p.provider_spend || 0;
 }});

 const kpis = [
  {{ label: 'Замовлення з промо', value: uniquePromoOrders + ' / ' + totalOrders, cls: 'neutral', text: promoRate.toFixed(1) + '% від усіх' }},
  {{ label: 'Загальна знижка', value: '₴' + totalDiscount.toLocaleString('uk-UA'), cls: 'neutral', text: 'за тиждень' }},
  {{ label: 'Витрати Bolt', value: '₴' + totalBolt.toLocaleString('uk-UA'), cls: 'neutral', text: 'за рахунок Bolt' }},
  {{ label: 'Витрати закладу', value: '₴' + totalProv.toLocaleString('uk-UA'), cls: totalProv > 0 ? 'down' : 'neutral', text: 'за рахунок закладу' }},
 ];

 document.getElementById('promo-kpi').innerHTML = kpis.map(k =>
  '<div class="kpi-card"><div class="kpi-label">' + k.label + '</div><div class="kpi-value">' + k.value + '</div>' +
  '<div class="kpi-change ' + k.cls + '">' + k.text + '</div></div>'
 ).join('');

 let t = '<table class="data-table"><thead><tr><th>Назва промо</th><th>Тип</th><th class="text-right">Замовлення</th><th class="text-right">% від усіх</th><th class="text-right">Знижка (₴)</th><th class="text-right">Bolt (₴)</th><th class="text-right">Заклад (₴)</th></tr></thead><tbody>';
 if (filteredPromo.length === 0) {{
  t += '<tr><td colspan="7" class="text-center" style="padding:24px;color:var(--text2)">Немає промо за цей тиждень</td></tr>';
 }} else {{
  const grouped = {{}};
  filteredPromo.forEach(p => {{
   const key = p.name;
   if (!grouped[key]) grouped[key] = {{ name: p.name, type: p.type, orders: 0, discount: 0, bolt: 0, prov: 0 }};
   grouped[key].orders += p.orders;
   grouped[key].discount += p.discount || 0;
   grouped[key].bolt += p.bolt_spend || 0;
   grouped[key].prov += p.provider_spend || 0;
  }});
  const sorted = Object.values(grouped).sort((a, b) => b.orders - a.orders);
  sorted.forEach(p => {{
   const pct = totalOrders > 0 ? (p.orders / totalOrders * 100).toFixed(1) : '0.0';
   t += '<tr><td style="white-space:normal;max-width:350px;word-break:break-word">' + p.name + '</td>';
   t += '<td>' + p.type + '</td>';
   t += '<td class="text-right">' + p.orders + '</td>';
   t += '<td class="text-right">' + pct + '%</td>';
   t += '<td class="text-right">₴' + p.discount.toLocaleString('uk-UA') + '</td>';
   t += '<td class="text-right">₴' + p.bolt.toLocaleString('uk-UA') + '</td>';
   t += '<td class="text-right">₴' + p.prov.toLocaleString('uk-UA') + '</td></tr>';
  }});
  t += '<tr class="total-row"><td colspan="2">Всього (унікальних)</td>';
  t += '<td class="text-right">' + uniquePromoOrders + '</td>';
  t += '<td class="text-right">' + promoRate.toFixed(1) + '%</td>';
  t += '<td class="text-right">₴' + totalDiscount.toLocaleString('uk-UA') + '</td>';
  t += '<td class="text-right">₴' + totalBolt.toLocaleString('uk-UA') + '</td>';
  t += '<td class="text-right">₴' + totalProv.toLocaleString('uk-UA') + '</td></tr>';
 }}
 t += '</tbody></table>';
 document.getElementById('promo-table-wrap').innerHTML = t;
}}

function renderTopItems() {{
 const ids = getFilteredStoreIds();
 const selW = getSelectedWeek();
 const weekItems = D.top_items[selW] || {{}};
 document.getElementById('items-week-label').textContent = '— ' + selW;
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
 document.getElementById('items-grid').innerHTML = html || '<div style="padding:24px;color:var(--text2)">Немає даних.</div>';
}}

function renderAll() {{
 renderKPIs();
 renderInsights();
 renderOrdersCharts();
 renderOpsCharts();
 renderStoresTable();
 renderDailyAvailability();
 renderAvailLog();
 renderRevenueChart();
 renderOrdersDetail();
 renderComplaints();
 renderCancelled();
 renderPromo();
 renderTopItems();
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

document.getElementById('city-filter').addEventListener('change', function() {{
 selectedCity = this.value;
 populateWeekBar();
 populateOrdersStoreFilter();
 populateAvailStoreFilter();
 renderAll();
}});

document.getElementById('orders-store-filter').addEventListener('change', function() {{
 selectedOrdersStore = this.value;
 renderOrdersDetail();
}});

document.getElementById('avail-store-filter').addEventListener('change', function() {{
 selectedAvailStore = this.value;
 renderAvailLog();
}});

window.toggleDark = function() {{
 document.body.classList.toggle('dark');
 const isDark = document.body.classList.contains('dark');
 document.getElementById('theme-toggle').textContent = isDark ? '☀️' : '🌙';
 try {{ localStorage.setItem('kulinichi-dark', isDark ? '1' : '0') }} catch(e) {{}}
 Chart.defaults.color = isDark ? '#D1D5DB' : '#374151';
 renderAll();
}};
(function() {{ try {{ if (localStorage.getItem('kulinichi-dark') === '1') {{ document.body.classList.add('dark'); document.getElementById('theme-toggle').textContent = '☀️'; Chart.defaults.color = '#D1D5DB'; }} }} catch(e) {{}} }})();

populateCityFilter();
populateOrdersStoreFilter();
populateAvailStoreFilter();
populateWeekBar();
setupNav();
renderAll();
</script>
</body>
</html>"""


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M UTC")
    print(f"Starting Кулиничі report generation at {generated_at}")

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

        print("  Fetching daily availability…")
        daily_avail_df = fetch_daily_availability(conn)
        print(f"  → {len(daily_avail_df)} rows")

        print("  Fetching availability log…")
        avail_log_df = fetch_availability_log(conn)
        print(f"  → {len(avail_log_df)} rows")

        print("  Fetching promo data…")
        promo_df = fetch_promo_data(conn)
        print(f"  → {len(promo_df)} rows")

        print("  Fetching promo unique orders…")
        promo_unique_df = fetch_promo_unique_orders(conn)
        print(f"  → {len(promo_unique_df)} rows")
    finally:
        conn.close()

    data = build_data(weekly_df, ops_df, items_df, orders_df, complaints_df, cancelled_df, revenue_df, daily_avail_df, avail_log_df, promo_df, promo_unique_df)
    html = generate_html(data, generated_at)

    out_dir = REPO_ROOT / "kulinichi"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "index.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"  Saved: {out_path}")

    update_root_index()
    print("\nDone!")


def update_root_index():
    from config import PROVIDERS

    all_partners = {}
    for pid, info in PROVIDERS.items():
        all_partners[info["slug"]] = {"name": info["name"], "city": info["city"]}
    all_partners["burek"] = {"name": "BUREK", "city": "Львів / Ужгород"}
    all_partners["kulinichi"] = {"name": "Кулиничі", "city": "Львів / Рівне"}

    cards = ""
    for slug in sorted(all_partners, key=lambda s: all_partners[s]["name"]):
        info = all_partners[slug]
        cards += f"""
    <a href="{slug}/" class="report-card">
    <h3>{info['name']}</h3>
    <p>{info['city']}</p>
    <span class="badge">Тижневий звіт</span>
    </a>"""

    html = f"""<!DOCTYPE html>
<html lang="uk">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Partner Reports</title>
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; background:#0f172a; color:#e2e8f0; min-height:100vh; display:flex; flex-direction:column; align-items:center; padding:60px 20px; }}
.logo {{ font-size:48px; margin-bottom:8px; }}
h1 {{ font-size:28px; font-weight:700; margin-bottom:6px; }}
.subtitle {{ color:#94a3b8; font-size:15px; margin-bottom:48px; }}
.reports-grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(260px,1fr)); gap:16px; width:100%; max-width:900px; }}
.report-card {{ background:#1e293b; border:1px solid #334155; border-radius:12px; padding:24px; text-decoration:none; color:inherit; transition:all 0.2s; }}
.report-card:hover {{ border-color:#34d399; transform:translateY(-2px); box-shadow:0 8px 24px rgba(52,211,153,0.1); }}
.report-card h3 {{ font-size:18px; margin-bottom:8px; }}
.report-card p {{ color:#94a3b8; font-size:13px; }}
.badge {{ display:inline-block; background:#064e3b; color:#34d399; font-size:11px; padding:3px 8px; border-radius:6px; margin-top:12px; }}
</style>
</head>
<body>
<div class="logo">📊</div>
<h1>Partner Reports</h1>
<p class="subtitle">Тижневі звіти для партнерів</p>
<div class="reports-grid">
{cards}
</div>
</body>
</html>"""
    (REPO_ROOT / "index.html").write_text(html, encoding="utf-8")
    print("  Updated root index.html")


if __name__ == "__main__":
    main()
