"""
Generate a multi-store weekly HTML report for Ваш Лаваш by querying Databricks.
Produces vash-lavash/index.html — identical structure to the BUREK dashboard.
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
WEEKS_BACK = 8

VASH_LAVASH_PROVIDERS = {
    60918:  {"name": "Ваш Лаваш вул. Костомарова", "short": "Костомарова", "city": "Lviv"},
    63369:  {"name": "Ваш Лаваш Шувар", "short": "Шувар", "city": "Lviv"},
    63550:  {"name": "Ваш Лаваш Городоцька", "short": "Городоцька", "city": "Lviv"},
    62676:  {"name": "Ваш Лаваш Великого", "short": "Великого", "city": "Lviv"},
    62203:  {"name": "Ваш Лаваш вул. Бандери", "short": "Бандери", "city": "Lviv"},
    114843: {"name": "Ваш Лаваш вул. Трудова", "short": "Трудова", "city": "Khmelnytskyi"},
    154836: {"name": "Ваш Лаваш вул. Тернопільська", "short": "Тернопільська", "city": "Khmelnytskyi"},
    117457: {"name": "Ваш Лаваш вул. Грушевського", "short": "Грушевського", "city": "Rivne"},
    176277: {"name": "Ваш Лаваш вул. Чорновола", "short": "Чорновола", "city": "Rivne"},
    117465: {"name": "Ваш Лаваш вул. Валова", "short": "Валова", "city": "Ternopil"},
    117466: {"name": "Ваш Лаваш вул. Шептицького", "short": "Шептицького", "city": "Ternopil"},
    117474: {"name": "Ваш Лаваш вул. Київська", "short": "Київська", "city": "Zhytomyr"},
    117473: {"name": "Ваш Лаваш просп. Миру 37", "short": "пр. Миру 37", "city": "Zhytomyr"},
    167163: {"name": "Ваш Лаваш просп. Шевченка", "short": "пр. Шевченка", "city": "Vyshhorod"},
    185860: {"name": "Ваш Лаваш просп. Байрона", "short": "пр. Байрона", "city": "Kharkiv"},
    185731: {"name": "Ваш Лаваш вул. В. Бердичівська", "short": "Бердичівська", "city": "Zhytomyr"},
    185738: {"name": "Ваш Лаваш вул. І. Кочерги", "short": "Кочерги", "city": "Zhytomyr"},
    185758: {"name": "Ваш Лаваш вул. Вітрука", "short": "Вітрука", "city": "Zhytomyr"},
    185759: {"name": "Ваш Лаваш просп. Миру", "short": "пр. Миру", "city": "Zhytomyr"},
    185781: {"name": "Ваш Лаваш вул. Корольова", "short": "Корольова", "city": "Zhytomyr"},
    185784: {"name": "Ваш Лаваш вул. Покровська", "short": "Покровська", "city": "Zhytomyr"},
    185658: {"name": "Ваш Лаваш вул. Домбровського", "short": "Домбровського", "city": "Zhytomyr"},
    185651: {"name": "Ваш Лаваш вул. Лесі Українки", "short": "Л. Українки", "city": "Zhytomyr"},
    185782: {"name": "Ваш Лаваш вул. Чуднівська", "short": "Чуднівська", "city": "Zhytomyr"},
    185188: {"name": "Ваш Лаваш вул. Святошинська", "short": "Святошинська", "city": "Kyiv"},
    179394: {"name": "Ваш Лаваш вул. Івана Мазепи", "short": "Мазепи", "city": "Kolomyia"},
}

PROVIDER_IDS = ",".join(str(k) for k in VASH_LAVASH_PROVIDERS)

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
      ROUND(COALESCE(m.provider_commission_net_eur * m.currency_rate, 0), 2) AS fee_net,
      ROUND(COALESCE(m.provider_commission_gross_eur * m.currency_rate, 0), 2) AS fee_gross,
      ROUND(COALESCE(f.commission_local - COALESCE(m.provider_commission_net_eur * m.currency_rate, 0), 0), 2) AS bp_fee_net,
      ROUND(COALESCE((f.commission_local - COALESCE(m.provider_commission_net_eur * m.currency_rate, 0)) * 1.2, 0), 2) AS bp_fee_gross,
      ROUND(COALESCE(f.commission_local * 1.2, 0), 2) AS total_fee_gross,
      ROUND(COALESCE(f.total_refunded_amount, 0), 2) AS refund,
      ROUND(COALESCE(f.provider_price_after_discount, 0) - COALESCE(f.commission_local * 1.2, 0) - COALESCE(f.total_refunded_amount, 0), 2) AS net_income,
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
      ROUND(SUM(f.commission_local * 1.2), 0) AS total_fee_gross,
      ROUND(SUM(COALESCE(f.total_refunded_amount, 0)), 0) AS refund,
      ROUND(SUM(f.provider_price_after_discount) - SUM(f.commission_local * 1.2) - SUM(COALESCE(f.total_refunded_amount, 0)), 0) AS net_income
    FROM ng_delivery_spark.fact_order_delivery f
    WHERE f.provider_id IN ({PROVIDER_IDS})
      AND f.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
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

def build_data(weekly_df, ops_df, items_df, orders_df, complaints_df, cancelled_df, revenue_df, campaigns_df):
    data = {}

    stores_map = {}
    for _, r in weekly_df.iterrows():
        pid = to_native(r["provider_id"])
        info = VASH_LAVASH_PROVIDERS.get(int(pid), {})
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
        raw_state = row.get("order_state", "") or ""
        row["order_state_raw"] = raw_state
        row["order_state"] = ORDER_STATE_UA.get(raw_state, raw_state)
        pid = row.get("provider_id")
        if pid and int(pid) in VASH_LAVASH_PROVIDERS:
            row["provider_short"] = VASH_LAVASH_PROVIDERS[int(pid)]["short"]
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
        if pid and int(pid) in VASH_LAVASH_PROVIDERS:
            row["provider_short"] = VASH_LAVASH_PROVIDERS[int(pid)]["short"]
        else:
            row["provider_short"] = row.get("provider_name", "")
    data["complaints"] = comp_list

    canc_list = safe_json(cancelled_df)
    for row in canc_list:
        raw_state = row.get("order_state", "") or ""
        row["order_state"] = ORDER_STATE_UA.get(raw_state, raw_state)
        pid = row.get("provider_id")
        if pid and int(pid) in VASH_LAVASH_PROVIDERS:
            row["provider_short"] = VASH_LAVASH_PROVIDERS[int(pid)]["short"]
        else:
            row["provider_short"] = row.get("provider_name", "")
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
            "net_income": to_native(r["net_income"]),
        }
    revenue = dict(sorted(revenue.items(), key=lambda x: week_sort_key(x[0])))
    data["revenue"] = revenue

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
            "provider_short": VASH_LAVASH_PROVIDERS.get(pid, {}).get("short", str(pid)),
            "order_week": str(r["order_week"]),
            "orders": to_native(r["orders"]),
            "total_discount": to_native(r["total_discount_uah"]),
            "bolt_spend": to_native(r["bolt_spend_uah"]),
            "provider_spend": to_native(r["provider_spend_uah"]),
        })
    data["campaigns"] = campaigns

    return data


# ── HTML ─────────────────────────────────────────────────────────────────

CITY_UA_JSON = json.dumps(CITY_UA, ensure_ascii=False)

def generate_html(data, generated_at):
    return f"""<!DOCTYPE html>
<html lang="uk">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ВАШ ЛАВАШ | тижневий звіт</title>
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
#city-filter,#store-filter{{padding:8px 14px;border:1px solid var(--border);border-radius:8px;font-size:13px;font-family:inherit;background:var(--card);cursor:pointer;min-width:180px}}
#city-filter:focus,#store-filter:focus{{outline:none;border-color:var(--orange)}}
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
body.dark #city-filter,body.dark #store-filter{{background:var(--card);color:var(--text);border-color:var(--border)}}
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
    <h1>ВАШ ЛАВАШ | тижневий звіт</h1>
  </div>
  <div class="header-right">
    <select id="city-filter"><option value="__all__">Всі міста</option></select>
    <select id="store-filter"><option value="__all__">Всі заклади</option></select>
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
</nav>

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
    <div class="section-insight">Найпопулярніші позиції за обраний тиждень. Кількість замовлених одиниць та виручка (₴).</div>
    <div class="items-grid" id="items-grid"></div>
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
let selectedWeekIdx = allWeeks.length - 1;
let selectedCity = '__all__';
let selectedStore = '__all__';
let selectedBP = '__all__';
let selectedState = '__all__';
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

function populateStoreFilter() {{
  const sel = document.getElementById('store-filter');
  const prev = sel.value;
  sel.innerHTML = '<option value="__all__">Всі заклади</option>';
  const cityIds = selectedCity === '__all__'
    ? Object.keys(D.stores).map(Number)
    : Object.entries(D.stores).filter(([_, s]) => s.city_en === selectedCity).map(([id]) => Number(id));
  cityIds.sort((a, b) => (D.stores[a].short || '').localeCompare(D.stores[b].short || '', 'uk')).forEach(id => {{
    const s = D.stores[id];
    if (s) {{
      const o = document.createElement('option');
      o.value = id; o.textContent = s.short + ' (' + s.city + ')';
      sel.appendChild(o);
    }}
  }});
  if ([...sel.options].some(o => o.value === prev)) sel.value = prev;
  else {{ sel.value = '__all__'; selectedStore = '__all__'; }}
}}

function getFilteredStoreIds() {{
  if (selectedStore !== '__all__') return [Number(selectedStore)];
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

function populateWeekBar() {{
  const bar = document.getElementById('week-bar');
  let html = '<div class="week-bar-label">Тиждень:</div>';
  const weeks = getFilteredWeeks();
  weeks.forEach(w => {{
    const idx = allWeeks.indexOf(w);
    html += '<div class="week-pill' + (idx === selectedWeekIdx ? ' active' : '') + '" data-idx="' + idx + '">' + w + '</div>';
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
    '<span class="kpi-change ' + k.cls + '">' + k.text + '</span></div>'
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

  let o = '<b>' + selW + '</b>. ';
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

  let ops = '<b>' + selW + '</b>. ';
  ops += 'Доступність — <b>' + avgAvail.toFixed(1) + '%</b>. Прийняття — <b>' + avgAccept.toFixed(1) + '%</b>. Погані замовлення — <b>' + badRate.toFixed(1) + '%</b>. ';
  if (avgAvail < 80) ops += '<span class="insight-bad">Доступність критично низька!</span> ';
  else if (avgAvail >= 95) ops += '<span class="insight-good">Відмінна доступність!</span> ';
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
    sumHtml += '<tr><td>' + (s ? s.short : id) + '</td><td class="text-right">' + (r.orders || 0) + '</td><td class="text-right">₴' + (r.food_revenue || 0).toLocaleString('uk-UA') + '</td><td class="text-right" style="color:var(--neg)">₴' + (r.total_fee_gross || 0).toLocaleString('uk-UA') + '</td><td class="text-right">₴' + (r.refund || 0).toLocaleString('uk-UA') + '</td><td class="text-right" style="color:var(--pos)">₴' + (r.net_income || 0).toLocaleString('uk-UA') + '</td></tr>';
  }});
  sumHtml += '<tr class="total-row"><td>Всього</td><td class="text-right">' + tOrd + '</td><td class="text-right">₴' + tFood.toLocaleString('uk-UA') + '</td><td class="text-right" style="color:var(--neg)">₴' + tFee.toLocaleString('uk-UA') + '</td><td class="text-right">₴' + tRef.toLocaleString('uk-UA') + '</td><td class="text-right" style="color:var(--pos)">₴' + tNet.toLocaleString('uk-UA') + '</td></tr>';
  sumHtml += '</tbody></table>';
  document.getElementById('revenue-summary').innerHTML = sumHtml;
}}

function renderCampaigns() {{
  const ids = getFilteredStoreIds();
  const selW = getSelectedWeek();
  document.getElementById('campaigns-week-label').textContent = '— ' + selW;

  const rows = (D.campaigns || []).filter(r => r.order_week === selW && ids.includes(r.provider_id));

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
    summaryEl.innerHTML = '<b>' + selW + '</b>. Немає активних кампаній для обраних закладів.';
  }} else {{
    summaryEl.innerHTML = '<b>' + selW + '</b>. Активних кампаній: <b>' + campList.length + '</b>. '
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
    t += '<tr><td colspan="8" style="text-align:center;color:var(--text2);padding:24px">Немає кампаній за цей тиждень</td></tr>';
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
  const selW = getSelectedWeek();
  document.getElementById('orders-detail-week-label').textContent = '— ' + selW;

  let rows = (D.orders || []).filter(r => r.order_week === selW && ids.includes(r.provider_id));
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
  const selW = getSelectedWeek();
  const rows = (D.complaints || []).filter(r => r.order_week === selW && ids.includes(r.provider_id));
  document.getElementById('comp-count').textContent = '(' + rows.length + ' за ' + selW + ')';

  let t = '<table class="data-table"><thead><tr><th>Дата</th><th>Order Ref</th><th>Заклад</th><th class="text-right">Сума</th><th>Тип проблеми</th><th>Винний</th><th class="text-center">Рейтинг</th><th>Коментар</th></tr></thead><tbody>';
  if (rows.length === 0) {{
    t += '<tr><td colspan="8" style="text-align:center;color:var(--text2);padding:24px">Немає скарг за цей тиждень</td></tr>';
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
  const selW = getSelectedWeek();
  const rows = (D.cancelled || []).filter(r => r.order_week === selW && ids.includes(r.provider_id));
  document.getElementById('canc-count').textContent = '(' + rows.length + ' за ' + selW + ')';

  let t = '<table class="data-table"><thead><tr><th>Дата</th><th>Order Ref</th><th>Заклад</th><th>Статус</th><th>Причина</th><th>Коментар</th></tr></thead><tbody>';
  if (rows.length === 0) {{
    t += '<tr><td colspan="6" style="text-align:center;color:var(--text2);padding:24px">Немає скасованих за цей тиждень</td></tr>';
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
  document.getElementById('items-grid').innerHTML = html || '<p style="color:var(--text2);padding:24px;text-align:center">Немає даних.</p>';
}}

function renderAll() {{
  renderKPIs();
  renderInsights();
  renderOrdersCharts();
  renderOpsCharts();
  renderStoresTable();
  renderRevenueChart();
  renderCampaigns();
  renderOrdersDetail();
  renderComplaints();
  renderCancelled();
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
  selectedStore = '__all__';
  document.getElementById('store-filter').value = '__all__';
  populateStoreFilter();
  populateWeekBar();
  renderAll();
}});

document.getElementById('store-filter').addEventListener('change', function() {{
  selectedStore = this.value;
  populateWeekBar();
  renderAll();
}});

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
  try {{ localStorage.setItem('vash-lavash-dark', isDark ? '1' : '0') }} catch(e) {{}}
  Chart.defaults.color = isDark ? '#D1D5DB' : '#374151';
  renderAll();
}};
(function() {{ try {{ if (localStorage.getItem('vash-lavash-dark') === '1') {{ document.body.classList.add('dark'); document.getElementById('theme-toggle').textContent = '☀️'; Chart.defaults.color = '#D1D5DB'; }} }} catch(e) {{}} }})();

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
    print(f"Starting Ваш Лаваш report generation at {generated_at}")

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
    finally:
        conn.close()

    data = build_data(weekly_df, ops_df, items_df, orders_df, complaints_df, cancelled_df, revenue_df, campaigns_df)
    html = generate_html(data, generated_at)

    out_dir = REPO_ROOT / "vash-lavash"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "index.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"  Saved: {out_path}")
    print("\nDone!")


if __name__ == "__main__":
    main()
