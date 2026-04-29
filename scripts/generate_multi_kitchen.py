"""
Generate a multi-store weekly HTML report for Multi-Kitchen partner
(ШАВУХА ТОПчик, TOP BURGER, БОРЩ УКРАЇНСЬКИЙ, Yum Yum Asia).
Produces multi-kitchen/index.html.
"""

import os
import sys
import json
import math
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal
from collections import defaultdict

sys.stdout.reconfigure(line_buffering=True)

from databricks import sql
import pandas as pd

from config import SERVER_HOSTNAME, HTTP_PATH

REPO_ROOT = Path(__file__).resolve().parent.parent
WEEKS_BACK = 16

MULTI_KITCHEN_PROVIDERS = {
    79017:  {"name": "ШАВУХА ТОПчик Хрещатик",       "short": "ШТ Хрещатик",     "city": "Kyiv", "brand": "ШАВУХА ТОПчик"},
    79018:  {"name": "ШАВУХА ТОПчик Васильківська",   "short": "ШТ Васильків.",    "city": "Kyiv", "brand": "ШАВУХА ТОПчик"},
    79019:  {"name": "ШАВУХА ТОПчик Борщагівка",      "short": "ШТ Борщагівка",    "city": "Kyiv", "brand": "ШАВУХА ТОПчик"},
    128521: {"name": "ШАВУХА ТОПчик Макіївська",      "short": "ШТ Макіївська",    "city": "Kyiv", "brand": "ШАВУХА ТОПчик"},
    90216:  {"name": "TOP BURGER Хрещатик",            "short": "TB Хрещатик",      "city": "Kyiv", "brand": "TOP BURGER"},
    90217:  {"name": "TOP BURGER Борщагівка",          "short": "TB Борщагівка",    "city": "Kyiv", "brand": "TOP BURGER"},
    128508: {"name": "TOP BURGER Макіївська",          "short": "TB Макіївська",    "city": "Kyiv", "brand": "TOP BURGER"},
    79106:  {"name": "БОРЩ Хрещатик",                  "short": "Б Хрещатик",       "city": "Kyiv", "brand": "БОРЩ"},
    79108:  {"name": "БОРЩ Борщагівка",                "short": "Б Борщагівка",     "city": "Kyiv", "brand": "БОРЩ"},
    128524: {"name": "БОРЩ Макіївська",                "short": "Б Макіївська",     "city": "Kyiv", "brand": "БОРЩ"},
    159537: {"name": "БОРЩ Луцьк",                     "short": "Б Луцьк",          "city": "Lutsk", "brand": "БОРЩ"},
    159764: {"name": "Yum Yum Asia Луцьк",             "short": "YY Луцьк",         "city": "Lutsk", "brand": "Yum Yum Asia"},
    159783: {"name": "Yum Yum Asia Вінграновського",   "short": "YY Вінгранов.",    "city": "Kyiv", "brand": "Yum Yum Asia"},
    159786: {"name": "Yum Yum Asia Миколайчука",       "short": "YY Миколайч.",     "city": "Kyiv", "brand": "Yum Yum Asia"},
    159787: {"name": "Yum Yum Asia Макіївська",        "short": "YY Макіївська",    "city": "Kyiv", "brand": "Yum Yum Asia"},
    159789: {"name": "Yum Yum Asia Хрещатик",          "short": "YY Хрещатик",      "city": "Kyiv", "brand": "Yum Yum Asia"},
}

PROVIDER_IDS = ",".join(str(k) for k in MULTI_KITCHEN_PROVIDERS)

BRANDS = defaultdict(list)
for pid, info in MULTI_KITCHEN_PROVIDERS.items():
    BRANDS[info["brand"]].append(pid)
BRANDS = dict(BRANDS)


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


def main():
    print("Connecting to Databricks…")
    conn = connect()

    queries = {
        "orders": f"""
            SELECT
                DATE_TRUNC('week', order_created_date) AS week_date,
                provider_id,
                SUM(CASE WHEN order_state='delivered' THEN 1 ELSE 0 END) AS delivered,
                COUNT(*) AS total_orders,
                SUM(CASE WHEN order_state!='delivered' THEN 1 ELSE 0 END) AS failed,
                ROUND(AVG(CASE WHEN order_state='delivered' THEN provider_price_before_discount END),0) AS avg_check
            FROM ng_delivery_spark.fact_order_delivery
            WHERE provider_id IN ({PROVIDER_IDS})
              AND order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
              AND city_country_code = 'ua'
            GROUP BY 1,2 ORDER BY 1,2
        """,
        "revenue": f"""
            SELECT
                DATE_TRUNC('week', order_created_date) AS week_date,
                provider_id,
                SUM(CASE WHEN order_state='delivered' THEN 1 ELSE 0 END) AS delivered,
                ROUND(SUM(CASE WHEN order_state='delivered' THEN provider_price_before_discount ELSE 0 END),0) AS rev_before,
                ROUND(SUM(CASE WHEN order_state='delivered' THEN provider_price_after_discount ELSE 0 END),0) AS rev_after,
                ROUND(SUM(CASE WHEN order_state='delivered' THEN commission_local ELSE 0 END),0) AS bolt_comm,
                ROUND(SUM(CASE WHEN order_state='delivered' THEN delivery_price ELSE 0 END),0) AS del_fee,
                SUM(CASE WHEN order_state='delivered' AND is_bolt_plus_order=true THEN 1 ELSE 0 END) AS bp_orders,
                ROUND(SUM(CASE WHEN order_state='delivered' AND is_bolt_plus_order=true THEN provider_price_before_discount ELSE 0 END),0) AS bp_rev
            FROM ng_delivery_spark.fact_order_delivery
            WHERE provider_id IN ({PROVIDER_IDS})
              AND order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
              AND city_country_code = 'ua'
            GROUP BY 1,2 ORDER BY 1,2
        """,
        "ops": f"""
            SELECT
                DATE_TRUNC('week', order_created_date) AS week_date,
                provider_id,
                SUM(CASE WHEN order_state='delivered' THEN 1 ELSE 0 END) AS delivered,
                SUM(CASE WHEN order_state='delivered' AND is_bad_order=true THEN 1 ELSE 0 END) AS bad_orders,
                SUM(CASE WHEN order_state='delivered' AND has_ticket=true THEN 1 ELSE 0 END) AS complaints
            FROM ng_delivery_spark.fact_order_delivery
            WHERE provider_id IN ({PROVIDER_IDS})
              AND order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
              AND city_country_code = 'ua'
            GROUP BY 1,2 ORDER BY 1,2
        """,
        "avail": f"""
            SELECT date, provider_id,
                ROUND(availability_rate_last_7d * 100, 1) AS avail_7d,
                ROUND(acceptance_rate_last_7d * 100, 1) AS accept_7d
            FROM ng_public_spark.etl_incentives_provider_targeting_features
            WHERE provider_id IN ({PROVIDER_IDS})
              AND date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
              AND provider_country_code = 'ua'
              AND DAYOFWEEK(date) = 7
            ORDER BY date, provider_id
        """,
        "disc": f"""
            SELECT DATE_TRUNC('week', order_created_date) AS week_date,
                m.provider_id,
                ROUND(SUM(COALESCE(m.provider_spend_local,0)),0) AS prov_disc,
                ROUND(SUM(COALESCE(m.bolt_spend_local,0)),0) AS del_disc,
                COUNT(DISTINCT m.order_id) AS promo_orders,
                COUNT(DISTINCT m.campaign_id) AS campaigns_used
            FROM ng_public_spark.etl_delivery_campaign_order_metrics m
            WHERE m.provider_id IN ({PROVIDER_IDS})
              AND m.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
            GROUP BY 1,2 ORDER BY 1,2
        """,
        "disc_by_type": f"""
            SELECT DATE_TRUNC('week', order_created_date) AS week_date,
                m.provider_id, m.spend_objective,
                ROUND(SUM(COALESCE(m.provider_spend_local,0)),0) AS prov_disc,
                ROUND(SUM(COALESCE(m.bolt_spend_local,0)),0) AS bolt_disc,
                ROUND(SUM(COALESCE(m.discount_value_local,0)),0) AS total_disc,
                COUNT(DISTINCT m.order_id) AS promo_orders
            FROM ng_public_spark.etl_delivery_campaign_order_metrics m
            WHERE m.provider_id IN ({PROVIDER_IDS})
              AND m.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
            GROUP BY 1,2,3 ORDER BY 1,2,3
        """,
        "smart_promo": f"""
            SELECT provider_id, smart_promo_enrollment_state,
                smart_promo_offer_type, campaign_spend_objective,
                smart_promo_offer_start_ts, smart_promo_offer_end_ts,
                is_valid_promotion
            FROM core_models_spark.fact_provider_smart_promo_offer_campaign_enrollment
            WHERE provider_id IN ({PROVIDER_IDS})
              AND smart_promo_offer_start_ts >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
            ORDER BY provider_id, smart_promo_offer_start_ts DESC
        """,
        "smart_promo_status": f"""
            SELECT provider_id, smart_promo_enrollment_state,
                smart_promo_provider_enrollment_start_ts,
                smart_promo_provider_enrollment_end_ts,
                MAX(smart_promo_offer_end_ts) AS last_offer_end,
                SUM(CASE WHEN smart_promo_offer_end_ts >= CURRENT_TIMESTAMP() THEN 1 ELSE 0 END) AS active_offers_now
            FROM core_models_spark.fact_provider_smart_promo_offer_campaign_enrollment
            WHERE provider_id IN ({PROVIDER_IDS})
            GROUP BY 1,2,3,4 ORDER BY provider_id
        """,
        "order_details": f"""
            SELECT f.order_id, f.order_created_date,
                DATE_FORMAT(f.order_created_at_local, 'HH:mm') AS order_time,
                f.provider_id, f.provider_name, f.order_state,
                ROUND(f.provider_price_before_discount,0) AS check_before,
                ROUND(f.delivery_price,0) AS del_fee,
                f.is_bolt_plus_order AS bp,
                f.order_food_rating_value AS rating,
                ROUND(f.order_delivery_minutes,0) AS del_min,
                ROUND(f.order_actual_cooking_time_minutes,0) AS cook_min,
                f.has_ticket AS ticket,
                f.is_bad_order AS bad
            FROM ng_delivery_spark.fact_order_delivery f
            WHERE f.provider_id IN ({PROVIDER_IDS})
              AND f.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
              AND f.city_country_code = 'ua'
            ORDER BY f.order_created_date DESC, f.order_created_at_local DESC
            LIMIT 3000
        """,
        "top_items": f"""
            SELECT provider_id, basket_item_name,
                SUM(basket_item_amount) AS qty,
                ROUND(SUM(provider_price_before_discount_local),0) AS rev
            FROM ng_delivery_spark.dim_basket_item_delivery
            WHERE provider_id IN ({PROVIDER_IDS})
              AND basket_item_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
              AND order_state = 'delivered'
            GROUP BY 1,2 ORDER BY provider_id, qty DESC
        """,
    }

    data = {}
    for key, sql_q in queries.items():
        print(f"  [{key}] …", end=" ", flush=True)
        df = query(conn, sql_q)
        data[key] = safe_json(df)
        print(f"{len(data[key])} rows")

    conn.close()

    # Keep top 10 items per provider
    top = {}
    for r in data["top_items"]:
        pid = r["provider_id"]
        top.setdefault(pid, [])
        if len(top[pid]) < 10:
            top[pid].append(r)
    data["top_items"] = [item for items in top.values() for item in items]

    # Build provider display names and Smart Promo status
    providers_display = {}
    cities_display = {}
    for pid, info in MULTI_KITCHEN_PROVIDERS.items():
        providers_display[str(pid)] = info["short"]
        cities_display[str(pid)] = info["city"]

    sp_status = {}
    for r in data["smart_promo_status"]:
        pid = r["provider_id"]
        state = r["smart_promo_enrollment_state"]
        active_now = r["active_offers_now"] or 0
        if pid not in sp_status or (state == "active" and active_now > 0):
            sp_status[pid] = {
                "provider_id": pid,
                "state": state,
                "active_now": active_now,
                "enrollment_start": r.get("smart_promo_provider_enrollment_start_ts"),
                "enrollment_end": r.get("smart_promo_provider_enrollment_end_ts"),
                "last_offer_end": r.get("last_offer_end"),
            }

    sp_weekly = defaultdict(lambda: defaultdict(lambda: {"offers": 0, "objectives": []}))
    for r in data["smart_promo"]:
        pid = r["provider_id"]
        offer_start = r.get("smart_promo_offer_start_ts")
        if offer_start:
            from datetime import date as dt_date, timedelta
            d = dt_date.fromisoformat(str(offer_start)[:10])
            week_start = d - timedelta(days=d.weekday())
            w = week_start.isoformat()
            sp_weekly[w][str(pid)]["offers"] += 1
            obj = r.get("campaign_spend_objective", "")
            if obj and obj not in sp_weekly[w][str(pid)]["objectives"]:
                sp_weekly[w][str(pid)]["objectives"].append(obj)

    weeks = sorted(set(r["week_date"][:10] for r in data["orders"]))

    data_js = json.dumps({
        "providers": providers_display,
        "providerIds": sorted(MULTI_KITCHEN_PROVIDERS.keys()),
        "brands": {k: v for k, v in BRANDS.items()},
        "cities": cities_display,
        "weeks": weeks,
        "orders": data["orders"],
        "revenue": data["revenue"],
        "ops": data["ops"],
        "avail": data["avail"],
        "disc": data["disc"],
        "discByType": data["disc_by_type"],
        "orderDetails": data["order_details"],
        "topItems": data["top_items"],
        "smartPromoStatus": list(sp_status.values()),
        "smartPromoWeekly": dict(sp_weekly),
    }, default=str)

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Read the template from the local report file
    template_path = REPO_ROOT / "scripts" / "multi_kitchen_template.html"
    with open(template_path, "r", encoding="utf-8") as f:
        html = f.read()

    html = html.replace("/*DATA_PLACEHOLDER*/null", data_js)
    html = html.replace("__UPDATE_TIME__", now_str)

    out_dir = REPO_ROOT / "multi-kitchen"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / "index.html"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n✅ Report: {out_path} ({len(html)/1024:.0f} KB)")


if __name__ == "__main__":
    main()
