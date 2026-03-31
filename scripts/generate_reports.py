"""
Generate weekly HTML reports for each provider by querying Databricks.
Produces one index.html per provider folder under the repo root.
"""

import os
import json
import sys
from pathlib import Path
from datetime import datetime
from decimal import Decimal

from databricks import sql
import pandas as pd

from config import SERVER_HOSTNAME, HTTP_PATH, PROVIDERS, WEEKS_BACK

REPO_ROOT = Path(__file__).resolve().parent.parent


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
        import math
        return default if math.isnan(f) else f
    except (TypeError, ValueError):
        return str(val)


def safe_list(series):
    return [to_native(v) for v in series.tolist()]


def safe_json(df):
    return json.loads(df.to_json(orient="records", date_format="iso"))


# ── Queries ──────────────────────────────────────────────────────────────

def fetch_weekly_summary(conn, provider_id):
    return query(conn, f"""
        SELECT
            order_week,
            COUNT(*) AS delivered_orders,
            ROUND(AVG(order_gmv), 0) AS avg_check_local,
            ROUND(SUM(order_gmv), 0) AS total_gmv_local,
            ROUND(AVG(order_actual_cooking_time_minutes), 1) AS avg_cooking_min,
            SUM(CASE WHEN is_bad_order = true THEN 1 ELSE 0 END) AS bad_orders,
            SUM(CASE WHEN has_ticket = true THEN 1 ELSE 0 END) AS orders_with_tickets
        FROM ng_delivery_spark.fact_order_delivery
        WHERE provider_id = {provider_id}
          AND order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
          AND order_state = 'delivered'
        GROUP BY order_week
        ORDER BY order_week
    """)


def fetch_revenue_weekly(conn, provider_id):
    return query(conn, f"""
        SELECT
            f.order_week,
            COUNT(*) AS orders,
            ROUND(SUM(f.provider_price_after_discount), 0) AS food_revenue,
            ROUND(SUM(f.commission_local * 1.2), 0) AS total_fee_gross,
            ROUND(SUM(COALESCE(f.total_refunded_amount, 0)), 0) AS refund,
            ROUND(SUM(f.provider_price_after_discount) - SUM(f.commission_local * 1.2) - SUM(COALESCE(f.total_refunded_amount, 0)), 0) AS net_income
        FROM ng_delivery_spark.fact_order_delivery f
        WHERE f.provider_id = {provider_id}
          AND f.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
          AND f.order_state = 'delivered'
        GROUP BY f.order_week
        ORDER BY f.order_week
    """)


def fetch_has_bolt_plus_campaign(conn, provider_id):
    """Check if any order for this provider has a Bolt Plus agency fee (>0.5 UAH)."""
    df = query(conn, f"""
        SELECT COUNT(*) AS cnt
        FROM ng_delivery_spark.fact_order_delivery f
        JOIN ng_public_spark.etl_delivery_order_monetary_metrics m ON f.order_id = m.order_id
        WHERE f.provider_id = {provider_id}
          AND f.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
          AND m.order_created_date >= DATE_FORMAT(DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7}), 'yyyy-MM-dd')
          AND f.order_state = 'delivered'
          AND (f.commission_local - m.provider_commission_net_eur * m.currency_rate) > 0.5
    """)
    return int(df.iloc[0]["cnt"]) > 0 if len(df) else False


def fetch_orders_detail(conn, provider_id):
    return query(conn, f"""
        SELECT
            f.order_id,
            f.order_reference_id,
            f.order_created_date,
            f.order_week,
            f.order_state,
            CASE WHEN f.is_bolt_plus_order THEN 'Так' ELSE 'Ні' END AS bolt_plus,
            f.is_bolt_plus_order,
            ROUND(f.provider_price_before_discount, 2) AS food_before_discount,
            ROUND(f.total_order_item_discount, 2) AS total_discount,
            ROUND((COALESCE(m.bolt_delivery_campaign_cost_eur, 0) + COALESCE(m.bolt_menu_campaign_cost_eur, 0)) * m.currency_rate, 2) AS bolt_discount,
            ROUND((COALESCE(m.provider_delivery_campaign_cost_eur, 0) + COALESCE(m.provider_menu_campaign_cost_eur, 0)) * m.currency_rate, 2) AS provider_discount,
            ROUND(f.provider_price_after_discount, 2) AS food_revenue,
            ROUND(m.provider_commission_net_eur * m.currency_rate, 2) AS fee_net,
            ROUND(m.provider_commission_gross_eur * m.currency_rate, 2) AS fee_gross,
            ROUND(f.commission_local - m.provider_commission_net_eur * m.currency_rate, 2) AS bp_fee_net,
            ROUND((f.commission_local - m.provider_commission_net_eur * m.currency_rate) * 1.2, 2) AS bp_fee_gross,
            ROUND(f.commission_local * 1.2, 2) AS total_fee_gross,
            ROUND(COALESCE(f.total_refunded_amount, 0), 2) AS refund,
            ROUND(f.provider_price_after_discount - f.commission_local * 1.2 - COALESCE(f.total_refunded_amount, 0), 2) AS net_income
        FROM ng_delivery_spark.fact_order_delivery f
        JOIN ng_public_spark.etl_delivery_order_monetary_metrics m ON f.order_id = m.order_id
        WHERE f.provider_id = {provider_id}
          AND f.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
          AND m.order_created_date >= DATE_FORMAT(DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7}), 'yyyy-MM-dd')
          AND f.order_state = 'delivered'
        ORDER BY f.order_created_date DESC, f.order_id DESC
    """)


def fetch_cancelled(conn, provider_id):
    return query(conn, f"""
        SELECT
            f.order_id,
            f.order_reference_id,
            f.order_created_date,
            f.order_week,
            f.order_state,
            CASE
                WHEN f.is_rejected_by_provider = true THEN 'Відхилено закладом'
                WHEN f.is_not_responded_by_provider = true THEN 'Без відповіді від закладу'
                WHEN f.order_state = 'failed' THEN 'Помилка'
                ELSE 'Скасовано'
            END AS reason,
            d.failed_order_comment AS comment
        FROM ng_delivery_spark.fact_order_delivery f
        LEFT JOIN ng_delivery_spark.dim_order_delivery d ON f.order_id = d.order_id
        WHERE f.provider_id = {provider_id}
          AND f.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
          AND f.order_state IN ('rejected', 'cancelled', 'failed')
        ORDER BY f.order_created_date DESC
    """)


def fetch_complaints(conn, provider_id):
    return query(conn, f"""
        SELECT
            d.order_id,
            f.order_reference_id,
            f.order_created_date,
            f.order_week,
            ROUND(f.order_gmv, 0) AS sum_uah,
            ROUND(f.order_actual_cooking_time_minutes, 1) AS cooking_min,
            d.provider_rating_value AS rating,
            d.bad_order_type,
            d.bad_order_actor_at_fault AS fault,
            d.provider_rating_comment,
            d.provider_rating_highlights,
            d.order_quality_cs_ticket_types AS cs_ticket_type,
            d.order_quality_rating_tags AS rating_tags,
            d.refund_reason,
            d.missing_or_wrong_items_cs_ticket_types AS missing_items,
            d.failed_order_comment,
            d.succeeded_order_comment
        FROM ng_delivery_spark.dim_order_delivery d
        JOIN ng_delivery_spark.fact_order_delivery f ON d.order_id = f.order_id
        WHERE f.provider_id = {provider_id}
          AND f.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
          AND (d.is_bad_order = true OR d.is_cs_ticket_order = true)
        ORDER BY f.order_created_date DESC
    """)


# ── Helpers ──────────────────────────────────────────────────────────────

BAD_ORDER_TYPE_UA = {
    "late_delivery_order_15min": "Пізня доставка (>15 хв)",
    "timing_quality_cs_ticket": "Скарга на час доставки",
    "delivery_quality_cs_ticket": "Скарга на доставку",
    "order_quality_cs_ticket": "Скарга на якість замовлення",
    "missing_or_wrong_items_cs_ticket": "Відсутні/неправильні товари",
    "failed_order_after_provider_accepted": "Невдале після прийняття",
    "bad_rating_order": "Поганий рейтинг",
    "bad_provider_rating_order": "Поганий рейтинг закладу",
    "bad_courier_rating_order": "Поганий рейтинг кур'єра",
}

FAULT_UA = {
    "provider": "Заклад",
    "courier": "Кур'єр",
    "bolt": "Bolt",
    "eater": "Клієнт",
    "unknown": "Невідомо",
}


def translate_bad_type(t):
    if not t:
        return "—"
    return BAD_ORDER_TYPE_UA.get(t, t)


def translate_fault(f):
    if not f:
        return "—"
    return FAULT_UA.get(f, f)


def clean_internal_info(text):
    """Remove internal admin info, beehive links, agent names from comments."""
    import re
    if not text:
        return ""
    t = str(text)
    t = re.sub(r'Admin:\s*[A-Z][\w]*\s+[A-Z][\w]*\s*', '', t)
    t = re.sub(r'Reason:\s*', '', t)
    t = re.sub(r'https?://beehive\.bolt\.eu\S*', '', t)
    t = re.sub(r'https?://\S*bolt\S*', '', t)
    t = re.sub(r'https?://\S+', '', t)
    t = t.replace("\\n", " ").replace("\n", " ")
    t = re.sub(r'\s{2,}', ' ', t).strip()
    t = t.strip(" -,;:|")
    return t if t else "—"


RATING_TAG_UA = {
    "order took longer": "Довге очікування",
    "unhappy with customer support": "Незадоволений підтримкою",
    "did not receive a promised refund": "Не отримав обіцяний рефанд",
    "my order arrived cold": "Замовлення прибуло холодним",
    "good taste": "Смачно",
    "perfect taste": "Чудовий смак",
    "perfect portions": "Чудові порції",
    "large portions": "Великі порції",
    "bad taste": "Несмачно",
    "small portions": "Малі порції",
    "missing items": "Відсутні позиції",
    "wrong items": "Неправильні позиції",
}


def translate_rating_tag(tag):
    tag_clean = tag.strip().lower().replace("_", " ")
    return RATING_TAG_UA.get(tag_clean, tag_clean.capitalize())


def build_comment(row):
    parts = []
    for field in ["provider_rating_comment"]:
        val = row.get(field)
        if val and str(val) not in ("None", "nan", "", "[]"):
            parts.append(clean_internal_info(val))

    refund = row.get("refund_reason")
    if refund and str(refund) not in ("None", "nan", "", "[]"):
        tags = [translate_rating_tag(t) for t in str(refund).split(",") if t.strip()]
        parts.append(", ".join(tags))

    for field in ["failed_order_comment", "succeeded_order_comment"]:
        val = row.get(field)
        if val and str(val) not in ("None", "nan", "", "[]"):
            cleaned = clean_internal_info(val)
            if cleaned:
                parts.append(cleaned)

    highlights = row.get("provider_rating_highlights")
    if highlights and str(highlights) not in ("None", "nan", "", "[]"):
        tags_raw = str(highlights).replace("[", "").replace("]", "").replace('"', '').split(",")
        tags = [translate_rating_tag(t) for t in tags_raw if t.strip()]
        if tags:
            parts.append("Відгук: " + ", ".join(tags))

    return " | ".join(p for p in parts if p) if parts else "—"


def esc(val):
    if val is None:
        return "—"
    return str(val).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


# ── HTML Generation ─────────────────────────────────────────────────────

def generate_html(provider_id, info, weekly, rev_weekly, orders_detail,
                  cancelled, complaints, generated_at, has_bp_campaign=False):
    weeks = weekly["order_week"].tolist() if len(weekly) else []
    delivered = safe_list(weekly["delivered_orders"]) if len(weekly) else []
    avg_check = safe_list(weekly["avg_check_local"]) if len(weekly) else []
    cooking = safe_list(weekly["avg_cooking_min"]) if len(weekly) else []
    bad = safe_list(weekly["bad_orders"]) if len(weekly) else []

    rev_weeks = rev_weekly["order_week"].tolist() if len(rev_weekly) else []
    net_income = safe_list(rev_weekly["net_income"]) if len(rev_weekly) else []
    food_rev = safe_list(rev_weekly["food_revenue"]) if len(rev_weekly) else []
    fees = safe_list(rev_weekly["total_fee_gross"]) if len(rev_weekly) else []
    refunds = safe_list(rev_weekly["refund"]) if len(rev_weekly) else []

    cancelled_json = safe_json(cancelled) if len(cancelled) else []
    complaints_json = safe_json(complaints) if len(complaints) else []
    orders_json = safe_json(orders_detail) if len(orders_detail) else []

    # Build weekly data dict for JS (merge summary + revenue by week key)
    weekly_data = {}
    for i, w in enumerate(weeks):
        wk = str(w)
        weekly_data[wk] = {
            "delivered": delivered[i], "avg_check": avg_check[i],
            "cooking": cooking[i], "bad": bad[i],
        }
    for i, w in enumerate(rev_weeks):
        wk = str(w)
        if wk not in weekly_data:
            weekly_data[wk] = {}
        weekly_data[wk].update({
            "food_rev": food_rev[i], "fees": fees[i],
            "net_income": net_income[i], "refund": refunds[i],
        })
    # Count cancelled and complaints per week
    for r in cancelled_json:
        wk = str(r.get("order_week", ""))
        if wk in weekly_data:
            weekly_data[wk]["cancelled"] = weekly_data[wk].get("cancelled", 0) + 1
    for r in complaints_json:
        wk = str(r.get("order_week", ""))
        if wk in weekly_data:
            weekly_data[wk]["complaints"] = weekly_data[wk].get("complaints", 0) + 1

    ORDER_STATE_UA = {
        "delivered": "Доставлено",
        "cancelled": "Скасовано",
        "rejected": "Відхилено",
        "failed": "Помилка",
    }

    orders_rows = ""
    for r in orders_json:
        bolt_disc = r.get('bolt_discount', 0) or 0
        prov_disc = r.get('provider_discount', 0) or 0
        total_disc = r.get('total_discount', 0) or 0
        if total_disc > 0:
            disc_parts = []
            if bolt_disc > 0:
                disc_parts.append(f"Bolt: {bolt_disc:,.0f}")
            if prov_disc > 0:
                disc_parts.append(f"Заклад: {prov_disc:,.0f}")
            if not disc_parts:
                disc_parts.append(f"{total_disc:,.0f}")
            disc_text = " / ".join(disc_parts)
        else:
            disc_text = "—"

        if has_bp_campaign:
            order_bp_fee = (r.get('bp_fee_net') or 0) > 0.5
            bp_label = "Bolt Plus" if order_bp_fee else "Ні"
            bp_class = " class='bp'" if order_bp_fee else ""
        else:
            bp_label = "Ні"
            bp_class = ""

        fee_net = r.get('fee_net', 0) or 0
        fee_gross = r.get('fee_gross', 0) or 0
        fee_vat = round(fee_gross - fee_net, 2)
        bp_fee_net = r.get('bp_fee_net', 0) or 0
        bp_fee_gross = r.get('bp_fee_gross', 0) or 0
        bp_fee_vat = round(bp_fee_gross - bp_fee_net, 2)
        total_fee = r.get('total_fee_gross', 0) or 0

        fee_text = f"{fee_net:,.0f} + {fee_vat:,.0f} = {fee_gross:,.0f}"
        bp_text = f"{bp_fee_net:,.0f} + {bp_fee_vat:,.0f} = {bp_fee_gross:,.0f}" if (has_bp_campaign and bp_fee_net > 0.5) else "—"

        state_raw = r.get('order_state', 'delivered') or 'delivered'
        state_ua = ORDER_STATE_UA.get(state_raw, state_raw)
        row_week = str(r.get('order_week', ''))

        orders_rows += (
            f"<tr data-week='{esc(row_week)}'>"
            f"<td>{esc(r.get('order_created_date',''))}</td>"
            f"<td>{esc(r.get('order_reference_id',''))}</td>"
            f"<td>{state_ua}</td>"
            f"<td{bp_class}>{bp_label}</td>"
            f"<td class='num'>{r.get('food_before_discount',''):,.2f}</td>"
            f"<td class='num'>{disc_text}</td>"
            f"<td class='num'>{r.get('food_revenue',''):,.2f}</td>"
            f"<td class='num sm'>{fee_text}</td>"
            f"<td class='num sm'>{bp_text}</td>"
            f"<td class='num'>{total_fee:,.2f}</td>"
            f"<td class='num'>{r.get('refund',''):,.2f}</td>"
            f"<td class='num highlight'>{r.get('net_income',''):,.2f}</td>"
            f"</tr>\n"
        )

    canc_rows = ""
    for r in cancelled_json:
        raw_comment = r.get('comment') or ''
        cleaned_comment = clean_internal_info(raw_comment) if raw_comment else "—"
        row_week = str(r.get('order_week', ''))
        canc_rows += (
            f"<tr data-week='{esc(row_week)}'><td>{esc(r.get('order_created_date',''))}</td>"
            f"<td>{esc(r.get('order_reference_id',''))}</td>"
            f"<td>{esc(r.get('order_state',''))}</td>"
            f"<td>{esc(r.get('reason',''))}</td>"
            f"<td class='comment-cell'>{esc(cleaned_comment)}</td></tr>\n"
        )

    comp_rows = ""
    for r in complaints_json:
        comment = build_comment(r)
        row_week = str(r.get('order_week', ''))
        comp_rows += (
            f"<tr data-week='{esc(row_week)}'><td>{esc(r.get('order_created_date',''))}</td>"
            f"<td>{esc(r.get('order_reference_id',''))}</td>"
            f"<td>{r.get('order_id','')}</td>"
            f"<td>{r.get('sum_uah',''):,.0f} ₴</td>"
            f"<td>{translate_bad_type(r.get('bad_order_type'))}</td>"
            f"<td>{translate_fault(r.get('fault'))}</td>"
            f"<td>{r.get('rating','') or '—'}</td>"
            f"<td class='comment-cell'>{esc(comment)}</td></tr>\n"
        )

    all_weeks_sorted = sorted(set(weeks) | set(rev_weeks))

    return f"""<!DOCTYPE html>
<html lang="uk">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{info['name']} — Тижневий звіт</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; background:#0f172a; color:#e2e8f0; padding:24px; }}
.header {{ text-align:center; margin-bottom:24px; }}
.header h1 {{ font-size:26px; font-weight:700; }}
.header .sub {{ color:#94a3b8; font-size:14px; margin-top:4px; }}
.header .updated {{ color:#64748b; font-size:12px; margin-top:8px; }}

.week-filter {{ display:flex; flex-wrap:wrap; gap:8px; justify-content:center; margin-bottom:24px; }}
.week-btn {{ background:#1e293b; border:1px solid #334155; color:#94a3b8; border-radius:8px; padding:8px 16px; cursor:pointer; font-size:13px; transition:all .2s; }}
.week-btn:hover {{ border-color:#60a5fa; color:#e2e8f0; }}
.week-btn.active {{ background:#1d4ed8; border-color:#3b82f6; color:#fff; font-weight:600; }}

.kpi-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(170px,1fr)); gap:12px; margin-bottom:28px; }}
.kpi {{ background:#1e293b; border:1px solid #334155; border-radius:10px; padding:16px; text-align:center; position:relative; }}
.kpi .value {{ font-size:26px; font-weight:700; color:#f8fafc; }}
.kpi .label {{ font-size:11px; color:#94a3b8; margin-top:4px; }}
.kpi.alert .value {{ color:#f87171; }}
.kpi.green .value {{ color:#34d399; }}
.kpi.blue .value {{ color:#60a5fa; }}
.wow {{ display:inline-block; font-size:12px; font-weight:600; margin-left:6px; vertical-align:middle; }}
.wow.up {{ color:#34d399; }}
.wow.down {{ color:#f87171; }}
.wow.neutral {{ color:#94a3b8; }}

.charts {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(380px,1fr)); gap:16px; margin-bottom:28px; }}
.chart-card {{ background:#1e293b; border:1px solid #334155; border-radius:10px; padding:16px; }}
.chart-card h3 {{ font-size:14px; color:#94a3b8; margin-bottom:12px; }}
canvas {{ max-height:260px; }}

.section {{ margin-bottom:28px; }}
.section h2 {{ font-size:18px; margin-bottom:12px; color:#e2e8f0; }}
.section .count {{ color:#64748b; font-size:14px; font-weight:normal; }}

.tbl-wrap {{ overflow-x:auto; border-radius:10px; border:1px solid #334155; }}
table {{ width:100%; border-collapse:collapse; background:#1e293b; white-space:nowrap; }}
th {{ background:#334155; color:#94a3b8; font-size:11px; text-transform:uppercase; padding:10px 12px; text-align:left; position:sticky; top:0; z-index:1; }}
td {{ padding:8px 12px; border-top:1px solid #293548; font-size:13px; }}
td.num {{ text-align:right; font-variant-numeric:tabular-nums; }}
td.highlight {{ color:#34d399; font-weight:600; }}
td.comment-cell {{ white-space:normal; max-width:400px; font-size:12px; color:#cbd5e1; }}
td.bp {{ color:#a78bfa; font-weight:600; }}
td.sm {{ font-size:11px; color:#94a3b8; }}
tr:nth-child(even) {{ background:#1e293b; }}
tr:nth-child(odd) {{ background:#162032; }}
tr.hidden {{ display:none; }}
.empty {{ text-align:center; color:#64748b; padding:24px; }}

.scroll-table {{ max-height:600px; overflow-y:auto; }}

@media(max-width:600px) {{
  .charts {{ grid-template-columns:1fr; }}
  .kpi-grid {{ grid-template-columns:repeat(2,1fr); }}
  body {{ padding:12px; }}
  .week-filter {{ gap:4px; }}
  .week-btn {{ padding:6px 10px; font-size:12px; }}
}}
</style>
</head>
<body>
<div class="header">
  <h1>{info['name']}</h1>
  <div class="sub">{info['city']} — Останні {WEEKS_BACK} тижнів</div>
  <div class="updated">Оновлено: {generated_at}</div>
</div>

<div class="week-filter" id="weekFilter">
  <button class="week-btn active" data-week="all">Всі тижні</button>
</div>

<div class="kpi-grid" id="kpiGrid">
  <div class="kpi green" id="kpiDelivered"><div class="value"><span id="vDelivered"></span><span class="wow" id="wDelivered"></span></div><div class="label">Виконано замовлень</div></div>
  <div class="kpi blue" id="kpiCheck"><div class="value"><span id="vCheck"></span><span class="wow" id="wCheck"></span></div><div class="label">Середній чек</div></div>
  <div class="kpi" id="kpiCooking"><div class="value"><span id="vCooking"></span><span class="wow" id="wCooking"></span></div><div class="label">Час приготування</div></div>
  <div class="kpi green" id="kpiFoodRev"><div class="value"><span id="vFoodRev"></span><span class="wow" id="wFoodRev"></span></div><div class="label">Дохід від їжі</div></div>
  <div class="kpi" id="kpiFees"><div class="value"><span id="vFees"></span><span class="wow" id="wFees"></span></div><div class="label">Комісія (брутто)</div></div>
  <div class="kpi green" id="kpiNet"><div class="value"><span id="vNet"></span><span class="wow" id="wNet"></span></div><div class="label">Чистий дохід</div></div>
  <div class="kpi alert" id="kpiCancelled"><div class="value"><span id="vCancelled"></span><span class="wow" id="wCancelled"></span></div><div class="label">Скасовано</div></div>
  <div class="kpi alert" id="kpiComplaints"><div class="value"><span id="vComplaints"></span><span class="wow" id="wComplaints"></span></div><div class="label">Скарги / Bad Orders</div></div>
</div>

<div class="charts">
  <div class="chart-card"><h3>Виконані замовлення по тижнях</h3><canvas id="ordersChart"></canvas></div>
  <div class="chart-card"><h3>Середній чек (₴) по тижнях</h3><canvas id="checkChart"></canvas></div>
  <div class="chart-card"><h3>Час приготування (хв) по тижнях</h3><canvas id="cookingChart"></canvas></div>
  <div class="chart-card"><h3>Дохід по тижнях (₴)</h3><canvas id="revenueChart"></canvas></div>
</div>

<div class="section">
  <h2>Дохідність по замовленнях <span class="count" id="ordersCount"></span></h2>
  <div class="tbl-wrap scroll-table">
    <table id="ordersTable">
      <thead><tr>
        <th>Дата</th><th>Order Ref</th><th>Статус</th><th>Bolt+</th>
        <th>Ціна до знижки</th><th>Знижка (за чий рахунок)</th><th>Дохід від їжі</th>
        <th>Комісія (нетто+ПДВ=брутто)</th><th>Bolt Plus комісія</th><th>Всього комісія</th>
        <th>Повернення</th><th>Чистий дохід</th>
      </tr></thead>
      <tbody>{orders_rows}</tbody>
    </table>
  </div>
</div>

<div class="section">
  <h2>Замовлення зі скаргами <span class="count" id="compCount"></span></h2>
  <div class="tbl-wrap" id="compSection">
    <table id="compTable">
      <thead><tr>
        <th>Дата</th><th>Order Ref</th><th>Order ID</th><th>Сума</th>
        <th>Тип проблеми</th><th>Винний</th><th>Рейтинг</th><th>Коментар / Деталі</th>
      </tr></thead>
      <tbody>{comp_rows}</tbody>
    </table>
  </div>
</div>

<div class="section">
  <h2>Скасовані замовлення <span class="count" id="cancCount"></span></h2>
  <div class="tbl-wrap" id="cancSection">
    <table id="cancTable">
      <thead><tr><th>Дата</th><th>Order Ref</th><th>Статус</th><th>Причина</th><th>Коментар</th></tr></thead>
      <tbody>{canc_rows}</tbody>
    </table>
  </div>
</div>

<script>
const W = {json.dumps(weekly_data)};
const allWeeks = {json.dumps(all_weeks_sorted)};
const weeks = {json.dumps(weeks)};
const delivered = {json.dumps(delivered)};
const avgCheck = {json.dumps(avg_check)};
const cookingTime = {json.dumps(cooking)};
const revWeeks = {json.dumps(rev_weeks)};
const netIncome = {json.dumps(net_income)};
const foodRev = {json.dumps(food_rev)};
const feesData = {json.dumps(fees)};

// Build week filter buttons
const filterEl = document.getElementById('weekFilter');
allWeeks.forEach(w => {{
  const btn = document.createElement('button');
  btn.className = 'week-btn';
  btn.dataset.week = w;
  btn.textContent = w;
  filterEl.appendChild(btn);
}});

// Charts
Chart.defaults.color = '#94a3b8';
Chart.defaults.borderColor = '#334155';
const barOpts = {{ responsive:true, plugins:{{ legend:{{ display:false }} }}, scales:{{ y:{{ beginAtZero:true }} }} }};

const ordersChart = new Chart(document.getElementById('ordersChart'), {{
  type:'bar', data:{{ labels:weeks, datasets:[{{ data:[...delivered], backgroundColor:weeks.map(()=>'#34d399'), borderRadius:4 }}] }}, options:barOpts
}});
const checkChart = new Chart(document.getElementById('checkChart'), {{
  type:'line', data:{{ labels:weeks, datasets:[{{ data:[...avgCheck], borderColor:'#60a5fa', backgroundColor:'rgba(96,165,250,0.1)', fill:true, tension:0.3, pointRadius:weeks.map(()=>4), pointBackgroundColor:weeks.map(()=>'#60a5fa') }}] }}, options:barOpts
}});
const cookingChart = new Chart(document.getElementById('cookingChart'), {{
  type:'line', data:{{ labels:weeks, datasets:[{{ data:[...cookingTime], borderColor:'#fbbf24', backgroundColor:'rgba(251,191,36,0.1)', fill:true, tension:0.3, pointRadius:weeks.map(()=>4), pointBackgroundColor:weeks.map(()=>'#fbbf24') }}] }}, options:barOpts
}});
const revenueChart = new Chart(document.getElementById('revenueChart'), {{
  type:'bar', data:{{ labels:revWeeks, datasets:[
    {{ label:'Дохід від їжі', data:[...foodRev], backgroundColor:revWeeks.map(()=>'#818cf8'), borderRadius:4 }},
    {{ label:'Комісія', data:[...feesData], backgroundColor:revWeeks.map(()=>'#f87171'), borderRadius:4 }},
    {{ label:'Чистий дохід', data:[...netIncome], backgroundColor:revWeeks.map(()=>'#34d399'), borderRadius:4 }}
  ] }}, options:{{ ...barOpts, plugins:{{ legend:{{ display:true, position:'top' }} }} }}
}});

function fmt(n, suffix) {{
  if (n === null || n === undefined) return '—';
  return n.toLocaleString('uk-UA', {{maximumFractionDigits: suffix === ' хв' ? 1 : 0}}) + (suffix || '');
}}

function wow(cur, prev, invert) {{
  if (prev === null || prev === undefined || prev === 0) return '<span class="wow neutral">—</span>';
  const pct = ((cur - prev) / Math.abs(prev) * 100).toFixed(0);
  const isUp = cur > prev;
  const cls = invert ? (isUp ? 'down' : 'up') : (isUp ? 'up' : 'down');
  if (cur === prev) return '<span class="wow neutral">0%</span>';
  const arrow = isUp ? '&#9650;' : '&#9660;';
  return `<span class="wow ${{cls}}">${{arrow}} ${{Math.abs(pct)}}%</span>`;
}}

function getWeekData(wk) {{
  return W[wk] || {{}};
}}

function sumAll(key) {{
  return allWeeks.reduce((s, w) => s + (W[w]?.[key] || 0), 0);
}}

function avgWeighted(valKey, weightKey) {{
  let num = 0, den = 0;
  allWeeks.forEach(w => {{
    const d = W[w] || {{}};
    num += (d[valKey] || 0) * (d[weightKey] || 0);
    den += (d[weightKey] || 0);
  }});
  return den > 0 ? num / den : 0;
}}

function filterRows(tableId, week) {{
  const rows = document.querySelectorAll(`#${{tableId}} tbody tr`);
  let visible = 0;
  rows.forEach(r => {{
    if (week === 'all' || r.dataset.week === week) {{
      r.classList.remove('hidden');
      visible++;
    }} else {{
      r.classList.add('hidden');
    }}
  }});
  return visible;
}}

function highlightChart(chart, labels, week, colors) {{
  if (typeof colors === 'string') colors = labels.map(() => colors);
  const ds = chart.data.datasets[0];
  if (week === 'all') {{
    if (ds.backgroundColor instanceof Array) ds.backgroundColor = colors;
    if (ds.pointRadius instanceof Array) ds.pointRadius = labels.map(() => 4);
    if (ds.pointBackgroundColor instanceof Array) ds.pointBackgroundColor = colors;
  }} else {{
    if (ds.backgroundColor instanceof Array) {{
      ds.backgroundColor = labels.map(l => l === week ? colors[0] : colors[0] + '33');
    }}
    if (ds.pointRadius instanceof Array) {{
      ds.pointRadius = labels.map(l => l === week ? 8 : 3);
    }}
    if (ds.pointBackgroundColor instanceof Array) {{
      ds.pointBackgroundColor = labels.map(l => l === week ? colors[0] : colors[0] + '66');
    }}
  }}
  chart.update();
}}

function highlightRevenueChart(week) {{
  const ds = revenueChart.data.datasets;
  const cols = ['#818cf8', '#f87171', '#34d399'];
  ds.forEach((d, i) => {{
    if (week === 'all') {{
      d.backgroundColor = revWeeks.map(() => cols[i]);
    }} else {{
      d.backgroundColor = revWeeks.map(l => l === week ? cols[i] : cols[i] + '33');
    }}
  }});
  revenueChart.update();
}}

function updateView(week) {{
  // Update button states
  document.querySelectorAll('.week-btn').forEach(b => {{
    b.classList.toggle('active', b.dataset.week === week);
  }});

  let d, prevD;
  if (week === 'all') {{
    d = {{
      delivered: sumAll('delivered'), avg_check: avgWeighted('avg_check', 'delivered'),
      cooking: avgWeighted('cooking', 'delivered'), food_rev: sumAll('food_rev'),
      fees: sumAll('fees'), net_income: sumAll('net_income'),
      cancelled: sumAll('cancelled'), complaints: sumAll('complaints')
    }};
    prevD = null;
  }} else {{
    const wd = getWeekData(week);
    d = {{
      delivered: wd.delivered || 0, avg_check: wd.avg_check || 0,
      cooking: wd.cooking || 0, food_rev: wd.food_rev || 0,
      fees: wd.fees || 0, net_income: wd.net_income || 0,
      cancelled: wd.cancelled || 0, complaints: wd.complaints || 0
    }};
    const idx = allWeeks.indexOf(week);
    if (idx > 0) {{
      const pw = getWeekData(allWeeks[idx - 1]);
      prevD = {{
        delivered: pw.delivered || 0, avg_check: pw.avg_check || 0,
        cooking: pw.cooking || 0, food_rev: pw.food_rev || 0,
        fees: pw.fees || 0, net_income: pw.net_income || 0,
        cancelled: pw.cancelled || 0, complaints: pw.complaints || 0
      }};
    }} else {{
      prevD = null;
    }}
  }}

  document.getElementById('vDelivered').textContent = fmt(d.delivered, '');
  document.getElementById('vCheck').textContent = fmt(d.avg_check, ' ₴');
  document.getElementById('vCooking').textContent = fmt(d.cooking, ' хв');
  document.getElementById('vFoodRev').textContent = fmt(d.food_rev, ' ₴');
  document.getElementById('vFees').textContent = fmt(d.fees, ' ₴');
  document.getElementById('vNet').textContent = fmt(d.net_income, ' ₴');
  document.getElementById('vCancelled').textContent = fmt(d.cancelled, '');
  document.getElementById('vComplaints').textContent = fmt(d.complaints, '');

  if (prevD) {{
    document.getElementById('wDelivered').innerHTML = wow(d.delivered, prevD.delivered, false);
    document.getElementById('wCheck').innerHTML = wow(d.avg_check, prevD.avg_check, false);
    document.getElementById('wCooking').innerHTML = wow(d.cooking, prevD.cooking, true);
    document.getElementById('wFoodRev').innerHTML = wow(d.food_rev, prevD.food_rev, false);
    document.getElementById('wFees').innerHTML = wow(d.fees, prevD.fees, true);
    document.getElementById('wNet').innerHTML = wow(d.net_income, prevD.net_income, false);
    document.getElementById('wCancelled').innerHTML = wow(d.cancelled, prevD.cancelled, true);
    document.getElementById('wComplaints').innerHTML = wow(d.complaints, prevD.complaints, true);
  }} else {{
    ['wDelivered','wCheck','wCooking','wFoodRev','wFees','wNet','wCancelled','wComplaints'].forEach(id => {{
      document.getElementById(id).innerHTML = '';
    }});
  }}

  const oVis = filterRows('ordersTable', week);
  const cVis = filterRows('compTable', week);
  const xVis = filterRows('cancTable', week);

  document.getElementById('ordersCount').textContent = `(${{oVis}} замовлень, всі суми в ₴)`;
  document.getElementById('compCount').textContent = `(${{cVis}})`;
  document.getElementById('cancCount').textContent = `(${{xVis}})`;

  highlightChart(ordersChart, weeks, week, weeks.map(() => '#34d399'));
  highlightChart(checkChart, weeks, week, weeks.map(() => '#60a5fa'));
  highlightChart(cookingChart, weeks, week, weeks.map(() => '#fbbf24'));
  highlightRevenueChart(week);
}}

// Event listeners
document.getElementById('weekFilter').addEventListener('click', e => {{
  if (e.target.classList.contains('week-btn')) {{
    updateView(e.target.dataset.week);
  }}
}});

// Initial render
updateView('all');
</script>
</body>
</html>"""


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M UTC")
    print(f"Starting report generation at {generated_at}")

    conn = connect()
    try:
        for pid, info in PROVIDERS.items():
            print(f"\n--- {info['name']} (ID: {pid}) ---")
            folder = REPO_ROOT / info["slug"]
            folder.mkdir(parents=True, exist_ok=True)

            has_bp = fetch_has_bolt_plus_campaign(conn, pid)
            print(f"  Bolt Plus campaign: {'Yes' if has_bp else 'No'}")

            weekly = fetch_weekly_summary(conn, pid)
            print(f"  Weekly summary: {len(weekly)} weeks")

            rev_weekly = fetch_revenue_weekly(conn, pid)
            print(f"  Revenue weekly: {len(rev_weekly)} weeks")

            orders_detail = fetch_orders_detail(conn, pid)
            print(f"  Orders detail: {len(orders_detail)} orders")

            cancelled = fetch_cancelled(conn, pid)
            print(f"  Cancelled: {len(cancelled)}")

            complaints = fetch_complaints(conn, pid)
            print(f"  Complaints: {len(complaints)}")

            html = generate_html(pid, info, weekly, rev_weekly, orders_detail,
                                 cancelled, complaints, generated_at,
                                 has_bp_campaign=has_bp)
            out_path = folder / "index.html"
            out_path.write_text(html, encoding="utf-8")
            print(f"  Saved: {out_path}")
    finally:
        conn.close()

    update_index()
    print("\nDone!")


def update_index():
    cards = ""
    for pid, info in sorted(PROVIDERS.items(), key=lambda x: x[1]["name"]):
        cards += f"""
        <a class="report-card" href="{info['slug']}/">
            <h3>{info['name']}</h3>
            <p>{info['city']}</p>
            <span class="badge">Тижневий звіт</span>
        </a>"""

    html = f"""<!DOCTYPE html>
<html lang="uk">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
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
<div class="reports-grid">{cards}
</div>
</body>
</html>"""
    (REPO_ROOT / "index.html").write_text(html, encoding="utf-8")
    print("Updated root index.html")


if __name__ == "__main__":
    main()
