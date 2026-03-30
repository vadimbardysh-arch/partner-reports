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


def to_native(val):
    if val is None:
        return None
    if isinstance(val, Decimal):
        return float(val)
    if hasattr(val, "item"):
        return val.item()
    try:
        return float(val)
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
            order_week,
            COUNT(*) AS orders,
            ROUND(SUM(provider_price_before_discount), 0) AS food_before_discount,
            ROUND(SUM(total_order_item_discount), 0) AS menu_discount,
            ROUND(SUM(provider_price_after_discount), 0) AS food_revenue,
            ROUND(SUM(commission_local), 0) AS fee_with_vat,
            ROUND(SUM(total_refunded_amount), 0) AS refund,
            ROUND(SUM(provider_price_after_discount) - SUM(commission_local) - SUM(COALESCE(total_refunded_amount, 0)), 0) AS net_income
        FROM ng_delivery_spark.fact_order_delivery
        WHERE provider_id = {provider_id}
          AND order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
          AND order_state = 'delivered'
        GROUP BY order_week
        ORDER BY order_week
    """)


def fetch_orders_detail(conn, provider_id):
    return query(conn, f"""
        SELECT
            order_id,
            order_reference_id,
            order_created_date,
            order_state,
            CASE WHEN is_bolt_plus_order THEN 'Так' ELSE 'Ні' END AS bolt_plus,
            ROUND(provider_price_before_discount, 2) AS food_before_discount,
            ROUND(total_order_item_discount, 2) AS menu_discount,
            ROUND(provider_price_after_discount, 2) AS food_revenue,
            ROUND(commission_local, 2) AS fee_with_vat,
            ROUND(COALESCE(total_refunded_amount, 0), 2) AS refund,
            ROUND(provider_price_after_discount - commission_local - COALESCE(total_refunded_amount, 0), 2) AS net_income
        FROM ng_delivery_spark.fact_order_delivery
        WHERE provider_id = {provider_id}
          AND order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
          AND order_state = 'delivered'
        ORDER BY order_created_date DESC, order_id DESC
    """)


def fetch_cancelled(conn, provider_id):
    return query(conn, f"""
        SELECT
            f.order_id,
            f.order_reference_id,
            f.order_created_date,
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
    "timing_quality_cs_ticket": "Скарга на час (CS тікет)",
    "delivery_quality_cs_ticket": "Скарга на доставку (CS тікет)",
    "order_quality_cs_ticket": "Скарга на якість (CS тікет)",
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


def build_comment(row):
    parts = []
    for field in ["provider_rating_comment", "refund_reason", "failed_order_comment",
                   "succeeded_order_comment", "cs_ticket_type", "rating_tags", "missing_items"]:
        val = row.get(field)
        if val and str(val) not in ("None", "nan", "", "[]"):
            cleaned = str(val).replace("_eater", "").replace("_", " ").replace("[", "").replace("]", "").replace('"', '')
            parts.append(cleaned)
    highlights = row.get("provider_rating_highlights")
    if highlights and str(highlights) not in ("None", "nan", "", "[]"):
        cleaned = str(highlights).replace("_eater", "").replace("_", " ").replace("[", "").replace("]", "").replace('"', '')
        parts.append(f"Відгук: {cleaned}")
    return " | ".join(parts) if parts else "—"


def esc(val):
    if val is None:
        return "—"
    return str(val).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


# ── HTML Generation ─────────────────────────────────────────────────────

def generate_html(provider_id, info, weekly, rev_weekly, orders_detail,
                  cancelled, complaints, generated_at):
    weeks = weekly["order_week"].tolist() if len(weekly) else []
    delivered = safe_list(weekly["delivered_orders"]) if len(weekly) else []
    avg_check = safe_list(weekly["avg_check_local"]) if len(weekly) else []
    cooking = safe_list(weekly["avg_cooking_min"]) if len(weekly) else []
    bad = safe_list(weekly["bad_orders"]) if len(weekly) else []

    rev_weeks = rev_weekly["order_week"].tolist() if len(rev_weekly) else []
    net_income = safe_list(rev_weekly["net_income"]) if len(rev_weekly) else []
    food_rev = safe_list(rev_weekly["food_revenue"]) if len(rev_weekly) else []
    fees = safe_list(rev_weekly["fee_with_vat"]) if len(rev_weekly) else []

    total_delivered = int(sum(delivered))
    total_cancelled = len(cancelled)
    total_complaints = len(complaints)
    avg_check_total = round(sum(c * o for c, o in zip(avg_check, delivered)) / max(total_delivered, 1), 0)
    avg_cooking_total = round(sum(c * o for c, o in zip(cooking, delivered)) / max(total_delivered, 1), 1)
    total_food_rev = round(sum(food_rev), 0)
    total_fees = round(sum(fees), 0)
    total_net = round(sum(net_income), 0)

    cancelled_json = safe_json(cancelled) if len(cancelled) else []
    complaints_json = safe_json(complaints) if len(complaints) else []
    orders_json = safe_json(orders_detail) if len(orders_detail) else []

    # Build orders table rows
    orders_rows = ""
    for r in orders_json:
        orders_rows += (
            f"<tr>"
            f"<td>{esc(r.get('order_created_date',''))}</td>"
            f"<td>{esc(r.get('order_reference_id',''))}</td>"
            f"<td>{esc(r.get('bolt_plus',''))}</td>"
            f"<td class='num'>{r.get('food_before_discount',''):,.2f}</td>"
            f"<td class='num'>{r.get('menu_discount',''):,.2f}</td>"
            f"<td class='num'>{r.get('food_revenue',''):,.2f}</td>"
            f"<td class='num'>{r.get('fee_with_vat',''):,.2f}</td>"
            f"<td class='num'>{r.get('refund',''):,.2f}</td>"
            f"<td class='num highlight'>{r.get('net_income',''):,.2f}</td>"
            f"</tr>\n"
        )

    # Build cancelled table
    canc_rows = ""
    for r in cancelled_json:
        canc_rows += (
            f"<tr><td>{esc(r.get('order_created_date',''))}</td>"
            f"<td>{esc(r.get('order_reference_id',''))}</td>"
            f"<td>{esc(r.get('order_state',''))}</td>"
            f"<td>{esc(r.get('reason',''))}</td>"
            f"<td>{esc(r.get('comment',''))}</td></tr>\n"
        )

    # Build complaints table
    comp_rows = ""
    for r in complaints_json:
        comment = build_comment(r)
        comp_rows += (
            f"<tr><td>{esc(r.get('order_created_date',''))}</td>"
            f"<td>{esc(r.get('order_reference_id',''))}</td>"
            f"<td>{r.get('order_id','')}</td>"
            f"<td>{r.get('sum_uah',''):,.0f} ₴</td>"
            f"<td>{translate_bad_type(r.get('bad_order_type'))}</td>"
            f"<td>{translate_fault(r.get('fault'))}</td>"
            f"<td>{r.get('rating','') or '—'}</td>"
            f"<td class='comment-cell'>{esc(comment)}</td></tr>\n"
        )

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
.header {{ text-align:center; margin-bottom:32px; }}
.header h1 {{ font-size:26px; font-weight:700; }}
.header .sub {{ color:#94a3b8; font-size:14px; margin-top:4px; }}
.header .updated {{ color:#64748b; font-size:12px; margin-top:8px; }}

.kpi-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:12px; margin-bottom:28px; }}
.kpi {{ background:#1e293b; border:1px solid #334155; border-radius:10px; padding:16px; text-align:center; }}
.kpi .value {{ font-size:26px; font-weight:700; color:#f8fafc; }}
.kpi .label {{ font-size:11px; color:#94a3b8; margin-top:4px; }}
.kpi.alert .value {{ color:#f87171; }}
.kpi.green .value {{ color:#34d399; }}
.kpi.blue .value {{ color:#60a5fa; }}

.charts {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(380px,1fr)); gap:16px; margin-bottom:28px; }}
.chart-card {{ background:#1e293b; border:1px solid #334155; border-radius:10px; padding:16px; }}
.chart-card h3 {{ font-size:14px; color:#94a3b8; margin-bottom:12px; }}
canvas {{ max-height:260px; }}

.section {{ margin-bottom:28px; }}
.section h2 {{ font-size:18px; margin-bottom:12px; color:#e2e8f0; }}
.section .count {{ color:#64748b; font-size:14px; font-weight:normal; }}

.tbl-wrap {{ overflow-x:auto; border-radius:10px; border:1px solid #334155; }}
table {{ width:100%; border-collapse:collapse; background:#1e293b; white-space:nowrap; }}
th {{ background:#334155; color:#94a3b8; font-size:11px; text-transform:uppercase; padding:10px 12px; text-align:left; position:sticky; top:0; }}
td {{ padding:8px 12px; border-top:1px solid #293548; font-size:13px; }}
td.num {{ text-align:right; font-variant-numeric:tabular-nums; }}
td.highlight {{ color:#34d399; font-weight:600; }}
td.comment-cell {{ white-space:normal; max-width:400px; font-size:12px; color:#cbd5e1; }}
tr:nth-child(even) {{ background:#1e293b; }}
tr:nth-child(odd) {{ background:#162032; }}
.empty {{ text-align:center; color:#64748b; padding:24px; }}

.scroll-table {{ max-height:500px; overflow-y:auto; }}

@media(max-width:600px) {{
  .charts {{ grid-template-columns:1fr; }}
  .kpi-grid {{ grid-template-columns:repeat(2,1fr); }}
  body {{ padding:12px; }}
}}
</style>
</head>
<body>
<div class="header">
  <h1>{info['name']}</h1>
  <div class="sub">{info['city']} — Останні {WEEKS_BACK} тижні</div>
  <div class="updated">Оновлено: {generated_at}</div>
</div>

<div class="kpi-grid">
  <div class="kpi green"><div class="value">{total_delivered}</div><div class="label">Виконано замовлень</div></div>
  <div class="kpi blue"><div class="value">{avg_check_total:,.0f} ₴</div><div class="label">Середній чек</div></div>
  <div class="kpi"><div class="value">{avg_cooking_total} хв</div><div class="label">Час приготування</div></div>
  <div class="kpi green"><div class="value">{total_food_rev:,.0f} ₴</div><div class="label">Дохід від їжі</div></div>
  <div class="kpi"><div class="value">{total_fees:,.0f} ₴</div><div class="label">Комісія (з ПДВ)</div></div>
  <div class="kpi green"><div class="value">{total_net:,.0f} ₴</div><div class="label">Чистий дохід</div></div>
  <div class="kpi alert"><div class="value">{total_cancelled}</div><div class="label">Скасовано</div></div>
  <div class="kpi alert"><div class="value">{total_complaints}</div><div class="label">Скарги / Bad Orders</div></div>
</div>

<div class="charts">
  <div class="chart-card">
    <h3>Виконані замовлення по тижнях</h3>
    <canvas id="ordersChart"></canvas>
  </div>
  <div class="chart-card">
    <h3>Середній чек (₴) по тижнях</h3>
    <canvas id="checkChart"></canvas>
  </div>
  <div class="chart-card">
    <h3>Час приготування (хв) по тижнях</h3>
    <canvas id="cookingChart"></canvas>
  </div>
  <div class="chart-card">
    <h3>Дохід по тижнях (₴)</h3>
    <canvas id="revenueChart"></canvas>
  </div>
</div>

<div class="section">
  <h2>Дохідність по замовленнях <span class="count">({total_delivered} замовлень, всі суми в ₴)</span></h2>
  <div class="tbl-wrap scroll-table">
    <table>
      <thead><tr>
        <th>Дата</th><th>Order Ref</th><th>Bolt+</th>
        <th>Ціна до знижки</th><th>Знижка</th><th>Дохід від їжі</th>
        <th>Комісія (ПДВ)</th><th>Повернення</th><th>Чистий дохід</th>
      </tr></thead>
      <tbody>{orders_rows}</tbody>
    </table>
  </div>
</div>

<div class="section">
  <h2>Замовлення зі скаргами <span class="count">({total_complaints})</span></h2>
  {f'<p class="empty">Немає скарг за цей період</p>' if total_complaints == 0 else
  f'''<div class="tbl-wrap">
    <table>
      <thead><tr>
        <th>Дата</th><th>Order Ref</th><th>Order ID</th><th>Сума</th>
        <th>Тип проблеми</th><th>Винний</th><th>Рейтинг</th><th>Коментар / Деталі</th>
      </tr></thead>
      <tbody>{comp_rows}</tbody>
    </table>
  </div>'''}
</div>

<div class="section">
  <h2>Скасовані замовлення <span class="count">({total_cancelled})</span></h2>
  {f'<p class="empty">Немає скасованих замовлень за цей період</p>' if total_cancelled == 0 else
  f'''<div class="tbl-wrap">
    <table>
      <thead><tr><th>Дата</th><th>Order Ref</th><th>Статус</th><th>Причина</th><th>Коментар</th></tr></thead>
      <tbody>{canc_rows}</tbody>
    </table>
  </div>'''}
</div>

<script>
const weeks = {json.dumps(weeks)};
const delivered = {json.dumps(delivered)};
const avgCheck = {json.dumps(avg_check)};
const cookingTime = {json.dumps(cooking)};
const revWeeks = {json.dumps(rev_weeks)};
const netIncome = {json.dumps(net_income)};
const foodRev = {json.dumps(food_rev)};
const feesData = {json.dumps(fees)};

Chart.defaults.color = '#94a3b8';
Chart.defaults.borderColor = '#334155';
const barOpts = {{ responsive:true, plugins:{{ legend:{{ display:false }} }}, scales:{{ y:{{ beginAtZero:true }} }} }};

new Chart(document.getElementById('ordersChart'), {{
  type:'bar', data:{{ labels:weeks, datasets:[{{ data:delivered, backgroundColor:'#34d399', borderRadius:4 }}] }}, options:barOpts
}});
new Chart(document.getElementById('checkChart'), {{
  type:'line', data:{{ labels:weeks, datasets:[{{ data:avgCheck, borderColor:'#60a5fa', backgroundColor:'rgba(96,165,250,0.1)', fill:true, tension:0.3, pointRadius:4 }}] }}, options:barOpts
}});
new Chart(document.getElementById('cookingChart'), {{
  type:'line', data:{{ labels:weeks, datasets:[{{ data:cookingTime, borderColor:'#fbbf24', backgroundColor:'rgba(251,191,36,0.1)', fill:true, tension:0.3, pointRadius:4 }}] }}, options:barOpts
}});
new Chart(document.getElementById('revenueChart'), {{
  type:'bar', data:{{ labels:revWeeks, datasets:[
    {{ label:'Дохід від їжі', data:foodRev, backgroundColor:'#818cf8', borderRadius:4 }},
    {{ label:'Комісія', data:feesData, backgroundColor:'#f87171', borderRadius:4 }},
    {{ label:'Чистий дохід', data:netIncome, backgroundColor:'#34d399', borderRadius:4 }}
  ] }}, options:{{ ...barOpts, plugins:{{ legend:{{ display:true, position:'top' }} }} }}
}});
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
                                 cancelled, complaints, generated_at)
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
