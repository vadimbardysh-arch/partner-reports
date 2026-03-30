"""
Generate weekly HTML reports for each provider by querying Databricks.
Produces one index.html per provider folder under the repo root.
"""

import os
import json
import sys
from pathlib import Path
from datetime import datetime, timedelta

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


def fetch_revenue(conn, provider_id):
    return query(conn, f"""
        SELECT
            f.order_week,
            COUNT(*) AS orders,
            ROUND(SUM(m.gmv_eur), 2) AS gmv_eur,
            ROUND(SUM(m.net_income_eur), 2) AS net_income_eur,
            ROUND(SUM(m.menu_price_full_eur), 2) AS menu_price_eur,
            ROUND(SUM(m.menu_price_after_discount_eur), 2) AS menu_after_discount_eur,
            ROUND(SUM(m.provider_commission_gross_eur), 2) AS commission_eur,
            ROUND(SUM(m.total_discount_eur), 2) AS total_discount_eur,
            ROUND(SUM(m.delivery_full_price_eur), 2) AS delivery_price_eur,
            ROUND(SUM(m.service_fee_eur), 2) AS service_fee_eur
        FROM ng_public_spark.etl_delivery_order_monetary_metrics m
        JOIN ng_delivery_spark.fact_order_delivery f ON m.order_id = f.order_id
        WHERE m.provider_id = {provider_id}
          AND m.order_created_date >= DATE_FORMAT(DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7}), 'yyyy-MM-dd')
          AND f.order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
          AND f.order_state = 'delivered'
        GROUP BY f.order_week
        ORDER BY f.order_week
    """)


def fetch_cancelled(conn, provider_id):
    return query(conn, f"""
        SELECT
            order_id,
            order_reference_id,
            order_created_date,
            order_state,
            CASE
                WHEN is_rejected_by_provider = true THEN 'Відхилено закладом'
                WHEN is_not_responded_by_provider = true THEN 'Без відповіді від закладу'
                WHEN order_state = 'failed' THEN 'Помилка'
                ELSE 'Скасовано'
            END AS reason
        FROM ng_delivery_spark.fact_order_delivery
        WHERE provider_id = {provider_id}
          AND order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
          AND order_state IN ('rejected', 'cancelled', 'failed')
        ORDER BY order_created_date DESC
    """)


def fetch_bad_orders(conn, provider_id):
    return query(conn, f"""
        SELECT
            order_id,
            order_reference_id,
            order_created_date,
            ROUND(order_gmv, 0) AS gmv_local,
            ROUND(order_actual_cooking_time_minutes, 1) AS cooking_min,
            order_food_rating_value AS rating,
            has_ticket,
            cases_per_order
        FROM ng_delivery_spark.fact_order_delivery
        WHERE provider_id = {provider_id}
          AND order_created_date >= DATE_SUB(CURRENT_DATE(), {WEEKS_BACK * 7})
          AND (has_ticket = true OR is_bad_order = true)
          AND order_state = 'delivered'
        ORDER BY order_created_date DESC
    """)


def to_native(val):
    """Convert Decimal/numpy types to Python native for JSON."""
    if val is None:
        return None
    if hasattr(val, "item"):
        return val.item()
    try:
        return float(val)
    except (TypeError, ValueError):
        return str(val)


def safe_list(series):
    return [to_native(v) for v in series.tolist()]


def safe_json(df):
    """Convert DataFrame to JSON-safe list of dicts."""
    return json.loads(df.to_json(orient="records", date_format="iso"))


def generate_html(provider_id, info, weekly, revenue, cancelled, bad_orders, generated_at):
    weeks = weekly["order_week"].tolist() if len(weekly) else []
    delivered = safe_list(weekly["delivered_orders"]) if len(weekly) else []
    avg_check = safe_list(weekly["avg_check_local"]) if len(weekly) else []
    cooking = safe_list(weekly["avg_cooking_min"]) if len(weekly) else []
    bad = safe_list(weekly["bad_orders"]) if len(weekly) else []

    rev_weeks = revenue["order_week"].tolist() if len(revenue) else []
    gmv = safe_list(revenue["gmv_eur"]) if len(revenue) else []
    net_income = safe_list(revenue["net_income_eur"]) if len(revenue) else []

    total_delivered = sum(delivered)
    total_cancelled = len(cancelled)
    total_bad = sum(bad)
    avg_check_total = round(sum(c * o for c, o in zip(avg_check, delivered)) / max(total_delivered, 1), 0)
    avg_cooking_total = round(sum(c * o for c, o in zip(cooking, delivered)) / max(total_delivered, 1), 1)
    total_gmv_eur = round(sum(gmv), 2)
    total_net_eur = round(sum(net_income), 2)

    cancelled_json = safe_json(cancelled) if len(cancelled) else []
    bad_json = safe_json(bad_orders) if len(bad_orders) else []

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
.kpi-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:12px; margin-bottom:28px; }}
.kpi {{ background:#1e293b; border:1px solid #334155; border-radius:10px; padding:16px; text-align:center; }}
.kpi .value {{ font-size:28px; font-weight:700; color:#f8fafc; }}
.kpi .label {{ font-size:12px; color:#94a3b8; margin-top:4px; }}
.kpi.alert .value {{ color:#f87171; }}
.kpi.green .value {{ color:#34d399; }}
.charts {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(400px,1fr)); gap:16px; margin-bottom:28px; }}
.chart-card {{ background:#1e293b; border:1px solid #334155; border-radius:10px; padding:16px; }}
.chart-card h3 {{ font-size:14px; color:#94a3b8; margin-bottom:12px; }}
canvas {{ max-height:260px; }}
.section {{ margin-bottom:28px; }}
.section h2 {{ font-size:18px; margin-bottom:12px; color:#e2e8f0; }}
table {{ width:100%; border-collapse:collapse; background:#1e293b; border-radius:10px; overflow:hidden; }}
th {{ background:#334155; color:#94a3b8; font-size:12px; text-transform:uppercase; padding:10px 12px; text-align:left; }}
td {{ padding:10px 12px; border-top:1px solid #1e293b; font-size:13px; }}
tr:nth-child(even) {{ background:#1e293b; }}
tr:nth-child(odd) {{ background:#162032; }}
.empty {{ text-align:center; color:#64748b; padding:24px; }}
@media(max-width:600px) {{
  .charts {{ grid-template-columns:1fr; }}
  .kpi-grid {{ grid-template-columns:repeat(2,1fr); }}
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
  <div class="kpi"><div class="value">{avg_check_total:.0f} ₴</div><div class="label">Середній чек</div></div>
  <div class="kpi"><div class="value">{avg_cooking_total} хв</div><div class="label">Час приготування</div></div>
  <div class="kpi"><div class="value">{total_gmv_eur} €</div><div class="label">Дохід (GMV)</div></div>
  <div class="kpi"><div class="value">{total_net_eur} €</div><div class="label">Чистий дохід</div></div>
  <div class="kpi alert"><div class="value">{total_cancelled}</div><div class="label">Скасовано</div></div>
  <div class="kpi alert"><div class="value">{total_bad}</div><div class="label">Скарги / Bad Orders</div></div>
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
    <h3>Дохід (€) по тижнях</h3>
    <canvas id="revenueChart"></canvas>
  </div>
</div>

<div class="section" id="cancelled-section">
  <h2>Скасовані замовлення ({total_cancelled})</h2>
  {"<p class='empty'>Немає скасованих замовлень за цей період</p>" if total_cancelled == 0 else
  "<table><thead><tr><th>Дата</th><th>Order ID</th><th>Order Ref</th><th>Статус</th><th>Причина</th></tr></thead><tbody>" +
  "".join(f"<tr><td>{r.get('order_created_date','')}</td><td>{r.get('order_id','')}</td><td>{r.get('order_reference_id','')}</td><td>{r.get('order_state','')}</td><td>{r.get('reason','')}</td></tr>" for r in cancelled_json) +
  "</tbody></table>"}
</div>

<div class="section" id="bad-section">
  <h2>Замовлення зі скаргами / Bad Orders ({len(bad_json)})</h2>
  {"<p class='empty'>Немає замовлень зі скаргами за цей період</p>" if len(bad_json) == 0 else
  "<table><thead><tr><th>Дата</th><th>Order ID</th><th>Order Ref</th><th>Сума (₴)</th><th>Час прг. (хв)</th><th>Рейтинг</th><th>Тікет</th></tr></thead><tbody>" +
  "".join(f"<tr><td>{r.get('order_created_date','')}</td><td>{r.get('order_id','')}</td><td>{r.get('order_reference_id','')}</td><td>{r.get('gmv_local','')}</td><td>{r.get('cooking_min','')}</td><td>{r.get('rating','—') or '—'}</td><td>{'Так' if r.get('has_ticket') else 'Ні'}</td></tr>" for r in bad_json) +
  "</tbody></table>"}
</div>

<script>
const weeks = {json.dumps(weeks)};
const delivered = {json.dumps(delivered)};
const avgCheck = {json.dumps(avg_check)};
const cookingTime = {json.dumps(cooking)};
const revWeeks = {json.dumps(rev_weeks)};
const gmvEur = {json.dumps(gmv)};
const netEur = {json.dumps(net_income)};

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
    {{ label:'GMV', data:gmvEur, backgroundColor:'#818cf8', borderRadius:4 }},
    {{ label:'Чистий дохід', data:netEur, backgroundColor:'#34d399', borderRadius:4 }}
  ] }}, options:{{ ...barOpts, plugins:{{ legend:{{ display:true, position:'top' }} }} }}
}});
</script>
</body>
</html>"""


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

            revenue = fetch_revenue(conn, pid)
            print(f"  Revenue: {len(revenue)} weeks")

            cancelled = fetch_cancelled(conn, pid)
            print(f"  Cancelled orders: {len(cancelled)}")

            bad = fetch_bad_orders(conn, pid)
            print(f"  Bad orders: {len(bad)}")

            html = generate_html(pid, info, weekly, revenue, cancelled, bad, generated_at)
            out_path = folder / "index.html"
            out_path.write_text(html, encoding="utf-8")
            print(f"  Saved: {out_path}")
    finally:
        conn.close()

    update_index()
    print("\nDone!")


def update_index():
    """Regenerate the root index.html with links to all provider reports."""
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
