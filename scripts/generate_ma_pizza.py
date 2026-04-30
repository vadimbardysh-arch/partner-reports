"""
Generate a multi-store weekly HTML report for MA Pizza (Lviv) by querying Databricks.
Produces ma-pizza/index.html with Smart Promo & Sponsored Listing analytics.
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
WEEKS_BACK = 12

MA_PIZZA_PROVIDERS = {
    51222: {"name": "MA Pizza Пулюя", "short": "Пулюя"},
    51223: {"name": "MA Pizza Тернопільська", "short": "Тернопільська"},
    51224: {"name": "MA Pizza Малоголосківська", "short": "Малоголосківська"},
    51226: {"name": "MA Pizza Залізнична", "short": "Залізнична"},
    51229: {"name": "MA Pizza Івана Франка", "short": "Івана Франка"},
    64470: {"name": "MA Pizza Пасічна", "short": "Пасічна"},
    115837: {"name": "MA Pizza Героїв УПА", "short": "Героїв УПА"},
}

PROVIDER_IDS = ",".join(str(k) for k in MA_PIZZA_PROVIDERS)


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


def fetch_all(conn):
    data = {}
    days = WEEKS_BACK * 7

    print("  → orders...")
    data["orders"] = safe_json(query(conn, f"""
        SELECT DATE_TRUNC('week', order_created_date) AS week_date,
               provider_id,
               COUNT(CASE WHEN order_state='delivered' THEN 1 END) AS delivered,
               COUNT(*) AS total_orders,
               COUNT(CASE WHEN order_state!='delivered' THEN 1 END) AS failed,
               ROUND(AVG(CASE WHEN order_state='delivered' THEN provider_price_before_discount END),0) AS avg_check
        FROM ng_delivery_spark.fact_order_delivery
        WHERE provider_id IN ({PROVIDER_IDS})
          AND order_created_date >= DATE_SUB(CURRENT_DATE(), {days})
        GROUP BY 1,2 ORDER BY 1,2
    """))

    print("  → revenue...")
    data["revenue"] = safe_json(query(conn, f"""
        SELECT DATE_TRUNC('week', m.order_created) AS week_date,
               m.provider_id,
               COUNT(*) AS delivered,
               ROUND(SUM(m.menu_price_full_eur * m.currency_rate),0) AS rev_before,
               ROUND(SUM(m.menu_price_after_discount_eur * m.currency_rate),0) AS rev_after,
               ROUND(SUM(m.provider_commission_gross_eur * m.currency_rate),0) AS bolt_comm,
               ROUND(SUM(m.delivery_price_after_discount_eur * m.currency_rate),0) AS del_fee,
               ROUND(SUM(m.provider_menu_campaign_cost_eur * m.currency_rate),0) AS prov_disc,
               ROUND(SUM(m.bolt_menu_campaign_cost_eur * m.currency_rate),0) AS bolt_disc,
               ROUND(SUM(m.provider_delivery_campaign_cost_eur * m.currency_rate),0) AS prov_del_disc,
               ROUND(SUM(m.bolt_delivery_campaign_cost_eur * m.currency_rate),0) AS bolt_del_disc
        FROM ng_public_spark.etl_delivery_order_monetary_metrics m
        JOIN ng_delivery_spark.fact_order_delivery f ON m.order_id = f.order_id
        WHERE m.provider_id IN ({PROVIDER_IDS})
          AND m.order_created_date >= DATE_FORMAT(DATE_SUB(CURRENT_DATE(), {days + 7}), 'yyyy-MM-dd')
          AND f.order_created_date >= DATE_SUB(CURRENT_DATE(), {days + 7})
          AND f.order_state = 'delivered'
        GROUP BY 1,2 ORDER BY 1,2
    """))

    print("  → ops...")
    data["ops"] = safe_json(query(conn, f"""
        SELECT DATE_TRUNC('week', order_created_date) AS week_date,
               provider_id,
               COUNT(CASE WHEN order_state='delivered' THEN 1 END) AS delivered,
               COUNT(CASE WHEN order_state='delivered' AND is_bad_order=true THEN 1 END) AS bad_orders,
               COUNT(CASE WHEN order_state='delivered' AND has_ticket=true THEN 1 END) AS complaints
        FROM ng_delivery_spark.fact_order_delivery
        WHERE provider_id IN ({PROVIDER_IDS})
          AND order_created_date >= DATE_SUB(CURRENT_DATE(), {days})
        GROUP BY 1,2 ORDER BY 1,2
    """))

    print("  → availability...")
    data["avail"] = safe_json(query(conn, f"""
        SELECT provider_id, date,
               ROUND(availability_rate_last_7d * 100, 1) AS avail_7d,
               ROUND(acceptance_rate_last_7d * 100, 1) AS accept_7d
        FROM ng_public_spark.etl_incentives_provider_targeting_features
        WHERE provider_id IN ({PROVIDER_IDS})
          AND date >= DATE_SUB(CURRENT_DATE(), {days + 7})
          AND DAYOFWEEK(date) = 7
        ORDER BY provider_id, date
    """))

    print("  → campaigns...")
    data["campaigns"] = safe_json(query(conn, f"""
        SELECT DATE_TRUNC('week', order_created_date) AS week_date,
               provider_id, spend_objective,
               COUNT(DISTINCT order_id) AS promo_orders,
               ROUND(SUM(discount_value_local),0) AS total_discount,
               ROUND(SUM(provider_spend_local),0) AS provider_spend,
               ROUND(SUM(bolt_spend_local),0) AS bolt_spend
        FROM ng_public_spark.etl_delivery_campaign_order_metrics
        WHERE provider_id IN ({PROVIDER_IDS})
          AND order_created_date >= DATE_SUB(CURRENT_DATE(), {days})
        GROUP BY 1,2,3 ORDER BY 1,2,3
    """))

    print("  → smart promo check...")
    sp_df = query(conn, f"""
        SELECT COUNT(*) AS cnt
        FROM core_models_spark.fact_provider_smart_promo_offer_campaign_enrollment
        WHERE provider_id IN ({PROVIDER_IDS})
    """)
    data["smart_promo_count"] = int(to_native(sp_df.iloc[0]["cnt"]))

    print("  → order details...")
    data["order_details"] = safe_json(query(conn, f"""
        SELECT f.order_id, f.provider_id,
               DATE(f.order_created_date) AS order_created_date,
               DATE_FORMAT(f.order_created_at, 'HH:mm') AS order_time,
               f.order_state,
               ROUND(f.provider_price_before_discount,0) AS check_before,
               ROUND(f.delivery_price,0) AS del_fee,
               f.is_bolt_plus_order AS bp,
               f.is_bad_order AS bad,
               f.has_ticket AS ticket,
               ROUND(f.actual_delivery_time_min,0) AS del_min,
               p.provider_name
        FROM ng_delivery_spark.fact_order_delivery f
        JOIN ng_delivery_spark.dim_provider_v2 p ON f.provider_id = p.provider_id
        WHERE f.provider_id IN ({PROVIDER_IDS})
          AND f.order_created_date >= DATE_SUB(CURRENT_DATE(), {days})
        ORDER BY f.order_created_date DESC, f.order_created_at DESC
    """))

    print("  → top items...")
    ti_df = query(conn, f"""
        SELECT provider_id, basket_item_name,
               SUM(basket_item_amount) AS qty,
               ROUND(SUM(item_price_before_discount_with_vat_local * basket_item_amount),0) AS rev
        FROM ng_delivery_spark.dim_basket_item_delivery
        WHERE provider_id IN ({PROVIDER_IDS})
          AND basket_item_created_date >= DATE_SUB(CURRENT_DATE(), 28)
          AND order_state = 'delivered'
          AND basket_item_is_dish = true
        GROUP BY 1,2 ORDER BY 1, qty DESC
    """)
    data["top_items"] = safe_json(ti_df.groupby("provider_id").head(10))

    return data


def build_html(data):
    providers_js = {str(k): v["short"] for k, v in MA_PIZZA_PROVIDERS.items()}
    provider_ids_js = sorted(MA_PIZZA_PROVIDERS.keys())

    data_js = json.dumps({
        "providers": providers_js,
        "providerIds": provider_ids_js,
        "weeks": sorted({str(r["week_date"])[:10] for r in data["orders"]}),
        "orders": data["orders"],
        "revenue": data["revenue"],
        "ops": data["ops"],
        "avail": data["avail"],
        "campaigns": data["campaigns"],
        "smartPromoCount": data["smart_promo_count"],
        "orderDetails": data["order_details"],
        "topItems": data["top_items"],
    }, default=str)

    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    # Read the HTML template that was previously stored inline
    # For maintainability, the full HTML is generated inline below
    html = _build_full_html(data_js, now_str)
    return html


def _build_full_html(data_js, now_str):
    return f'''<!DOCTYPE html>
<html lang="uk">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MA PIZZA | тижневий звіт</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<style>
:root{{
 --green:#34D186;--green-bg:rgba(52,209,134,.08);--dark:#1A1D21;
 --bg:#F3F4F6;--card:#FFF;--text:#111827;--text2:#6B7280;--border:#E5E7EB;
 --pos:#10B981;--neg:#EF4444;--warn:#F59E0B;--blue:#3B82F6;--accent:#EF4444;
 --r:12px;--shadow:0 1px 3px rgba(0,0,0,.06),0 1px 2px rgba(0,0,0,.04);
}}
[data-theme=dark]{{
 --bg:#111827;--card:#1F2937;--text:#F9FAFB;--text2:#9CA3AF;--border:#374151;
 --shadow:0 1px 3px rgba(0,0,0,.3);
}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Inter',system-ui,sans-serif;background:var(--bg);color:var(--text);line-height:1.5}}
a{{text-decoration:none;color:inherit}}
.header{{position:sticky;top:0;z-index:102;background:var(--card);border-bottom:1px solid var(--border);padding:12px 24px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px}}
.header-left{{display:flex;align-items:center;gap:12px}}
.header-left h1{{font-size:20px;font-weight:800;letter-spacing:-.3px}}
.brand-dot{{width:10px;height:10px;border-radius:50%;background:var(--accent);display:inline-block}}
.header-right{{display:flex;align-items:center;gap:10px;flex-wrap:wrap}}
.ms-wrap{{position:relative;min-width:180px}}
.ms-btn{{padding:8px 32px 8px 14px;border:1px solid var(--border);border-radius:8px;font-size:13px;font-family:inherit;background:var(--card);cursor:pointer;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:260px;display:block;width:100%;text-align:left;color:var(--text);position:relative}}
.ms-btn::after{{content:'\\25BE';position:absolute;right:10px;top:50%;transform:translateY(-50%);font-size:11px;color:var(--text2)}}
.ms-btn:hover,.ms-btn.open{{border-color:var(--accent)}}
.ms-panel{{display:none;position:absolute;top:calc(100% + 4px);left:0;min-width:100%;max-height:320px;overflow-y:auto;background:var(--card);border:1px solid var(--border);border-radius:8px;box-shadow:0 8px 24px rgba(0,0,0,.12);z-index:1000;padding:4px 0}}
.ms-panel.open{{display:block}}
.ms-item{{display:flex;align-items:center;gap:8px;padding:6px 14px;font-size:13px;cursor:pointer;white-space:nowrap}}
.ms-item:hover{{background:var(--bg)}}
.ms-item input{{accent-color:var(--accent);width:15px;height:15px;cursor:pointer;flex-shrink:0}}
.ms-item.all-item{{border-bottom:1px solid var(--border);padding-bottom:8px;margin-bottom:2px;font-weight:600}}
.reset-btn{{background:transparent;border:1px solid var(--border);color:var(--text2);border-radius:8px;padding:7px 11px;font-size:14px;cursor:pointer;transition:all .15s;line-height:1}}
.reset-btn:hover{{background:var(--neg);color:#fff;border-color:var(--neg)}}
.theme-toggle{{background:transparent;border:1px solid var(--border);color:var(--text2);border-radius:8px;padding:7px 12px;font-size:16px;cursor:pointer;transition:all .15s;line-height:1}}
.theme-toggle:hover{{background:var(--bg);color:var(--text)}}
.last-update{{font-size:12px;color:var(--text2)}}
.main-nav{{position:sticky;top:52px;z-index:100;background:var(--card);border-bottom:1px solid var(--border);display:flex;gap:0;overflow-x:auto;scrollbar-width:none;-webkit-overflow-scrolling:touch;padding:0 16px}}
.main-nav::-webkit-scrollbar{{display:none}}
.nav-link{{padding:12px 16px;font-size:13px;font-weight:500;color:var(--text2);white-space:nowrap;border-bottom:2px solid transparent;transition:all .15s;cursor:pointer}}
.nav-link:hover{{color:var(--text);background:var(--bg)}}
.nav-link.active{{color:var(--accent);border-bottom-color:var(--accent)}}
.week-bar{{position:sticky;top:94px;z-index:99;background:var(--card);border-bottom:1px solid var(--border);display:flex;align-items:center;gap:6px;padding:8px 16px;overflow-x:auto;scrollbar-width:none;-webkit-overflow-scrolling:touch}}
.week-bar::-webkit-scrollbar{{display:none}}
.week-bar-label{{font-size:12px;font-weight:600;color:var(--text2);white-space:nowrap;margin-right:4px}}
.week-pill{{padding:5px 14px;border-radius:20px;font-size:12px;font-weight:500;background:var(--bg);color:var(--text2);cursor:pointer;white-space:nowrap;border:1px solid transparent;transition:all .15s;user-select:none}}
.week-pill:hover{{background:rgba(239,68,68,.08);color:var(--accent)}}
.week-pill.active{{background:var(--accent);color:#fff;border-color:var(--accent)}}
.main-content{{max-width:1360px;margin:0 auto;padding:20px}}
.section{{margin-bottom:32px;display:none}}
.section.visible{{display:block}}
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
.data-table td{{padding:9px 14px;border-bottom:1px solid var(--border)}}
.data-table tr:hover td{{background:rgba(239,68,68,.04)}}
.data-table .num{{text-align:right;font-variant-numeric:tabular-nums}}
.data-table .total-row td{{font-weight:700;background:var(--bg);border-top:2px solid var(--border)}}
.tag{{display:inline-block;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:600}}
.tag.green{{background:rgba(16,185,129,.1);color:#059669}}
.tag.red{{background:rgba(239,68,68,.1);color:#DC2626}}
.tag.blue{{background:rgba(59,130,246,.1);color:#2563EB}}
.tag.orange{{background:rgba(245,158,11,.1);color:#D97706}}
.tag.purple{{background:rgba(139,92,246,.1);color:#7C3AED}}
.filter-row{{display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap;align-items:center}}
.filter-row label{{font-size:12px;font-weight:600;color:var(--text2)}}
.filter-row select{{padding:6px 12px;border:1px solid var(--border);border-radius:8px;font-size:13px;font-family:inherit;background:var(--card);color:var(--text)}}
.top-items-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:16px}}
.top-item-card{{background:var(--card);border-radius:var(--r);box-shadow:var(--shadow);border:1px solid var(--border);padding:18px;overflow:hidden}}
.top-item-card h4{{font-size:14px;font-weight:700;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid var(--border)}}
.top-row{{display:flex;justify-content:space-between;align-items:center;padding:5px 0;font-size:12px;border-bottom:1px solid rgba(0,0,0,.04)}}
.top-row:last-child{{border-bottom:none}}
.top-rank{{width:20px;text-align:center;font-weight:700;color:var(--accent)}}
.top-name{{flex:1;margin:0 8px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.top-qty{{width:45px;text-align:right;font-weight:600}}
.top-rev{{width:65px;text-align:right;color:var(--text2);font-variant-numeric:tabular-nums}}
.promo-status-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:14px;margin-bottom:24px}}
.promo-status-card{{background:var(--card);border-radius:var(--r);padding:16px 20px;box-shadow:var(--shadow);border:1px solid var(--border)}}
.promo-status-card .ps-name{{font-size:14px;font-weight:700;margin-bottom:8px}}
.promo-status-card .ps-row{{display:flex;justify-content:space-between;padding:4px 0;font-size:12px}}
.promo-status-card .ps-label{{color:var(--text2)}}
.alert-box{{background:#FEF2F2;border:2px solid #FECACA;border-radius:12px;padding:20px;margin-bottom:24px;color:#991B1B}}
.alert-box h4{{font-size:15px;font-weight:700;margin-bottom:8px}}
.alert-box p{{font-size:13px;line-height:1.7}}
@media(max-width:900px){{
 .charts-grid{{grid-template-columns:1fr}}
 .kpi-grid{{grid-template-columns:repeat(2,1fr)}}
 .header{{padding:10px 16px}}
}}
</style>
</head>
<body>
<div class="header">
 <div class="header-left"><span class="brand-dot"></span><h1>MA PIZZA — Львів</h1></div>
 <div class="header-right">
  <div class="ms-wrap" id="storeFilter"><button class="ms-btn" id="storeBtnLabel">Всі заклади</button><div class="ms-panel" id="storePanel"></div></div>
  <button class="reset-btn" onclick="resetFilters()" title="Скинути фільтри">&#10005;</button>
  <button class="theme-toggle" onclick="toggleTheme()">&#x1F319;</button>
  <span class="last-update">Оновлено: {now_str}</span>
 </div>
</div>
<nav class="main-nav" id="mainNav">
 <span class="nav-link active" data-tab="overview">Огляд</span>
 <span class="nav-link" data-tab="orders">Замовлення</span>
 <span class="nav-link" data-tab="ops">Операції</span>
 <span class="nav-link" data-tab="stores">Деталі закладів</span>
 <span class="nav-link" data-tab="revenue">Дохідність</span>
 <span class="nav-link" data-tab="campaigns">Кампанії</span>
 <span class="nav-link" data-tab="smartpromo">Smart Promo & SL</span>
 <span class="nav-link" data-tab="orderdetails">Деталі замовлень</span>
 <span class="nav-link" data-tab="complaints">Скарги</span>
 <span class="nav-link" data-tab="cancelled">Скасовані</span>
 <span class="nav-link" data-tab="topitems">Топ позиції</span>
</nav>
<div class="week-bar" id="weekBar"></div>
<div class="main-content">
<div class="section visible" id="sec-overview"><div class="kpi-grid" id="kpiGrid"></div><div class="charts-grid"><div class="chart-card"><h3>Замовлення по тижнях</h3><div class="chart-wrap"><canvas id="chartOrders"></canvas></div></div><div class="chart-card"><h3>Середній чек (&#8372;) по тижнях</h3><div class="chart-wrap"><canvas id="chartAvgCheck"></canvas></div></div></div></div>
<div class="section" id="sec-orders"><div class="section-title"><span class="section-icon">&#128230;</span> Замовлення</div><div class="charts-grid"><div class="chart-card"><h3>Замовлення по тижнях</h3><div class="chart-wrap"><canvas id="chartOrdersFull"></canvas></div></div><div class="chart-card"><h3>Середній чек (&#8372;) по тижнях</h3><div class="chart-wrap"><canvas id="chartAvgCheckFull"></canvas></div></div></div></div>
<div class="section" id="sec-ops"><div class="section-title"><span class="section-icon">&#9881;&#65039;</span> Операційні показники</div><div class="charts-grid"><div class="chart-card"><h3>Доступність та Прийняття (%)</h3><div class="chart-wrap"><canvas id="chartAvail"></canvas></div></div><div class="chart-card"><h3>Рівень поганих замовлень (%)</h3><div class="chart-wrap"><canvas id="chartBadOrder"></canvas></div></div></div></div>
<div class="section" id="sec-stores"><div class="section-title"><span class="section-icon">&#127978;</span> Деталі по закладах</div><div class="table-wrap"><table class="data-table" id="storeTable"></table></div></div>
<div class="section" id="sec-revenue"><div class="section-title"><span class="section-icon">&#128176;</span> Дохідність по тижнях</div><div class="charts-grid" style="margin-bottom:20px"><div class="chart-card"><h3>Дохід по тижнях (&#8372;)</h3><div class="chart-wrap"><canvas id="chartRevenue"></canvas></div></div><div class="chart-card"><h3>Витрати на знижки по тижнях (&#8372;)</h3><div class="chart-wrap"><canvas id="chartDiscounts"></canvas></div></div></div><div class="table-wrap"><table class="data-table" id="revTable"></table></div></div>
<div class="section" id="sec-campaigns"><div class="section-title"><span class="section-icon">&#127919;</span> Кампанії по типах</div><div class="charts-grid" style="margin-bottom:20px"><div class="chart-card"><h3>Витрати партнера на кампанії (&#8372;)</h3><div class="chart-wrap"><canvas id="chartProvDisc"></canvas></div></div><div class="chart-card"><h3>Витрати Bolt на кампанії (&#8372;)</h3><div class="chart-wrap"><canvas id="chartBoltDisc"></canvas></div></div></div><div class="section-title"><span class="section-icon">&#128202;</span> Кампанії — деталі по типах</div><div class="table-wrap"><table class="data-table" id="campTable"></table></div></div>
<div class="section" id="sec-smartpromo"><div class="section-title"><span class="section-icon">&#128640;</span> Smart Promo & Sponsored Listing</div><div id="smartPromoContent"></div></div>
<div class="section" id="sec-orderdetails"><div class="section-title"><span class="section-icon">&#129534;</span> Дохідність по замовленнях</div><div class="filter-row"><label>Bolt Plus:</label><select id="filterBP"><option value="all">Всі</option><option value="yes">Bolt Plus</option><option value="no">Без Bolt Plus</option></select><label>Статус:</label><select id="filterStatus"><option value="all">Всі</option><option value="delivered">Доставлені</option><option value="failed">Невдалі / Скасовані</option></select></div><div class="table-wrap" style="max-height:600px;overflow-y:auto"><table class="data-table" id="orderTable"></table></div></div>
<div class="section" id="sec-complaints"><div class="section-title"><span class="section-icon">&#9888;&#65039;</span> Замовлення зі скаргами</div><div class="table-wrap" style="max-height:600px;overflow-y:auto"><table class="data-table" id="complaintTable"></table></div></div>
<div class="section" id="sec-cancelled"><div class="section-title"><span class="section-icon">&#10060;</span> Скасовані замовлення</div><div class="table-wrap" style="max-height:600px;overflow-y:auto"><table class="data-table" id="cancelledTable"></table></div></div>
<div class="section" id="sec-topitems"><div class="section-title"><span class="section-icon">&#127829;</span> Топ-10 позицій по закладах</div><p style="font-size:13px;color:var(--text2);margin-bottom:16px">Найпопулярніші позиції за останні 4 тижні.</p><div class="top-items-grid" id="topItemsGrid"></div></div>
</div>
<script>
const RAW={data_js};
const COLORS=['#EF4444','#3B82F6','#10B981','#F59E0B','#8B5CF6','#EC4899','#14B8A6'];
const PROV=RAW.providers;const PIDS=RAW.providerIds;
let selectedStores=new Set(PIDS.map(String));let selectedWeek=null;let activeTab='overview';let charts={{}};
function n(v){{return v==null?0:Number(v)}}function fmt(v){{if(v==null)return'\\u2014';let x=Number(v);return isNaN(x)?'\\u2014':x.toLocaleString('uk-UA')}}
function weekLabel(w){{let d=new Date(w+'T00:00:00');let e=new Date(d);e.setDate(e.getDate()+6);return d.toLocaleDateString('uk-UA',{{day:'numeric',month:'short'}})+' \\u2014 '+e.toLocaleDateString('uk-UA',{{day:'numeric',month:'short'}})}}
function initStoreFilter(){{const panel=document.getElementById('storePanel');const btn=document.getElementById('storeBtnLabel');let html='<div class="ms-item all-item"><input type="checkbox" id="cbAll" checked><label for="cbAll">Всі заклади</label></div>';PIDS.forEach(pid=>{{html+=`<div class="ms-item"><input type="checkbox" id="cb${{pid}}" value="${{pid}}" checked><label for="cb${{pid}}">${{PROV[pid]}}</label></div>`}});panel.innerHTML=html;btn.onclick=()=>panel.classList.toggle('open');document.addEventListener('click',e=>{{if(!document.getElementById('storeFilter').contains(e.target))panel.classList.remove('open')}});panel.querySelectorAll('input').forEach(cb=>{{cb.addEventListener('change',()=>{{if(cb.id==='cbAll')panel.querySelectorAll('input:not(#cbAll)').forEach(c=>c.checked=cb.checked);else document.getElementById('cbAll').checked=[...panel.querySelectorAll('input:not(#cbAll)')].every(c=>c.checked);selectedStores=new Set([...panel.querySelectorAll('input:not(#cbAll):checked')].map(c=>c.value));updateLabel();render()}})}}); function updateLabel(){{if(selectedStores.size===PIDS.length)btn.textContent='Всі заклади';else if(selectedStores.size===0)btn.textContent='Оберіть заклад';else if(selectedStores.size<=2)btn.textContent=[...selectedStores].map(id=>PROV[id]).join(', ');else btn.textContent=selectedStores.size+' закладів'}}}}
function initWeekBar(){{const bar=document.getElementById('weekBar');let html='<span class="week-bar-label">Тиждень:</span>';RAW.weeks.forEach(w=>{{html+=`<span class="week-pill${{selectedWeek===w?' active':''}}" data-week="${{w}}">${{weekLabel(w)}}</span>`}});bar.innerHTML=html;bar.querySelectorAll('.week-pill').forEach(pill=>{{pill.onclick=()=>{{if(selectedWeek===pill.dataset.week)selectedWeek=null;else selectedWeek=pill.dataset.week;bar.querySelectorAll('.week-pill').forEach(p=>p.classList.remove('active'));if(selectedWeek)pill.classList.add('active');render()}}}});if(bar.lastElementChild)bar.lastElementChild.scrollIntoView({{inline:'end',block:'nearest'}})}}
function initNav(){{document.querySelectorAll('.nav-link').forEach(link=>{{link.onclick=()=>{{document.querySelectorAll('.nav-link').forEach(l=>l.classList.remove('active'));link.classList.add('active');activeTab=link.dataset.tab;document.querySelectorAll('.section').forEach(s=>s.classList.remove('visible'));document.getElementById('sec-'+activeTab).classList.add('visible');render()}}}})}}
function filt(arr){{return arr.filter(r=>selectedStores.has(String(r.provider_id)))}}function filtWeek(arr,ws){{if(!ws)return filt(arr);let ww=Array.isArray(ws)?ws:[ws];return arr.filter(r=>selectedStores.has(String(r.provider_id))&&ww.includes((r.week_date||r.date||'').substring(0,10)))}}function getWeeks(){{return selectedWeek?[selectedWeek]:RAW.weeks}}function destroyChart(id){{if(charts[id]){{charts[id].destroy();delete charts[id]}}}}function addDays(ds,d){{let dt=new Date(ds+'T00:00:00');dt.setDate(dt.getDate()+d);return dt.toISOString().substring(0,10)}}
function makeLineChart(cid,labels,datasets){{destroyChart(cid);const ctx=document.getElementById(cid);if(!ctx)return;charts[cid]=new Chart(ctx,{{type:'line',data:{{labels,datasets}},options:{{responsive:true,maintainAspectRatio:false,interaction:{{mode:'index',intersect:false}},plugins:{{legend:{{position:'bottom',labels:{{boxWidth:12,padding:10,font:{{size:11}}}}}}}},scales:{{x:{{grid:{{display:false}},ticks:{{font:{{size:11}},maxRotation:45}}}},y:{{beginAtZero:true,grid:{{color:'rgba(0,0,0,.06)'}},ticks:{{font:{{size:11}}}}}}}}}}}})}}
function makeBarChart(cid,labels,datasets){{destroyChart(cid);const ctx=document.getElementById(cid);if(!ctx)return;charts[cid]=new Chart(ctx,{{type:'bar',data:{{labels,datasets}},options:{{responsive:true,maintainAspectRatio:false,interaction:{{mode:'index',intersect:false}},plugins:{{legend:{{position:'bottom',labels:{{boxWidth:12,padding:10,font:{{size:11}}}}}}}},scales:{{x:{{stacked:true,grid:{{display:false}},ticks:{{font:{{size:11}},maxRotation:45}}}},y:{{stacked:true,beginAtZero:true,grid:{{color:'rgba(0,0,0,.06)'}},ticks:{{font:{{size:11}}}}}}}}}}}})}}
function renderKPI(){{const ws=getWeeks();const od=filtWeek(RAW.orders,ws),rv=filtWeek(RAW.revenue,ws),op=filtWeek(RAW.ops,ws);const nw=ws.length||1;const tD=od.reduce((s,r)=>s+n(r.delivered),0),tAll=od.reduce((s,r)=>s+n(r.total_orders),0),tF=od.reduce((s,r)=>s+n(r.failed),0),tR=rv.reduce((s,r)=>s+n(r.rev_before),0),tBad=op.reduce((s,r)=>s+n(r.bad_orders),0),tComp=op.reduce((s,r)=>s+n(r.complaints),0);const avgC=tD?Math.round(tR/tD):0,pW=Math.round(tD/nw*10)/10,fR=tAll?Math.round(tF/tAll*1000)/10:0,bR=tD?Math.round(tBad/tD*1000)/10:0;document.getElementById('kpiGrid').innerHTML=`<div class="kpi-card"><div class="kpi-label">Замовлень (delivered)</div><div class="kpi-value">${{fmt(tD)}}</div><div class="kpi-change neutral">${{pW}} / тиж</div></div><div class="kpi-card"><div class="kpi-label">Середній чек</div><div class="kpi-value">${{fmt(avgC)}} \\u20B4</div></div><div class="kpi-card"><div class="kpi-label">Виручка</div><div class="kpi-value">${{fmt(tR)}} \\u20B4</div></div><div class="kpi-card"><div class="kpi-label">Fail Rate</div><div class="kpi-value" style="color:${{fR>10?'var(--neg)':fR>5?'var(--warn)':'var(--pos)'}}">${{fR}}%</div></div><div class="kpi-card"><div class="kpi-label">Bad Order Rate</div><div class="kpi-value" style="color:${{bR>10?'var(--neg)':bR>5?'var(--warn)':'var(--pos)'}}">${{bR}}%</div></div><div class="kpi-card"><div class="kpi-label">Smart Promo</div><div class="kpi-value" style="color:var(--warn)">${{RAW.smartPromoCount>0?'\\u2705 Активний':'\\u274C Не активний'}}</div><div class="kpi-change ${{RAW.smartPromoCount>0?'up':'down'}}">${{RAW.smartPromoCount}} кампаній</div></div><div class="kpi-card"><div class="kpi-label">Скарг</div><div class="kpi-value">${{tComp}}</div></div>`}}
function renderOrderCharts(prefix){{const labels=RAW.weeks.map(weekLabel);const c1=prefix?'chartOrdersFull':'chartOrders',c2=prefix?'chartAvgCheckFull':'chartAvgCheck';if(selectedStores.size<=3&&selectedStores.size>0){{const ds1=[],ds2=[];[...selectedStores].forEach((pid,i)=>{{ds1.push({{label:PROV[pid],data:RAW.weeks.map(w=>{{const r=RAW.orders.find(x=>x.week_date.substring(0,10)===w&&x.provider_id==pid);return r?n(r.delivered):0}}),borderColor:COLORS[i%COLORS.length],backgroundColor:COLORS[i%COLORS.length]+'33',tension:.3,borderWidth:2,pointRadius:3,fill:false}});ds2.push({{label:PROV[pid],data:RAW.weeks.map(w=>{{const r=RAW.orders.find(x=>x.week_date.substring(0,10)===w&&x.provider_id==pid);return r&&n(r.avg_check)?n(r.avg_check):null}}),borderColor:COLORS[i%COLORS.length],tension:.3,borderWidth:2,pointRadius:3,fill:false}})}});makeLineChart(c1,labels,ds1);makeLineChart(c2,labels,ds2)}}else{{const aggW={{}};RAW.weeks.forEach(w=>{{const wd=filtWeek(RAW.orders,[w]);aggW[w]={{d:wd.reduce((s,r)=>s+n(r.delivered),0),ac:wd.reduce((s,r)=>s+n(r.delivered),0)?Math.round(wd.filter(r=>n(r.avg_check)>0).reduce((s,r)=>s+n(r.avg_check)*n(r.delivered),0)/wd.reduce((s,r)=>s+n(r.delivered),0)):0}}}});makeLineChart(c1,labels,[{{label:'Замовлення',data:RAW.weeks.map(w=>aggW[w].d),borderColor:'#EF4444',backgroundColor:'rgba(239,68,68,.1)',tension:.3,borderWidth:2,pointRadius:4,fill:true}}]);makeLineChart(c2,labels,[{{label:'Сер. чек \\u20B4',data:RAW.weeks.map(w=>aggW[w].ac||null),borderColor:'#3B82F6',backgroundColor:'rgba(59,130,246,.1)',tension:.3,borderWidth:2,pointRadius:4,fill:true}}])}}}}
function renderOpsCharts(){{const labels=RAW.weeks.map(weekLabel);const sats=RAW.weeks.map(w=>{{let d=new Date(w+'T00:00:00');d.setDate(d.getDate()+5);return d.toISOString().substring(0,10)}});if(selectedStores.size<=3&&selectedStores.size>0){{const dsA=[],dsAc=[],dsB=[];[...selectedStores].forEach((pid,i)=>{{dsA.push({{label:PROV[pid]+' \\u2014 Дост.',data:sats.map(s=>{{const r=RAW.avail.find(x=>x.date.substring(0,10)===s&&x.provider_id==pid);return r?n(r.avail_7d):null}}),borderColor:COLORS[i%COLORS.length],tension:.3,borderWidth:2,pointRadius:3,fill:false}});dsAc.push({{label:PROV[pid]+' \\u2014 Прийн.',data:sats.map(s=>{{const r=RAW.avail.find(x=>x.date.substring(0,10)===s&&x.provider_id==pid);return r&&r.accept_7d!=null?n(r.accept_7d):null}}),borderColor:COLORS[i%COLORS.length],borderDash:[5,5],tension:.3,borderWidth:2,pointRadius:3,fill:false}});dsB.push({{label:PROV[pid],data:RAW.weeks.map(w=>{{const o=RAW.ops.find(x=>x.week_date.substring(0,10)===w&&x.provider_id==pid);return o&&n(o.delivered)?Math.round(n(o.bad_orders)/n(o.delivered)*1000)/10:null}}),borderColor:COLORS[i%COLORS.length],tension:.3,borderWidth:2,pointRadius:3,fill:false}})}});makeLineChart('chartAvail',labels,[...dsA,...dsAc]);makeLineChart('chartBadOrder',labels,dsB)}}else{{const aA=sats.map(s=>{{const rr=RAW.avail.filter(x=>x.date.substring(0,10)===s&&selectedStores.has(String(x.provider_id))&&x.avail_7d!=null);return rr.length?Math.round(rr.reduce((s,r)=>s+n(r.avail_7d),0)/rr.length*10)/10:null}});const aAc=sats.map(s=>{{const rr=RAW.avail.filter(x=>x.date.substring(0,10)===s&&selectedStores.has(String(x.provider_id))&&x.accept_7d!=null);return rr.length?Math.round(rr.reduce((s,r)=>s+n(r.accept_7d),0)/rr.length*10)/10:null}});const bR=RAW.weeks.map(w=>{{const rr=RAW.ops.filter(x=>x.week_date.substring(0,10)===w&&selectedStores.has(String(x.provider_id)));const d=rr.reduce((s,r)=>s+n(r.delivered),0),b=rr.reduce((s,r)=>s+n(r.bad_orders),0);return d?Math.round(b/d*1000)/10:null}});makeLineChart('chartAvail',labels,[{{label:'Доступність %',data:aA,borderColor:'#10B981',backgroundColor:'rgba(16,185,129,.1)',tension:.3,borderWidth:2,pointRadius:4,fill:true}},{{label:'Прийняття %',data:aAc,borderColor:'#3B82F6',backgroundColor:'rgba(59,130,246,.1)',tension:.3,borderWidth:2,pointRadius:4,fill:true}}]);makeLineChart('chartBadOrder',labels,[{{label:'Bad Order %',data:bR,borderColor:'#EF4444',backgroundColor:'rgba(239,68,68,.1)',tension:.3,borderWidth:2,pointRadius:4,fill:true}}])}}}}
function renderStoreTable(){{const ws=getWeeks(),nW=ws.length;let html='<thead><tr><th>Заклад</th><th class="num">Зам/тиж</th><th class="num">Delivered</th><th class="num">Failed</th><th class="num">Сер.чек \\u20B4</th><th class="num">Виручка \\u20B4</th><th class="num">Доступність</th><th class="num">Bad %</th></tr></thead><tbody>';let tD=0,tF=0,tR=0;[...selectedStores].sort((a,b)=>a-b).forEach(pid=>{{const od=filtWeek(RAW.orders,ws).filter(r=>r.provider_id==pid),rv=filtWeek(RAW.revenue,ws).filter(r=>r.provider_id==pid),op=filtWeek(RAW.ops,ws).filter(r=>r.provider_id==pid);const del=od.reduce((s,r)=>s+n(r.delivered),0),fail=od.reduce((s,r)=>s+n(r.failed),0),rev=rv.reduce((s,r)=>s+n(r.rev_before),0),bad=op.reduce((s,r)=>s+n(r.bad_orders),0);const avgC=del?Math.round(rev/del):0,pW=Math.round(del/nW*10)/10,badP=del?Math.round(bad/del*1000)/10:0;const la=RAW.avail.filter(r=>r.provider_id==pid).sort((a,b)=>b.date.localeCompare(a.date))[0];const av=la?(la.avail_7d!=null?la.avail_7d+'%':'\\u2014'):'\\u2014';tD+=del;tF+=fail;tR+=rev;html+=`<tr><td><strong>${{PROV[pid]}}</strong></td><td class="num">${{pW}}</td><td class="num">${{del}}</td><td class="num">${{fail||'\\u2014'}}</td><td class="num">${{fmt(avgC)}}</td><td class="num">${{fmt(rev)}}</td><td class="num">${{av}}</td><td class="num" style="color:${{badP>10?'var(--neg)':badP>5?'var(--warn)':'inherit'}}">${{badP}}%</td></tr>`}});html+=`<tr class="total-row"><td>РАЗОМ</td><td class="num">${{Math.round(tD/nW*10)/10}}</td><td class="num">${{tD}}</td><td class="num">${{tF}}</td><td class="num">${{tD?fmt(Math.round(tR/tD)):'\\u2014'}}</td><td class="num">${{fmt(tR)}}</td><td class="num">\\u2014</td><td class="num">\\u2014</td></tr></tbody>`;document.getElementById('storeTable').innerHTML=html}}
function renderRevenueCharts(){{const labels=RAW.weeks.map(weekLabel);if(selectedStores.size<=5&&selectedStores.size>0){{const ds=[],dd=[];[...selectedStores].sort((a,b)=>a-b).forEach((pid,i)=>{{ds.push({{label:PROV[pid],data:RAW.weeks.map(w=>{{const r=RAW.revenue.find(x=>x.week_date.substring(0,10)===w&&x.provider_id==pid);return r?n(r.rev_before):0}}),backgroundColor:COLORS[i%COLORS.length]+'99',borderColor:COLORS[i%COLORS.length],borderWidth:1}});dd.push({{label:PROV[pid],data:RAW.weeks.map(w=>{{const r=RAW.revenue.find(x=>x.week_date.substring(0,10)===w&&x.provider_id==pid);return r?n(r.prov_disc)+n(r.prov_del_disc):0}}),backgroundColor:COLORS[i%COLORS.length]+'99',borderColor:COLORS[i%COLORS.length],borderWidth:1}})}});makeBarChart('chartRevenue',labels,ds);makeBarChart('chartDiscounts',labels,dd)}}else{{const tRW=RAW.weeks.map(w=>filt(RAW.revenue).filter(r=>r.week_date.substring(0,10)===w).reduce((s,r)=>s+n(r.rev_before),0)),tDW=RAW.weeks.map(w=>filt(RAW.revenue).filter(r=>r.week_date.substring(0,10)===w).reduce((s,r)=>s+n(r.prov_disc)+n(r.prov_del_disc),0));makeBarChart('chartRevenue',labels,[{{label:'Виручка \\u20B4',data:tRW,backgroundColor:'#EF444499',borderColor:'#EF4444',borderWidth:1}}]);makeBarChart('chartDiscounts',labels,[{{label:'Витрати партнера \\u20B4',data:tDW,backgroundColor:'#F59E0B99',borderColor:'#F59E0B',borderWidth:1}}])}}}}
function renderRevenueTable(){{const ws=getWeeks();let html='<thead><tr><th>Заклад</th><th class="num">Зам.</th><th class="num">Виручка \\u20B4</th><th class="num">Після знижок \\u20B4</th><th class="num">Комісія Bolt \\u20B4</th><th class="num">Дост. \\u20B4</th><th class="num">Знижки партнера \\u20B4</th><th class="num">Знижки Bolt \\u20B4</th></tr></thead><tbody>';let t={{d:0,rb:0,ra:0,c:0,df:0,pd:0,bd:0}};[...selectedStores].sort((a,b)=>a-b).forEach(pid=>{{const rd=filtWeek(RAW.revenue,ws).filter(r=>r.provider_id==pid);const d=rd.reduce((s,r)=>s+n(r.delivered),0),rb=rd.reduce((s,r)=>s+n(r.rev_before),0),ra=rd.reduce((s,r)=>s+n(r.rev_after),0),c=rd.reduce((s,r)=>s+n(r.bolt_comm),0),df=rd.reduce((s,r)=>s+n(r.del_fee),0),pd=rd.reduce((s,r)=>s+n(r.prov_disc)+n(r.prov_del_disc),0),bd=rd.reduce((s,r)=>s+n(r.bolt_disc)+n(r.bolt_del_disc),0);t.d+=d;t.rb+=rb;t.ra+=ra;t.c+=c;t.df+=df;t.pd+=pd;t.bd+=bd;html+=`<tr><td><strong>${{PROV[pid]}}</strong></td><td class="num">${{d}}</td><td class="num">${{fmt(rb)}}</td><td class="num">${{fmt(ra)}}</td><td class="num">${{fmt(c)}}</td><td class="num">${{fmt(df)}}</td><td class="num">${{fmt(pd)}}</td><td class="num">${{fmt(bd)}}</td></tr>`}});html+=`<tr class="total-row"><td>РАЗОМ</td><td class="num">${{t.d}}</td><td class="num">${{fmt(t.rb)}}</td><td class="num">${{fmt(t.ra)}}</td><td class="num">${{fmt(t.c)}}</td><td class="num">${{fmt(t.df)}}</td><td class="num">${{fmt(t.pd)}}</td><td class="num">${{fmt(t.bd)}}</td></tr></tbody>`;document.getElementById('revTable').innerHTML=html}}
const OBJ_LABELS={{'provider_campaign_portal':'Portal (партнер)','provider_campaign_obligations_commitments':'Obligations','bolt_plus_campaign':'Bolt Plus','marketing_3rd_party_partnership':'3rd Party (Visa)','engagement':'Engagement','activation':'Activation','acquisition':'Acquisition','reactivation':'Reactivation','other':'Інше'}};
const OBJ_COLORS={{'provider_campaign_portal':'#F59E0B','provider_campaign_obligations_commitments':'#3B82F6','bolt_plus_campaign':'#8B5CF6','marketing_3rd_party_partnership':'#EC4899','engagement':'#10B981','activation':'#14B8A6','acquisition':'#6366F1','reactivation':'#F97316','other':'#6B7280'}};
function renderCampaignCharts(){{const labels=RAW.weeks.map(weekLabel);const objs=[...new Set(RAW.campaigns.map(r=>r.spend_objective))];const dsP=[],dsB=[];objs.forEach(obj=>{{dsP.push({{label:OBJ_LABELS[obj]||obj,data:RAW.weeks.map(w=>filt(RAW.campaigns).filter(r=>r.week_date.substring(0,10)===w&&r.spend_objective===obj).reduce((s,r)=>s+n(r.provider_spend),0)),backgroundColor:(OBJ_COLORS[obj]||'#999')+'99',borderColor:OBJ_COLORS[obj]||'#999',borderWidth:1}});dsB.push({{label:OBJ_LABELS[obj]||obj,data:RAW.weeks.map(w=>filt(RAW.campaigns).filter(r=>r.week_date.substring(0,10)===w&&r.spend_objective===obj).reduce((s,r)=>s+n(r.bolt_spend),0)),backgroundColor:(OBJ_COLORS[obj]||'#999')+'99',borderColor:OBJ_COLORS[obj]||'#999',borderWidth:1}})}});makeBarChart('chartProvDisc',labels,dsP);makeBarChart('chartBoltDisc',labels,dsB)}}
function renderCampaignTable(){{const ws=getWeeks();const cd=filtWeek(RAW.campaigns,ws);const byObj={{}};cd.forEach(r=>{{const k=r.spend_objective;if(!byObj[k])byObj[k]={{orders:0,disc:0,prov:0,bolt:0}};byObj[k].orders+=n(r.promo_orders);byObj[k].disc+=n(r.total_discount);byObj[k].prov+=n(r.provider_spend);byObj[k].bolt+=n(r.bolt_spend)}});let html='<thead><tr><th>Тип кампанії</th><th class="num">Промо замовлення</th><th class="num">Знижка \\u20B4</th><th class="num">Партнер \\u20B4</th><th class="num">Bolt \\u20B4</th></tr></thead><tbody>';let tO=0,tD=0,tP=0,tB=0;Object.entries(byObj).sort((a,b)=>b[1].orders-a[1].orders).forEach(([k,v])=>{{html+=`<tr><td><strong>${{OBJ_LABELS[k]||k}}</strong></td><td class="num">${{v.orders}}</td><td class="num">${{fmt(v.disc)}}</td><td class="num">${{fmt(v.prov)}}</td><td class="num">${{fmt(v.bolt)}}</td></tr>`;tO+=v.orders;tD+=v.disc;tP+=v.prov;tB+=v.bolt}});html+=`<tr class="total-row"><td>РАЗОМ</td><td class="num">${{tO}}</td><td class="num">${{fmt(tD)}}</td><td class="num">${{fmt(tP)}}</td><td class="num">${{fmt(tB)}}</td></tr></tbody>`;document.getElementById('campTable').innerHTML=html}}
function renderSmartPromo(){{const ws=getWeeks(),nW=ws.length;let html='';html+='<div class="alert-box"><h4>\\u{1F6AB} Smart Promo \\u2014 НЕ АКТИВНИЙ на жодній точці</h4><p>За всю історію жоден заклад MA Pizza не використовував Smart Promo. Це означає, що партнер втрачає можливість автоматичного таргетування клієнтів зі знижками. Smart Promo дає в середньому <strong>+20\\u201330% апліфт замовлень</strong> при активації. Рекомендуємо активувати на всіх 7 точках.</p></div>';html+='<div class="alert-box"><h4>\\u{1F6AB} Sponsored Listing \\u2014 НІКОЛИ не використовувався</h4><p>Жоден заклад MA Pizza не використовував Sponsored Listings. Це інструмент платної видимості у додатку Bolt Food \\u2014 заклад з\\\'являється вище у пошуку та на головній сторінці. Конкуренти у Львові скоріше за все вже використовують цей інструмент. Рекомендуємо тестувати з бюджетом <strong>400\\u2013600 \\u20B4/тиждень на точку</strong>.</p></div>';html+='<div class="section-title" style="margin-top:8px"><span class="section-icon">\\u{1F4CD}</span> Статус промо по точках</div><div class="promo-status-grid">';[...selectedStores].sort((a,b)=>a-b).forEach(pid=>{{const od=filtWeek(RAW.orders,ws).filter(r=>r.provider_id==pid);const del=od.reduce((s,r)=>s+n(r.delivered),0);const avgC=del?Math.round(od.filter(r=>n(r.avg_check)>0).reduce((s,r)=>s+n(r.avg_check)*n(r.delivered),0)/del):0;const cd=filtWeek(RAW.campaigns,ws).filter(r=>r.provider_id==pid);const hasPortal=cd.some(r=>r.spend_objective==='provider_campaign_portal');const hasObl=cd.some(r=>r.spend_objective==='provider_campaign_obligations_commitments');const provSpend=cd.reduce((s,r)=>s+n(r.provider_spend),0);const potentialSP10=Math.round(avgC*0.1*(del/nW)*2);html+=`<div class="promo-status-card"><div class="ps-name">${{PROV[pid]}}</div><div class="ps-row"><span class="ps-label">Smart Promo</span><span class="tag red">Не активний</span></div><div class="ps-row"><span class="ps-label">Sponsored Listing</span><span class="tag red">Не активний</span></div><div class="ps-row"><span class="ps-label">Portal кампанії</span><span class="tag ${{hasPortal?'green':'orange'}}">${{hasPortal?'Активні':'Ні'}}</span></div><div class="ps-row"><span class="ps-label">Obligations</span><span class="tag ${{hasObl?'green':'orange'}}">${{hasObl?'Активні':'Ні'}}</span></div><div class="ps-row"><span class="ps-label">Витрати партнера</span><span style="font-weight:600">${{fmt(provSpend)}} \\u20B4</span></div><div class="ps-row"><span class="ps-label">Зам/тиж</span><span style="font-weight:600">${{Math.round(del/nW*10)/10}}</span></div><div class="ps-row"><span class="ps-label">Сер.чек</span><span style="font-weight:600">${{fmt(avgC)}} \\u20B4</span></div><div class="ps-row"><span class="ps-label">Потенціал SP 10% / 2тиж</span><span style="font-weight:600;color:var(--accent)">~${{fmt(potentialSP10)}} \\u20B4</span></div></div>`}});html+='</div>';html+='<div class="section-title" style="margin-top:16px"><span class="section-icon">\\u{1F4CA}</span> Замовлення з промо по типах</div><div class="table-wrap"><table class="data-table"><thead><tr><th>Заклад</th><th class="num">Portal</th><th class="num">Obligations</th><th class="num">Bolt Plus</th><th class="num">Engagement</th><th class="num">3rd Party</th><th class="num">Інше</th><th class="num">Разом промо</th><th class="num">Всього зам.</th><th class="num">% з промо</th></tr></thead><tbody>';let gt={{p:0,o:0,bp:0,e:0,tp:0,ot:0,pr:0,al:0}};[...selectedStores].sort((a,b)=>a-b).forEach(pid=>{{const cd=filtWeek(RAW.campaigns,ws).filter(r=>r.provider_id==pid),od=filtWeek(RAW.orders,ws).filter(r=>r.provider_id==pid);const allDel=od.reduce((s,r)=>s+n(r.delivered),0);const p=cd.filter(r=>r.spend_objective==='provider_campaign_portal').reduce((s,r)=>s+n(r.promo_orders),0),o=cd.filter(r=>r.spend_objective==='provider_campaign_obligations_commitments').reduce((s,r)=>s+n(r.promo_orders),0),bp=cd.filter(r=>r.spend_objective==='bolt_plus_campaign').reduce((s,r)=>s+n(r.promo_orders),0),e=cd.filter(r=>['engagement','activation','acquisition','reactivation'].includes(r.spend_objective)).reduce((s,r)=>s+n(r.promo_orders),0),tp=cd.filter(r=>r.spend_objective==='marketing_3rd_party_partnership').reduce((s,r)=>s+n(r.promo_orders),0),ot=cd.filter(r=>r.spend_objective==='other').reduce((s,r)=>s+n(r.promo_orders),0),pr=p+o+bp+e+tp+ot,pct=allDel?Math.round(pr/allDel*1000)/10:0;gt.p+=p;gt.o+=o;gt.bp+=bp;gt.e+=e;gt.tp+=tp;gt.ot+=ot;gt.pr+=pr;gt.al+=allDel;html+=`<tr><td><strong>${{PROV[pid]}}</strong></td><td class="num">${{p||'\\u2014'}}</td><td class="num">${{o||'\\u2014'}}</td><td class="num">${{bp||'\\u2014'}}</td><td class="num">${{e||'\\u2014'}}</td><td class="num">${{tp||'\\u2014'}}</td><td class="num">${{ot||'\\u2014'}}</td><td class="num" style="font-weight:600">${{pr}}</td><td class="num">${{allDel}}</td><td class="num">${{pct}}%</td></tr>`}});const gPct=gt.al?Math.round(gt.pr/gt.al*1000)/10:0;html+=`<tr class="total-row"><td>РАЗОМ</td><td class="num">${{gt.p}}</td><td class="num">${{gt.o}}</td><td class="num">${{gt.bp}}</td><td class="num">${{gt.e}}</td><td class="num">${{gt.tp}}</td><td class="num">${{gt.ot}}</td><td class="num">${{gt.pr}}</td><td class="num">${{gt.al}}</td><td class="num">${{gPct}}%</td></tr></tbody></table></div>`;document.getElementById('smartPromoContent').innerHTML=html}}
function renderOrderDetails(){{const ws=getWeeks();const bpF=document.getElementById('filterBP').value,stF=document.getElementById('filterStatus').value;let rows=RAW.orderDetails.filter(r=>selectedStores.has(String(r.provider_id)));if(ws.length<RAW.weeks.length)rows=rows.filter(r=>ws.some(w=>r.order_created_date>=w&&r.order_created_date<addDays(w,7)));if(bpF==='yes')rows=rows.filter(r=>r.bp===true);if(bpF==='no')rows=rows.filter(r=>r.bp!==true);if(stF==='delivered')rows=rows.filter(r=>r.order_state==='delivered');if(stF==='failed')rows=rows.filter(r=>r.order_state!=='delivered');let html='<thead><tr><th>Дата</th><th>Час</th><th>Заклад</th><th>Статус</th><th class="num">Чек \\u20B4</th><th class="num">Дост. \\u20B4</th><th>BP</th><th class="num">Дост. хв</th></tr></thead><tbody>';rows.slice(0,300).forEach(r=>{{const st=r.order_state==='delivered'?'<span class="tag green">Доставлено</span>':'<span class="tag red">'+(r.order_state||'\\u2014')+'</span>';const bp=r.bp?'<span class="tag purple">Plus</span>':'';html+=`<tr><td>${{r.order_created_date||''}}</td><td>${{r.order_time||''}}</td><td>${{PROV[r.provider_id]||r.provider_name}}</td><td>${{st}}</td><td class="num">${{fmt(r.check_before)}}</td><td class="num">${{fmt(r.del_fee)}}</td><td>${{bp}}</td><td class="num">${{r.del_min||'\\u2014'}}</td></tr>`}});if(rows.length>300)html+='<tr><td colspan="8" style="text-align:center;color:var(--text2);padding:12px">Показано 300 з '+rows.length+'</td></tr>';html+='</tbody>';document.getElementById('orderTable').innerHTML=html}}
function renderComplaints(){{const ws=getWeeks();let rows=RAW.orderDetails.filter(r=>selectedStores.has(String(r.provider_id))&&r.ticket===true);if(ws.length<RAW.weeks.length)rows=rows.filter(r=>ws.some(w=>r.order_created_date>=w&&r.order_created_date<addDays(w,7)));let html='<thead><tr><th>Дата</th><th>Час</th><th>Заклад</th><th class="num">Чек \\u20B4</th><th class="num">Дост. хв</th><th>Bad</th></tr></thead><tbody>';if(!rows.length)html+='<tr><td colspan="6" style="text-align:center;padding:20px;color:var(--text2)">Немає скарг за обраний період</td></tr>';rows.forEach(r=>{{html+=`<tr><td>${{r.order_created_date}}</td><td>${{r.order_time||''}}</td><td>${{PROV[r.provider_id]||r.provider_name}}</td><td class="num">${{fmt(r.check_before)}}</td><td class="num">${{r.del_min||'\\u2014'}}</td><td>${{r.bad?'<span class="tag red">Bad</span>':''}}</td></tr>`}});html+='</tbody>';document.getElementById('complaintTable').innerHTML=html}}
function renderCancelled(){{const ws=getWeeks();let rows=RAW.orderDetails.filter(r=>selectedStores.has(String(r.provider_id))&&r.order_state!=='delivered');if(ws.length<RAW.weeks.length)rows=rows.filter(r=>ws.some(w=>r.order_created_date>=w&&r.order_created_date<addDays(w,7)));let html='<thead><tr><th>Дата</th><th>Час</th><th>Заклад</th><th>Статус</th><th class="num">Чек \\u20B4</th></tr></thead><tbody>';if(!rows.length)html+='<tr><td colspan="5" style="text-align:center;padding:20px;color:var(--text2)">Немає скасованих за обраний період</td></tr>';rows.forEach(r=>{{html+=`<tr><td>${{r.order_created_date}}</td><td>${{r.order_time||''}}</td><td>${{PROV[r.provider_id]||r.provider_name}}</td><td><span class="tag red">${{r.order_state}}</span></td><td class="num">${{fmt(r.check_before)}}</td></tr>`}});html+='</tbody>';document.getElementById('cancelledTable').innerHTML=html}}
function renderTopItems(){{const grid=document.getElementById('topItemsGrid');let html='';[...selectedStores].sort((a,b)=>a-b).forEach(pid=>{{const items=RAW.topItems.filter(r=>r.provider_id==pid).slice(0,10);if(!items.length)return;html+=`<div class="top-item-card"><h4>${{PROV[pid]}}</h4>`;items.forEach((it,i)=>{{html+=`<div class="top-row"><span class="top-rank">${{i+1}}</span><span class="top-name">${{it.basket_item_name}}</span><span class="top-qty">${{it.qty}} шт</span><span class="top-rev">${{fmt(it.rev)}} \\u20B4</span></div>`}});html+='</div>'}});grid.innerHTML=html}}
function render(){{if(activeTab==='overview'){{renderKPI();renderOrderCharts()}}if(activeTab==='orders')renderOrderCharts('full');if(activeTab==='ops')renderOpsCharts();if(activeTab==='stores')renderStoreTable();if(activeTab==='revenue'){{renderRevenueCharts();renderRevenueTable()}}if(activeTab==='campaigns'){{renderCampaignCharts();renderCampaignTable()}}if(activeTab==='smartpromo')renderSmartPromo();if(activeTab==='orderdetails')renderOrderDetails();if(activeTab==='complaints')renderComplaints();if(activeTab==='cancelled')renderCancelled();if(activeTab==='topitems')renderTopItems()}}
function resetFilters(){{selectedWeek=null;selectedStores=new Set(PIDS.map(String));document.querySelectorAll('#storePanel input').forEach(cb=>cb.checked=true);document.getElementById('storeBtnLabel').textContent='Всі заклади';initWeekBar();render()}}
function toggleTheme(){{const b=document.documentElement;b.setAttribute('data-theme',b.getAttribute('data-theme')==='dark'?'light':'dark')}}
document.getElementById('filterBP').addEventListener('change',renderOrderDetails);document.getElementById('filterStatus').addEventListener('change',renderOrderDetails);
initStoreFilter();initWeekBar();initNav();render();
</script>
</body>
</html>'''


def main():
    print("=" * 60)
    print("MA Pizza (Lviv) — Weekly Report Generator")
    print("=" * 60)

    conn = connect()
    try:
        print("Fetching data from Databricks...")
        data = fetch_all(conn)
    finally:
        conn.close()

    print("Building HTML report...")
    html = build_html(data)

    out_dir = REPO_ROOT / "ma-pizza"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / "index.html"

    out_path.write_text(html, encoding="utf-8")
    print(f"Report saved to {out_path}")
    print(f"File size: {len(html)/1024:.0f} KB")


if __name__ == "__main__":
    main()
