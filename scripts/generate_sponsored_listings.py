"""
Generate interactive Sponsored Listings dashboard for Ukraine.
Queries Databricks for all providers with sponsored listing history,
billing data, and campaign details, then produces a single-page HTML report.
"""

import os
import json
import re
import sys
from pathlib import Path
from datetime import datetime
from decimal import Decimal
import math

from databricks import sql
import pandas as pd

from config import SERVER_HOSTNAME, HTTP_PATH

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = REPO_ROOT / "sponsored-listings"


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
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return default
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, (pd.Timestamp, datetime)):
        return val.isoformat()
    if hasattr(val, "item"):
        return val.item()
    try:
        f = float(val)
        return default if math.isnan(f) else f
    except (TypeError, ValueError):
        return str(val)


def df_to_records(df):
    records = []
    for _, row in df.iterrows():
        records.append({col: to_native(row[col]) for col in df.columns})
    return records


# ── Queries ────────────────────────────────────────────────────────────────

def fetch_providers(conn):
    """All Ukraine providers that ever had a sponsored listing."""
    return query(conn, """
        SELECT DISTINCT
            p.provider_id,
            p.provider_name,
            p.city_name,
            p.city_id,
            p.zone_name,
            p.zone_segment,
            p.account_manager_name,
            p.provider_status,
            p.brand_name,
            p.business_segment
        FROM ng_delivery_spark.dim_provider_v2 p
        WHERE p.country_code = 'ua'
          AND p.provider_id IN (
              SELECT DISTINCT s.provider_id
              FROM ng_delivery_spark.delivery_paid_visibility_signup s
              JOIN ng_delivery_spark.dim_delivery_city c ON s.city_id = c.city_id
              WHERE c.country_code = 'ua'
          )
        ORDER BY p.city_name, p.zone_name, p.provider_name
    """)


def fetch_campaigns(conn):
    """All campaigns for Ukraine with placement info."""
    return query(conn, """
        SELECT
            c.id AS campaign_id,
            a.external_id AS provider_id,
            c.name AS campaign_name,
            c.pricing AS pricing_json,
            c.state AS campaign_state,
            c.start AS campaign_start,
            c.end AS campaign_end,
            cl.content_type AS placement,
            cl.city_id
        FROM ng_public_spark.ads_campaign_campaign c
        JOIN ng_public_spark.ads_campaign_advertiser a ON c.advertiser_id = a.id
        LEFT JOIN ng_public_spark.ads_campaign_content_link cl ON cl.campaign_id = c.id
        WHERE a.country = 'ua'
        ORDER BY a.external_id, c.start
    """)


def fetch_signups(conn):
    """All enrollment signups for Ukraine."""
    return query(conn, """
        SELECT
            s.id AS signup_id,
            s.provider_id,
            s.ad_id AS campaign_id,
            s.city_id,
            s.start AS enrollment_start,
            s.end AS enrollment_end,
            s.state AS enrollment_state,
            s.enrollment_type
        FROM ng_delivery_spark.delivery_paid_visibility_signup s
        JOIN ng_delivery_spark.dim_delivery_city c ON s.city_id = c.city_id
        WHERE c.country_code = 'ua'
        ORDER BY s.provider_id, s.start
    """)


def fetch_billing(conn):
    """All billing records for Ukraine."""
    return query(conn, """
        SELECT
            r.external_id AS provider_id,
            r.campaign_id,
            CAST(r.free_days AS DOUBLE) AS free_days,
            CAST(r.pricing AS DOUBLE) AS weekly_charge,
            DATE(bp.start) AS period_start,
            DATE(bp.end) AS period_end
        FROM ng_public_spark.ads_reporting_provider_reporting r
        JOIN ng_public_spark.ads_reporting_billing_period bp ON r.period_id = bp.id
        WHERE r.country = 'ua'
        ORDER BY r.external_id, bp.start, r.campaign_id
    """)


def fetch_active_listing_counts(conn):
    """Current active listing count per provider."""
    return query(conn, """
        SELECT
            s.provider_id,
            COUNT(*) AS active_listings
        FROM ng_delivery_spark.delivery_paid_visibility_signup s
        JOIN ng_delivery_spark.dim_delivery_city c ON s.city_id = c.city_id
        WHERE c.country_code = 'ua'
          AND s.state = 'active'
        GROUP BY s.provider_id
    """)


# ── Processing ─────────────────────────────────────────────────────────────

def parse_pricing(pricing_json):
    """Extract price and currency from pricing JSON string."""
    if not pricing_json:
        return None, None
    try:
        p = json.loads(pricing_json)
        price_str = p.get("price", "")
        match = re.match(r"(\d+(?:\.\d+)?)\s*(\w+)", price_str)
        if match:
            return float(match.group(1)), match.group(2).upper()
        return None, None
    except (json.JSONDecodeError, AttributeError):
        return None, None


def placement_label(content_type):
    if not content_type:
        return "Unknown"
    mapping = {
        "provider_category": "Home Screen",
        "provider_search_result": "Search",
    }
    return mapping.get(content_type, content_type)


def process_data(providers_df, campaigns_df, signups_df, billing_df, active_df):
    """Process raw DataFrames into structured JSON-ready dicts."""

    active_map = {}
    if len(active_df):
        for _, row in active_df.iterrows():
            active_map[int(row["provider_id"])] = int(row["active_listings"])

    providers = []
    for _, row in providers_df.iterrows():
        pid = int(row["provider_id"])
        providers.append({
            "id": pid,
            "name": str(row["provider_name"] or ""),
            "city": str(row["city_name"] or ""),
            "cityId": int(row["city_id"]) if row["city_id"] else 0,
            "zone": str(row["zone_name"] or "Undefined"),
            "zoneSegment": str(row["zone_segment"] or ""),
            "am": str(row["account_manager_name"] or "—"),
            "status": str(row["provider_status"] or ""),
            "brand": str(row["brand_name"] or ""),
            "segment": str(row["business_segment"] or ""),
            "activeListings": active_map.get(pid, 0),
        })

    campaigns = []
    for _, row in campaigns_df.iterrows():
        price, currency = parse_pricing(row.get("pricing_json"))
        campaigns.append({
            "campaignId": int(row["campaign_id"]),
            "providerId": int(row["provider_id"]),
            "name": str(row["campaign_name"] or ""),
            "placement": placement_label(row.get("placement")),
            "pricePerDay": price,
            "currency": currency,
            "state": str(row["campaign_state"] or ""),
            "start": to_native(row["campaign_start"]),
            "end": to_native(row["campaign_end"]),
            "cityId": int(row["city_id"]) if row.get("city_id") else None,
        })

    signups = []
    for _, row in signups_df.iterrows():
        signups.append({
            "signupId": int(row["signup_id"]),
            "providerId": int(row["provider_id"]),
            "campaignId": int(row["campaign_id"]),
            "cityId": int(row["city_id"]) if row["city_id"] else None,
            "start": to_native(row["enrollment_start"]),
            "end": to_native(row["enrollment_end"]),
            "state": str(row["enrollment_state"] or ""),
            "type": str(row["enrollment_type"] or ""),
        })

    billing = []
    for _, row in billing_df.iterrows():
        billing.append({
            "providerId": int(row["provider_id"]),
            "campaignId": int(row["campaign_id"]),
            "freeDays": to_native(row["free_days"], 0),
            "weeklyCharge": to_native(row["weekly_charge"], 0),
            "periodStart": to_native(row["period_start"]),
            "periodEnd": to_native(row["period_end"]),
        })

    cities = sorted(set(p["city"] for p in providers if p["city"]))
    ams = sorted(set(p["am"] for p in providers if p["am"] and p["am"] != "—"))
    zones_by_city = {}
    for p in providers:
        c = p["city"]
        z = p["zone"]
        if c not in zones_by_city:
            zones_by_city[c] = set()
        zones_by_city[c].add(z)
    zones_by_city = {c: sorted(zs) for c, zs in zones_by_city.items()}

    return {
        "providers": providers,
        "campaigns": campaigns,
        "signups": signups,
        "billing": billing,
        "filters": {
            "cities": cities,
            "ams": ams,
            "zonesByCity": zones_by_city,
        },
        "generatedAt": datetime.now(tz=__import__('datetime').timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }


# ── HTML Template ──────────────────────────────────────────────────────────

def generate_html(data):
    data_json = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    generated_at = data["generatedAt"]

    return f"""<!DOCTYPE html>
<html lang="uk">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Sponsored Listings — Україна</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/flatpickr/dist/flatpickr.min.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/flatpickr/dist/themes/dark.css">
<script src="https://cdn.jsdelivr.net/npm/flatpickr"></script>
<script src="https://cdn.jsdelivr.net/npm/xlsx@0.18.5/dist/xlsx.full.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/jspdf-autotable/3.8.2/jspdf.plugin.autotable.min.js"></script>
<style>
:root {{
  --bg: #0f172a; --bg2: #1e293b; --bg3: #334155; --border: #475569;
  --text: #e2e8f0; --text2: #94a3b8; --text3: #64748b;
  --green: #34d399; --green-bg: #064e3b; --blue: #60a5fa; --blue-bg: #1e3a5f;
  --red: #f87171; --red-bg: #7f1d1d; --orange: #fbbf24; --purple: #a78bfa;
  --radius: 10px;
}}
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; background:var(--bg); color:var(--text); }}

.top-bar {{ background:var(--bg2); border-bottom:1px solid var(--bg3); padding:16px 24px; display:flex; align-items:center; justify-content:space-between; position:sticky; top:0; z-index:100; }}
.top-bar h1 {{ font-size:20px; font-weight:700; }}
.top-bar .meta {{ color:var(--text3); font-size:12px; }}

.layout {{ display:flex; min-height:calc(100vh - 60px); }}

.sidebar {{ width:280px; background:var(--bg2); border-right:1px solid var(--bg3); padding:16px; flex-shrink:0; position:sticky; top:60px; height:calc(100vh - 60px); overflow-y:auto; }}
.sidebar h3 {{ font-size:12px; text-transform:uppercase; color:var(--text3); margin:16px 0 8px; letter-spacing:0.5px; }}
.sidebar h3:first-child {{ margin-top:0; }}
.filter-select {{ width:100%; background:var(--bg3); color:var(--text); border:1px solid var(--border); border-radius:6px; padding:8px 10px; font-size:13px; cursor:pointer; appearance:none; -webkit-appearance:none; }}
.filter-select option {{ background:var(--bg2); }}
.search-input {{ width:100%; background:var(--bg3); color:var(--text); border:1px solid var(--border); border-radius:6px; padding:8px 10px; font-size:13px; margin-bottom:8px; }}
.search-input::placeholder {{ color:var(--text3); }}

.sidebar-stats {{ margin-top:16px; padding-top:16px; border-top:1px solid var(--bg3); }}
.stat-row {{ display:flex; justify-content:space-between; padding:4px 0; font-size:13px; }}
.stat-row .label {{ color:var(--text2); }}
.stat-row .value {{ font-weight:600; }}

.main {{ flex:1; padding:24px; overflow-y:auto; }}

.kpi-bar {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:12px; margin-bottom:24px; }}
.kpi {{ background:var(--bg2); border:1px solid var(--bg3); border-radius:var(--radius); padding:16px; text-align:center; }}
.kpi .val {{ font-size:24px; font-weight:700; }}
.kpi .lbl {{ font-size:11px; color:var(--text2); margin-top:2px; }}
.kpi.green .val {{ color:var(--green); }}
.kpi.blue .val {{ color:var(--blue); }}
.kpi.orange .val {{ color:var(--orange); }}
.kpi.red .val {{ color:var(--red); }}

.zone-section {{ margin-bottom:24px; }}
.zone-header {{ font-size:16px; font-weight:600; padding:8px 0; border-bottom:1px solid var(--bg3); margin-bottom:12px; display:flex; align-items:center; gap:8px; cursor:pointer; }}
.zone-header .badge {{ font-size:11px; background:var(--bg3); color:var(--text2); padding:2px 8px; border-radius:10px; }}
.zone-header .chevron {{ transition:transform 0.2s; font-size:12px; color:var(--text3); }}
.zone-header.collapsed .chevron {{ transform:rotate(-90deg); }}

.provider-grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(300px,1fr)); gap:12px; }}
.provider-card {{ background:var(--bg2); border:1px solid var(--bg3); border-radius:var(--radius); padding:14px 16px; cursor:pointer; transition:all 0.15s; position:relative; }}
.provider-card:hover {{ border-color:var(--blue); transform:translateY(-1px); box-shadow:0 4px 12px rgba(96,165,250,0.1); }}
.provider-card .name {{ font-size:14px; font-weight:600; margin-bottom:4px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
.provider-card .meta-row {{ display:flex; gap:8px; font-size:12px; color:var(--text2); flex-wrap:wrap; }}
.provider-card .meta-row span {{ display:inline-flex; align-items:center; gap:3px; }}
.status-dot {{ width:8px; height:8px; border-radius:50%; display:inline-block; flex-shrink:0; }}
.status-dot.active {{ background:var(--green); box-shadow:0 0 6px rgba(52,211,153,0.4); }}
.status-dot.inactive {{ background:var(--text3); }}
.listing-badges {{ display:flex; gap:4px; margin-top:8px; flex-wrap:wrap; }}
.listing-badge {{ font-size:11px; padding:2px 8px; border-radius:6px; }}
.listing-badge.home {{ background:var(--green-bg); color:var(--green); }}
.listing-badge.search {{ background:var(--blue-bg); color:var(--blue); }}
.listing-badge.ended {{ background:var(--bg3); color:var(--text3); }}

/* Modal */
.modal-overlay {{ display:none; position:fixed; inset:0; background:rgba(0,0,0,0.7); z-index:200; justify-content:center; align-items:flex-start; padding:40px 20px; overflow-y:auto; }}
.modal-overlay.open {{ display:flex; }}
.modal {{ background:var(--bg); border:1px solid var(--bg3); border-radius:12px; width:100%; max-width:1200px; max-height:calc(100vh - 80px); overflow-y:auto; }}
.modal-header {{ position:sticky; top:0; background:var(--bg2); padding:16px 24px; border-bottom:1px solid var(--bg3); display:flex; justify-content:space-between; align-items:center; z-index:10; border-radius:12px 12px 0 0; }}
.modal-header h2 {{ font-size:18px; }}
.modal-header .close-btn {{ background:none; border:none; color:var(--text2); font-size:24px; cursor:pointer; padding:4px 8px; }}
.modal-header .close-btn:hover {{ color:var(--text); }}
.modal-body {{ padding:24px; }}

.modal-meta {{ display:flex; gap:16px; flex-wrap:wrap; margin-bottom:20px; }}
.modal-meta .tag {{ font-size:12px; padding:4px 10px; background:var(--bg2); border:1px solid var(--bg3); border-radius:6px; color:var(--text2); }}

.detail-toolbar {{ display:flex; gap:8px; flex-wrap:wrap; margin-bottom:16px; align-items:center; }}
.detail-toolbar .date-input {{ background:var(--bg3); color:var(--text); border:1px solid var(--border); border-radius:6px; padding:6px 10px; font-size:13px; width:220px; cursor:pointer; }}
.detail-toolbar .btn {{ padding:6px 14px; border-radius:6px; font-size:12px; font-weight:600; cursor:pointer; border:1px solid var(--border); transition:all 0.15s; }}
.btn-primary {{ background:var(--blue); color:#fff; border-color:var(--blue) !important; }}
.btn-primary:hover {{ opacity:0.9; }}
.btn-outline {{ background:transparent; color:var(--text2); }}
.btn-outline:hover {{ background:var(--bg3); color:var(--text); }}
.btn-green {{ background:var(--green-bg); color:var(--green); border-color:var(--green-bg) !important; }}
.btn-green:hover {{ opacity:0.9; }}

.detail-section {{ margin-bottom:24px; }}
.detail-section h3 {{ font-size:14px; color:var(--text2); margin-bottom:10px; text-transform:uppercase; letter-spacing:0.5px; }}

.tbl-wrap {{ overflow-x:auto; border-radius:var(--radius); border:1px solid var(--bg3); }}
table {{ width:100%; border-collapse:collapse; background:var(--bg2); white-space:nowrap; font-size:13px; }}
th {{ background:var(--bg3); color:var(--text2); font-size:11px; text-transform:uppercase; padding:10px 12px; text-align:left; position:sticky; top:0; z-index:1; }}
td {{ padding:8px 12px; border-top:1px solid rgba(51,65,85,0.5); }}
td.num {{ text-align:right; font-variant-numeric:tabular-nums; }}
tr:nth-child(even) {{ background:var(--bg2); }}
tr:nth-child(odd) {{ background:rgba(22,32,50,0.5); }}

.state-badge {{ display:inline-block; font-size:11px; padding:2px 8px; border-radius:6px; font-weight:600; }}
.state-badge.active {{ background:var(--green-bg); color:var(--green); }}
.state-badge.finished {{ background:var(--bg3); color:var(--text2); }}
.state-badge.aborted {{ background:var(--red-bg); color:var(--red); }}
.state-badge.disabled {{ background:#78350f; color:var(--orange); }}

.empty-state {{ text-align:center; color:var(--text3); padding:40px; }}

.scroll-table {{ max-height:500px; overflow-y:auto; }}

@media(max-width:900px) {{
  .layout {{ flex-direction:column; }}
  .sidebar {{ width:100%; height:auto; position:static; border-right:none; border-bottom:1px solid var(--bg3); }}
  .provider-grid {{ grid-template-columns:1fr; }}
  .kpi-bar {{ grid-template-columns:repeat(2,1fr); }}
}}
</style>
</head>
<body>

<div class="top-bar">
  <h1>Sponsored Listings — Україна</h1>
  <div class="meta">Оновлено: {generated_at}</div>
</div>

<div class="layout">
  <div class="sidebar">
    <h3>Пошук провайдера</h3>
    <input type="text" class="search-input" id="providerSearch" placeholder="Назва або ID...">

    <h3>Місто</h3>
    <select class="filter-select" id="cityFilter"><option value="">Всі міста</option></select>

    <h3>Аккаунт менеджер</h3>
    <select class="filter-select" id="amFilter"><option value="">Всі AM</option></select>

    <h3>Зона</h3>
    <select class="filter-select" id="zoneFilter"><option value="">Всі зони</option></select>

    <h3>Статус лістингу</h3>
    <select class="filter-select" id="statusFilter">
      <option value="">Всі</option>
      <option value="active">Активний лістинг</option>
      <option value="inactive">Неактивний</option>
    </select>

    <div class="sidebar-stats" id="sidebarStats"></div>
  </div>

  <div class="main">
    <div class="kpi-bar" id="kpiBar"></div>
    <div id="zoneContainer"></div>
  </div>
</div>

<div class="modal-overlay" id="modalOverlay">
  <div class="modal" id="modal">
    <div class="modal-header">
      <h2 id="modalTitle"></h2>
      <button class="close-btn" id="modalClose">&times;</button>
    </div>
    <div class="modal-body" id="modalBody"></div>
  </div>
</div>

<script>
const DATA = {data_json};

const providers = DATA.providers;
const campaigns = DATA.campaigns;
const signups = DATA.signups;
const billing = DATA.billing;
const filters = DATA.filters;

// Build lookup maps
const campaignsByProvider = {{}};
campaigns.forEach(c => {{
  if (!campaignsByProvider[c.providerId]) campaignsByProvider[c.providerId] = [];
  campaignsByProvider[c.providerId].push(c);
}});

const signupsByProvider = {{}};
signups.forEach(s => {{
  if (!signupsByProvider[s.providerId]) signupsByProvider[s.providerId] = [];
  signupsByProvider[s.providerId].push(s);
}});

const billingByProvider = {{}};
billing.forEach(b => {{
  if (!billingByProvider[b.providerId]) billingByProvider[b.providerId] = [];
  billingByProvider[b.providerId].push(b);
}});

const campaignMap = {{}};
campaigns.forEach(c => {{ campaignMap[c.campaignId] = c; }});

const signupByCampaign = {{}};
signups.forEach(s => {{ signupByCampaign[s.campaignId] = s; }});

function resolveStatus(campaign) {{
  const now = new Date();
  const ended = campaign.end && new Date(campaign.end) < now;
  const su = signupByCampaign[campaign.campaignId];

  if (su) {{
    if (su.state === 'disabled') return 'disabled';
    if (su.state === 'cost_exceeding_revenue_aborted') return 'aborted';
    if (su.state === 'active' && ended) return 'finished';
    if (su.state === 'active') return 'active';
  }}
  if (campaign.state === 'aborted') return 'aborted';
  if (ended) return 'finished';
  if (campaign.state === 'approved') return 'active';
  return campaign.state;
}}

const STATUS_LABEL = {{
  active: 'Active', finished: 'Finished', aborted: 'Aborted', disabled: 'Disabled'
}};
const STATUS_CLASS = {{
  active: 'active', finished: 'finished', aborted: 'aborted', disabled: 'disabled'
}};

// Recompute activeListings client-side using resolveStatus (accounts for end date)
providers.forEach(p => {{
  const pCamps = campaignsByProvider[p.id] || [];
  p.activeListings = pCamps.filter(c => resolveStatus(c) === 'active').length;
}});

// Populate filter dropdowns
const citySelect = document.getElementById('cityFilter');
const amSelect = document.getElementById('amFilter');
const zoneSelect = document.getElementById('zoneFilter');

filters.cities.forEach(c => {{
  const o = document.createElement('option');
  o.value = c; o.textContent = c;
  citySelect.appendChild(o);
}});
filters.ams.forEach(a => {{
  const o = document.createElement('option');
  o.value = a; o.textContent = a;
  amSelect.appendChild(o);
}});

function updateZoneOptions(city) {{
  zoneSelect.innerHTML = '<option value="">Всі зони</option>';
  const zones = city ? (filters.zonesByCity[city] || []) : Object.values(filters.zonesByCity).flat();
  const unique = [...new Set(zones)].sort();
  unique.forEach(z => {{
    const o = document.createElement('option');
    o.value = z; o.textContent = z;
    zoneSelect.appendChild(o);
  }});
}}
updateZoneOptions('');

citySelect.addEventListener('change', () => {{
  updateZoneOptions(citySelect.value);
  render();
}});
amSelect.addEventListener('change', render);
zoneSelect.addEventListener('change', render);
document.getElementById('statusFilter').addEventListener('change', render);
document.getElementById('providerSearch').addEventListener('input', render);

function getFilteredProviders() {{
  const city = citySelect.value;
  const am = amSelect.value;
  const zone = zoneSelect.value;
  const status = document.getElementById('statusFilter').value;
  const search = document.getElementById('providerSearch').value.toLowerCase().trim();

  return providers.filter(p => {{
    if (city && p.city !== city) return false;
    if (am && p.am !== am) return false;
    if (zone && p.zone !== zone) return false;
    if (status === 'active' && p.activeListings === 0) return false;
    if (status === 'inactive' && p.activeListings > 0) return false;
    if (search && !p.name.toLowerCase().includes(search) && !String(p.id).includes(search)) return false;
    return true;
  }});
}}

function formatNum(n) {{
  if (n === null || n === undefined) return '—';
  return n.toLocaleString('uk-UA', {{maximumFractionDigits: 2}});
}}

function formatCurrency(n) {{
  if (n === null || n === undefined) return '—';
  return n.toLocaleString('uk-UA', {{maximumFractionDigits: 0}}) + ' ₴';
}}

function render() {{
  const filtered = getFilteredProviders();

  // KPIs
  const totalProviders = filtered.length;
  const activeProviders = filtered.filter(p => p.activeListings > 0).length;
  const totalActiveListings = filtered.reduce((s, p) => s + p.activeListings, 0);

  let totalSpend = 0;
  filtered.forEach(p => {{
    const bl = billingByProvider[p.id] || [];
    bl.forEach(b => {{ totalSpend += b.weeklyCharge || 0; }});
  }});

  document.getElementById('kpiBar').innerHTML = `
    <div class="kpi blue"><div class="val">${{totalProviders}}</div><div class="lbl">Провайдерів</div></div>
    <div class="kpi green"><div class="val">${{activeProviders}}</div><div class="lbl">З активним лістингом</div></div>
    <div class="kpi orange"><div class="val">${{totalActiveListings}}</div><div class="lbl">Активних кампаній</div></div>
    <div class="kpi"><div class="val">${{formatCurrency(totalSpend)}}</div><div class="lbl">Загальні витрати (all time)</div></div>
  `;

  // Group by city > zone
  const grouped = {{}};
  filtered.forEach(p => {{
    const key = p.city;
    if (!grouped[key]) grouped[key] = {{}};
    const zKey = p.zone || 'Undefined';
    if (!grouped[key][zKey]) grouped[key][zKey] = [];
    grouped[key][zKey].push(p);
  }});

  const container = document.getElementById('zoneContainer');
  container.innerHTML = '';

  if (filtered.length === 0) {{
    container.innerHTML = '<div class="empty-state">Немає провайдерів за обраними фільтрами</div>';
    updateSidebarStats(filtered);
    return;
  }}

  const sortedCities = Object.keys(grouped).sort();
  sortedCities.forEach(city => {{
    const cityDiv = document.createElement('div');
    cityDiv.style.marginBottom = '32px';

    const cityHeader = document.createElement('h2');
    cityHeader.style.cssText = 'font-size:18px;margin-bottom:16px;padding-bottom:8px;border-bottom:2px solid var(--bg3);color:var(--blue);';
    const cityProvCount = Object.values(grouped[city]).flat().length;
    cityHeader.innerHTML = `${{city}} <span style="color:var(--text3);font-size:14px;font-weight:400">(${{cityProvCount}} провайдерів)</span>`;
    cityDiv.appendChild(cityHeader);

    const sortedZones = Object.keys(grouped[city]).sort();
    sortedZones.forEach(zone => {{
      const provs = grouped[city][zone];
      const zoneDiv = document.createElement('div');
      zoneDiv.className = 'zone-section';

      const activeInZone = provs.filter(p => p.activeListings > 0).length;
      const zoneHeader = document.createElement('div');
      zoneHeader.className = 'zone-header';
      zoneHeader.innerHTML = `
        <span class="chevron">&#9660;</span>
        ${{zone}}
        <span class="badge">${{provs.length}} провайдерів</span>
        <span class="badge" style="background:var(--green-bg);color:var(--green)">${{activeInZone}} активних</span>
      `;

      const grid = document.createElement('div');
      grid.className = 'provider-grid';

      zoneHeader.addEventListener('click', () => {{
        zoneHeader.classList.toggle('collapsed');
        grid.style.display = grid.style.display === 'none' ? 'grid' : 'none';
      }});

      provs.sort((a, b) => b.activeListings - a.activeListings || a.name.localeCompare(b.name));
      provs.forEach(p => {{
        const card = document.createElement('div');
        card.className = 'provider-card';
        card.addEventListener('click', () => openProviderDetail(p.id));

        const isActive = p.activeListings > 0;
        const pCampaigns = campaignsByProvider[p.id] || [];
        
        let badges = '';
        const activePlacements = new Set();
        const allPlacements = new Set();
        pCampaigns.forEach(c => {{
          const st = resolveStatus(c);
          allPlacements.add(c.placement);
          if (st === 'active') activePlacements.add(c.placement);
        }});
        if (activePlacements.has('Home Screen')) badges += '<span class="listing-badge home">Home Screen</span>';
        if (activePlacements.has('Search')) badges += '<span class="listing-badge search">Search</span>';
        if (!activePlacements.has('Home Screen') && allPlacements.has('Home Screen')) badges += '<span class="listing-badge ended">Home Screen (завершено)</span>';
        if (!activePlacements.has('Search') && allPlacements.has('Search')) badges += '<span class="listing-badge ended">Search (завершено)</span>';

        card.innerHTML = `
          <div class="name"><span class="status-dot ${{isActive ? 'active' : 'inactive'}}"></span> ${{escHtml(p.name)}}</div>
          <div class="meta-row">
            <span>ID: ${{p.id}}</span>
            <span>AM: ${{escHtml(p.am)}}</span>
          </div>
          <div class="listing-badges">${{badges}}</div>
        `;
        grid.appendChild(card);
      }});

      zoneDiv.appendChild(zoneHeader);
      zoneDiv.appendChild(grid);
      cityDiv.appendChild(zoneDiv);
    }});

    container.appendChild(cityDiv);
  }});

  updateSidebarStats(filtered);
}}

function updateSidebarStats(filtered) {{
  const stats = document.getElementById('sidebarStats');
  const cities = new Set(filtered.map(p => p.city));
  const zones = new Set(filtered.map(p => p.zone));
  const ams = new Set(filtered.map(p => p.am).filter(a => a !== '—'));
  stats.innerHTML = `
    <div class="stat-row"><span class="label">Міст:</span><span class="value">${{cities.size}}</span></div>
    <div class="stat-row"><span class="label">Зон:</span><span class="value">${{zones.size}}</span></div>
    <div class="stat-row"><span class="label">AM:</span><span class="value">${{ams.size}}</span></div>
    <div class="stat-row"><span class="label">Провайдерів:</span><span class="value">${{filtered.length}}</span></div>
  `;
}}

function escHtml(s) {{
  if (!s) return '—';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}}

// ── Provider Detail Modal ──────────────────────────────────────────────

let currentDetailPicker = null;
let currentProviderId = null;

function openProviderDetail(providerId) {{
  currentProviderId = providerId;
  const p = providers.find(x => x.id === providerId);
  if (!p) return;

  document.getElementById('modalTitle').textContent = `${{p.name}} (ID: ${{p.id}})`;
  const body = document.getElementById('modalBody');

  const pCampaigns = (campaignsByProvider[providerId] || []).slice().sort((a, b) => {{
    if (!a.start) return 1; if (!b.start) return -1;
    return new Date(b.start) - new Date(a.start);
  }});
  const pBilling = billingByProvider[providerId] || [];
  const pSignups = signupsByProvider[providerId] || [];

  const totalSpend = pBilling.reduce((s, b) => s + (b.weeklyCharge || 0), 0);
  const totalFreeDays = pBilling.reduce((s, b) => s + (b.freeDays || 0), 0);

  body.innerHTML = `
    <div class="modal-meta">
      <span class="tag">Місто: ${{escHtml(p.city)}}</span>
      <span class="tag">Зона: ${{escHtml(p.zone)}}</span>
      <span class="tag">AM: ${{escHtml(p.am)}}</span>
      <span class="tag">Статус: ${{escHtml(p.status)}}</span>
      <span class="tag">Сегмент: ${{escHtml(p.segment)}}</span>
    </div>

    <div class="kpi-bar" style="margin-bottom:20px;">
      <div class="kpi"><div class="val">${{pCampaigns.length}}</div><div class="lbl">Кампаній</div></div>
      <div class="kpi green"><div class="val">${{pCampaigns.filter(c=>resolveStatus(c)==='active').length}}</div><div class="lbl">Активних</div></div>
      <div class="kpi orange"><div class="val">${{formatCurrency(totalSpend)}}</div><div class="lbl">Загальні витрати</div></div>
      <div class="kpi blue"><div class="val">${{totalFreeDays.toFixed(1)}}</div><div class="lbl">Безкоштовних днів</div></div>
    </div>

    <div class="detail-toolbar">
      <input type="text" class="date-input" id="detailDateRange" placeholder="Фільтр по датах...">
      <button class="btn btn-outline" onclick="resetDateFilter()">Скинути дати</button>
      <div style="flex:1"></div>
      <button class="btn btn-green" onclick="exportExcel(${{providerId}})">Excel</button>
      <button class="btn btn-primary" onclick="exportPDF(${{providerId}})">PDF</button>
    </div>

    <div class="detail-section">
      <h3>Кампанії (категорії)</h3>
      <div class="tbl-wrap" id="campaignsTableWrap">
        ${{buildCampaignsTable(pCampaigns)}}
      </div>
    </div>

    <div class="detail-section">
      <h3>Біллінг по тижнях</h3>
      <div class="tbl-wrap scroll-table" id="billingTableWrap">
        ${{buildBillingTable(pBilling, pCampaigns)}}
      </div>
    </div>

    <div class="detail-section">
      <h3>Розрахунок по днях (24-годинні періоди)</h3>
      <div class="tbl-wrap scroll-table" id="dailyTableWrap">
        ${{buildDailyBreakdown(pBilling, pCampaigns, pSignups)}}
      </div>
    </div>
  `;

  document.getElementById('modalOverlay').classList.add('open');
  document.body.style.overflow = 'hidden';

  if (currentDetailPicker) currentDetailPicker.destroy();
  currentDetailPicker = flatpickr('#detailDateRange', {{
    mode: 'range',
    dateFormat: 'Y-m-d',
    theme: 'dark',
    onChange: function(dates) {{
      if (dates.length === 2) filterDetailByDate(dates[0], dates[1], providerId);
    }}
  }});
}}

function buildCampaignsTable(camps) {{
  if (!camps.length) return '<div class="empty-state">Немає кампаній</div>';
  let rows = '';
  camps.forEach(c => {{
    const st = resolveStatus(c);
    const stClass = STATUS_CLASS[st] || 'finished';
    const stLabel = STATUS_LABEL[st] || st;
    const price = c.pricePerDay ? `${{c.pricePerDay}} ${{c.currency || ''}}/день` : '—';
    const start = c.start ? new Date(c.start).toLocaleDateString('uk-UA') : '—';
    const end = c.end ? new Date(c.end).toLocaleDateString('uk-UA') : '—';
    rows += `<tr data-start="${{c.start || ''}}" data-end="${{c.end || ''}}">
      <td>${{c.campaignId}}</td>
      <td>${{escHtml(c.placement)}}</td>
      <td>${{price}}</td>
      <td><span class="state-badge ${{stClass}}">${{stLabel}}</span></td>
      <td>${{start}}</td>
      <td>${{end}}</td>
    </tr>`;
  }});
  return `<table><thead><tr><th>ID</th><th>Розміщення</th><th>Ціна/день</th><th>Статус</th><th>Початок</th><th>Кінець</th></tr></thead><tbody>${{rows}}</tbody></table>`;
}}

function buildBillingTable(bills, camps) {{
  if (!bills.length) return '<div class="empty-state">Немає даних по біллінгу</div>';
  let rows = '';
  bills.forEach(b => {{
    const camp = campaignMap[b.campaignId];
    const placement = camp ? camp.placement : '—';
    const price = camp && camp.pricePerDay ? `${{camp.pricePerDay}} ${{camp.currency || ''}}/день` : '—';
    const ps = b.periodStart ? new Date(b.periodStart).toLocaleDateString('uk-UA') : '—';
    const pe = b.periodEnd ? new Date(b.periodEnd).toLocaleDateString('uk-UA') : '—';
    rows += `<tr data-start="${{b.periodStart || ''}}" data-end="${{b.periodEnd || ''}}">
      <td>${{ps}} — ${{pe}}</td>
      <td>${{escHtml(placement)}}</td>
      <td>${{price}}</td>
      <td class="num">${{formatNum(b.freeDays)}}</td>
      <td class="num" style="font-weight:600;color:var(--orange)">${{formatCurrency(b.weeklyCharge)}}</td>
    </tr>`;
  }});
  return `<table><thead><tr><th>Період</th><th>Розміщення</th><th>Ціна/день</th><th>Безкошт. дні</th><th>Списання</th></tr></thead><tbody>${{rows}}</tbody></table>`;
}}

function buildDailyBreakdown(bills, camps, sups) {{
  if (!bills.length) return '<div class="empty-state">Немає даних</div>';

  const signupMap = {{}};
  sups.forEach(s => {{ signupMap[s.campaignId] = s; }});

  let rows = '';
  bills.forEach(b => {{
    const camp = campaignMap[b.campaignId];
    if (!camp) return;
    const su = signupMap[b.campaignId];
    
    const periodStart = new Date(b.periodStart);
    const periodEnd = new Date(b.periodEnd);
    const campStart = camp.start ? new Date(camp.start) : null;
    const campEnd = camp.end ? new Date(camp.end) : null;
    const enrollStart = su && su.start ? new Date(su.start) : campStart;
    const enrollEnd = su && su.end ? new Date(su.end) : campEnd;

    if (!enrollStart || !enrollEnd) return;

    const activeStart = new Date(Math.max(enrollStart, periodStart));
    const activeEnd = new Date(Math.min(enrollEnd, periodEnd));

    if (activeEnd <= activeStart) return;

    const activeHours = (activeEnd - activeStart) / 3600000;
    const active24h = activeHours / 24;
    const rate = camp.pricePerDay || 0;
    const expectedCharge = active24h * rate;

    const ps = periodStart.toLocaleDateString('uk-UA');
    const pe = periodEnd.toLocaleDateString('uk-UA');
    const asStr = activeStart.toLocaleDateString('uk-UA') + ' ' + activeStart.toLocaleTimeString('uk-UA', {{hour:'2-digit',minute:'2-digit'}});
    const aeStr = activeEnd.toLocaleDateString('uk-UA') + ' ' + activeEnd.toLocaleTimeString('uk-UA', {{hour:'2-digit',minute:'2-digit'}});

    rows += `<tr data-start="${{b.periodStart || ''}}" data-end="${{b.periodEnd || ''}}">
      <td>${{ps}} — ${{pe}}</td>
      <td>${{escHtml(camp.placement)}}</td>
      <td>${{rate}} ₴/день</td>
      <td>${{asStr}}</td>
      <td>${{aeStr}}</td>
      <td class="num">${{activeHours.toFixed(1)}} год</td>
      <td class="num">${{active24h.toFixed(2)}}</td>
      <td class="num">${{formatCurrency(expectedCharge)}}</td>
      <td class="num">${{formatNum(b.freeDays)}}</td>
      <td class="num" style="font-weight:600;color:var(--orange)">${{formatCurrency(b.weeklyCharge)}}</td>
    </tr>`;
  }});

  if (!rows) return '<div class="empty-state">Немає даних</div>';
  return `<table><thead><tr>
    <th>Період</th><th>Розміщення</th><th>Ціна/день</th>
    <th>Активний з</th><th>Активний до</th><th>Год. активності</th>
    <th>24-год періоди</th><th>Очікув. списання</th><th>Безкошт. дні</th><th>Факт. списання</th>
  </tr></thead><tbody>${{rows}}</tbody></table>`;
}}

function filterDetailByDate(from, to, providerId) {{
  const tables = ['campaignsTableWrap', 'billingTableWrap', 'dailyTableWrap'];
  tables.forEach(tid => {{
    const trs = document.querySelectorAll(`#${{tid}} tbody tr`);
    trs.forEach(tr => {{
      const s = tr.dataset.start;
      const e = tr.dataset.end;
      if (!s && !e) {{ tr.style.display = ''; return; }}
      const rowStart = s ? new Date(s) : null;
      const rowEnd = e ? new Date(e) : null;
      const show = (!rowStart || rowStart <= to) && (!rowEnd || rowEnd >= from);
      tr.style.display = show ? '' : 'none';
    }});
  }});
}}

function resetDateFilter() {{
  if (currentDetailPicker) currentDetailPicker.clear();
  const tables = ['campaignsTableWrap', 'billingTableWrap', 'dailyTableWrap'];
  tables.forEach(tid => {{
    document.querySelectorAll(`#${{tid}} tbody tr`).forEach(tr => tr.style.display = '');
  }});
}}

// ── Export ──────────────────────────────────────────────────────────────

function getVisibleTableData(wrapperId) {{
  const rows = [];
  const table = document.querySelector(`#${{wrapperId}} table`);
  if (!table) return {{ headers: [], rows: [] }};
  const headers = [...table.querySelectorAll('thead th')].map(th => th.textContent.trim());
  table.querySelectorAll('tbody tr').forEach(tr => {{
    if (tr.style.display === 'none') return;
    rows.push([...tr.querySelectorAll('td')].map(td => td.textContent.trim()));
  }});
  return {{ headers, rows }};
}}

function exportExcel(providerId) {{
  const p = providers.find(x => x.id === providerId);
  const wb = XLSX.utils.book_new();

  const campaignsData = getVisibleTableData('campaignsTableWrap');
  if (campaignsData.rows.length) {{
    const ws1 = XLSX.utils.aoa_to_sheet([campaignsData.headers, ...campaignsData.rows]);
    XLSX.utils.book_append_sheet(wb, ws1, 'Кампанії');
  }}

  const billingData = getVisibleTableData('billingTableWrap');
  if (billingData.rows.length) {{
    const ws2 = XLSX.utils.aoa_to_sheet([billingData.headers, ...billingData.rows]);
    XLSX.utils.book_append_sheet(wb, ws2, 'Біллінг');
  }}

  const dailyData = getVisibleTableData('dailyTableWrap');
  if (dailyData.rows.length) {{
    const ws3 = XLSX.utils.aoa_to_sheet([dailyData.headers, ...dailyData.rows]);
    XLSX.utils.book_append_sheet(wb, ws3, 'Деталі по днях');
  }}

  const name = p ? p.name.replace(/[^a-zA-Zа-яА-ЯіїєґІЇЄҐ0-9]/g, '_') : providerId;
  XLSX.writeFile(wb, `Sponsored_Listings_${{name}}_${{providerId}}.xlsx`);
}}

function exportPDF(providerId) {{
  const p = providers.find(x => x.id === providerId);
  const {{ jsPDF }} = window.jspdf;
  const doc = new jsPDF({{ orientation: 'landscape', unit: 'mm', format: 'a4' }});

  const title = `${{p ? p.name : ''}} (ID: ${{providerId}})`;
  doc.setFontSize(14);
  doc.text(title, 14, 15);
  doc.setFontSize(10);
  doc.text(`Місто: ${{p?.city || ''}} | Зона: ${{p?.zone || ''}} | AM: ${{p?.am || ''}}`, 14, 22);

  let yPos = 28;

  const sections = [
    {{ title: 'Кампанії', id: 'campaignsTableWrap' }},
    {{ title: 'Біллінг по тижнях', id: 'billingTableWrap' }},
    {{ title: 'Розрахунок по днях', id: 'dailyTableWrap' }},
  ];

  sections.forEach(section => {{
    const data = getVisibleTableData(section.id);
    if (!data.rows.length) return;

    if (yPos > 180) {{
      doc.addPage();
      yPos = 15;
    }}

    doc.setFontSize(11);
    doc.text(section.title, 14, yPos);
    yPos += 4;

    doc.autoTable({{
      head: [data.headers],
      body: data.rows,
      startY: yPos,
      theme: 'grid',
      styles: {{ fontSize: 7, cellPadding: 1.5 }},
      headStyles: {{ fillColor: [30, 41, 59], textColor: [148, 163, 184], fontSize: 7 }},
      margin: {{ left: 14, right: 14 }},
    }});

    yPos = doc.lastAutoTable.finalY + 10;
  }});

  const name = p ? p.name.replace(/[^a-zA-Zа-яА-ЯіїєґІЇЄҐ0-9]/g, '_') : providerId;
  doc.save(`Sponsored_Listings_${{name}}_${{providerId}}.pdf`);
}}

// ── Modal close ────────────────────────────────────────────────────────

document.getElementById('modalClose').addEventListener('click', closeModal);
document.getElementById('modalOverlay').addEventListener('click', e => {{
  if (e.target === document.getElementById('modalOverlay')) closeModal();
}});
document.addEventListener('keydown', e => {{
  if (e.key === 'Escape') closeModal();
}});

function closeModal() {{
  document.getElementById('modalOverlay').classList.remove('open');
  document.body.style.overflow = '';
  if (currentDetailPicker) {{ currentDetailPicker.destroy(); currentDetailPicker = null; }}
}}

// Initial render
render();
</script>
</body>
</html>"""


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    print(f"[{datetime.now(tz=__import__('datetime').timezone.utc):%Y-%m-%d %H:%M UTC}] Starting Sponsored Listings report generation")

    conn = connect()
    try:
        print("  Fetching providers...")
        providers_df = fetch_providers(conn)
        print(f"    → {len(providers_df)} providers")

        print("  Fetching campaigns...")
        campaigns_df = fetch_campaigns(conn)
        print(f"    → {len(campaigns_df)} campaigns")

        print("  Fetching signups...")
        signups_df = fetch_signups(conn)
        print(f"    → {len(signups_df)} signups")

        print("  Fetching billing...")
        billing_df = fetch_billing(conn)
        print(f"    → {len(billing_df)} billing rows")

        print("  Fetching active listing counts...")
        active_df = fetch_active_listing_counts(conn)
        print(f"    → {len(active_df)} providers with active listings")

    finally:
        conn.close()

    print("  Processing data...")
    data = process_data(providers_df, campaigns_df, signups_df, billing_df, active_df)

    print("  Generating HTML...")
    html = generate_html(data)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / "index.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"  Saved: {out_path}")

    update_root_index()
    print("Done!")


def update_root_index():
    """Add sponsored-listings link to root index if not already present."""
    index_path = REPO_ROOT / "index.html"
    if not index_path.exists():
        return

    content = index_path.read_text(encoding="utf-8")
    if "sponsored-listings" in content:
        return

    new_card = """
        <a class="report-card" href="sponsored-listings/" style="border-color:#fbbf24;">
            <h3>Sponsored Listings</h3>
            <p>Україна — всі провайдери</p>
            <span class="badge" style="background:#78350f;color:#fbbf24;">Дашборд</span>
        </a>"""

    content = content.replace("</div>\n</body>", f"{new_card}\n</div>\n</body>")
    index_path.write_text(content, encoding="utf-8")
    print("  Updated root index.html with sponsored-listings link")


if __name__ == "__main__":
    main()
