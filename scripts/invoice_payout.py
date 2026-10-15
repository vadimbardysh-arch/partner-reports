"""Partner-invoice lines for the weekly venue payout table.

The venue compares the report to the weekly invoice, so the displayed lines
are the invoice lines, at two decimals:

  sales            food after discount + third-party menu spend
  agency_fee      per-order rounded gross commission − marketing fee
  bolt_invoice    Bolt menu compensation − third-party menu spend
  marketing_fee   32.4% of bolt_invoice (27% + VAT), rounded on the week
  venue_df_vat    venue delivery cofinance with 20% VAT
  sponsored       Sponsored Listing for the UTC billing week posted on this
                  local order week
  refund          refunds
  earnings        sales − agency + bolt_invoice − marketing − delivery
                  − sponsored − refund

Third-party menu spend (marketing_3rd_party_partnership, item price) is part
of Bolt menu cost in the warehouse, but the invoice books it into sales and
not into the fee-reduction line.

Sponsored Listing is not on the order. A local order week that starts on
Monday M (Europe/Kyiv) is charged the UTC week [M 00:00 UTC − 7 days,
M 00:00 UTC). That is the week the invoice shows: the three hours before
Monday 00:00 UTC fall on the previous statement because Ukraine is UTC+3.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

# States that never ran and must not be charged.
_SKIP_STATES = {"rejected", "cancelled", "canceled", "draft"}

UTC = timezone.utc
KYIV = timezone(timedelta(hours=3))
CENT = Decimal("0.01")
MARKETING_RATE = Decimal("0.324")  # 27% commission on Bolt menu spend, with 20% VAT


def money(value) -> Decimal:
    if value is None:
        return Decimal("0.00")
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


def num(value: Decimal) -> float:
    return float(money(value))


def as_utc(value):
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text or text.lower() == "none":
            return None
        value = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if getattr(value, "tzinfo", None) is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def week_key(local_day) -> str:
    iso = local_day.isocalendar()
    return f"{iso.year}-W{iso.week}"


def sponsored_by_week(rows) -> dict:
    """Map (order_week, provider_id) to the invoice Sponsored Listing charge.

    rows: iterable of mappings with provider_id, price_per_day, start_utc,
    end_utc (already coalesced), state.
    """
    totals: dict[tuple[str, int], Decimal] = {}
    for row in rows:
        state = str(row.get("state") or "").strip().lower()
        if state in _SKIP_STATES:
            continue
        price = row.get("price_per_day")
        if price is None:
            continue
        price = Decimal(str(price))
        if price == 0:
            continue
        start = as_utc(row.get("start_utc"))
        end = as_utc(row.get("end_utc"))
        if start is None or end is None or end <= start:
            continue
        provider_id = int(row["provider_id"])
        cursor = start.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        cursor -= timedelta(days=cursor.weekday())
        while cursor < end:
            week_end = cursor + timedelta(days=7)
            overlap_start = max(start, cursor)
            overlap_end = min(end, week_end)
            seconds = (overlap_end - overlap_start).total_seconds()
            if seconds > 0:
                local_monday = week_end.astimezone(KYIV).date()
                key = (week_key(local_monday), provider_id)
                totals[key] = totals.get(key, Decimal("0")) + price * Decimal(str(seconds)) / Decimal("86400")
            cursor = week_end
    return {key: money(amount) for key, amount in totals.items()}


def invoice_row(orders, food, third, bolt, fee, df_vat, refund, sponsored) -> dict:
    """Invoice lines for one venue-week. Inputs are warehouse aggregates."""
    food = money(food)
    third = money(third)
    bolt = money(bolt)
    fee = money(fee)
    df_vat = money(df_vat)
    refund = money(refund)
    sponsored = money(sponsored)
    sales = money(food + third)
    bolt_invoice = money(bolt - third)
    marketing = money(bolt_invoice * MARKETING_RATE)
    agency = money(fee - marketing)
    earnings = money(sales - agency + bolt_invoice - marketing - df_vat - refund - sponsored)
    return {
        "orders": int(orders or 0),
        "sales": num(sales),
        "agency_fee": num(agency),
        "bolt_menu_comp": num(bolt_invoice),
        "marketing_fee": num(marketing),
        "venue_df_vat": num(df_vat),
        "sponsored": num(sponsored),
        "refund": num(refund),
        # Chart series: sales volume, total commission, invoice earnings.
        "food_revenue": num(sales),
        "total_fee_gross": num(agency + marketing),
        "net_income": num(earnings),
    }


REVENUE_SUM_KEYS = (
    "orders",
    "sales",
    "agency_fee",
    "bolt_menu_comp",
    "marketing_fee",
    "venue_df_vat",
    "sponsored",
    "refund",
    "food_revenue",
    "total_fee_gross",
    "net_income",
)


def empty_invoice_row() -> dict:
    row = {key: 0 for key in REVENUE_SUM_KEYS}
    row["orders"] = 0
    return row


def weekly_components_sql(provider_ids: str, days_back: int, exclude_current_week: bool = False) -> str:
    week_cut = ""
    if exclude_current_week:
        week_cut = "\n      AND f.order_created_date < DATE_TRUNC('WEEK', CURRENT_DATE())"
    return f"""
    SELECT
      f.order_week,
      f.provider_id,
      COUNT(*) AS orders,
      ROUND(SUM(COALESCE(f.provider_price_after_discount, 0)), 2) AS food,
      ROUND(SUM(COALESCE(tp.third_party, 0)), 2) AS third_party,
      ROUND(SUM(COALESCE(m.bolt_menu_campaign_cost_eur * m.currency_rate, 0)), 2) AS bolt_menu,
      ROUND(SUM(ROUND(COALESCE(f.commission_local, 0) * 1.2, 2)), 2) AS fee_rounded,
      ROUND(SUM(COALESCE(m.provider_delivery_campaign_cost_eur * m.currency_rate * 1.2, 0)), 2) AS df_vat,
      ROUND(SUM(COALESCE(f.total_refunded_amount, 0)), 2) AS refund
    FROM ng_delivery_spark.fact_order_delivery f
    LEFT JOIN ng_public_spark.etl_delivery_order_monetary_metrics m
      ON f.order_id = m.order_id
      AND m.order_created_date >= DATE_FORMAT(DATE_SUB(CURRENT_DATE(), {days_back}), 'yyyy-MM-dd')
    LEFT JOIN (
      SELECT
        order_id,
        SUM(CASE
              WHEN spend_objective = 'marketing_3rd_party_partnership' AND target = 'item_price'
              THEN COALESCE(bolt_spend_local, 0) ELSE 0
            END) AS third_party
      FROM ng_public_spark.etl_delivery_campaign_order_metrics
      WHERE provider_id IN ({provider_ids})
        AND order_created_date >= DATE_SUB(CURRENT_DATE(), {days_back})
      GROUP BY order_id
    ) tp ON tp.order_id = f.order_id
    WHERE f.provider_id IN ({provider_ids})
      AND f.order_created_date >= DATE_SUB(CURRENT_DATE(), {days_back})
      AND f.order_state = 'delivered'{week_cut}
    GROUP BY f.order_week, f.provider_id
    ORDER BY f.order_week, f.provider_id
    """


def listings_sql(provider_ids: str, days_back: int) -> str:
    return f"""
    SELECT
      provider_id,
      CAST(offer_price_per_day_local AS DOUBLE) AS price_per_day,
      sponsored_listing_start_ts AS start_utc,
      COALESCE(sponsored_listing_actual_end_ts, sponsored_listing_default_end_ts) AS end_utc,
      sponsored_listing_state AS state
    FROM ng_delivery_spark.dim_sponsored_listing
    WHERE provider_id IN ({provider_ids})
      AND COALESCE(sponsored_listing_actual_end_ts, sponsored_listing_default_end_ts, sponsored_listing_start_ts)
          >= DATE_SUB(CURRENT_DATE(), {days_back + 14})
    """


def in_report_window(order_week: str, days_back: int, today=None) -> bool:
    today = today or datetime.now(KYIV).date()
    year_s, week_s = str(order_week).split("-W")
    monday = datetime.fromisocalendar(int(year_s), int(week_s), 1).date()
    return monday >= today - timedelta(days=days_back) and monday <= today + timedelta(days=1)


def build_revenue(components_df, listings_df, days_back: int) -> dict:
    """Venue-week invoice rows keyed by order_week then provider_id."""
    sponsored = sponsored_by_week(listings_df.to_dict("records")) if listings_df is not None and len(listings_df) else {}
    revenue: dict = {}
    used = set()
    if components_df is not None:
        for _, r in components_df.iterrows():
            week = str(r["order_week"])
            pid = int(r["provider_id"])
            sl = sponsored.get((week, pid), Decimal("0"))
            used.add((week, pid))
            revenue.setdefault(week, {})[pid] = invoice_row(
                r["orders"], r["food"], r["third_party"], r["bolt_menu"],
                r["fee_rounded"], r["df_vat"], r["refund"], sl,
            )
    for (week, pid), sl in sponsored.items():
        if (week, pid) in used or sl == 0:
            continue
        if not in_report_window(week, days_back):
            continue
        revenue.setdefault(week, {})[pid] = invoice_row(0, 0, 0, 0, 0, 0, 0, sl)
    return revenue


def period_week(value) -> str:
    """ISO week of an invoicing period start. Monday 2026-09-21 is 2026-W39."""
    if value is None:
        return ""
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        day = value
    else:
        day = datetime.fromisoformat(str(value)[:10]).date()
    return week_key(day)


def billing_weekly_sql(provider_ids: str, days_back: int) -> str:
    """One row per venue and invoicing week, using the invoice's own amounts.

    Agency fee includes the Bolt Plus subset, the way the invoice prints the
    main line. Sponsored listing sits on a single row of the week, so it is
    summed, not taken from campaign timestamps.
    """
    return f"""
    SELECT
      provider_id,
      DATE(invoicing_period_start) AS period_start,
      COUNT(order_id) AS orders,
      ROUND(SUM(COALESCE(sales_sum_with_vat, 0)), 2) AS sales,
      ROUND(SUM(COALESCE(marketplace_fee_sum_with_vat, 0) + COALESCE(bolt_plus_agency_fee_sum_with_vat, 0)), 2) AS agency,
      ROUND(SUM(COALESCE(reduction_for_campaigns_sum_with_vat, 0)), 2) AS reduction,
      ROUND(SUM(COALESCE(campaign_fee_sum_with_vat, 0)), 2) AS marketing,
      ROUND(SUM(COALESCE(marketing_cost_delivery_sum_with_vat, 0)), 2) AS delivery,
      ROUND(SUM(COALESCE(refund_deduction_sum_with_vat, 0)), 2) AS refund,
      ROUND(SUM(COALESCE(invoice_ads_monetisation_sum_with_vat, 0)), 2) AS sponsored,
      MAX(weekly_payout_sum) AS weekly_payout,
      MAX(aggregated_invoice_public_id) AS invoice_id,
      MIN(aggregated_invoice_public_id) AS invoice_id_min,
      MAX(aggregated_invoice_link) AS invoice_link,
      MIN(aggregated_invoice_link) AS invoice_link_min
    FROM main.ng_commerce_billing.reporting_billing_delivery_provider_sales_report
    WHERE provider_id IN ({provider_ids})
      AND created_date_local >= DATE_SUB(CURRENT_DATE(), {days_back})
      AND invoicing_period_start IS NOT NULL
    GROUP BY provider_id, DATE(invoicing_period_start)
    """


def _links(invoice_id, invoice_url, invoice_id_min, invoice_url_min) -> list:
    links = []
    seen = set()
    for inv_id, url in ((invoice_id, invoice_url), (invoice_id_min, invoice_url_min)):
        inv_id = "" if inv_id is None else str(inv_id)
        url = "" if url is None else str(url)
        if not inv_id or inv_id in seen or inv_id.lower() == "none":
            continue
        seen.add(inv_id)
        links.append({"id": inv_id, "url": url})
    return links


def revenue_from_billing(df) -> dict:
    """Venue-week rows whose figures are the summed invoice lines."""
    revenue: dict = {}
    if df is None:
        return revenue
    for _, r in df.iterrows():
        week = period_week(r["period_start"])
        if not week:
            continue
        pid = int(r["provider_id"])
        sales = money(r["sales"])
        agency = money(r["agency"])
        reduction = money(r["reduction"])
        marketing = money(r["marketing"])
        delivery = money(r["delivery"])
        refund = money(r["refund"])
        sponsored = money(r["sponsored"])
        earnings = money(sales - agency - refund - delivery + reduction - marketing - sponsored)
        revenue.setdefault(week, {})[pid] = {
            "orders": int(r["orders"] or 0),
            "sales": num(sales),
            "agency_fee": num(agency),
            "bolt_menu_comp": num(reduction),
            "marketing_fee": num(marketing),
            "venue_df_vat": num(delivery),
            "sponsored": num(sponsored),
            "refund": num(refund),
            "food_revenue": num(sales),
            "total_fee_gross": num(agency),
            "net_income": num(earnings),
            "invoice_links": _links(
                r.get("invoice_id"), r.get("invoice_link"),
                r.get("invoice_id_min"), r.get("invoice_link_min"),
            ),
        }
    return revenue
