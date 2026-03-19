"""
scripts/post_process_fixer.py
=================================
Phase 2 — Accuracy Improvement Script
Run AFTER the main pipeline to fix systemic issues in all dashboard JSONs:

  Fix 1: ROCE — recalculate from EBIT + Capital Employed already in the JSON
  Fix 2: Exceptional Items — subtract from net_profit to get core PAT
  Fix 3: Financial sector revenue — use total_income where revenue_from_operations is zero
  Fix 4: INFY currency — detect USD-filed companies and apply INR conversion
  Fix 5: Unit mismatch — detect and correct B/S scale vs P&L scale

Run: python3 scripts/post_process_fixer.py
"""

import json, glob, os, logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger("PostFixer")
DATA_DIR = "data"
BACKUP_DIR = "data/backups"

# ── Config ────────────────────────────────────────────────────────────────────

# Companies that file in USD (report in USD Lakhs on NSE form)
USD_COMPANIES = {"INFY", "WIPRO", "HCLTECH", "TCS", "TECHM"}
INR_USD_RATE = 83.5   # approximate average FY2025 rate

# Companies classified as "Financial" — use total_income as revenue
FINANCIAL_SECTOR_SYMBOLS = {
    "HDFCBANK", "ICICIBANK", "KOTAKBANK", "AXISBANK", "INDUSINDBK",
    "SBIN", "BAJFINANCE", "BAJAJFINSV", "SBILIFE", "HDFCLIFE"
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def safe_val(d, key, default=None):
    v = d.get(key)
    return v if v not in [None, 0, ""] else default

def backup(filepath):
    os.makedirs(BACKUP_DIR, exist_ok=True)
    bak = os.path.join(BACKUP_DIR, os.path.basename(filepath) + ".postfix.bak")
    if not os.path.exists(bak):
        with open(filepath) as s, open(bak, "w") as d:
            d.write(s.read())

def clamp(v, lo, hi):
    if v is None: return None
    return max(lo, min(hi, v))

# ── Fix 1: ROCE Recalculation ─────────────────────────────────────────────────

def fix_roce(sym, pl_ann, bs_ann, dm):
    """Recompute ROCE = EBIT / (Total Assets - Current Liabilities) for every year."""
    fixed = 0
    for yr in pl_ann:
        pl = pl_ann.get(yr, {})
        bs = bs_ann.get(yr, {})
        ebit = safe_val(pl, "ebit")
        assets = safe_val(bs, "total_assets")
        curr_liab = safe_val(bs, "current_liabilities")

        if ebit is None or assets is None:
            continue

        # Use current_liabilities if available, else 0
        cap_employed = assets - (curr_liab or 0)
        if cap_employed <= 0:
            continue

        roce = round((ebit / cap_employed) * 100, 2)
        roce = clamp(roce, -200, 500)   # sanity bounds

        if yr not in dm:
            dm[yr] = {}
        dm[yr]["roce"] = roce
        fixed += 1

    if fixed:
        log.info(f"  [{sym}] ROCE fixed for {fixed} year(s)")
    return fixed

# ── Fix 2: Exceptional Items → Core PAT ──────────────────────────────────────

def fix_exceptional_items(sym, pl_ann):
    """
    If exceptional_items is non-zero, compute core_net_profit separately.
    We do NOT override net_profit (to preserve the full SEBI figure),
    but we add 'core_net_profit' which is what Screener shows as PAT.
    """
    fixed = 0
    for yr, pl in pl_ann.items():
        exc = safe_val(pl, "exceptional_items")
        np_ = safe_val(pl, "net_profit")
        if exc is not None and exc != 0 and np_ is not None:
            core = round(np_ - exc, 2)
            pl["core_net_profit"] = core
            fixed += 1

    if fixed:
        log.info(f"  [{sym}] core_net_profit added for {fixed} year(s) (stripped exceptional items)")
    return fixed

# ── Fix 3: Financial Sector Revenue ──────────────────────────────────────────

def fix_financial_revenue(sym, pl_ann):
    """
    For banks/NBFCs/insurers, if revenue_from_operations is 0 or missing,
    fall back to total_income as the headline revenue figure.
    """
    if sym not in FINANCIAL_SECTOR_SYMBOLS:
        return 0
    fixed = 0
    for yr, pl in pl_ann.items():
        rev = safe_val(pl, "revenue_from_operations")
        total_inc = safe_val(pl, "total_income")
        if (rev is None or rev == 0) and total_inc:
            pl["revenue_from_operations"] = total_inc
            fixed += 1
    if fixed:
        log.info(f"  [{sym}] Financial sector revenue patched for {fixed} year(s)")
    return fixed

# ── Fix 4: USD → INR Currency Conversion ─────────────────────────────────────

TYPICAL_INR_REV = {
    "INFY": 150000, "TCS": 235000, "WIPRO": 85000,
    "HCLTECH": 105000, "TECHM": 52000
}

def fix_usd_currency(sym, pl_ann, bs_ann, cf_ann):
    """
    If a USD-filing company shows revenue far below the expected INR range,
    it was likely not converted. Multiply all P&L, B/S, C/F figures by INR_USD_RATE.
    """
    if sym not in USD_COMPANIES:
        return 0

    # Check latest year revenue
    latest = sorted(pl_ann.keys(), reverse=True)[0] if pl_ann else None
    if not latest:
        return 0

    rev = safe_val(pl_ann[latest], "revenue_from_operations")
    expected = TYPICAL_INR_REV.get(sym, 50000)

    if rev is None or rev > expected * 0.5:
        # Looks fine (already in INR range)
        return 0

    log.warning(f"  [{sym}] Revenue={rev} looks like USD. Applying INR conversion (×{INR_USD_RATE})")

    PL_FIELDS = ["revenue_from_operations","other_income","total_income","operating_expenses",
                 "ebitda","ebit","interest","depreciation","profit_before_tax","tax","net_profit",
                 "exceptional_items","core_net_profit"]
    BS_FIELDS = ["equity_share_capital","reserves","total_equity","total_debt","long_term_borrowings",
                 "short_term_borrowings","total_assets","total_liabilities","current_assets",
                 "current_liabilities","ppe","cash_and_equivalents","investments"]
    CF_FIELDS = ["cash_from_operations","capital_expenditure","cash_from_investing",
                 "cash_from_financing","free_cash_flow","net_cash_flow"]

    fixed = 0
    for yr_dict, fields in [(pl_ann, PL_FIELDS), (bs_ann, BS_FIELDS), (cf_ann, CF_FIELDS)]:
        for yr, row in yr_dict.items():
            for f in fields:
                v = row.get(f)
                if v is not None and v != 0:
                    row[f] = round(v * INR_USD_RATE, 2)
                    fixed += 1
    log.info(f"  [{sym}] USD→INR applied to {fixed} fields")
    return 1

# ── Fix 5: Balance Sheet Unit Mismatch ───────────────────────────────────────

def fix_bs_unit(sym, pl_ann, bs_ann):
    """
    If B/S total_assets is 10x–100x bigger than P&L revenue,
    it's likely a Lakhs vs Crores mismatch — divide B/S by 100.
    """
    fixed = 0
    for yr in pl_ann:
        if yr not in bs_ann:
            continue
        rev = safe_val(pl_ann[yr], "revenue_from_operations")
        assets = safe_val(bs_ann[yr], "total_assets")
        if rev is None or assets is None:
            continue

        ratio = assets / rev
        if ratio > 50:
            # B/S is 50x bigger than revenue — almost certainly a unit mismatch
            log.warning(f"  [{sym}] {yr}: B/S assets/revenue ratio={ratio:.1f}x → dividing all B/S by 100")
            BS_FIELDS = ["equity_share_capital","reserves","total_equity","total_debt",
                         "long_term_borrowings","short_term_borrowings","total_assets",
                         "total_liabilities","current_assets","current_liabilities",
                         "ppe","cash_and_equivalents","investments"]
            for f in BS_FIELDS:
                v = bs_ann[yr].get(f)
                if v is not None:
                    bs_ann[yr][f] = round(v / 100.0, 2)
            fixed += 1

    if fixed:
        log.info(f"  [{sym}] B/S unit fix applied for {fixed} year(s)")
    return fixed

# ── Fix 6: Re-derive Metrics from Fixed Data  ─────────────────────────────────

def recompute_derived(sym, pl_ann, bs_ann, cf_ann, dm):
    """After all fixes, recalculate key derived metrics for accuracy."""
    for yr in pl_ann:
        pl = pl_ann.get(yr, {})
        bs = bs_ann.get(yr, {})
        cf = cf_ann.get(yr, {})

        rev = safe_val(pl, "revenue_from_operations")
        np_ = safe_val(pl, "net_profit")
        ebit = safe_val(pl, "ebit")
        ebitda = safe_val(pl, "ebitda")
        equity = safe_val(bs, "total_equity")
        assets = safe_val(bs, "total_assets")
        debt = safe_val(bs, "total_debt")
        curr_liab = safe_val(bs, "current_liabilities")
        cfo = safe_val(cf, "cash_from_operations")

        if yr not in dm:
            dm[yr] = {}
        row = dm[yr]

        if rev and np_:
            row["net_profit_margin"] = round(np_ / rev * 100, 2)
        if rev and ebitda:
            row["operating_margin"] = round(ebitda / rev * 100, 2)
        if equity and equity != 0 and np_:
            roe = round(np_ / equity * 100, 2)
            if abs(roe) < 500:   # sanity cap
                row["roe"] = roe
        if assets and assets != 0 and np_:
            row["roa"] = round(np_ / assets * 100, 2)
        if debt is not None and equity and equity != 0:
            row["debt_to_equity"] = round(debt / equity, 2)
        if cfo and np_ and np_ != 0:
            row["cfo_to_net_profit"] = round(cfo / np_, 2)

# ── Main Loop ──────────────────────────────────────────────────────────────────

def run():
    files = sorted(glob.glob(os.path.join(DATA_DIR, "**", "company_financials.json"), recursive=True))
    files = [f for f in files if not f.endswith("companies.json")]

    total_fixes = 0
    total_files = 0
    summary = []

    log.info(f"Post-Processing {len(files)} company files...")

    for filepath in files:
        try:
            with open(filepath) as f:
                data = json.load(f)
        except Exception as e:
            log.error(f"Cannot read {filepath}: {e}")
            continue

        sym = data.get("company", {}).get("symbol", "UNK")
        pl_ann = data.get("profit_loss", {}).get("annual", {})
        bs_ann = data.get("balance_sheet", {}).get("annual", {})
        cf_ann = data.get("cash_flow", {}).get("annual", {})
        dm     = data.get("derived_metrics", {})

        if not pl_ann:
            log.warning(f"  [{sym}] Skipping — no annual P&L data")
            continue

        log.info(f"Processing [{sym}]...")
        backup(filepath)

        fixes = 0
        fixes += fix_usd_currency(sym, pl_ann, bs_ann, cf_ann)    # Fix 4 first (needs correct scale)
        fixes += fix_bs_unit(sym, pl_ann, bs_ann)                  # Fix 5
        fixes += fix_financial_revenue(sym, pl_ann)               # Fix 3
        fixes += fix_exceptional_items(sym, pl_ann)               # Fix 2
        fix_roce(sym, pl_ann, bs_ann, dm)                          # Fix 1 (always run)
        recompute_derived(sym, pl_ann, bs_ann, cf_ann, dm)        # Fix 6

        # Write back
        data["derived_metrics"] = dm
        data["profit_loss"]["annual"] = pl_ann
        data["balance_sheet"]["annual"] = bs_ann
        data["cash_flow"]["annual"] = cf_ann
        data.setdefault("metadata", {})["post_processed"] = datetime.now().isoformat()

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

        total_fixes += fixes
        total_files += 1
        summary.append((sym, fixes))

    log.info("\n" + "="*60)
    log.info(f"POST-PROCESS COMPLETE: {total_files} files, {total_fixes} patches applied")
    log.info("="*60)
    for sym, n in summary:
        if n > 0:
            log.info(f"  {sym}: {n} fix(es)")

if __name__ == "__main__":
    run()
