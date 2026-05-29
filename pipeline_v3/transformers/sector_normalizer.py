from __future__ import annotations

import logging
from typing import Any

from .financial_mapper import CompanyFinancials, ProfitLoss

logger = logging.getLogger(__name__)


BANK_SYMBOLS = {"HDFCBANK", "ICICIBANK", "SBIN", "AXISBANK", "KOTAKBANK", "INDUSINDBK", "BANKBARODA", "CANBK", "PNB", "UNIONBANK", "IDFCFIRSTB"}
NBFC_SYMBOLS = {"BAJFINANCE", "BAJAJFINSV", "CHOLAFIN", "JIOFIN", "MUTHOOTFIN", "PFC", "RECLTD", "SHRIRAMFIN", "IRFC", "TATACAP"}
INSURANCE_SYMBOLS = {"HDFCLIFE", "SBILIFE", "ICICIGI"}
UTILITY_SYMBOLS = {"NTPC", "POWERGRID"}
OIL_GAS_SYMBOLS = {"RELIANCE", "ONGC", "BPCL"}
CONGLOMERATE_SYMBOLS = {"RELIANCE", "GRASIM", "LT", "ADANIENT", "ADANIPORTS", "M&M", "TATAMOTORS"}


def resolve_accounting_schema(symbol: str, sector: str = "", industry: str = "") -> str:
    """Return the accounting model used for validation and confidence scoring."""
    sym = (symbol or "").upper()
    text = f"{sector} {industry}".lower()

    if sym in BANK_SYMBOLS or "bank" in text:
        return "banking"
    if sym in INSURANCE_SYMBOLS or "insurance" in text:
        return "insurance"
    if sym in NBFC_SYMBOLS or "nbfc" in text or "finance" in text:
        return "nbfc"
    if sym in UTILITY_SYMBOLS or "power" in text or "utility" in text:
        return "power_utility"
    if sym in OIL_GAS_SYMBOLS or "oil" in text or "gas" in text or "refiner" in text:
        return "oil_gas_psu"
    if sym in CONGLOMERATE_SYMBOLS or "construction" in text or "commercial services" in text:
        return "conglomerate_jv_nci"
    return "standard_indas"


def _apply_owner_pat(pl: ProfitLoss) -> None:
    """Keep total PAT and owner PAT separate while maintaining legacy net_profit."""
    pbt = pl.profit_before_tax
    tax = pl.tax
    share = pl.share_of_associates_jv or 0.0

    if pl.profit_for_period is None and pbt is not None and tax is not None:
        pl.profit_for_period = round(float(pbt) - float(tax) + float(share), 2)

    if pl.net_profit_attributable_to_owners is None:
        if pl.net_profit is not None and pl.profit_for_period is not None:
            # Existing `net_profit` can come from owner-attributable tags or APIs.
            # Preserve it as owner PAT when it differs from total PAT.
            if abs(float(pl.net_profit) - float(pl.profit_for_period)) > max(5.0, abs(float(pl.profit_for_period)) * 0.002):
                pl.net_profit_attributable_to_owners = pl.net_profit
        if pl.net_profit_attributable_to_owners is None and pl.profit_for_period is not None and pl.nci_profit is not None:
            pl.net_profit_attributable_to_owners = round(float(pl.profit_for_period) - float(pl.nci_profit), 2)

    if pl.net_profit_attributable_to_owners is not None:
        pl.net_profit = pl.net_profit_attributable_to_owners
    elif pl.net_profit is None and pl.profit_for_period is not None:
        pl.net_profit = pl.profit_for_period
    if pl.profit_for_period is not None:
        pl.screener_net_profit = pl.profit_for_period
    elif pl.net_profit is not None:
        pl.screener_net_profit = pl.net_profit
    if pl.net_profit_attributable_to_owners is not None:
        pl.pat_attributable_to_owners = pl.net_profit_attributable_to_owners
    if pl.nci_profit is not None:
        pl.minority_interest_profit = pl.nci_profit


def _repair_absurd_nci(pl: ProfitLoss, *, symbol: str, period_name: str, label: str) -> None:
    """Drop impossible NCI facts before they contaminate owner PAT."""
    if pl.nci_profit is None:
        return
    try:
        nci = float(pl.nci_profit)
        basis_values = [
            pl.profit_for_period,
            pl.profit_before_tax,
            pl.tax,
            pl.revenue_from_operations,
            pl.total_income,
        ]
        basis = max(abs(float(v)) for v in basis_values if v is not None)
    except Exception:
        return

    if basis <= 0:
        return

    # NCI can be material in conglomerates, but it cannot reasonably be many
    # times larger than the entire income statement. NSE XBRL occasionally
    # exposes stray percentage/marker contexts (for example 100000) under NCI.
    if abs(nci) > max(10_000.0, basis * 2.0):
        logger.info(
            "[SECTOR-NORMALIZER] %s %s %s: dropping impossible NCI %.2f (basis %.2f)",
            symbol,
            period_name,
            label,
            nci,
            basis,
        )
        pl.nci_profit = None
        if pl.profit_for_period is not None:
            total_pat = round(float(pl.profit_for_period), 2)
            pl.net_profit_attributable_to_owners = total_pat
            pl.net_profit = total_pat


def _reconcile_pat_fields(pl: ProfitLoss, *, symbol: str, period_name: str, label: str, accounting_schema: str) -> None:
    """Prefer explicit owner PAT + NCI over formula-derived total PAT when contexts disagree."""
    try:
        owner = float(pl.net_profit_attributable_to_owners) if pl.net_profit_attributable_to_owners is not None else None
        nci = float(pl.nci_profit) if pl.nci_profit is not None else None
        total = float(pl.profit_for_period) if pl.profit_for_period is not None else None
    except Exception:
        return

    if owner is not None and nci is not None and abs(owner) > 0:
        expected_total = round(owner + nci, 2)
        if total is None or abs(expected_total - total) > max(100.0, abs(expected_total) * 0.05):
            logger.info(
                "[SECTOR-NORMALIZER] %s %s %s: reconciling total PAT %.2f -> %.2f from owner PAT + NCI",
                symbol,
                period_name,
                label,
                total or 0.0,
                expected_total,
            )
            pl.profit_for_period = expected_total
            total = expected_total

    if total is not None and nci is not None:
        expected_owner = round(total - nci, 2)
        current_owner = owner
        if current_owner is None or abs(expected_owner - current_owner) > max(100.0, abs(expected_owner) * 0.05):
            # Zero owner PAT alongside a large reported total PAT is a parser
            # artifact, not a valid economic result.
            if current_owner in (None, 0.0) and abs(expected_owner) > 100.0:
                logger.info(
                    "[SECTOR-NORMALIZER] %s %s %s: repairing owner PAT %.2f -> %.2f from total PAT - NCI",
                    symbol,
                    period_name,
                    label,
                    current_owner or 0.0,
                    expected_owner,
                )
                pl.net_profit_attributable_to_owners = expected_owner
                pl.net_profit = expected_owner

    if (
        pl.profit_for_period is not None
        and pl.tax is not None
        and pl.profit_before_tax is not None
        and accounting_schema in {"standard_indas", "conglomerate_jv_nci"}
    ):
        try:
            share = float(pl.share_of_associates_jv or 0.0)
            implied_pbt = round(float(pl.profit_for_period) + float(pl.tax) - share, 2)
            pbt = float(pl.profit_before_tax)
            revenue_basis = float(pl.total_income or pl.revenue_from_operations or 0.0)
        except Exception:
            return
        if revenue_basis > 0 and abs(implied_pbt) > revenue_basis * 2.0:
            return
        if abs(implied_pbt - pbt) > max(100.0, abs(implied_pbt) * 0.05):
            logger.info(
                "[SECTOR-NORMALIZER] %s %s %s: repairing PBT %.2f -> %.2f from reconciled PAT+Tax-Assoc",
                symbol,
                period_name,
                label,
                pbt,
                implied_pbt,
            )
            pl.profit_before_tax = implied_pbt


def _repair_pbt_tieout(pl: ProfitLoss, *, symbol: str, period_name: str, label: str) -> None:
    """Repair obvious wrong-context PBT facts using total PAT + tax - associate/JV share."""
    if pl.profit_before_tax is None or pl.tax is None or pl.profit_for_period is None:
        return
    try:
        pbt = float(pl.profit_before_tax)
        tax = float(pl.tax)
        total_pat = float(pl.profit_for_period)
        share = float(pl.share_of_associates_jv or 0.0)
    except Exception:
        return

    implied_pbt = round(total_pat + tax - share, 2)
    revenue_basis = float(pl.total_income or pl.revenue_from_operations or 0.0)
    if revenue_basis > 0 and abs(implied_pbt) > revenue_basis * 2.0:
        return
    delta = abs(pbt - implied_pbt)
    tolerance = max(1000.0, abs(implied_pbt) * 0.50)
    if delta <= tolerance:
        return

    sign_conflict = (pbt < 0 < implied_pbt) or (implied_pbt < 0 < pbt)
    extreme_scale = abs(pbt) > max(abs(total_pat), abs(tax), abs(implied_pbt), 1.0) * 20
    if sign_conflict or extreme_scale:
        logger.info(
            "[SECTOR-NORMALIZER] %s %s %s: repairing PBT %.2f -> %.2f from PAT+Tax-Assoc",
            symbol,
            period_name,
            label,
            pbt,
            implied_pbt,
        )
        pl.profit_before_tax = implied_pbt


def _drop_absurd_tax(pl: ProfitLoss, *, symbol: str, period_name: str, label: str) -> None:
    if pl.tax is None:
        return
    try:
        tax = abs(float(pl.tax))
        revenue_basis = abs(float(pl.total_income or pl.revenue_from_operations or 0.0))
        pbt = abs(float(pl.profit_before_tax or 0.0))
    except Exception:
        return
    basis = max(revenue_basis, pbt, abs(float(pl.profit_for_period or 0.0)), 1.0)
    if tax > max(10_000.0, basis * 2.0):
        logger.info(
            "[SECTOR-NORMALIZER] %s %s %s: dropping impossible tax %.2f (basis %.2f)",
            symbol,
            period_name,
            label,
            float(pl.tax),
            basis,
        )
        pl.tax = None


def _blank_non_applicable_metrics(pl: ProfitLoss, accounting_schema: str) -> None:
    if accounting_schema in {"banking", "nbfc", "insurance"}:
        # EBITDA / EBIT style metrics are not meaningful in BFSI statements.
        pl.ebitda = None
        if accounting_schema in {"banking", "insurance"}:
            pl.depreciation = None


def normalize_by_sector(fin: CompanyFinancials, *, symbol: str, sector: str = "", industry: str = "") -> str:
    """Apply sector-aware final statement normalization in-place."""
    accounting_schema = resolve_accounting_schema(symbol, sector, industry)
    fin.metadata["accounting_schema"] = accounting_schema

    pruned: list[str] = []
    for period_name, bucket in (("annual", fin.profit_loss.get("annual", {})), ("quarterly", fin.profit_loss.get("quarterly", {}))):
        for label, pl in list(bucket.items()):
            if not isinstance(pl, ProfitLoss):
                continue
            _repair_absurd_nci(pl, symbol=symbol, period_name=period_name, label=label)
            _drop_absurd_tax(pl, symbol=symbol, period_name=period_name, label=label)
            _apply_owner_pat(pl)
            _reconcile_pat_fields(pl, symbol=symbol, period_name=period_name, label=label, accounting_schema=accounting_schema)
            if accounting_schema not in {"banking", "nbfc", "insurance"}:
                _repair_pbt_tieout(pl, symbol=symbol, period_name=period_name, label=label)
            _blank_non_applicable_metrics(pl, accounting_schema)

            if accounting_schema in {"banking", "nbfc", "insurance"}:
                if pl.total_income is None and pl.revenue_from_operations is not None:
                    pl.total_income = round((pl.revenue_from_operations or 0) + (pl.other_income or 0), 2)

            has_topline = pl.revenue_from_operations is not None or pl.total_income is not None
            has_pat = pl.net_profit is not None or pl.profit_for_period is not None or pl.net_profit_attributable_to_owners is not None
            has_tax = pl.tax is not None
            # NSE legacy filings often expose segment PBT-only rows. They are not a
            # usable financial period and should not pollute UI history or confidence.
            if not has_topline and not has_pat and not has_tax:
                bucket.pop(label, None)
                pruned.append(f"{period_name}:{label}")

    if pruned:
        fin.metadata["pruned_incomplete_periods"] = pruned

    return accounting_schema
