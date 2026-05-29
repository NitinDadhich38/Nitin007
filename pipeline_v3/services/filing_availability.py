from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from .index_registry import DASHBOARD_DATA_DIR


def period_end_to_labels(period_end: str) -> Dict[str, str]:
    """Return annual and quarterly keys used by dashboard JSON for a period end date."""
    dt = datetime.fromisoformat(period_end)
    annual = f"FY{dt.year}" if dt.month == 3 and dt.day == 31 else ""
    quarter = dt.strftime("%b %Y")
    return {"annual": annual, "quarterly": quarter}


def _has_statement(bucket: Dict[str, Any], period_key: str) -> bool:
    return bool(bucket.get(period_key)) if isinstance(bucket, dict) else False


def _has_any_financial_data(data: Dict[str, Any], period_key: str, statement_period: str) -> bool:
    financials = data.get("financials") or {}
    for result_type in ("consolidated", "standalone"):
        layer = financials.get(result_type) or {}
        statements = layer.get(statement_period) or {}
        if any(_has_statement(statements.get(stmt) or {}, period_key) for stmt in ("profit_loss", "balance_sheet", "cash_flow")):
            return True
    return False


def _available_periods(data: Dict[str, Any], statement_period: str) -> list[str]:
    def sort_key(period: str) -> tuple[int, int]:
        if period.startswith("FY") and period[2:].isdigit():
            return (int(period[2:]), 12)
        try:
            dt = datetime.strptime(period, "%b %Y")
            return (dt.year, dt.month)
        except ValueError:
            return (0, 0)

    periods: set[str] = set()
    financials = data.get("financials") or {}
    for result_type in ("consolidated", "standalone"):
        layer = financials.get(result_type) or {}
        statements = layer.get(statement_period) or {}
        for statement in ("profit_loss", "balance_sheet", "cash_flow"):
            bucket = statements.get(statement) or {}
            if isinstance(bucket, dict):
                periods.update(str(period) for period, values in bucket.items() if values)
    return sorted(periods, key=sort_key, reverse=True)


def _has_any_transformed_financials(data: Dict[str, Any]) -> bool:
    return bool(_available_periods(data, "annual") or _available_periods(data, "quarterly"))


def check_company_filing(symbol: str, period_end: str, data_dir: Path = DASHBOARD_DATA_DIR) -> Dict[str, Any]:
    symbol = symbol.upper()
    path = data_dir / f"{symbol}.json"
    labels = period_end_to_labels(period_end)

    if not path.exists():
        return {
            "symbol": symbol,
            "period_end": period_end,
            "status": "MISSING",
            "parsed": False,
            "has_xbrl": False,
            "has_pdf": False,
            "result_type": None,
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "message": "No parsed dashboard data is available for this company yet.",
        }

    import json

    data = json.loads(path.read_text(encoding="utf-8"))
    annual_key = labels["annual"]
    quarterly_key = labels["quarterly"]
    has_annual = bool(annual_key and _has_any_financial_data(data, annual_key, "annual"))
    has_quarterly = _has_any_financial_data(data, quarterly_key, "quarterly")
    annual_periods = _available_periods(data, "annual")
    quarterly_periods = _available_periods(data, "quarterly")
    has_parsed_data = _has_any_transformed_financials(data)
    sources = data.get("metadata", {}).get("data_sources", [])
    source_names = [s.get("type") if isinstance(s, dict) else str(s) for s in sources]
    has_xbrl = any("XBRL" in str(name).upper() for name in source_names)
    filing_assets = data.get("metadata", {}).get("filing_assets", {})

    if has_annual or has_quarterly or has_parsed_data:
        status = "PARSED"
    elif has_xbrl:
        status = "AVAILABLE_RAW"
    else:
        status = "MISSING"

    financials = data.get("financials") or {}
    result_type: Optional[str] = None
    if financials.get("consolidated"):
        result_type = "consolidated"
    elif financials.get("standalone"):
        result_type = "standalone"

    return {
        "symbol": symbol,
        "period_end": period_end,
        "status": status,
        "parsed": status == "PARSED",
        "has_xbrl": has_xbrl,
        "has_pdf": bool(filing_assets.get("pdf")),
        "raw_xbrl": filing_assets.get("xbrl"),
        "pdf": filing_assets.get("pdf"),
        "result_type": result_type,
        "available_periods": {
            "annual": annual_key if has_annual else (annual_periods[0] if annual_periods else None),
            "quarterly": quarterly_key if has_quarterly else (quarterly_periods[0] if quarterly_periods else None),
        },
        "requested_period_available": has_annual or has_quarterly,
        "source_names": source_names,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }
