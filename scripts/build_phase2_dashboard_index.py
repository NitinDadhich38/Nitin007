import json
import sys
from pathlib import Path
from typing import Dict

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline_v3.services.filing_availability import check_company_filing
from pipeline_v3.services.index_registry import DASHBOARD_DATA_DIR, DASHBOARD_DIR, load_constituents, load_indices, symbol_to_indexes


DEFAULT_PERIOD_END = "2026-03-31"


def _company_from_payload(symbol: str) -> Dict:
    path = DASHBOARD_DATA_DIR / f"{symbol}.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8")).get("company", {})


def main() -> None:
    index_memberships = symbol_to_indexes()
    by_symbol = {}
    for row in load_constituents():
        company = _company_from_payload(row.symbol)
        availability = check_company_filing(row.symbol, DEFAULT_PERIOD_END)
        by_symbol[row.symbol] = {
            "symbol": row.symbol,
            "name": company.get("name") or row.name,
            "sector": company.get("sector") or row.industry,
            "industry": company.get("industry") or row.industry,
            "isin": row.isin,
            "indexes": index_memberships.get(row.symbol, []),
            "coverage_status": "parsed" if availability["status"] == "PARSED" else "missing",
            "filing_status": availability["status"],
            "period_end": DEFAULT_PERIOD_END,
            "path": f"data/{row.symbol}.json" if (DASHBOARD_DATA_DIR / f"{row.symbol}.json").exists() else None,
        }

    companies = sorted(by_symbol.values(), key=lambda c: c["symbol"])
    active_symbols = set(by_symbol)
    for stale in DASHBOARD_DATA_DIR.glob("*.json"):
        if stale.stem.upper() not in active_symbols:
            stale.unlink()
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    (DASHBOARD_DIR / "companies.json").write_text(json.dumps(companies, ensure_ascii=False, indent=2), encoding="utf-8")
    (DASHBOARD_DIR / "indices.json").write_text(json.dumps(load_indices(), ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote dashboard index with {len(companies)} companies")


if __name__ == "__main__":
    main()
