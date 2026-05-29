"""
Compare generated dashboard JSON against locally saved Screener-style baselines.

This script deliberately does not scrape Screener. Put parsed baseline JSON files
in benchmarks/screener/parsed/{SYMBOL}.json with the same high-level shape as
dashboard/data/{SYMBOL}.json, then run:

    python3 scripts/benchmark_against_screener.py
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


ROOT = Path(__file__).resolve().parent.parent
PIPELINE_DIR = ROOT / "dashboard" / "data"
BASELINE_DIR = ROOT / "benchmarks" / "screener" / "parsed"
REPORT_DIR = ROOT / "benchmarks" / "screener" / "reports"


FIELD_MAP = {
    "profit_loss": [
        ("sales_screener_basis", "sales"),
        ("operating_profit", "operating_profit"),
        ("other_income", "other_income"),
        ("interest", "interest"),
        ("depreciation", "depreciation"),
        ("profit_before_tax", "profit_before_tax"),
        ("screener_net_profit", "net_profit"),
        ("eps", "eps"),
    ],
    "balance_sheet": [
        ("equity_share_capital", "equity_capital"),
        ("reserves", "reserves"),
        ("screener_borrowings", "borrowings"),
        ("deposits", "deposits"),
        ("total_liabilities", "total_liabilities"),
        ("screener_fixed_assets", "fixed_assets"),
        ("capital_work_in_progress", "cwip"),
        ("screener_investments", "investments"),
        ("other_assets", "other_assets"),
        ("total_assets", "total_assets"),
    ],
    "cash_flow": [
        ("cash_from_operations", "cfo"),
        ("cash_from_investing", "cfi"),
        ("cash_from_financing", "cff"),
        ("net_cash_flow", "net_cash_flow"),
        ("free_cash_flow", "free_cash_flow"),
    ],
}


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def get_layer(data: Dict[str, Any], result_type: str, period_type: str, statement: str) -> Dict[str, Dict]:
    return (((data.get("financials") or {}).get(result_type) or {}).get(period_type) or {}).get(statement) or {}


def num(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def compare_value(pipeline_value: Any, screener_value: Any) -> tuple[Optional[float], Optional[float], str]:
    pv = num(pipeline_value)
    sv = num(screener_value)
    if pv is None or sv is None:
        return None, None, "missing"
    absolute = pv - sv
    pct = abs(absolute) / max(abs(sv), 1.0) * 100.0
    if pct <= 0.5:
        status = "exact"
    elif pct <= 2.0:
        status = "near"
    else:
        status = "mismatch"
    return round(absolute, 4), round(pct, 4), status


def iter_rows(symbol: str, pipeline: Dict[str, Any], baseline: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    for result_type in ("consolidated", "standalone"):
        for period_type in ("quarterly", "annual"):
            for statement, fields in FIELD_MAP.items():
                p_bucket = get_layer(pipeline, result_type, period_type, statement)
                s_bucket = get_layer(baseline, result_type, period_type, statement)
                for period in sorted(set(p_bucket) | set(s_bucket)):
                    p_row = p_bucket.get(period) or {}
                    s_row = s_bucket.get(period) or {}
                    for p_field, s_field in fields:
                        abs_diff, pct_diff, status = compare_value(p_row.get(p_field), s_row.get(s_field))
                        yield {
                            "symbol": symbol,
                            "result_type": result_type,
                            "statement": statement,
                            "period_type": period_type,
                            "field": p_field,
                            "period": period,
                            "pipeline_value": p_row.get(p_field),
                            "screener_value": s_row.get(s_field),
                            "absolute_diff": abs_diff,
                            "pct_diff": pct_diff,
                            "match_status": status,
                        }


def main() -> int:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    rows = []
    for baseline_path in sorted(BASELINE_DIR.glob("*.json")):
        symbol = baseline_path.stem.upper()
        pipeline_path = PIPELINE_DIR / f"{symbol}.json"
        if not pipeline_path.exists():
            continue
        rows.extend(iter_rows(symbol, load_json(pipeline_path), load_json(baseline_path)))

    report_csv = REPORT_DIR / "nifty100_accuracy_report.csv"
    fieldnames = [
        "symbol", "result_type", "statement", "period_type", "field", "period",
        "pipeline_value", "screener_value", "absolute_diff", "pct_diff", "match_status",
    ]
    with report_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    comparable = [r for r in rows if r["match_status"] != "missing"]
    exactish = [r for r in comparable if r["match_status"] in {"exact", "near"}]
    summary = {
        "baseline_company_count": len({r["symbol"] for r in rows}),
        "comparison_count": len(rows),
        "comparable_count": len(comparable),
        "accuracy_pct": round(len(exactish) / len(comparable) * 100.0, 2) if comparable else None,
        "report": str(report_csv),
    }
    (REPORT_DIR / "nifty100_accuracy_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
