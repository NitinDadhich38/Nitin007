import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline_v3.main_v2 import PipelineV2
from pipeline_v3.utils.universe import load_universe


UNIVERSE_PATH = ROOT / "pipeline_v3" / "config" / "nifty100_universe.json"
DATA_DIR = ROOT / "data"
LOG_PATH = ROOT / "exports" / "nifty100_ingestion_status.json"


def has_final_data(symbol: str) -> bool:
    return (DATA_DIR / symbol.lower() / "final" / "company_financials.json").exists()


def main() -> int:
    ap = argparse.ArgumentParser(description="Process missing NIFTY100 companies through PipelineV2.")
    ap.add_argument("--force", action="store_true", help="Reprocess companies even when final data exists.")
    ap.add_argument("--limit", type=int, default=0, help="Optional cap for smoke tests.")
    ap.add_argument("--symbols", nargs="*", default=[], help="Specific NSE symbols to process.")
    ap.add_argument("--xbrl-lookback-days", type=int, default=1400)
    ap.add_argument("--max-quarterly-filings", type=int, default=16)
    ap.add_argument("--max-annual-filings", type=int, default=8)
    args = ap.parse_args()

    universe = load_universe(str(UNIVERSE_PATH))
    if args.symbols:
        wanted = {symbol.upper() for symbol in args.symbols}
        targets = [company for company in universe if company.symbol.upper() in wanted]
    else:
        targets = [company for company in universe if args.force or not has_final_data(company.symbol)]
    if args.limit:
        targets = targets[: args.limit]

    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    status = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "universe": str(UNIVERSE_PATH),
        "target_count": len(targets),
        "processed": [],
        "failed": [],
    }
    LOG_PATH.write_text(json.dumps(status, indent=2), encoding="utf-8")

    pipe = PipelineV2(
        xbrl_lookback_days=args.xbrl_lookback_days,
        max_quarterly_filings=args.max_quarterly_filings,
        max_annual_filings=args.max_annual_filings,
    )
    for index, company in enumerate(targets, start=1):
        symbol = company.symbol.upper()
        print(f"[{index}/{len(targets)}] Processing {symbol}")
        try:
            result = pipe.process_company(company)
            status["processed"].append(
                {
                    "symbol": symbol,
                    "confidence_score": result.get("metadata", {}).get("confidence_score"),
                    "validation_passed": result.get("metadata", {}).get("validation_passed"),
                    "processed_at": datetime.now(timezone.utc).isoformat(),
                }
            )
        except Exception as exc:
            status["failed"].append(
                {
                    "symbol": symbol,
                    "error": str(exc),
                    "failed_at": datetime.now(timezone.utc).isoformat(),
                }
            )
        LOG_PATH.write_text(json.dumps(status, indent=2), encoding="utf-8")

    status["finished_at"] = datetime.now(timezone.utc).isoformat()
    LOG_PATH.write_text(json.dumps(status, indent=2), encoding="utf-8")
    print(json.dumps({"processed": len(status["processed"]), "failed": len(status["failed"]), "log": str(LOG_PATH)}, indent=2))
    return 0 if not status["failed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
