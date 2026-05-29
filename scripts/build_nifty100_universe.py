import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline_v3.services.index_registry import INDEX_CONSTITUENTS_PATH, load_constituents


EXISTING_UNIVERSE = ROOT / "pipeline_v3" / "config" / "nifty50_universe.json"
NIFTY100_UNIVERSE = ROOT / "pipeline_v3" / "config" / "nifty100_universe.json"


def main() -> None:
    existing = {}
    if EXISTING_UNIVERSE.exists():
        payload = json.loads(EXISTING_UNIVERSE.read_text(encoding="utf-8"))
        existing = {item["symbol"].upper(): item for item in payload.get("companies", [])}

    companies = []
    for row in load_constituents("NIFTY100", INDEX_CONSTITUENTS_PATH):
        old = existing.get(row.symbol, {})
        companies.append(
            {
                "symbol": row.symbol,
                "name": old.get("name") or row.name,
                "scrip_code": old.get("scrip_code"),
                "cin": old.get("cin"),
                "isin": old.get("isin") or row.isin,
                "ir_urls": old.get("ir_urls") or [],
                "sector": old.get("sector") or row.industry,
                "industry": old.get("industry") or row.industry,
                "index_memberships": sorted(set((old.get("index_memberships") or []) + ["NIFTY100"] + (["NIFTY50"] if row.symbol in existing else []))),
            }
        )

    NIFTY100_UNIVERSE.write_text(
        json.dumps(
            {
                "as_of": datetime.now(timezone.utc).date().isoformat(),
                "source": "Official Nifty Indices NIFTY100 constituent CSV",
                "notes": "Generated from pipeline_v3/config/index_constituents.json. Existing CIN/BSE/IR metadata is preserved where available.",
                "companies": companies,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"Wrote {NIFTY100_UNIVERSE} with {len(companies)} companies")


if __name__ == "__main__":
    main()

