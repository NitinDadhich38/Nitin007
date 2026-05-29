import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline_v3.services.index_registry import (
    CONFIG_DIR,
    INDEX_CONSTITUENTS_PATH,
    build_index_payload,
    parse_nifty_csv,
)


INDEX_SOURCES = {
    "NIFTY50": {
        "name": "Nifty 50",
        "description": "Large-cap Nifty 50 index coverage.",
        "csv_url": "https://www.niftyindices.com/IndexConstituent/ind_nifty50list.csv",
    },
    "NIFTY100": {
        "name": "Nifty 100",
        "description": "Nifty 100 index coverage, excluding temporary dummy demerger rows.",
        "csv_url": "https://www.niftyindices.com/IndexConstituent/ind_nifty100list.csv",
    },
}


def fetch_csv(url: str) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/csv,*/*",
        "Referer": "https://www.niftyindices.com/indices/equity/broad-based-indices/nifty-100",
    }
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.text


def main() -> None:
    verified_at = datetime.now(timezone.utc).isoformat()
    all_constituents = []
    indices: Dict[str, Dict] = {}

    for index_code, meta in INDEX_SOURCES.items():
        csv_text = fetch_csv(meta["csv_url"])
        rows = parse_nifty_csv(index_code, csv_text, meta["csv_url"], verified_at)
        indices[index_code] = {
            "code": index_code,
            "name": meta["name"],
            "description": meta["description"],
            "company_count": len(rows),
            "source_url": meta["csv_url"],
            "last_verified_at": verified_at,
        }
        all_constituents.extend(rows)

    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    INDEX_CONSTITUENTS_PATH.write_text(
        json.dumps(build_index_payload(indices, all_constituents), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"Wrote {INDEX_CONSTITUENTS_PATH} with {len(all_constituents)} index memberships")


if __name__ == "__main__":
    main()
