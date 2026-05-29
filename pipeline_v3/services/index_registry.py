import csv
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional


ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = ROOT / "pipeline_v3" / "config"
INDEX_CONSTITUENTS_PATH = CONFIG_DIR / "index_constituents.json"
DASHBOARD_DIR = ROOT / "dashboard"
DASHBOARD_DATA_DIR = DASHBOARD_DIR / "data"


@dataclass(frozen=True)
class IndexConstituent:
    index_code: str
    symbol: str
    name: str
    industry: Optional[str] = None
    isin: Optional[str] = None
    series: Optional[str] = None
    source_url: Optional[str] = None
    last_verified_at: Optional[str] = None

    @property
    def normalized_symbol(self) -> str:
        return self.symbol.upper()


def _load_payload(path: Path = INDEX_CONSTITUENTS_PATH) -> Dict:
    if not path.exists():
        return {"indices": {}, "constituents": []}
    return json.loads(path.read_text(encoding="utf-8"))


def load_indices(path: Path = INDEX_CONSTITUENTS_PATH) -> Dict[str, Dict]:
    return _load_payload(path).get("indices", {})


def load_constituents(index_code: Optional[str] = None, path: Path = INDEX_CONSTITUENTS_PATH) -> List[IndexConstituent]:
    rows = []
    for item in _load_payload(path).get("constituents", []):
        if index_code and item.get("index_code") != index_code.upper():
            continue
        rows.append(IndexConstituent(**item))
    return rows


def symbol_to_indexes(path: Path = INDEX_CONSTITUENTS_PATH) -> Dict[str, List[str]]:
    mapping: Dict[str, List[str]] = {}
    for item in load_constituents(path=path):
        mapping.setdefault(item.symbol, [])
        if item.index_code not in mapping[item.symbol]:
            mapping[item.symbol].append(item.index_code)
    return mapping


def companies_for_index(index_code: str) -> List[Dict]:
    companies = []
    for item in load_constituents(index_code):
        data_path = DASHBOARD_DATA_DIR / f"{item.symbol}.json"
        coverage = "parsed" if data_path.exists() else "missing"
        companies.append(
            {
                "symbol": item.symbol,
                "name": item.name,
                "sector": item.industry,
                "industry": item.industry,
                "isin": item.isin,
                "indexes": symbol_to_indexes().get(item.symbol, [item.index_code]),
                "coverage_status": coverage,
                "data_path": f"data/{item.symbol}.json" if data_path.exists() else None,
            }
        )
    return companies


def build_index_payload(indices: Dict[str, Dict], constituents: Iterable[IndexConstituent]) -> Dict:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "indices": indices,
        "constituents": [asdict(c) for c in constituents],
    }


def parse_nifty_csv(index_code: str, csv_text: str, source_url: str, verified_at: str) -> List[IndexConstituent]:
    rows = []
    for row in csv.DictReader(csv_text.splitlines()):
        symbol = (row.get("Symbol") or "").strip().upper()
        if not symbol or symbol.startswith("DUMMY"):
            continue
        rows.append(
            IndexConstituent(
                index_code=index_code.upper(),
                symbol=symbol,
                name=(row.get("Company Name") or symbol).strip(),
                industry=(row.get("Industry") or "").strip() or None,
                isin=(row.get("ISIN Code") or "").strip() or None,
                series=(row.get("Series") or "").strip() or None,
                source_url=source_url,
                last_verified_at=verified_at,
            )
        )
    return rows

