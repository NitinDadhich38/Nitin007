import json
import logging
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, List
import sqlite3

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import yfinance as yf
import pandas as pd
import io

logger = logging.getLogger("NiftyAPI")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ─── Paths (Adjusted for Vercel /api/ structure) ──────────────────────────────
ROOT          = Path(__file__).parent
PROJECT_ROOT  = ROOT.parent
DASHBOARD_DIR = PROJECT_ROOT / "dashboard"
DATA_DIR      = DASHBOARD_DIR / "data"
CACHE_DIR     = Path("/tmp/cache")
CACHE_DIR.mkdir(exist_ok=True, parents=True)
CACHE_TTL_HOURS = 24

# ─── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Nifty50 Financial Intelligence API",
    description="Zero-hallucination financial data from NSE/BSE official filings only.",
    version="3.1-PhaseA",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

DB_PATH = PROJECT_ROOT / "financials.db"

def get_db_connection():
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        logger.error(f"DB connection fail: {e}")
        raise Exception("DB unavailable")


# ─── Cache helpers ────────────────────────────────────────────────────────────
def _cache_path(key: str) -> Path:
    return CACHE_DIR / f"{key}.json"

def _cache_valid(path: Path) -> bool:
    if not path.exists():
        return False
    age = datetime.now(timezone.utc) - datetime.fromtimestamp(
        path.stat().st_mtime, tz=timezone.utc
    )
    return age < timedelta(hours=CACHE_TTL_HOURS)

def _read_cache(key: str) -> Optional[Dict]:
    p = _cache_path(key)
    if _cache_valid(p):
        with open(p) as f:
            return json.load(f)
    return None

def _write_cache(key: str, data: Any):
    try:
        with open(_cache_path(key), "w") as f:
            json.dump(data, f)
    except:
        pass


# ─── Data loader ──────────────────────────────────────────────────────────────
def _load_company(symbol: str) -> Dict:
    path = DATA_DIR / f"{symbol.upper()}.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Company {symbol} not found")
    with open(path) as f:
        return json.load(f)


# ─── API Routes ───────────────────────────────────────────────────────────────

@app.get("/api/companies")
def list_companies():
    """List all available companies."""
    companies_path = DASHBOARD_DIR / "companies.json"
    if not companies_path.exists():
        raise HTTPException(status_code=503, detail="Companies index not found.")
    with open(companies_path) as f:
        return json.load(f)


@app.get("/api/live/{symbol}")
def get_live_market(symbol: str):
    """Bridge for real-time market metrics from NSE."""
    cache_key = f"live_{symbol.upper()}"
    cached = _read_cache(cache_key)
    if cached:
        age_seconds = (datetime.now(timezone.utc) - datetime.fromtimestamp(_cache_path(cache_key).stat().st_mtime, tz=timezone.utc)).total_seconds()
        if age_seconds < 60:
            return cached

    try:
        t = yf.Ticker(f"{symbol.upper()}.NS")
        info = t.info
        metrics = {
            "price": info.get("currentPrice", info.get("regularMarketPrice")),
            "market_cap": info.get("marketCap", 0) / 1e7,
            "pe": info.get("trailingPE", 0.0),
            "dividend_yield": info.get("dividendYield", 0.0) * 100,
            "day_high": info.get("dayHigh"),
            "day_low": info.get("dayLow"),
            "volume": info.get("volume"),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        _write_cache(cache_key, metrics)
        return metrics
    except Exception as e:
        logger.error(f"Live fetch fail for {symbol}: {e}")
        co = _load_company(symbol)
        return co.get("market_data", {})

@app.get("/api/sector/{sector}")
def get_sector_peers(sector: str):
    """Returns key metrics for all companies in a sector."""
    companies_path = DASHBOARD_DIR / "companies.json"
    if not companies_path.exists():
        raise HTTPException(status_code=503, detail="Companies index not found")
        
    with open(companies_path) as f:
        all_cos = json.load(f)
        
    peers = [c for c in all_cos if c.get("sector", "").lower() == sector.lower()]
    
    results = []
    for p in peers:
        symbol = p["symbol"]
        try:
            co_data = _load_company(symbol)
            md = co_data.get("market_data", {})
            latest_yr = sorted(co_data.get("profit_loss", {}).get("annual", {}).keys(), reverse=True)[0] if co_data.get("profit_loss", {}).get("annual") else None
            pl = co_data.get("profit_loss", {}).get("annual", {}).get(latest_yr, {}) if latest_yr else {}
            
            results.append({
                "symbol": symbol,
                "name": p["name"],
                "price": md.get("price"),
                "market_cap": md.get("market_cap"),
                "pe": md.get("pe"),
                "revenue": pl.get("revenue"),
                "net_profit": pl.get("net_profit"),
                "sector": sector
            })
        except Exception as e:
            continue
            
    return results

@app.get("/api/screener")
def run_screener(
    min_market_cap: float = Query(None),
    max_pe: float = Query(None),
    min_roe: float = Query(None),
    min_div_yield: float = Query(None),
    sector: str = Query(None)
):
    """Simple screener using key metrics with JSON fallback."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """
        SELECT c.*, am.roe, am.revenue, am.net_profit
        FROM companies c
        LEFT JOIN annual_metrics am ON c.symbol = am.symbol AND am.year = (SELECT MAX(year) FROM annual_metrics WHERE symbol = c.symbol)
        WHERE 1=1
        """
        params: List[Any] = []
        if min_market_cap:
            query += " AND c.market_cap >= ?"
            params.append(min_market_cap)
        if max_pe:
            query += " AND c.pe <= ?"
            params.append(max_pe)
        if min_roe:
            query += " AND am.roe >= ?"
            params.append(min_roe)
        if min_div_yield:
            query += " AND c.dividend_yield >= ?"
            params.append(min_div_yield)
        if sector:
            query += " AND c.sector = ?"
            params.append(sector)
            
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except:
        # Fallback to JSON-only scan for Vercel
        with open(DASHBOARD_DIR / "companies.json") as f:
            all_cos = json.load(f)
        
        results = []
        for c in all_cos:
            try:
                if sector and c.get("sector", "").lower() != sector.lower(): continue
                md = c.get("market_data", {})
                if min_market_cap and (md.get("market_cap") or 0) < min_market_cap: continue
                if max_pe and (md.get("pe") or 999) > max_pe: continue
                results.append(c)
            except: continue
        return results

@app.get("/api/metrics/all/{symbol}")
def get_all_metrics(symbol: str):
    """Returns all available latest metrics for a company, fallback to JSON if DB fails."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM annual_metrics WHERE symbol = ? ORDER BY year DESC LIMIT 1", (symbol.upper(),))
        row = cursor.fetchone()
        cursor.execute("SELECT * FROM companies WHERE symbol = ?", (symbol.upper(),))
        co_row = cursor.fetchone()
        conn.close()
        
        if not co_row: raise Exception("Not in DB")
            
        res = dict(co_row)
        if row:
            for k, v in dict(row).items():
                if v is not None: res[k] = v
        return res
    except Exception:
        try:
            data = _load_company(symbol)
            ratios = data.get("ratios", {})
            return {
                "symbol": symbol.upper(),
                "pe": ratios.get("pe"),
                "roe": ratios.get("roe"),
                "roce": ratios.get("roce"),
                "market_cap": data.get("company_info", {}).get("market_cap"),
                "dividend_yield": ratios.get("dividend_yield")
            }
        except:
            raise HTTPException(status_code=404, detail="Data unavailable")

@app.get("/api/financials/{symbol}")
def get_financials(symbol: str, type: str = "consolidated", period: str = "annual"):
    data = _load_company(symbol)
    financials = data.get("financials", {}).get(type, {})
    if not financials: return {"available": False}
    return financials.get(period, {})

@app.get("/api/charts/{symbol}")
def get_historical_charts(symbol: str):
    try:
        ticker = yf.Ticker(symbol + ".NS")
        hist = ticker.history(period="1y")
        if hist.empty: return {"error": "No historical data found"}
        return [{"date": i.strftime('%Y-%m-%d'), "price": round(r['Close'], 2)} for i, r in hist.iterrows()]
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/health")
def health():
    return {"status": "ok", "db": str(DB_PATH.exists())}
