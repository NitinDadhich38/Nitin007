import json
import logging
import os
import sqlite3
import io
import yfinance as yf
import pandas as pd
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, List
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

# ─── Configuration & Logging ──────────────────────────────────────────────────
logger = logging.getLogger("NiftyAPI")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ─── Robust Path Resolution for Vercel ─────────────────────────────────────────
# On Vercel, the project root is usually /var/task
BASE_DIR      = Path(os.getcwd())
DASHBOARD_DIR = BASE_DIR / "dashboard"
DATA_DIR      = DASHBOARD_DIR / "data"
DB_PATH       = BASE_DIR / "financials.db"
# Vercel serverless functions ONLY have write access to /tmp
CACHE_DIR     = Path("/tmp/cache")
CACHE_DIR.mkdir(exist_ok=True, parents=True)

# ─── FastAPI App ──────────────────────────────────────────────────────────────
app = FastAPI(
    title="Nifty50 Financial Intelligence API",
    description="Official financial data from NSE filings, serverless on Vercel.",
    version="3.2-Harden"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ─── Helper Functions ─────────────────────────────────────────────────────────

def get_db_connection():
    """Establishes connection to SQLite with a check for file existence."""
    if not DB_PATH.exists():
        logger.error(f"FATAL: Database not found at {DB_PATH}")
        raise Exception("DATABASE_NOT_FOUND")
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        logger.error(f"SQL Connection Exception: {e}")
        raise Exception("DB_CONNECT_ERROR")

def _load_company_json(symbol: str) -> Dict:
    """Safe loader for company JSON files from the data directory."""
    path = DATA_DIR / f"{symbol.upper()}.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Company {symbol} filings not found.")
    with open(path, "r") as f:
        return json.load(f)

# ─── API Endpoints ────────────────────────────────────────────────────────────

@app.get("/api/health")
def health_check():
    """Diagnostics for Vercel deployment."""
    return {
        "status": "online",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "db_exists": DB_PATH.exists(),
        "dashboard_dir_exists": DASHBOARD_DIR.exists(),
        "data_dir_exists": DATA_DIR.exists(),
        "base_dir": str(BASE_DIR),
        "files": os.listdir(str(BASE_DIR))[:10] if BASE_DIR.exists() else []
    }

@app.get("/api/companies")
def list_companies():
    """Returns the list of all indexed companies."""
    p = DASHBOARD_DIR / "companies.json"
    if not p.exists():
        raise HTTPException(status_code=503, detail="Companies index missing.")
    with open(p, "r") as f:
        return json.load(f)

@app.get("/api/live/{symbol}")
def get_live_metrics(symbol: str):
    """Bridge for yfinance market data."""
    try:
        t = yf.Ticker(f"{symbol.upper()}.NS")
        info = t.info
        return {
            "price": info.get("currentPrice", info.get("regularMarketPrice")),
            "market_cap": (info.get("marketCap", 0) / 1e7) if info.get("marketCap") else 0,
            "pe": info.get("trailingPE"),
            "dividend_yield": (info.get("dividendYield", 0) * 100) if info.get("dividendYield") else 0,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Live fetch error: {e}")
        # Fallback to local stored data
        try:
            data = _load_company_json(symbol)
            return data.get("market_data", {})
        except:
            raise HTTPException(status_code=500, detail="Live data unavailable")

@app.get("/api/metrics/all/{symbol}")
def get_all_metrics(symbol: str):
    """Unified metrics fetcher with DB-to-JSON fallback."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get core company info
        cursor.execute("SELECT * FROM companies WHERE symbol = ?", (symbol.upper(),))
        co = cursor.fetchone()
        
        # Get latest annuals
        cursor.execute("SELECT * FROM annual_metrics WHERE symbol = ? ORDER BY year DESC LIMIT 1", (symbol.upper(),))
        am = cursor.fetchone()
        conn.close()
        
        if not co:
            raise Exception("MISSING_IN_DB")
            
        res = dict(co)
        if am:
            for k, v in dict(am).items():
                if v is not None: res[k] = v
        return res
    except Exception as e:
        logger.warning(f"DB Fetch Fallback for {symbol}: {e}")
        # Fallback to static JSON scan
        try:
            data = _load_company_json(symbol)
            r = data.get("ratios", {})
            inf = data.get("company_info", {})
            return {
                "symbol": symbol.upper(),
                "name": inf.get("name"),
                "pe": r.get("pe"),
                "roe": r.get("roe"),
                "roce": r.get("roce"),
                "market_cap": inf.get("market_cap"),
                "dividend_yield": r.get("dividend_yield")
            }
        except:
            raise HTTPException(status_code=404, detail="Company metrics not found.")

@app.get("/api/sector/{sector}")
def get_sector_peers(sector: str):
    """Returns peer group comparisons for a sector."""
    try:
        # Try DB first
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM companies WHERE sector = ?", (sector,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except:
        # Fallback to companies.json
        with open(DASHBOARD_DIR / "companies.json", "r") as f:
            all_cos = json.load(f)
        return [c for c in all_cos if c.get("sector", "").lower() == sector.lower()]

@app.get("/api/charts/{symbol}")
def get_ticker_chart(symbol: str):
    """Returns 1-year historical price data from yfinance."""
    try:
        ticker = yf.Ticker(f"{symbol.upper()}.NS")
        hist = ticker.history(period="1y")
        if hist.empty: return {"error": "No data"}
        return [
            {"date": i.strftime('%Y-%m-%d'), "price": round(r['Close'], 2)} 
            for i, r in hist.iterrows()
        ]
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/screener")
def run_screener(
    min_market_cap: float = None,
    max_pe: float = None,
    min_roe: float = None,
    sector: str = None
):
    """Filters companies based on fundamental metrics."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = "SELECT c.*, am.roe FROM companies c LEFT JOIN annual_metrics am ON c.symbol = am.symbol AND am.year = 2024 WHERE 1=1"
        params = []
        if min_market_cap:
            query += " AND c.market_cap >= ?"; params.append(min_market_cap)
        if max_pe:
            query += " AND c.pe <= ?"; params.append(max_pe)
        if min_roe:
            query += " AND am.roe >= ?"; params.append(min_roe)
        if sector:
            query += " AND c.sector = ?"; params.append(sector)
            
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except:
        # Ultimate fallback: return all companies
        with open(DASHBOARD_DIR / "companies.json", "r") as f:
            return json.load(f)

# Mount the static directory for the frontend
app.mount("/", StaticFiles(directory=str(DASHBOARD_DIR), html=True), name="static")
