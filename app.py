"""
app.py — Minimal FastAPI Backend (Phase A)
==========================================
Replaces: python3 -m http.server 8080
Run with: uvicorn app:app --host 0.0.0.0 --port 8080 --reload

Architecture:
  - Serves dashboard/ as static files (same as before)
  - Adds /api/* endpoints for dynamic queries
  - File-based caching with 24h TTL (no Redis required)
  - /api/chat is a Phase B stub (returns 503 until LLM is wired)

Install: pip install fastapi uvicorn[standard] yfinance
"""

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

# ─── Paths ────────────────────────────────────────────────────────────────────
ROOT          = Path(__file__).parent
DASHBOARD_DIR = ROOT / "dashboard"
DATA_DIR      = DASHBOARD_DIR / "data"
CACHE_DIR     = ROOT / "cache"
CACHE_DIR.mkdir(exist_ok=True)
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

DB_PATH = ROOT / "financials.db"

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


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
    with open(_cache_path(key), "w") as f:
        json.dump(data, f)


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
        raise HTTPException(status_code=503, detail="Companies index not found. Run generate_dashboard_data.py first.")
    with open(companies_path) as f:
        return json.load(f)


@app.get("/api/live/{symbol}")
def get_live_market(symbol: str):
    """Bridge for real-time market metrics from NSE."""
    cache_key = f"live_{symbol.upper()}"
    cached = _read_cache(cache_key)
    # We use a 1-minute TTL for live market data cache
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
        # Fallback to local stored market data
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
            dm = co_data.get("derived_metrics", {})
            # Get latest annual revenue and profit
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
            logger.error(f"Error loading peer {symbol}: {e}")
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
    """Simple screener using key metrics."""
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

@app.get("/api/financials/{symbol}")
def get_financials(
    symbol: str,
    type:   str = Query("consolidated", enum=["consolidated", "standalone"]),
    period: str = Query("annual",       enum=["annual", "quarterly"]),
):
    """
    Returns financial statements for a company.
    type=consolidated (default) | standalone
    period=annual | quarterly

    NOTE: quarterly only returns profit_loss (Balance Sheet and Cash Flow are
    annual-only under Indian GAAP filings).
    """
    cache_key = f"financials_{symbol}_{type}_{period}"
    cached = _read_cache(cache_key)
    if cached:
        return cached

    data = _load_company(symbol)
    financials = data.get("financials", {})
    fin_type = financials.get(type)

    if fin_type is None:
        if type == "standalone":
            return JSONResponse(
                status_code=200,
                content={
                    "available": False,
                    "message": "Standalone filing not separately fetched. Only consolidated data available.",
                    "symbol": symbol.upper(),
                }
            )
        raise HTTPException(status_code=404, detail="Financial data not found")

    if period == "quarterly":
        result = {
            "profit_loss": fin_type.get("quarterly", {}).get("profit_loss", {}),
            "note": "Balance Sheet and Cash Flow are not available in quarterly view (annual filing only under Indian GAAP)"
        }
    else:
        result = fin_type.get("annual", {})

    _write_cache(cache_key, result)
    return result


@app.get("/api/metrics/{symbol}")
def get_derived_metrics(symbol: str):
    """Returns pre-computed derived metrics (ROE, D/E, margins, growth %)."""
    cache_key = f"metrics_{symbol}"
    cached = _read_cache(cache_key)
    if cached:
        return cached

    data = _load_company(symbol)
    result = {
        "derived_metrics": data.get("derived_metrics", {}),
        "confidence":      data.get("confidence", {}).get("derived_metrics", {}),
        "source":          "DERIVED_FROM_FILINGS",
    }
    _write_cache(cache_key, result)
    return result


@app.get("/api/graph/{symbol}")
def get_graph_data(
    symbol: str,
    metric: str = Query("revenue", description="Metric key: revenue, net_profit, ebitda, eps, total_debt, ..."),
    period: str = Query("quarterly", enum=["quarterly", "annual"]),
):
    """
    Returns time-series data for charting.
    Rules: No interpolation. Minimum 2 points. Missing periods skipped.
    """
    data     = _load_company(symbol)
    gd       = data.get("graph_data", {})
    metric_d = gd.get(metric)

    if metric_d is None:
        return {
            "available": False,
            "message":   f"Metric '{metric}' has insufficient filing data for charting (requires ≥2 data points).",
        }

    series = metric_d.get(period, [])
    if len(series) < 2:
        alt = "annual" if period == "quarterly" else "quarterly"
        series = metric_d.get(alt, [])
        if len(series) < 2:
            return {
                "available": False,
                "message":   "Insufficient data points for chart rendering.",
            }

    return {
        "available": True,
        "metric":    metric,
        "label":     metric_d.get("label", metric),
        "unit":      metric_d.get("unit", "₹ Crores"),
        "period":    period,
        "series":    series,
        "note":      "No interpolation applied. Missing periods are skipped.",
    }


@app.get("/api/insights/{symbol}")
def get_insights(symbol: str):
    """Returns rule-based insights and anomaly flags."""
    data = _load_company(symbol)
    return {
        "insights":  data.get("insights",  []),
        "anomalies": data.get("anomalies", []),
        "note":      "Rule-based only. LLM insights available in Phase B.",
    }

@app.get("/api/metrics/all/{symbol}")
def get_all_metrics(symbol: str):
    """Returns all available latest metrics for a company from the DB."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get latest annual metrics
    cursor.execute("""
    SELECT * FROM annual_metrics 
    WHERE symbol = ? 
    ORDER BY year DESC LIMIT 1
    """, (symbol.upper(),))
    row = cursor.fetchone()
    
    # Get company info
    cursor.execute("SELECT * FROM companies WHERE symbol = ?", (symbol.upper(),))
    co_row = cursor.fetchone()
    
    conn.close()
    
    if not co_row:
        raise HTTPException(status_code=404, detail="Company not found")
        
    res = dict(co_row)
    if row:
        row_dict = dict(row)
        for k, v in row_dict.items():
            if v is not None:
                res[k] = v
                
    # Try to patch with live market data from cache
    live_cache = _read_cache(f"live_{symbol.upper()}")
    if live_cache:
        for k, v in live_cache.items():
            if v:
                res[k] = v
                
    return res

@app.get("/api/charts/{symbol}")
def get_historical_charts(symbol: str):
    try:
        ticker_symbol = symbol + ".NS"
        ticker = yf.Ticker(ticker_symbol)
        hist = ticker.history(period="1y")
        
        if hist.empty:
            return {"error": "No historical data found"}
            
        data = []
        for index, row in hist.iterrows():
            data.append({
                "date": index.strftime('%Y-%m-%d'),
                "price": round(row['Close'], 2),
                "volume": int(row['Volume'])
            })
        return data
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/export/{symbol}")
def export_excel(symbol: str):
    """Exports financial statements to an Excel file."""
    try:
        data = _load_company(symbol)
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Profit & Loss
            pl_data = data.get("profit_loss", {}).get("annual", {})
            if pl_data:
                df_pl = pd.DataFrame(pl_data).T
                df_pl.to_excel(writer, sheet_name='Profit & Loss')
            
            # Balance Sheet
            bs_data = data.get("balance_sheet", {}).get("annual", {})
            if bs_data:
                df_bs = pd.DataFrame(bs_data).T
                df_bs.to_excel(writer, sheet_name='Balance Sheet')
                
            # Cash Flow
            cf_data = data.get("cash_flow", {}).get("annual", {})
            if cf_data:
                df_cf = pd.DataFrame(cf_data).T
                df_cf.to_excel(writer, sheet_name='Cash Flow')
        
        output.seek(0)
        return FileResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=f"{symbol.upper()}_Financials.xlsx"
        )
    except Exception as e:
        logger.error(f"Export fail: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/chat")
def chat(symbol: str, query: str):
    """
    [Phase B Stub] Live financial Q&A via Groq API.
    Returns 503 until LLM router is wired in Phase B.
    """
    return JSONResponse(
        status_code=503,
        content={
            "status":  "Phase B feature",
            "message": "Chat endpoint requires LLM_BACKEND=groq or LLM_BACKEND=ollama. "
                       "Set GROQ_API_KEY and restart to enable.",
            "phase":   "B",
        },
    )


@app.get("/api/health")
def health():
    return {
        "status":   "ok",
        "version":  "3.1-PhaseA",
        "llm":      os.getenv("LLM_BACKEND", "disabled"),
        "cache_dir": str(CACHE_DIR),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ─── Static File Serving (dashboard/) ─────────────────────────────────────────
# Mount AFTER API routes so /api/* takes priority
app.mount("/", StaticFiles(directory=str(DASHBOARD_DIR), html=True), name="dashboard")
