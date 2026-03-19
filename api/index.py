import os
import json
import sqlite3
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, List
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# --- Robust Path Detection for Vercel ---
# Vercel deploys files to /var/task. Path(__file__) is /var/task/api/index.py
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "dashboard" / "data"
DB_PATH = BASE_DIR / "financials.db"

app = FastAPI(title="Nifty50 API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

def get_db():
    if not DB_PATH.exists(): return None
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn

@app.get("/api/health")
def health():
    return {
        "status": "active",
        "base_dir": str(BASE_DIR),
        "db_exists": DB_PATH.exists(),
        "data_exists": DATA_DIR.exists()
    }

@app.get("/api/companies")
def companies():
    p = BASE_DIR / "dashboard" / "companies.json"
    if not p.exists(): return []
    with open(p) as f: return json.load(f)

@app.get("/api/metrics/all/{symbol}")
def metrics(symbol: str):
    # Try DB
    try:
        conn = get_db()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM companies WHERE symbol = ?", (symbol.upper(),))
            res = cursor.fetchone()
            conn.close()
            if res: return dict(res)
    except: pass
    
    # Fallback to JSON
    try:
        path = DATA_DIR / f"{symbol.upper()}.json"
        with open(path) as f:
            data = json.load(f)
            return {"symbol": symbol.upper(), "pe": data.get("ratios", {}).get("pe")}
    except:
        raise HTTPException(status_code=404)

@app.get("/api/charts/{symbol}")
def charts(symbol: str):
    import yfinance as yf
    try:
        t = yf.Ticker(f"{symbol.upper()}.NS")
        h = t.history(period="1y")
        return [{"date": i.strftime('%Y-%m-%d'), "price": round(r['Close'], 2)} for i, r in h.iterrows()]
    except: return []
