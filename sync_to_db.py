import json
import sqlite3
import os
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).parent
DATA_DIR = ROOT / "dashboard" / "data"
DB_PATH = ROOT / "financials.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Tables for core search and screening
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS companies (
        symbol TEXT PRIMARY KEY,
        name TEXT,
        sector TEXT,
        market_cap REAL,
        price REAL,
        pe REAL,
        dividend_yield REAL
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS annual_metrics (
        symbol TEXT,
        year TEXT,
        revenue REAL,
        net_profit REAL,
        ebitda REAL,
        eps REAL,
        roe REAL,
        roce REAL,
        debt_to_equity REAL,
        PRIMARY KEY (symbol, year)
    )
    """)
    
    conn.commit()
    return conn

def sync():
    conn = init_db()
    cursor = conn.cursor()
    
    companies_path = ROOT / "dashboard" / "companies.json"
    with open(companies_path) as f:
        all_cos = json.load(f)
        
    print(f"Syncing {len(all_cos)} companies...")
    
    for c in all_cos:
        symbol = c["symbol"]
        json_path = DATA_DIR / f"{symbol.upper()}.json"
        
        if not json_path.exists():
            continue
            
        with open(json_path) as f:
            data = json.load(f)
            
        md = data.get("market_data", {})
        dm = data.get("derived_metrics", {})
        # Actual path: financials -> consolidated -> annual -> profit_loss
        pl = data.get("financials", {}).get("consolidated", {}).get("annual", {}).get("profit_loss", {})
        
        # Insert/Update company core
        cursor.execute("""
        INSERT OR REPLACE INTO companies (symbol, name, sector, market_cap, price, pe, dividend_yield)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            symbol,
            c["name"],
            c["sector"],
            md.get("market_cap"),
            md.get("price"),
            md.get("pe"),
            md.get("dividend_yield")
        ))
        
        # Insert annual metrics
        if pl:
            for year, pl_metrics in pl.items():
                # Get derived metrics for that year if available
                year_dm = dm.get(year, {})
                
                cursor.execute("""
                INSERT OR REPLACE INTO annual_metrics (symbol, year, revenue, net_profit, ebitda, eps, roe, roce, debt_to_equity)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    symbol,
                    year,
                    pl_metrics.get("revenue"),
                    pl_metrics.get("net_profit"),
                    pl_metrics.get("ebitda"),
                    pl_metrics.get("eps"),
                    year_dm.get("roe"),
                    year_dm.get("roce"),
                    year_dm.get("debt_to_equity")
                ))
            
    conn.commit()
    conn.close()
    print("Sync complete.")

if __name__ == "__main__":
    sync()
