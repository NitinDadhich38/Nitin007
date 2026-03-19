import os
import json
import yfinance as yf
from datetime import datetime
import glob
import time

DATA_DIR = "dashboard/data"
BACKUP_DIR = "dashboard/backups"

def backup_file(filepath):
    """Ensure we have a copy of the original data before patching."""
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR)
    
    filename = os.path.basename(filepath)
    backup_path = os.path.join(BACKUP_DIR, f"{filename}.bak")
    
    if not os.path.exists(backup_path):
        with open(filepath, 'r') as src, open(backup_path, 'w') as dst:
            dst.write(src.read())
        print(f"Created backup: {backup_path}")

def get_market_metrics(symbol):
    """Fetch live headline metrics from yfinance for NSE stocks."""
    ext_symbol = f"{symbol}.NS"
    print(f"Fetching market metrics for {ext_symbol}...")
    try:
        ticker = yf.Ticker(ext_symbol)
        info = ticker.info
        
        metrics = {
            "price": info.get("currentPrice", info.get("regularMarketPrice")),
            "market_cap": info.get("marketCap", 0) / 1e7, # Convert to ₹ Crores
            "pe": info.get("trailingPE", 0.0),
            "dividend_yield": info.get("dividendYield", 0.0) * 100 # Convert to %
        }
        
        # Guard against sparse/invalid response
        if not metrics["price"] and not metrics["market_cap"]:
            return None
            
        return metrics
    except Exception as e:
        print(f"Error fetching from yfinance for {symbol}: {e}")
        return None

def fill_gaps():
    """Scan all company JSONs and patch missing data points."""
    print("Starting Gap-Filler Engine [Phase 2]...")
    
    target_files = glob.glob(os.path.join(DATA_DIR, "*.json"))
    
    # Skip companies.json
    target_files = [f for f in target_files if not f.endswith("companies.json")]
    
    for filepath in target_files:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        symbol = data.get("company", {}).get("symbol")
        if not symbol:
            continue
            
        modified = False
        
        # 1. Patch Market Data (Price, M-Cap, PE)
        md = data.get("market_data", {})
        if not md.get("market_cap") or md.get("market_cap") == 0 or not md.get("price"):
            metrics = get_market_metrics(symbol)
            if metrics:
                backup_file(filepath)
                data["market_data"] = {
                    "price": metrics["price"],
                    "market_cap": round(metrics["market_cap"], 2),
                    "pe": round(metrics["pe"], 2),
                    "dividend_yield": round(metrics["dividend_yield"], 2)
                }
                modified = True
                print(f"Patched Market Data for {symbol}")
        
        # 2. Patch Derived Metrics - Only if missing key ratios
        dm = data.get("derived_metrics", {})
        latest_yr = sorted(dm.keys(), reverse=True)[0] if dm else None
        if latest_yr and not dm[latest_yr].get("roe"):
             # We could recalculate here if needed, but for now we focus on fetching missing high-level fields
             pass

        if modified:
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"Updated: {filepath}")
            # Rate limiting stay nice to yfinance
            time.sleep(1)

if __name__ == "__main__":
    fill_gaps()
