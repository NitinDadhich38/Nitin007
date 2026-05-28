import json
from pathlib import Path
import glob

def find_missing_in_caches(symbol):
    print(f"Checking cache for {symbol}")
    for p in glob.glob(f"cache/financials_{symbol.upper()}_*.json"):
        with open(p) as f:
            d = json.load(f)
        ann = list(d.get('yearly', d.get('annual', {})).keys())
        qtr = list(d.get('quarterly', {}).keys())
        print(f"  {p}: Annual={ann}, Qtr={qtr}")
        
find_missing_in_caches('SBIN')
find_missing_in_caches('HDFCBANK')
find_missing_in_caches('RELIANCE')
find_missing_in_caches('HDFCLIFE')
