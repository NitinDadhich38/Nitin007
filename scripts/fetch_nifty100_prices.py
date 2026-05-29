import sys
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline_v3.market_data.price_fetcher import PriceFetcher
from pipeline_v3.market_data.price_storage import PriceStorage

def main():
    universe_path = ROOT / "pipeline_v3" / "config" / "nifty100_universe.json"
    with open(universe_path) as f:
        data = json.load(f)
    
    companies = data.get("companies", [])
    if not companies:
        print("No companies found in universe config")
        return

    fetcher = PriceFetcher()
    storage = PriceStorage(base_dir=str(ROOT / "dashboard" / "market_data"))
    
    for c in companies:
        symbol = c["symbol"]
        print(f"Fetching prices for {symbol}...")
        try:
            df = fetcher.fetch_historical(symbol, period="max")
            if not df.empty:
                storage.save_prices(symbol, df)
            else:
                print(f"No data for {symbol}")
        except Exception as e:
            print(f"Error for {symbol}: {e}")

if __name__ == "__main__":
    main()
