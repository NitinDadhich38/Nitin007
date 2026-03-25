import json
import logging
import os
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class PriceStorage:
    """Handles persistence of historical stock price data."""

    def __init__(self, base_dir: str = "storage/market_data"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def save_prices(self, symbol: str, df: pd.DataFrame) -> bool:
        """
        Symbol-specific storage: storage/market_data/{symbol}/prices.json
        """
        symbol = symbol.upper()
        target_dir = self.base_dir / symbol.lower()
        target_dir.mkdir(parents=True, exist_ok=True)
        
        target_file = target_dir / "prices.json"
        
        try:
            if df.empty:
                logger.warning(f"PriceStorage: Skipping empty DataFrame for {symbol}")
                return False

            # Convert index to ISO strings, numeric columns to floats
            # .to_json(orient='records') is useful, but we might want more control.
            
            # Reset index to make 'date' a column
            df_reset = df.reset_index()
            # Convert Timestamp to string
            df_reset['date'] = df_reset['date'].dt.strftime('%Y-%m-%d')
            
            # Output in required format: date | open | high | low | close | volume | dma_50 | dma_200
            data_records = df_reset.to_dict(orient='records')
            
            with open(target_file, "w") as f:
                json.dump({"prices": data_records}, f, indent=2)
            
            logger.info(f"PriceStorage: Saved {len(data_records)} days of prices for {symbol}")
            return True

        except Exception as e:
            logger.error(f"PriceStorage error for {symbol}: {e}")
            return False

    def load_prices(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Load prices from flat-file storage."""
        target_file = self.base_dir / symbol.lower() / "prices.json"
        
        if not target_file.exists():
            return None
        
        try:
            with open(target_file, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"PriceStorage load error: {e}")
            return None
