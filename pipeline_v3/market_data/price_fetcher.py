import logging
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from .technical_indicator_engine import TechnicalIndicatorEngine

logger = logging.getLogger(__name__)

class PriceFetcher:
    """Fetches, cleans, and augments OHLCV market data."""

    def __init__(self):
        self.indicator_engine = TechnicalIndicatorEngine()

    def fetch_historical(self, symbol: str, period: str = "1y") -> pd.DataFrame:
        """
        Fetch historical data from Yahoo Finance.
        Supports: 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max
        """
        yf_symbol = f"{symbol.upper()}.NS"
        logger.info(f"PriceFetcher: Retrieving {period} data for {yf_symbol}")
        
        try:
            # Note: auto_adjust=True for corporate action handling (splits, dividends)
            ticker = yf.Ticker(yf_symbol)
            df = ticker.history(period=period, auto_adjust=True)
            
            if df.empty:
                logger.warning(f"PriceFetcher: No data returned for {yf_symbol}")
                return df

            # Standardize column names
            df.index.name = "date"
            df = df.rename(columns={
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume"
            })

            # Select and order required columns
            df = df[["open", "high", "low", "close", "volume"]]

            # Clean and validate data (e.g., handle missing trading days or holidays implicitly by yf)
            df = self._clean_and_sort(df)

            # Compute indicators
            df = self.indicator_engine.compute_dmas(df)

            return df

        except Exception as e:
            logger.error(f"PriceFetcher error for {yf_symbol}: {e}")
            return pd.DataFrame()

    def _clean_and_sort(self, df: pd.DataFrame) -> pd.DataFrame:
        """Data cleaning: duplicates, sorting, and time normalization."""
        # Remove any potential duplicates in index
        df = df[~df.index.duplicated(keep='last')]
        
        # Chronological sort
        df = df.sort_index()

        # Fill potential NaNs in volume with 0, others with forward fill (very rare in adjusted yf data)
        df["volume"] = df["volume"].fillna(0)
        df = df.fillna(method="ffill").fillna(method="bfill")

        return df
