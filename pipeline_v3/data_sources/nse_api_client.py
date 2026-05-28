import logging
import requests
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
from .http_client import HTTPClient
from pipeline_v3.utils.periods import generate_active_periods

logger = logging.getLogger(__name__)

NSE_HOME = "https://www.nseindia.com"
NSE_XBRL_URL = "https://www.nseindia.com/api/results-comparision"
NSE_CORP_FILINGS_URL = "https://www.nseindia.com/api/corporate-announcements"

class NSEAPIClient:
    """Client for NSE Financial APIs."""
    
    def __init__(self, session=None):
        self.session = session or self._init_session()
        self.http = HTTPClient(session=self.session, timeout_s=20.0, max_retries=2, backoff_s=1.0, min_interval_s=0.2)

    def _init_session(self):
        s = requests.Session()
        s.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
        })
        # Handshake
        try:
            s.get(NSE_HOME, timeout=10)
            time.sleep(1)
            # Second hit to get more cookies
            s.get(f"{NSE_HOME}/get-quotes/equity?symbol=RELIANCE", timeout=10)
        except Exception as e:
            logger.warning(f"NSE Handshake failed: {e}")
        return s

    def fetch_results(self, symbol: str, period: str = "Quarterly", consolidated: bool = True) -> Dict[str, Any]:
        """Fetches results comparison from NSE."""
        params = {
            "index": "equities",
            "symbol": symbol.upper(),
            "period": period,
            "consolidated": "true" if consolidated else "false"
        }
        data, err = self.http.get_json(NSE_XBRL_URL, params=params)
        if err:
            logger.warning(f"NSE results-comparision failed ({symbol}, consolidated={consolidated}): {err}")
            return {}
        return data or {}

    def get_results_comparison(self, symbol: str, period: str, period_type: str) -> Optional[Dict[str, Any]]:
        """
        Fetch the NSE comparison endpoint and match the requested active period
        by its end date. This is used by period-loop callers; main_v2 still uses
        fetch_results() for broad bulk ingestion.
        """
        period_map = {
            "QUARTERLY": "Quarterly",
            "HALF_YEARLY": "Half-Yearly",
            "ANNUAL": "Annual",
            "quarterly": "Quarterly",
            "half_yearly": "Half-Yearly",
            "annual": "Annual",
        }
        nse_period = period_map.get(period_type, period_type or "Quarterly")
        data = self.fetch_results(symbol, period=nse_period, consolidated=True)
        results_list = data.get("data", data) if isinstance(data, dict) else data
        if not isinstance(results_list, list):
            logger.warning("[NSE_API] Unexpected response shape for %s %s", symbol, period)
            return None

        active_periods = generate_active_periods(lookback_years=5)
        period_meta = next(
            (
                p for p in active_periods
                if period in {p.get("period"), p.get("period_long"), p.get("label"), p.get("fy")}
            ),
            None,
        )
        if not period_meta:
            logger.warning("[NSE_API] Period %s is outside active window for %s", period, symbol)
            return None

        return self._match_period_in_results(results_list, period_meta)

    def _match_period_in_results(self, results_list: list, period_meta: dict) -> Optional[Dict[str, Any]]:
        target_end = period_meta["end_date"]
        for entry in results_list:
            if not isinstance(entry, dict):
                continue
            parsed_end = self._parse_nse_date(
                entry.get("toDate")
                or entry.get("to_date")
                or entry.get("period")
                or entry.get("xAxis")
                or ""
            )
            if parsed_end and abs((parsed_end.date() - target_end).days) <= 15:
                return entry
        return None

    @staticmethod
    def _parse_nse_date(value: str) -> Optional[datetime]:
        if not value:
            return None
        text = str(value).strip()
        for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d", "%b %Y", "%B %Y"):
            try:
                parsed = datetime.strptime(text, fmt)
                if fmt in ("%b %Y", "%B %Y"):
                    # Use the conventional month-end for comparison.
                    import calendar
                    last_day = calendar.monthrange(parsed.year, parsed.month)[1]
                    parsed = parsed.replace(day=last_day)
                return parsed
            except ValueError:
                continue
        return None

    def fetch_equity_quote(self, symbol: str) -> Dict[str, Any]:
        """Fetches real-time quote for an equity symbol."""
        params = {"symbol": symbol.upper()}
        # NSE sometimes requires a referer for this API
        headers = {"Referer": f"{NSE_HOME}/get-quotes/equity?symbol={symbol.upper()}"}
        data, err = self.http.get_json("https://www.nseindia.com/api/quote-equity", params=params, headers=headers)
        if err:
            logger.warning(f"NSE quote-equity failed ({symbol}): {err}")
            return {}
        return data or {}

    def fetch_corporate_filings(
        self,
        *,
        symbol: str,
        from_date: str,
        to_date: str,
        index: str = "equities",
    ) -> Dict[str, Any]:
        """
        Corporate announcements (includes financial results, annual reports, presentations).
        Dates expected by NSE are typically DD-MM-YYYY.
        """
        params = {
            "index": index,
            "symbol": symbol.upper(),
            "from_date": from_date,
            "to_date": to_date,
        }
        data, err = self.http.get_json(NSE_CORP_FILINGS_URL, params=params)
        if err:
            logger.warning(f"NSE corporate-announcements failed ({symbol}): {err}")
            return {}
        return data or {}
