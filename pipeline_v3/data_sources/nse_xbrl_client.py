import logging
import datetime
from typing import Any, Dict, List, Tuple
from .nse_api_client import NSEAPIClient

logger = logging.getLogger(__name__)

class NSEXBRLClient:
    """
    Tier-1: Official Exchange XBRL Client (NSE)
    
    Fetches the actual XML XBRL filings submitted by the company to the NSE.
    These filings contain Balance Sheet and Cash Flow data (in H1 and FY) 
    that the standard JSON API does not return.
    
    Update 2.0 fixes:
    - Increased lookback to 900 days (~2.5 years) to catch FY2023 and older filings
    - Multiple URL fallback patterns for XBRL downloads
    - Better period_type inference from announcement dates
    """

    def __init__(self, lookback_days: int = 2000, *, max_quarterly_filings: int = 40):
        self.lookback_days = lookback_days
        self.max_quarterly_filings = max_quarterly_filings
        self.max_annual_filings = 30
        self.nse_client = NSEAPIClient()

    def fetch_all_for_symbol(self, symbol: str) -> List[Tuple[bytes, Dict[str, Any]]]:
        """
        Returns a list of tuples: (raw_xbrl_bytes, metadata)
        """
        logger.info(f"Fetching NSE XBRL filings for {symbol} (lookback: {self.lookback_days} days)")
        today = datetime.datetime.now()
        from_date = (today - datetime.timedelta(days=self.lookback_days)).strftime("%d-%m-%Y")
        to_date = today.strftime("%d-%m-%Y")

        # 1. Fetch Quarterly/Half-yearly Financial Results XBRL from Announcements
        announcements = self.nse_client.fetch_corporate_filings(
            symbol=symbol,
            from_date=from_date,
            to_date=to_date
        )

        results = []
        import urllib.parse
        safe_symbol = urllib.parse.quote(symbol, safe="")
        
        # 2. Fetch Annual Reports XBRL directly from NSE's dedicated XBRL annual reports API
        logger.info(f"Fetching Dedicated Annual Reports XBRL for {symbol}")
        ar_url = f"https://www.nseindia.com/api/annual-reports-xbrl?index=equities&symbol={safe_symbol}"
        ar_data, err = self.nse_client.http.get_json(ar_url)
        
        if ar_data and isinstance(ar_data, dict) and "data" in ar_data:
            for ar in ar_data["data"]:
                file_name_url = ar.get("fileName", "")
                if file_name_url and file_name_url.endswith(".xml"):
                    logger.info(f"Downloading NSE Annual Report XBRL: {file_name_url}")
                    xml_data, xml_err = self.nse_client.http.get_bytes(file_name_url)
                    # XBRL instance root is commonly `<xbrli:xbrl>` (namespaced), but many
                    # NSE annual-report XBRLs are UTF-16 encoded, so the raw bytes contain
                    # NULs. Detect both UTF-8/ASCII and UTF-16LE/BE signatures.
                    head_raw = (xml_data or b"")[:8000]
                    head = head_raw.lower()
                    head_sans_nuls = head.replace(b"\x00", b"")  # UTF-16 safety
                    if xml_data and (b"<xbrli:xbrl" in head_sans_nuls or b"<xbrl" in head_sans_nuls):
                        results.append((xml_data, {
                            "seq_id": "AR_" + ar.get("fromYr", "") + "_" + ar.get("toYr", ""),
                            "attach_name": "Annual Report",
                            "ann_date": ar.get("broadcast_dttm", ""),
                            "period_type": "annual",
                            "desc": f"Annual Report XBRL IND-AS {ar.get('fromYr')}-{ar.get('toYr')}",
                            "source_url": file_name_url
                        }))
                    else:
                        logger.warning(f"Failed to fetch Annual Report XBRL from {file_name_url}: {xml_err}")

        # 3. Fetch Annual XBRL using the explicit financial results API.
        # This endpoint has materially better historical coverage than the
        # annual-reports-xbrl endpoint for many large caps.
        logger.info(f"Fetching Dedicated Annual Financial Results XBRL for {symbol}")
        afr_url = f"https://www.nseindia.com/api/corporates-financial-results?index=equities&symbol={safe_symbol}&period=Annual"
        afr_data, err = self.nse_client.http.get_json(afr_url)

        if afr_data and isinstance(afr_data, list):
            for fr in afr_data[: self.max_annual_filings]:
                xbrl_url = fr.get("xbrl", "")
                if xbrl_url and xbrl_url.endswith(".xml") and xbrl_url != "https://nsearchives.nseindia.com/corporate/xbrl/-":
                    logger.info(f"Downloading NSE Annual Financial Results XBRL: {xbrl_url}")
                    xml_data, xml_err = self.nse_client.http.get_bytes(xbrl_url)
                    head_raw = (xml_data or b"")[:8000]
                    head = head_raw.lower()
                    head_sans_nuls = head.replace(b"\x00", b"")
                    if xml_data and (b"<xbrli:xbrl" in head_sans_nuls or b"<xbrl" in head_sans_nuls):
                        results.append((xml_data, {
                            "seq_id": str(fr.get("seqNumber", "")),
                            "attach_name": str(fr.get("seqNumber", "")),
                            "ann_date": fr.get("broadCastDate") or fr.get("toDate", ""),
                            "period_type": "annual",
                            "desc": f"Annual Financial Results {fr.get('consolidated')} {fr.get('toDate')}",
                            "source_url": xbrl_url,
                        }))
                    else:
                        logger.warning(f"Failed to fetch Annual Financial Results XBRL from {xbrl_url}: {xml_err}")

        # 4. Fetch Quarterly XBRL using the explicit financial results API.
        # NOTE: We intentionally fetch more than 12 so we can reconstruct multi-year
        # annual series for BFSI symbols where annual XBRL is not published.
        logger.info(f"Fetching Dedicated Quarterly XBRL for {symbol}")
        fr_url = f"https://www.nseindia.com/api/corporates-financial-results?index=equities&symbol={safe_symbol}&period=Quarterly"
        fr_data, err = self.nse_client.http.get_json(fr_url)
        
        if fr_data and isinstance(fr_data, list):
            # Limit downloads to keep runtime reasonable (covers ~5 years by default).
            for fr in fr_data[: self.max_quarterly_filings]:
                xbrl_url = fr.get("xbrl", "")
                if xbrl_url and xbrl_url.endswith(".xml") and xbrl_url != "https://nsearchives.nseindia.com/corporate/xbrl/-":
                    logger.info(f"Downloading NSE Quarterly XBRL: {xbrl_url}")
                    xml_data, xml_err = self.nse_client.http.get_bytes(xbrl_url)
                    head_raw = (xml_data or b"")[:8000]
                    head = head_raw.lower()
                    head_sans_nuls = head.replace(b"\x00", b"")
                    if xml_data and (b"<xbrli:xbrl" in head_sans_nuls or b"<xbrl" in head_sans_nuls):
                        results.append((xml_data, {
                            "seq_id": str(fr.get("seqNumber", "")),
                            "attach_name": str(fr.get("seqNumber", "")),
                            "ann_date": fr.get("broadCastDate") or fr.get("toDate", ""),
                            "period_type": "quarterly",
                            "desc": f"Quarterly Results {fr.get('consolidated')} {fr.get('toDate')}",
                            "source_url": xbrl_url
                        }))
                    else:
                        logger.warning(f"Failed to fetch Quarterly XBRL from {xbrl_url}")

        logger.info(f"Total XBRL filings fetched for {symbol}: {len(results)}")
        return results

    def _infer_period_type(self, ann_date: str, attachment_text: str) -> str:
        """
        Infer whether the filing is annual, half_yearly, or quarterly
        based on announcement date and attachment description.
        """
        period_type = "quarterly"
        combined = (attachment_text or "").lower()
        
        if "audited" in combined and "un-audited" not in combined and "unaudited" not in combined:
            # Audited results filed Apr-Aug are typically annual results
            try:
                month_str = ann_date.split("-")[1]
                if month_str in ("Apr", "May", "Jun", "Jul", "Aug"):
                    period_type = "annual"
                elif month_str in ("Oct", "Nov"):
                    period_type = "half_yearly"
            except Exception:
                pass
        
        return period_type
