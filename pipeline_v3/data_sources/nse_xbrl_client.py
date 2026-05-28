import logging
import datetime
from typing import Any, Dict, List, Tuple
from .nse_api_client import NSEAPIClient
from pipeline_v3.utils.periods import generate_active_periods

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

    @staticmethod
    def _is_xml_payload(xml_data: bytes) -> bool:
        head = (xml_data or b"")[:8000].lower().replace(b"\x00", b"")
        return b"<xbrli:xbrl" in head or b"<xbrl" in head

    def _append_result_xbrls(
        self,
        results: List[Tuple[bytes, Dict[str, Any]]],
        seen_urls: set,
        rows: List[Dict[str, Any]],
        *,
        period_type: str = None,
        source_label: str,
        max_rows: int,
        source_name: str = "NSE_XBRL",
        source_priority: int = 500,
    ) -> None:
        for fr in rows[:max_rows]:
            xbrl_url = fr.get("xbrl", "")
            if not xbrl_url or not xbrl_url.endswith(".xml"):
                continue
            if xbrl_url == "https://nsearchives.nseindia.com/corporate/xbrl/-" or xbrl_url in seen_urls:
                continue

            logger.info(f"Downloading NSE {source_label} XBRL: {xbrl_url}")
            xml_data, xml_err = self.nse_client.http.get_bytes(xbrl_url)
            if xml_data and self._is_xml_payload(xml_data):
                row_period_type = period_type or self._infer_integrated_period_type(fr)
                seq_id = str(fr.get("seqNumber") or fr.get("seq_Id") or "")
                period_end = fr.get("toDate") or fr.get("qe_Date")
                ann_date = fr.get("broadCastDate") or fr.get("broadcast_Date") or fr.get("creation_Date") or period_end or ""
                seen_urls.add(xbrl_url)
                results.append((xml_data, {
                    "seq_id": seq_id,
                    "attach_name": seq_id,
                    "ann_date": ann_date,
                    "period_type": row_period_type,
                    "desc": f"{source_label} {fr.get('consolidated')} {period_end}",
                    "source_url": xbrl_url,
                    "from_date": fr.get("fromDate"),
                    "to_date": period_end,
                    "source_name": source_name,
                    "source_priority": source_priority,
                }))
            else:
                logger.warning(f"Failed to fetch {source_label} XBRL from {xbrl_url}: {xml_err}")

    @staticmethod
    def _infer_integrated_period_type(row: Dict[str, Any]) -> str:
        """Infer annual vs quarterly for NSE Integrated Filing rows."""
        qe_date = str(row.get("qe_Date") or row.get("toDate") or "").upper()
        audited = str(row.get("audited") or "").lower()
        is_audited = "audited" in audited and "un-audited" not in audited and "unaudited" not in audited
        if qe_date.startswith("31-MAR") and is_audited:
            return "annual"
        return "quarterly"

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
        seen_urls = set()
        import urllib.parse
        safe_symbol = urllib.parse.quote(symbol, safe="")
        active_periods = generate_active_periods(lookback_years=3)

        # 1b. Fetch the newer SEBI/NSE Integrated Filing - Financials XBRL.
        # NSE started publishing current FY2025/FY2026 filings here while the
        # older corporates-financial-results endpoint can lag or omit them.
        logger.info(f"Fetching NSE Integrated Filing Financials XBRL for {symbol}")
        integrated_url = (
            "https://www.nseindia.com/api/integrated-filing-results"
            f"?index=equities&symbol={safe_symbol}"
            "&type=Integrated%20Filing-%20Financials&page=1&size=50"
        )
        integrated_data, err = self.nse_client.http.get_json(integrated_url)
        if integrated_data and isinstance(integrated_data, dict):
            rows = integrated_data.get("data") or []
            if rows:
                self._append_result_xbrls(
                    results,
                    seen_urls,
                    rows,
                    period_type=None,
                    source_label="Integrated Filing Financials",
                    max_rows=self.max_quarterly_filings,
                    source_name="NSE_INTEGRATED_XBRL",
                    source_priority=525,
                )
        
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
                    if xml_data and self._is_xml_payload(xml_data):
                        seen_urls.add(file_name_url)
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
            self._append_result_xbrls(
                results,
                seen_urls,
                afr_data,
                period_type="annual",
                source_label="Annual Financial Results",
                max_rows=self.max_annual_filings,
            )

        # 3b. Date-window annual lookups for newly due FYs. NSE has changed
        # response behavior over time, so we keep the broad endpoint above and
        # union it with active-period searches instead of replacing it.
        for period in [p for p in active_periods if p["period_type"] == "ANNUAL"][: self.max_annual_filings]:
            from_date = period["start_date"].strftime("%d-%m-%Y")
            to_date = (period["end_date"] + datetime.timedelta(days=75)).strftime("%d-%m-%Y")
            url = (
                "https://www.nseindia.com/api/corporates-financial-results"
                f"?index=equities&symbol={safe_symbol}&period=Annual&from_date={from_date}&to_date={to_date}"
            )
            data, err = self.nse_client.http.get_json(url)
            if data and isinstance(data, list):
                self._append_result_xbrls(
                    results,
                    seen_urls,
                    data,
                    period_type="annual",
                    source_label=f"Annual Financial Results {period['period']}",
                    max_rows=10,
                )

        # 4. Fetch Quarterly XBRL using the explicit financial results API.
        # NOTE: We intentionally fetch more than 12 so we can reconstruct multi-year
        # annual series for BFSI symbols where annual XBRL is not published.
        logger.info(f"Fetching Dedicated Quarterly XBRL for {symbol}")
        fr_url = f"https://www.nseindia.com/api/corporates-financial-results?index=equities&symbol={safe_symbol}&period=Quarterly"
        fr_data, err = self.nse_client.http.get_json(fr_url)
        
        if fr_data and isinstance(fr_data, list):
            # Limit downloads to keep runtime reasonable (covers ~5 years by default).
            self._append_result_xbrls(
                results,
                seen_urls,
                fr_data,
                period_type="quarterly",
                source_label="Quarterly Results",
                max_rows=self.max_quarterly_filings,
            )

        # 4b. Date-window quarterly lookups for each due active quarter. This
        # is what makes FY2026 quarters appear automatically once NSE publishes
        # them, without a code change or hardcoded period list.
        for period in [p for p in active_periods if p["period_type"] == "QUARTERLY"][: min(self.max_quarterly_filings, 8)]:
            from_date = period["start_date"].strftime("%d-%m-%Y")
            to_date = (period["end_date"] + datetime.timedelta(days=60)).strftime("%d-%m-%Y")
            url = (
                "https://www.nseindia.com/api/corporates-financial-results"
                f"?index=equities&symbol={safe_symbol}&period=Quarterly&from_date={from_date}&to_date={to_date}"
            )
            data, err = self.nse_client.http.get_json(url)
            if data and isinstance(data, list):
                self._append_result_xbrls(
                    results,
                    seen_urls,
                    data,
                    period_type="quarterly",
                    source_label=f"Quarterly Results {period['period']}",
                    max_rows=10,
                )

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
