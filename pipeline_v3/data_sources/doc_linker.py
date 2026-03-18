import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from .nse_api_client import NSEAPIClient

logger = logging.getLogger(__name__)

class DocLinker:
    """
    Automated Document Ingestion Engine.
    Crawls NSE for financial documents, concalls, and presentations.
    """
    
    def __init__(self, nse_client: Optional[NSEAPIClient] = None):
        self.nse_client = nse_client or NSEAPIClient()

    def fetch_recent_documents(self, symbol: str, days: int = 3650) -> List[Dict[str, Any]]:
        """
        Fetches documents from the last N days for a given symbol.
        Classifies them into Annual Reports, Results, Concalls, etc.
        """
        to_date = datetime.now()
        from_date = to_date - timedelta(days=days)
        
        # Formats for NSE API are typically DD-MM-YYYY
        from_str = from_date.strftime("%d-%m-%Y")
        to_str = to_date.strftime("%d-%m-%Y")
        
        logger.info(f"Linking documents for {symbol} from {from_str} to {to_str}")
        
        filings = self.nse_client.fetch_corporate_filings(
            symbol=symbol,
            from_date=from_str,
            to_date=to_str
        )
        
        if not filings or not isinstance(filings, list):
            logger.warning(f"No documents found for {symbol}")
            return []
            
        docs = []
        for f in filings:
            subject = f.get("desc", "").lower()
            attachment_text = f.get("attchmntText", "")
            attachment_file = f.get("attchmntFile", "")
            date = f.get("dt", "") # 'dt' seems to be the main date field
            
            if not attachment_file:
                continue
                
            # Classify document
            doc_type = "Other"
            if any(k in subject for k in ["annual report", "social responsibility"]):
                doc_type = "Annual Report"
            elif any(k in subject for k in ["presentation", "investor presentation"]):
                doc_type = "Investor Presentation"
            elif any(k in subject for k in ["transcript", "concall", "audio", "recording", "analysts", "institutional investors meeting"]):
                doc_type = "Concall"
            elif any(k in subject for k in ["financial results", "quarterly results", "limited review report"]):
                doc_type = "Results"
            elif "credit rating" in subject:
                doc_type = "Credit Rating"
            elif "shareholding pattern" in subject:
                doc_type = "Shareholding"
                
            # Full link construction for NSE
            # Usually links are: /xml/data/corpfiling/AttachHis/XXXX.pdf
            full_url = attachment_file
            if not full_url.startswith("http"):
                full_url = f"https://www.nseindia.com{attachment_file}"
            
            docs.append({
                "title": f.get("desc", "Untitled Document"),
                "type": doc_type,
                "date": date,
                "url": full_url,
                "category": f.get("category", "")
            })
            
        logger.info(f"Found {len(docs)} documents for {symbol}")
        return docs

if __name__ == "__main__":
    # Test for Reliance
    logging.basicConfig(level=logging.INFO)
    linker = DocLinker()
    ril_docs = linker.fetch_recent_documents("RELIANCE")
    for d in ril_docs[:5]:
        print(f"[{d['type']}] {d['date']} - {d['title']} ({d['url']})")
