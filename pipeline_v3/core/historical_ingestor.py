import os
import requests
import logging
from pathlib import Path
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class HistoricalIngestor:
    """
    Downloads and caches historical Annual Reports for long-term data analysis.
    """
    
    def __init__(self, storage_root: str = "storage/historical"):
        self.storage_root = Path(storage_root)
        self.storage_root.mkdir(parents=True, exist_ok=True)

    def download_annual_reports(self, symbol: str, documents: List[Dict[str, Any]]):
        """
        Filters documents for 'Annual Report' and downloads them.
        """
        symbol_dir = self.storage_root / symbol.upper()
        symbol_dir.mkdir(exist_ok=True)
        
        ar_docs = [d for d in documents if d["type"] == "Annual Report"]
        
        for doc in ar_docs:
            filename = f"AR_{doc['date']}.pdf"
            local_path = symbol_dir / filename
            
            if local_path.exists():
                logger.debug(f"Already have {filename}")
                continue
                
            try:
                logger.info(f"Downloading AR for {symbol}: {doc['date']}")
                r = requests.get(doc["url"], timeout=30, stream=True)
                r.raise_for_status()
                with open(local_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                logger.info(f"Saved to {local_path}")
            except Exception as e:
                logger.error(f"Failed to download AR {doc['date']}: {e}")

if __name__ == "__main__":
    # Test script for Reliance
    from pipeline_v3.data_sources.doc_linker import DocLinker
    linker = DocLinker()
    ingestor = HistoricalIngestor()
    
    symbol = "RELIANCE"
    docs = linker.fetch_recent_documents(symbol, days=3650)
    ingestor.download_annual_reports(symbol, docs)
