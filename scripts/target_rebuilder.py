import logging
from pipeline_v3.hierarchical_pipeline import HierarchicalFinancialPipeline, _company_by_symbol
from pipeline_v3.utils.universe import load_universe

# Targets that had low accuracy
TARGETS = [
    "INFY", "TATAMOTORS", "ICICIBANK", "BHARTIARTL", 
    "HINDUNILVR", "ADANIENT", "INDUSINDBK", "UPL", 
    "TECHM", "ITC", "COALINDIA"
]

def run():
    universe = load_universe("pipeline_v3/config/nifty50_universe.json")
    pipe = HierarchicalFinancialPipeline(mca_base_dir="storage/raw/mca_xbrl")
    
    for sym in TARGETS:
        c = _company_by_symbol(universe, sym)
        if c:
            print(f"Triggering targeted rebuild for {sym}...")
            pipe.process_company(c, pdf_files=None)

if __name__ == "__main__":
    run()
