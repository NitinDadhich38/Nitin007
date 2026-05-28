import logging
from pipeline_v3.main_v2 import PipelineV2
from pipeline_v3.utils.universe import load_universe
import traceback

logging.basicConfig(level=logging.WARNING, format='%(levelname)s %(name)s: %(message)s')

if __name__ == '__main__':
    companies = load_universe()
    print(f"Starting sequential bulk process for {len(companies)} companies...")
    success = 0
    pipe = PipelineV2()
    for c in companies:
        try:
            print(f"Starting {c.symbol}...")
            pipe.process_company(c)
            success += 1
            print(f"✅ {c.symbol} completed.")
        except Exception as e:
            print(f"❌ {c.symbol} failed: {e}")
            traceback.print_exc()
            
    print(f"DONE! Successfully processed {success}/{len(companies)} companies.")
