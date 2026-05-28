import json
import os
import glob
try:
    import yfinance as yf
except ImportError:
    import os
    os.system('pip install yfinance')
    import yfinance as yf
import pandas as pd

def check_accuracy():
    json_files = glob.glob('dashboard/data/*.json')
    results = []
    
    print(f"Testing {len(json_files)} companies against Yahoo Finance for accuracy...")
    
    for filepath in json_files:
        filename = os.path.basename(filepath)
        symbol = filename.split('.')[0]
        
        # Load local JSON
        with open(filepath, 'r') as f:
            data = json.load(f)
            
        local_pl = data.get('profit_loss', {}).get('yearly', {})
        if not local_pl:
            continue
            
        # Get YF data
        yf_symbol = f"{symbol}.NS"
        if symbol == "BAJAJ-AUTO":
            yf_symbol = "BAJAJ-AUTO.NS"
        if symbol == "M&M":
            yf_symbol = "M&M.NS"
            
        ticker = yf.Ticker(yf_symbol)
        try:
            yf_financials = ticker.financials
        except Exception:
            continue
            
        if yf_financials.empty:
            continue
            
        # Compare FY 2024 or latest matched
        local_score = 0
        total_checks = 0
        discrepancies = []
        
        for local_fy, local_metrics in local_pl.items():
            if "2024" in local_fy:
                # Find matching year in YF
                matched_col = None
                for col in yf_financials.columns:
                    if col.year == 2024:
                        matched_col = col
                        break
                
                if matched_col is not None:
                    yf_data = yf_financials[matched_col]
                    
                    # 1. Total Revenue / Revenue
                    local_rev = local_metrics.get('revenue_from_operations') or local_metrics.get('total_income')
                    # YF revenue is usually 'Total Revenue' and in actual units, local might be in Crores (1e7)
                    if 'Total Revenue' in yf_data and not pd.isna(yf_data['Total Revenue']) and local_rev:
                        yf_rev_cr = yf_data['Total Revenue'] / 1e7
                        diff = abs(local_rev - yf_rev_cr) / max(yf_rev_cr, 1)
                        total_checks += 1
                        if diff < 0.05: # within 5%
                            local_score += 1
                        else:
                            discrepancies.append(f"Revenue Diff: Local {local_rev}Cr vs YF {yf_rev_cr:.2f}Cr")
                    
                    # 2. Net Income
                    local_ni = local_metrics.get('net_profit')
                    if 'Net Income' in yf_data and not pd.isna(yf_data['Net Income']) and local_ni:
                        yf_ni_cr = yf_data['Net Income'] / 1e7
                        diff = abs(local_ni - yf_ni_cr) / max(abs(yf_ni_cr), 1)
                        total_checks += 1
                        if diff < 0.05:
                            local_score += 1
                        else:
                            discrepancies.append(f"Net Income Diff: Local {local_ni}Cr vs YF {yf_ni_cr:.2f}Cr")

        accuracy = (local_score / total_checks * 100) if total_checks > 0 else 0
        results.append({
            'symbol': symbol,
            'accuracy': accuracy,
            'discrepancies': discrepancies
        })
        
    # Summarize
    results.sort(key=lambda x: x['accuracy'], reverse=True)
    
    print("\n--- ACCURACY RESULTS ---")
    perfect = []
    flawed = []
    for r in results:
        if r['accuracy'] >= 90:
            perfect.append(r)
        else:
            flawed.append(r)
            
    avg_acc = sum(r['accuracy'] for r in results) / len(results) if results else 0
    print(f"\nOverall Pipeline Data Accuracy (Proxy via Yahoo Finance): {avg_acc:.2f}%")
    
    print("\n[Less Accurate Companies]")
    for r in flawed:
        print(f"- {r['symbol']}: {r['accuracy']:.0f}%")
        for d in r['discrepancies']:
            print(f"  * {d}")

if __name__ == '__main__':
    check_accuracy()
