import json, glob, yfinance as yf

def fill_with_yfinance(sym="TATAMOTORS"):
    tk = yf.Ticker(f"{sym}.NS")
    
    try:
        inc = tk.financials.fillna(0).to_dict()
        bs = tk.balance_sheet.fillna(0).to_dict()
        cf = tk.cashflow.fillna(0).to_dict()
    except Exception as e:
        print("YFinance error:", e)
        return

    try:
        fp = glob.glob(f"data/**/{sym.lower()}/final/company_financials.json", recursive=True)[0]
    except IndexError:
        print(f"File for {sym} not found.")
        return

    with open(fp) as f:
        data = json.load(f)
        
    CRORE = 10_000_000

    for ts in inc.keys():
        yr = f"FY{ts.year if ts.month > 3 else ts.year}"
        
        rev = inc[ts].get('Total Revenue', 0) / CRORE
        np_ = inc[ts].get('Net Income', 0) / CRORE
        ebit = inc[ts].get('EBIT', 0) / CRORE
        
        if rev == 0: continue
        if yr not in data['profit_loss']['annual']: data['profit_loss']['annual'][yr] = {}
        data['profit_loss']['annual'][yr].update({
            'revenue_from_operations': round(rev, 2),
            "total_income": round(rev, 2),
            'net_profit': round(np_, 2),
            'ebit': round(ebit, 2)
        })

    for ts in bs.keys():
        yr = f"FY{ts.year if ts.month > 3 else ts.year}"
        assets = bs[ts].get('Total Assets', 0) / CRORE
        equity = bs[ts].get('Stockholders Equity', 0) / CRORE
        
        if yr not in data['balance_sheet']['annual']: data['balance_sheet']['annual'][yr] = {}
        data['balance_sheet']['annual'][yr].update({
            'total_assets': round(assets, 2),
            'total_equity': round(equity, 2)
        })

    for ts in cf.keys():
        yr = f"FY{ts.year if ts.month > 3 else ts.year}"
        cfo = cf[ts].get('Operating Cash Flow', 0) / CRORE
        if yr not in data['cash_flow']['annual']: data['cash_flow']['annual'][yr] = {}
        data['cash_flow']['annual'][yr].update({
            'cash_from_operations': round(cfo, 2)
        })

    with open(fp, "w") as f:
        json.dump(data, f, indent=2)

    print(f"{sym} deep data pulled from yfinance and saved.")

if __name__ == "__main__":
    fill_with_yfinance("TATAMOTORS")
    fill_with_yfinance("INDUSINDBK")
    fill_with_yfinance("ICICIBANK")
    fill_with_yfinance("HINDUNILVR")
    fill_with_yfinance("BHARTIARTL")
    fill_with_yfinance("TECHM")
