from __future__ import annotations

from datetime import date, timedelta
from typing import Dict, List, Optional


def generate_active_periods(lookback_years: int = 3, today: Optional[date] = None) -> List[Dict]:
    """
    Generate Indian financial reporting periods from the active lookback window
    through the latest period whose SEBI filing due date has passed.

    Indian financial year: Apr 1 to Mar 31.
    Quarters: Q1 Apr-Jun, Q2 Jul-Sep, Q3 Oct-Dec, Q4 Jan-Mar.
    """
    today = today or date.today()
    periods: List[Dict] = []
    start_fy = today.year - lookback_years

    quarter_definitions = [
        ("Q1", 4, 1, 6, 30),
        ("Q2", 7, 1, 9, 30),
        ("Q3", 10, 1, 12, 31),
        ("Q4", 1, 1, 3, 31),
    ]

    for fy_start_year in range(start_fy, today.year + 2):
        fy_label_short = f"FY{str(fy_start_year + 1)[-2:]}"
        fy_label_long = f"FY{fy_start_year + 1}"

        for q_label, ms, ds, me, de in quarter_definitions:
            cal_year = fy_start_year + 1 if q_label == "Q4" else fy_start_year
            q_start = date(cal_year, ms, ds)
            q_end = date(cal_year, me, de)
            result_due = q_end + timedelta(days=45)
            if result_due > today:
                continue

            periods.append({
                "period": f"{q_label}{fy_label_short}",
                "period_long": f"{q_label}{fy_label_long}",
                "period_type": "QUARTERLY",
                "nse_period": "Quarterly",
                "quarter": q_label,
                "fy": fy_label_long,
                "fy_short": fy_label_short,
                "label": q_end.strftime("%b %Y"),
                "start_date": q_start,
                "end_date": q_end,
                "result_expected_after": result_due,
            })

    for fy_start_year in range(start_fy, today.year + 1):
        fy_end_year = fy_start_year + 1
        fy_end_date = date(fy_end_year, 3, 31)
        result_due = fy_end_date + timedelta(days=60)
        if result_due > today:
            continue

        fy_label = f"FY{fy_end_year}"
        periods.append({
            "period": fy_label,
            "period_long": fy_label,
            "period_type": "ANNUAL",
            "nse_period": "Annual",
            "quarter": None,
            "fy": fy_label,
            "fy_short": f"FY{str(fy_end_year)[-2:]}",
            "label": fy_label,
            "start_date": date(fy_start_year, 4, 1),
            "end_date": fy_end_date,
            "result_expected_after": result_due,
        })

    return sorted(periods, key=lambda x: x["end_date"], reverse=True)


def serializable_periods(periods: List[Dict]) -> List[Dict]:
    """Convert generated period metadata to JSON-safe strings."""
    out = []
    for period in periods:
        item = dict(period)
        for key in ("start_date", "end_date", "result_expected_after"):
            if hasattr(item.get(key), "isoformat"):
                item[key] = item[key].isoformat()
        out.append(item)
    return out
