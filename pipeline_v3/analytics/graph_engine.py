"""
Graph Engine — Time-Series Dataset Builder
==========================================
Pre-computes chart-ready datasets from verified filing data.

Rules (non-negotiable):
  - No interpolation for missing periods
  - No smoothing or averaging across gaps
  - Minimum 2 data points required to produce a dataset
  - Data points are skipped (not filled) when null
  - Periods sorted chronologically ascending for Chart.js
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional


MIN_POINTS = 2   # minimum data points to include a metric in graph_data

# Metrics to extract per statement type
GRAPH_METRICS = {
    # key in unified schema → display label (must match expected JS keys)
    "revenue_from_operations": "revenue", # This will be the key in the graph_data dict
    "net_profit":              "net_profit",
    "ebitda":                  "ebitda",
    "eps":                     "eps",
    "profit_before_tax":       "profit_before_tax",
    "interest":                "interest",
}

BS_GRAPH_METRICS = {
    "total_debt":   "Total Debt",
    "total_equity": "Total Equity",
    "total_assets": "Total Assets",
}

CF_GRAPH_METRICS = {
    "cash_from_operations": "Cash from Operations",
    "free_cash_flow":        "Free Cash Flow",
    "capital_expenditure":   "Capex",
}


def _safe(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _sort_periods(periods: List[str]) -> List[str]:
    """
    Sort period labels chronologically ascending.
    Handles: FY2025, Mar 2024, Sep 2024, etc.
    """
    import re

    def sort_key(p: str):
        # FY2025 → (2025, 12)
        fy = re.match(r"FY(\d{4})", p)
        if fy:
            return (int(fy.group(1)), 12)
        # "Mar 2024", "Dec 2024" etc.
        MONTHS = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,
                  "Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
        m = re.match(r"([A-Za-z]{3})\s+(\d{4})", p)
        if m:
            mon = MONTHS.get(m.group(1).capitalize(), 0)
            return (int(m.group(2)), mon)
        # fallback: try to extract 4-digit year
        yr = re.search(r"(\d{4})", p)
        return (int(yr.group(1)), 0) if yr else (0, 0)

    return sorted(periods, key=sort_key)


def _build_series(
    bucket: Dict[str, Dict],
    metric_key: str,
) -> List[Dict[str, Any]]:
    """
    Build a list of {period, value} dicts for a single metric.
    Skips null values. Returns empty list if < MIN_POINTS.
    """
    periods = _sort_periods(list(bucket.keys()))
    points = []
    for p in periods:
        row = bucket.get(p, {})
        # Support both dataclasses and dicts
        if hasattr(row, metric_key):
            val = _safe(getattr(row, metric_key))
        elif isinstance(row, dict):
            val = _safe(row.get(metric_key))
        else:
            val = None
            
        if val is not None:
            points.append({"period": p, "value": val})
    return points if len(points) >= MIN_POINTS else []


class GraphEngine:
    """
    Builds pre-computed graph datasets for the dashboard.
    Returns a dict keyed by metric name, each with quarterly and annual arrays.
    """

    def compute(
        self,
        pl_quarterly: Dict[str, Dict],
        pl_annual:    Dict[str, Dict],
        bs_annual:    Dict[str, Dict],
        cf_annual:    Dict[str, Dict],
    ) -> Dict[str, Any]:
        graph_data: Dict[str, Any] = {}

        # ── P&L metrics ─────────────────────────────────────────────────
        for src_key, target_key in GRAPH_METRICS.items():
            q_series = _build_series(pl_quarterly, src_key) if pl_quarterly else []
            a_series = _build_series(pl_annual,    src_key) if pl_annual    else []

            if q_series or a_series:
                graph_data[target_key] = {
                    "label":     target_key.capitalize(),
                    "unit":      "EPS (₹)" if target_key == "eps" else "₹ Crores",
                    "quarterly": q_series,
                    "annual":    a_series,
                }

        # ── Balance Sheet metrics (annual only) ──────────────────────────
        for src_key, target_key in BS_GRAPH_METRICS.items():
            a_series = _build_series(bs_annual, src_key) if bs_annual else []
            if a_series:
                graph_data[target_key] = {
                    "label":     target_key.replace("_", " ").capitalize(),
                    "unit":      "₹ Crores",
                    "quarterly": [],
                    "annual":    a_series,
                }

        # ── Cash Flow metrics (annual only) ─────────────────────────────
        for src_key, target_key in CF_GRAPH_METRICS.items():
            a_series = _build_series(cf_annual, src_key) if cf_annual else []
            if a_series:
                graph_data[target_key] = {
                    "label":     target_key.replace("_", " ").capitalize(),
                    "unit":      "₹ Crores",
                    "quarterly": [],
                    "annual":    a_series,
                }

        return graph_data

    def get_sparkline(
        self,
        graph_data: Dict[str, Any],
        metric_key: str,
        prefer_quarterly: bool = True,
        max_points: int = 8,
    ) -> List[Dict[str, Any]]:
        """Returns a trimmed series suitable for inline sparklines."""
        metric = graph_data.get(metric_key, {})
        series = metric.get("quarterly" if prefer_quarterly else "annual", [])
        if not series:
            series = metric.get("annual", [])
        return series[-max_points:] if series else []
