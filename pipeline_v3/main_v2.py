"""
pipeline_v3/main_v2.py — Update 2.0 Entry Point
=================================================
Runs the v2 hierarchical pipeline for all Nifty 50 companies.

Data source hierarchy (strictly enforced):
  Tier 1: NSE_XBRL  (score 500)
  Tier 2: NSE_API   (score 450)
  Tier 3: PDF       (score 350, Smart-Trigger only)

REMOVED: MCA_XBRL, YFINANCE
"""

import argparse
import json
import logging
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline_v3.data_sources.nse_api_client import NSEAPIClient
from pipeline_v3.data_sources.nse_xbrl_client import NSEXBRLClient
from pipeline_v3.data_sources.pdf_parser_wrapper import PDFParser
from pipeline_v3.utils.periods import generate_active_periods, serializable_periods
from pipeline_v3.parsers.xbrl_parser import MCAXBRLInstanceParser
from pipeline_v3.transformers.schema_normalizer import SchemaNormalizer
from pipeline_v3.transformers.sector_normalizer import normalize_by_sector, resolve_accounting_schema
from pipeline_v3.transformers.financial_mapper import CompanyFinancials
from pipeline_v3.analytics.ratio_engine import RatioEngine
from pipeline_v3.utils.universe import Company, load_universe
from pipeline_v3.intelligence_layer import run_full_override_pipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger("PipelineV2")


class PipelineV2:
    """
    Update 2.0 — Zero-Hallucination Financial Pipeline.
    
    Sources:
      1. NSE XBRL (Annual + H1 filings → P&L, BS, CF)
      2. NSE API JSON (Quarterly P&L)
      3. PDF (Smart-Trigger fallback ONLY when core metrics missing)
    
    REMOVED: MCA XBRL, Yahoo Finance
    """

    def __init__(self):
        self.nse_api = NSEAPIClient()
        self.nse_xbrl = NSEXBRLClient()
        self.xbrl_parser = MCAXBRLInstanceParser(target_unit="INR_CRORE", prefer_consolidated=True)
        self.normalizer = SchemaNormalizer()
        self.ratio_engine = RatioEngine()

    @staticmethod
    def _label_to_fy(label: str) -> Optional[str]:
        """
        Map 'Mon YYYY' quarterly labels to FY buckets (Indian FY ends Mar).
        Example: 'Jun 2024' -> FY2025, 'Mar 2024' -> FY2024.
        Returns None for non-quarter labels.
        """
        if not isinstance(label, str) or " " not in label:
            return None
        mon, yr_s = label.split(" ", 1)
        try:
            yr = int(yr_s.strip())
        except Exception:
            return None
        if mon in {"Jan", "Feb", "Mar"}:
            return f"FY{yr}"
        if mon in {"Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}:
            return f"FY{yr+1}"
        return None

    def _synthesize_annual_from_quarterly(self, fin: CompanyFinancials) -> int:
        """
        If annual P&L is missing but we have quarterly P&L, derive annual P&L by summing
        the last 4 quarters per FY. This is primarily for BFSI symbols where NSE does not
        publish annual XBRL/JSON series.

        Returns number of annual periods synthesized (consolidated + standalone).
        """
        synthesized = 0

        def synth_bucket(q_bucket: Dict[str, Any], a_bucket: Dict[str, Any]) -> int:
            made = 0
            if not isinstance(q_bucket, dict) or not isinstance(a_bucket, dict):
                return 0
            # Group quarters by FY
            by_fy: Dict[str, Dict[str, Any]] = {}
            for label, pl in q_bucket.items():
                fy = self._label_to_fy(label)
                if fy is None:
                    continue
                by_fy.setdefault(fy, {})[label] = pl

            # For each FY, require 4 quarters.
            for fy, qmap in by_fy.items():
                if fy in a_bucket:
                    continue
                if len(qmap) < 4:
                    continue

                # Choose the 4 canonical quarter-ends for that FY (Jun/Sep/Dec/Mar).
                yr = int(fy[2:])
                need = [f"Jun {yr-1}", f"Sep {yr-1}", f"Dec {yr-1}", f"Mar {yr}"]
                if not all(k in qmap for k in need):
                    continue

                # Sum flow fields. EPS is not additive; keep None.
                from dataclasses import asdict
                import math
                sums: Dict[str, float] = {}
                for q in need:
                    d = asdict(qmap[q]) if hasattr(qmap[q], "__dataclass_fields__") else (qmap[q] or {})
                    for k, v in d.items():
                        if k in {"eps", "diluted_eps"}:
                            continue
                        if v is None:
                            continue
                        try:
                            fv = float(v)
                        except Exception:
                            continue
                        if not math.isfinite(fv):
                            continue
                        sums[k] = sums.get(k, 0.0) + fv

                # Build a ProfitLoss with summed fields
                from pipeline_v3.transformers.financial_mapper import ProfitLoss
                annual_pl = ProfitLoss(**{k: round(v, 2) for k, v in sums.items() if k in ProfitLoss.__dataclass_fields__})
                self.normalizer._apply_pnl_math(annual_pl)
                a_bucket[fy] = annual_pl
                made += 1
            return made

        # Consolidated
        synthesized += synth_bucket(fin.profit_loss.get("quarterly", {}), fin.profit_loss.get("annual", {}))
        synthesized += synth_bucket(fin.standalone_profit_loss.get("quarterly", {}), fin.standalone_profit_loss.get("annual", {}))
        return synthesized

    def process_company(self, company: Company) -> Dict[str, Any]:
        """Process a single company through the 3-tier pipeline."""
        symbol = company.symbol.upper()
        logger.info(f"{'='*60}")
        logger.info(f"Processing: {symbol} ({company.name})")
        logger.info(f"{'='*60}")

        fin = CompanyFinancials()
        fin.company_info = {
            "ticker": symbol,
            "symbol": symbol,
            "company_name": company.name or symbol,
            "sector": company.sector or "",
            "industry": company.industry or "",
            "unit": "₹ Crores",
        }
        data_sources = []

        # ══════════════════════════════════════════════════════════════
        # MARKET DATA (from NSE quote — institutional source)
        # ══════════════════════════════════════════════════════════════
        try:
            quote = self.nse_api.fetch_equity_quote(symbol)
            if quote:
                price_info = quote.get("priceInfo", {})
                security_info = quote.get("securityInfo", {})
                last_price = price_info.get("lastPrice")
                issued_shares = security_info.get("issuedSize")
                fin.company_info["price"] = last_price
                fin.company_info["shares_outstanding"] = issued_shares
                if last_price and issued_shares:
                    fin.company_info["market_cap"] = round(
                        (float(last_price) * float(issued_shares)) / 1e7, 2
                    )
                logger.info(f"  ✅ Market data: ₹{last_price}")
        except Exception as e:
            logger.warning(f"  ⚠️ Market data failed: {e}")

        # ══════════════════════════════════════════════════════════════
        # TIER 1: NSE XBRL (Score: 500) — Annual + H1 filings
        # Downloads actual XBRL XML from NSE corporate announcements.
        # Contains full P&L, Balance Sheet, and Cash Flow.
        # ══════════════════════════════════════════════════════════════
        xbrl_count = 0
        xbrl_source_counts = {}
        try:
            xbrl_results = self.nse_xbrl.fetch_all_for_symbol(symbol)
            if xbrl_results:
                for xml_bytes, meta in xbrl_results:
                    if not xml_bytes:
                        continue
                    raw_period_type = meta.get("period_type", "annual")
                    parsed = self.xbrl_parser.parse_bytes(xml_bytes, filing_period_type=raw_period_type)
                    stmts = parsed.get("statements", {})
                    
                    for stmt_type in ("pl", "bs", "cf"):
                        by_fy = stmts.get(stmt_type, {})
                        if not isinstance(by_fy, dict):
                            continue
                        for fy, payload in by_fy.items():
                            if not isinstance(payload, dict):
                                continue
                            payload_clean = {k: v for k, v in payload.items() if not str(k).startswith("_")}
                            norm = self.normalizer.normalize_statement_dict({stmt_type: payload_clean})
                            # Decide target bucket based on the parsed period label, not only the
                            # NSE "announcement type". Many symbols (notably BFSI) publish only
                            # "quarterly" XBRL filings, but the XBRL itself can still contain
                            # full-year contexts (FY/CY). We route FY*/CY* into annual.
                            if raw_period_type == "half_yearly":
                                raw_period_type = "annual"  # H1 BS/CF goes into annual bucket
                            if isinstance(fy, str) and (fy.startswith("FY") or fy.startswith("CY")):
                                period_type = "annual"
                            elif raw_period_type == "annual" and stmt_type == "pl":
                                # Integrated audited March filings contain both full-year
                                # (FY2026) and Q4-only (Mar 2026) P&L contexts. Keep the
                                # month label as quarterly instead of collapsing it into FY.
                                period_type = "quarterly"
                            elif raw_period_type == "annual":
                                # Skip non-FY balance-sheet/cash-flow fragments from annual
                                # filings. They are usually context artefacts, not a complete
                                # quarterly BS/CF statement.
                                continue
                            else:
                                period_type = raw_period_type
                                
                            desc_lower = meta.get("desc", "").lower()
                            is_std = "non-consolidated" in desc_lower or "standalone" in desc_lower
                            source_name = meta.get("source_name", "NSE_XBRL")
                            
                            self.normalizer.merge_financials(
                                fin, norm, fy,
                                period_type=period_type,
                                source_name=source_name,
                                source_priority=meta.get("source_priority"),
                                source_meta=meta,
                                is_standalone=is_std
                            )
                            xbrl_count += 1
                            xbrl_source_counts[source_name] = xbrl_source_counts.get(source_name, 0) + 1
                            if is_std:
                                fin.company_info["has_standalone"] = True

                if xbrl_count > 0:
                    for source_name, count in sorted(xbrl_source_counts.items()):
                        data_sources.append({"type": source_name, "fields_merged": count})
                    logger.info(f"  ✅ Tier 1 (NSE XBRL): {xbrl_count} field-periods merged")
        except Exception as e:
            logger.warning(f"  ⚠️ Tier 1 (NSE XBRL) failed: {e}")

        # ══════════════════════════════════════════════════════════════
        # TIER 2: NSE API JSON (Score: 450) — Quarterly + Annual P&L
        # ══════════════════════════════════════════════════════════════
        api_count = 0
        try:
            # Consolidated Quarterly
            nse_q = self.nse_api.fetch_results(symbol, period="Quarterly", consolidated=True)
            if nse_q:
                pnl = self.normalizer.normalize_nse_pnl(nse_q, requested_period="quarterly")
                for label, pl in pnl.items():
                    self.normalizer.merge_financials(fin, {"pl": pl}, label, period_type="quarterly", source_name="NSE_API")
                    api_count += 1

            # Consolidated Annual
            nse_a = self.nse_api.fetch_results(symbol, period="Annual", consolidated=True)
            if nse_a:
                apnl = self.normalizer.normalize_nse_pnl(nse_a, requested_period="annual")
                for label, pl in apnl.items():
                    self.normalizer.merge_financials(fin, {"pl": pl}, label, period_type="annual", source_name="NSE_API")
                    api_count += 1

            # Standalone Quarterly
            nse_std_q = self.nse_api.fetch_results(symbol, period="Quarterly", consolidated=False)
            if nse_std_q:
                spnl = self.normalizer.normalize_nse_pnl(nse_std_q, requested_period="quarterly")
                for label, pl in spnl.items():
                    self.normalizer.merge_financials(fin, {"pl": pl}, label, period_type="quarterly", source_name="NSE_API", is_standalone=True)
                    api_count += 1
                fin.company_info["has_standalone"] = True

            # Standalone Annual
            nse_std_a = self.nse_api.fetch_results(symbol, period="Annual", consolidated=False)
            if nse_std_a:
                sapnl = self.normalizer.normalize_nse_pnl(nse_std_a, requested_period="annual")
                for label, pl in sapnl.items():
                    self.normalizer.merge_financials(fin, {"pl": pl}, label, period_type="annual", source_name="NSE_API", is_standalone=True)
                    api_count += 1
                fin.company_info["has_standalone"] = True

            if api_count > 0:
                data_sources.append({"type": "NSE_API", "fields_merged": api_count})
                logger.info(f"  ✅ Tier 2 (NSE API): {api_count} periods merged")
        except Exception as e:
            logger.warning(f"  ⚠️ Tier 2 (NSE API) failed: {e}")

        # ══════════════════════════════════════════════════════════════
        # SMART PDF TRIGGER CHECK
        # PDF is *only* invoked if BOTH revenue AND net_profit are
        # missing after Tier 1 + Tier 2.
        # ══════════════════════════════════════════════════════════════
        has_annual_pl = bool(fin.profit_loss.get("annual", {}) or fin.standalone_profit_loss.get("annual", {}))
        has_quarterly_pl = bool(fin.profit_loss.get("quarterly", {}) or fin.standalone_profit_loss.get("quarterly", {}))

        if not has_annual_pl and not has_quarterly_pl:
            logger.warning(f"  🔴 SMART-TRIGGER: No P&L from Tier 1+2. PDF would be invoked here.")
            # PDF parsing stub — in production, this would download + parse
            # For now, log the trigger event
            data_sources.append({"type": "PDF_TRIGGER", "reason": "No P&L data from NSE sources"})
        else:
            logger.info(f"  ✅ SMART-TRIGGER: Core metrics present. PDF SKIPPED.")

        # BFSI fallback: derive annual P&L from quarterly when annual series is missing.
        # Must run before analytics so ratios/growth include derived annuals.
        derived_periods = self._synthesize_annual_from_quarterly(fin)
        if derived_periods > 0:
            data_sources.append({"type": "DERIVED_QUARTER_SUM", "periods": derived_periods})
            logger.info(f"  ✅ Derived annual P&L from quarterly: {derived_periods} FY period(s)")

        accounting_schema = resolve_accounting_schema(symbol, company.sector or "", company.industry or "")
        normalize_by_sector(fin, symbol=symbol, sector=company.sector or "", industry=company.industry or "")

        # ══════════════════════════════════════════════════════════════
        # ══════════════════════════════════════════════════════════════
        # ANALYTICS: Ratios & EPS Restatement
        # ══════════════════════════════════════════════════════════════
        
        # CRITICAL #2: RELIANCE 1:1 Bonus Restatement
        if symbol == "RELIANCE":
            # EPS needs to be /2 for all periods ending on or before Sep 2024
            for period_cat in ("annual", "quarterly"):
                for label, pl in fin.profit_loss.get(period_cat, {}).items():
                    try:
                        is_pre_bonus = False
                        if label.startswith("FY"):
                            if int(label[2:6]) <= 2024:
                                is_pre_bonus = True
                        else:
                            parts = label.split()
                            if len(parts) == 2:
                                mon, yr = parts[0], int(parts[1])
                                if yr < 2024 or (yr == 2024 and mon in ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep"]):
                                    is_pre_bonus = True
                        
                        if is_pre_bonus:
                            if pl.eps: pl.eps = round(pl.eps / 2.0, 2)
                            if pl.diluted_eps: pl.diluted_eps = round(pl.diluted_eps / 2.0, 2)
                    except Exception:
                        pass

        annual_years = sorted(
            set(fin.profit_loss.get("annual", {}).keys()) |
            set(fin.balance_sheet.get("annual", {}).keys()) |
            set(fin.cash_flow.get("annual", {}).keys()),
            reverse=True,
        )
        for fy in annual_years:
            pl = fin.profit_loss["annual"].get(fy)
            bs = fin.balance_sheet["annual"].get(fy)
            cf = fin.cash_flow["annual"].get(fy)

            # Auto-compute EPS if missing
            if pl and pl.net_profit and getattr(pl, 'eps', None) is None:
                shares = getattr(bs, 'shares_outstanding', None) or fin.company_info.get("shares_outstanding")
                if shares:
                    pl.eps = round((pl.net_profit * 1e7) / shares, 2)

            fin.ratios["annual"][fy] = self.ratio_engine.compute_all(pl, bs, cf)

        # ══════════════════════════════════════════════════════════════
        # FIX #5: ANOMALY DETECTION GUARDRAILS
        # Run before export to catch unit mismatches and impossible margins.
        # ══════════════════════════════════════════════════════════════
        audit_flags = self.normalizer.run_anomaly_checks(fin, symbol, accounting_schema=accounting_schema)
        if audit_flags:
            logger.warning(f"  🔍 ANOMALY FLAGS for {symbol}: {len(audit_flags)} issues detected")
            for flag in audit_flags:
                logger.warning(f"    {flag}")
            data_sources.append({"type": "ANOMALY_AUDIT", "flags": audit_flags})
        else:
            logger.info(f"  ✅ ANOMALY CHECK: {symbol} passed all guardrails.")

        # ══════════════════════════════════════════════════════════════
        # BUILD EXPORT DOCUMENT
        # ══════════════════════════════════════════════════════════════
        flat_ratios = {}
        for fy, ratio_dict in fin.ratios.get("annual", {}).items():
            flat_ratios[fy] = ratio_dict

        export_doc = {
            "company_info": fin.company_info,
            "profit_loss": {
                "quarterly": {k: asdict(v) for k, v in fin.profit_loss.get("quarterly", {}).items()},
                "yearly": {k: asdict(v) for k, v in fin.profit_loss.get("annual", {}).items()},
            },
            "standalone_profit_loss": {
                "quarterly": {k: asdict(v) for k, v in fin.standalone_profit_loss.get("quarterly", {}).items()},
                "yearly": {k: asdict(v) for k, v in fin.standalone_profit_loss.get("annual", {}).items()},
            },
            "balance_sheet": {
                "yearly": {k: asdict(v) for k, v in fin.balance_sheet.get("annual", {}).items()},
            },
            "standalone_balance_sheet": {
                "yearly": {k: asdict(v) for k, v in fin.standalone_balance_sheet.get("annual", {}).items()},
            },
            "cash_flow": {
                "yearly": {k: asdict(v) for k, v in fin.cash_flow.get("annual", {}).items()},
            },
            "standalone_cash_flow": {
                "yearly": {k: asdict(v) for k, v in fin.standalone_cash_flow.get("annual", {}).items()},
            },
            "ratios": flat_ratios,
            "metadata": {
                "data_sources": data_sources,
                "field_provenance": fin.metadata.get("provenance", {}),
                "last_updated": datetime.now(timezone.utc).isoformat(),
                "parser_version": "v2.2-IntelligenceLayer",
                "pipeline": "Update2.2-6LayerArchitecture",
                "active_period_window": serializable_periods(generate_active_periods(lookback_years=3)),
                "accounting_schema": accounting_schema,
                "validation_passed": len(audit_flags) == 0,
                "anomaly_flags": audit_flags,
            },
        }

        # ══════════════════════════════════════════════════════════════
        # LAYER 3+5+6: INTELLIGENCE LAYER
        # Override Registry → BFSI Routing → Confidence Scoring
        # ══════════════════════════════════════════════════════════════
        industry = company.industry or company.sector or ""
        export_doc = run_full_override_pipeline(export_doc, symbol, industry)

        # Save to data directory
        out_dir = Path(f"data/{symbol.lower()}/final")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / "company_financials.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(export_doc, f, indent=2, ensure_ascii=False, default=lambda o: getattr(o, "__dict__", str(o)))

        logger.info(f"  💾 Saved: {out_file}")

        # Also copy to dashboard/data
        dashboard_dir = Path("dashboard/data")
        dashboard_dir.mkdir(parents=True, exist_ok=True)
        dashboard_file = dashboard_dir / f"{symbol.upper()}.json"
        
        import shutil
        shutil.copy2(out_file, dashboard_file)
        logger.info(f"  🚀 Deployed to dashboard: {dashboard_file}")

        # Summary
        n_annual = len(fin.profit_loss.get("annual", {}))
        n_quarterly = len(fin.profit_loss.get("quarterly", {}))
        n_bs = len(fin.balance_sheet.get("annual", {}))
        n_cf = len(fin.cash_flow.get("annual", {}))
        logger.info(f"  📊 Summary: {n_annual} annual P&L | {n_quarterly} quarterly P&L | {n_bs} BS | {n_cf} CF")
        logger.info(f"  📡 Sources: {[s['type'] for s in data_sources]}")

        return export_doc


def main():
    ap = argparse.ArgumentParser(description="Update 2.0 — Zero-Hallucination Financial Pipeline")
    ap.add_argument("--symbol", help="Process a single symbol (e.g., RELIANCE)")
    ap.add_argument("--all", action="store_true", help="Process all Nifty 50 companies")
    ap.add_argument("--universe", default="pipeline_v3/config/nifty50_universe.json")
    ap.add_argument("--generate-dashboard", action="store_true", help="Also regenerate dashboard JSON after pipeline")
    args = ap.parse_args()

    universe = load_universe(args.universe)
    if not universe:
        logger.error("Universe file empty or not found.")
        return 1

    pipe = PipelineV2()
    results = []

    if args.all:
        logger.info(f"🚀 Processing ALL {len(universe)} companies...")
        for company in universe:
            try:
                result = pipe.process_company(company)
                results.append(result)
            except Exception as e:
                logger.error(f"❌ {company.symbol} FAILED: {e}", exc_info=True)
    elif args.symbol:
        target = None
        for c in universe:
            if c.symbol.upper() == args.symbol.upper():
                target = c
                break
        if not target:
            # Create ad-hoc company entry
            target = Company(symbol=args.symbol.upper(), name=args.symbol.upper())
        result = pipe.process_company(target)
        results.append(result)
    else:
        logger.error("Provide --symbol TICKER or --all")
        return 2

    logger.info(f"\n{'='*60}")
    logger.info(f"✅ Pipeline complete: {len(results)} companies processed")
    logger.info(f"{'='*60}")

    if args.generate_dashboard:
        logger.info("Regenerating dashboard data...")
        import subprocess
        subprocess.run([sys.executable, "scripts/generate_dashboard_data.py"], cwd=str(Path(__file__).parent.parent))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
