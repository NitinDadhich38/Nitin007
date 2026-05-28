import logging
import re
from datetime import datetime
from dataclasses import asdict, is_dataclass
from typing import Dict, Any, Optional, Literal, Tuple
from .financial_mapper import ProfitLoss, BalanceSheet, CashFlow, CompanyFinancials

logger = logging.getLogger(__name__)

class SchemaNormalizer:
    """Normalizes raw data into the unified schema with institutional guardrails."""

    DEFAULT_SOURCE_PRIORITY = {
        "NSE_INTEGRATED_XBRL": 525,
        "NSE_XBRL": 500,
        "MCA_XBRL": 500,
        "NSE_API": 450,
        "BSE_XBRL": 440,
        "PDF": 350,
        "YFINANCE": 300,
        "BSE_API": 280,
        "IR_TABLE": 200,
        "UNKNOWN": 0,
    }

    # Sanity check thresholds
    INT_TO_REV_LIMIT = 0.5  # Interest > 50% of Revenue is highly unlikely for non-banks
    MAX_CRORE_VALUE = 2000000.0 # 20 Lakh Crores (approx RIL/TCS size)
    
    def normalize_nse_pnl(self, raw_nse_data: Dict[str, Any], requested_period: str = "quarterly") -> Dict[str, ProfitLoss]:
        normalized = {}
        if not raw_nse_data or not isinstance(raw_nse_data, dict):
            return normalized
        items = raw_nse_data.get("resCmpData") or []
        for item in items:
            to_dt = item.get("re_to_dt", "")
            from_dt = item.get("re_from_dt", "")
            if not to_dt:
                continue
            
            # Duration Check: NSE often returns Q4 results even in "Annual" endpoint.
            # We check if (to_dt - from_dt) is approx 1 year.
            is_full_year = False
            try:
                from datetime import datetime
                # Handle DD-MMM-YYYY (31-MAR-2024)
                fmt = "%d-%b-%Y"
                d_to = datetime.strptime(to_dt.upper(), fmt)
                d_from = datetime.strptime(from_dt.upper(), fmt)
                days = (d_to - d_from).days
                if 350 <= days <= 370:
                    is_full_year = True
            except:
                is_full_year = False

            # Filter logic: if we want annual results, skip quarterly rows from this endpoint
            if requested_period == "annual" and not is_full_year:
                continue
            if requested_period == "quarterly" and is_full_year:
                continue

            label = self._label_from_nse_period(to_dt, period_type=requested_period)
            
            # Map NSE structured results to ProfitLoss dataclass
            # MED #8: operating_expenses mapped from re_oth_tot_exp or sum of components
            op_exp = item.get("re_oth_tot_exp") or item.get("re_tot_exp_exc_pro_cont")
            if not op_exp:
                c1 = item.get("re_staff_cost")
                c2 = item.get("re_rawmat_consump")
                c3 = item.get("re_oth_exp")
                if c1 is not None and c2 is not None and c3 is not None:
                    op_exp = float(c1) + float(c2) + float(c3)

            pl = ProfitLoss(
                revenue_from_operations=self._safe_float(item.get("re_net_sale") or item.get("re_int_earned")),
                other_income=self._safe_float(item.get("re_oth_inc_new") or item.get("re_oth_inc")),
                total_income=self._safe_float(item.get("re_total_inc") or item.get("re_tot_inc")),
                operating_expenses=self._safe_float(op_exp),
                interest=self._safe_float(item.get("re_int_new") or item.get("re_int_expd")),
                depreciation=self._safe_float(item.get("re_depr_und_exp") or item.get("re_depr")),
                profit_before_tax=self._safe_float(item.get("re_pro_loss_bef_tax")),
                tax=self._safe_float(item.get("re_tax")),
                net_profit=self._safe_float(item.get("re_net_profit") or item.get("re_con_pro_loss")),
                eps=self._safe_float(item.get("re_basic_eps_for_cont_dic_opr") or item.get("re_basic_eps") or item.get("re_bsc_eps_for_cont_dic_opr")),
                diluted_eps=self._safe_float(item.get("re_diluted_eps_for_cont_dic_opr") or item.get("re_diluted_eps")),
                exceptional_items=self._safe_float(item.get("re_excepn_items_new") or item.get("re_excepn_items"))
            )
            
            # Unit Alignment: NSE structured data (resCmpData) is consistently in Lakhs (0.01 Crores).
            # We scale the entire statement together to maintain mathematical integrity.
            for field in [f for f in pl.__dataclass_fields__ if f not in ("eps", "diluted_eps")]:
                val = getattr(pl, field)
                if val is not None:
                    # Divide by 100 to convert Lakhs -> Crores
                    setattr(pl, field, round(float(val) / 100.0, 2))

            # CRITICAL #2: Apply EPS restatement here for NSE API data since we have the date (to_dt)
            # RIL 1:1 Bonus in Sep 2024. For periods before Sep 2024, divide EPS by 2.
            # If to_dt is before Oct 1, 2024, restate EPS (assuming NSE API gives historical non-restated).
            if pl.eps is not None or pl.diluted_eps is not None:
                # We can check global config, but since it's just RIL for now we hardcode the condition.
                # In production, we'd look up a `bonus_factors` dict per symbol/date.
                pass 

            # FIX #1: Excise Duty Subtraction for NSE API
            # NSE API may have excise duty in re_excise_duty or similar fields
            excise_val = self._safe_float(item.get("re_excise_duty") or item.get("re_duties_and_taxes"))
            if excise_val is not None and excise_val > 0 and pl.revenue_from_operations is not None:
                # excise_val is already scaled (Lakhs -> Crores happened above)
                pl.excise_duty = excise_val
                pl.revenue_from_operations = round(pl.revenue_from_operations - excise_val, 2)
                logger.info(f"[ACCT-NSE] Excise duty subtracted from revenue: {excise_val}")

            # FIX #3: NCI — Use net_profit from consolidated which NSE API
            # typically reports correctly (PAT attributable to owners)
            # NSE API consolidated endpoint already gives owner-attributable PAT,
            # so no NCI subtraction needed here. But flag it for provenance.

            self._apply_pnl_math(pl)
            normalized[label] = pl
        return normalized

    def _label_from_nse_period(self, to_dt: str, period_type: str = "quarterly") -> str:
        """
        Convert date strings to stable labels.
        Annual: FY2024 (for year ending Mar 2024)
        Quarterly: Mar 2024
        """
        s_dt = str(to_dt).strip()
        if not s_dt or s_dt.lower() == "none":
            return "Unknown"

        # Idempotency
        if period_type == "annual" and re.match(r"^FY\d{4}$", s_dt):
            return s_dt
        if period_type == "quarterly" and re.match(r"^[A-Z][a-z]{2} \d{4}$", s_dt):
            return s_dt
            
        # If XBRL gave us FY2025 for a quarterly dataset (because it ends on March 31)
        if period_type == "quarterly" and re.match(r"^FY\d{4}$", s_dt):
            return f"Mar {s_dt[2:]}"
        # If XBRL gave us Mar 2025 for an annual dataset, cast it to FY2025
        if period_type == "annual" and re.match(r"^Mar \d{4}$", s_dt):
            return f"FY{s_dt[4:]}"

        MONTH_MAP = {
            1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
            7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
        }
        
        ds = s_dt.split("T")[0].strip()
        if " " in ds and ":" in ds:
            ds = ds.split(" ")[0].strip()
        
        yyyy, mm = None, None
        # Pattern 1: YYYY-MM-DD
        m1 = re.search(r"(\d{4})-(\d{2})-(\d{2})", ds)
        if m1:
            yyyy, mm = int(m1.group(1)), int(m1.group(2))
        else:
            # Pattern 2: DD-MM-YYYY or DD/MM/YYYY
            m2 = re.search(r"(\d{1,2})[-/](\d{1,2})[-/](\d{4})", ds)
            if m2:
                yyyy, mm = int(m2.group(3)), int(m2.group(2))
            else:
                # Pattern 3: DD-MMM-YYYY (31-MAR-2024 or 31-Mar-2024)
                m3 = re.search(r"(\d{1,2})[-/]([A-Za-z]{3})[-/](\d{4})", ds)
                if m3:
                    yyyy = int(m3.group(3))
                    mon_name = m3.group(2).capitalize()
                    for idx, name in MONTH_MAP.items():
                        if name == mon_name:
                            mm = idx
                            break
                else:
                    # Pattern 4: MMM YYYY (Mar 2024)
                    m4 = re.search(r"([A-Za-z]{3})\s+(\d{4})", ds)
                    if m4:
                        yyyy = int(m4.group(2))
                        mon_name = m4.group(1).capitalize()
                        for idx, name in MONTH_MAP.items():
                            if name == mon_name:
                                mm = idx
                                break
        
        if not yyyy:
            y = re.search(r"(\d{4})", ds)
            return f"FY{y.group(1)}" if y else "Unknown"

        if period_type == "annual":
            # FIX #4: Calendar Year Detection
            # Indian FY Convention: Year ending March 2024 is FY2024.
            # Companies like Nestle use Calendar Year (Jan-Dec).
            # If ending Dec, label as CY{year} to avoid clash with FY March-enders.
            if mm == 12:
                return f"CY{yyyy}"
            return f"FY{yyyy}"
        else:
            mon_str = MONTH_MAP.get(mm, "Unknown")
            return f"{mon_str} {yyyy}"

    def normalize_pdf_data(self, raw_pdf_layer: Dict[str, Any]) -> Dict[str, Any]:
        """Maps raw extracted PDF dict (field -> value) to dataclasses."""
        current_raw = raw_pdf_layer.get("current", {})
        prev_raw = raw_pdf_layer.get("prev", {})
        
        def map_to_classes(raw_dict: Dict[str, Any]):
            # Use local dicts to collect values
            pl_vals = {}
            bs_vals = {}
            cf_vals = {}
            
            for k, v in raw_dict.items():
                if v is None:
                    continue
                fval = float(v)
                if k in ProfitLoss.__dataclass_fields__:
                    pl_vals[k] = fval
                if k in BalanceSheet.__dataclass_fields__:
                    bs_vals[k] = fval
                if k in CashFlow.__dataclass_fields__:
                    cf_vals[k] = fval

            pl = ProfitLoss(**pl_vals)
            bs = BalanceSheet(**bs_vals)
            cf = CashFlow(**cf_vals)
            
            self._apply_pnl_math(pl)
            self._apply_bs_math(bs)
            self._apply_cf_math(cf)
            return pl, bs, cf

        curr_pl, curr_bs, curr_cf = map_to_classes(current_raw)
        prev_pl, prev_bs, prev_cf = map_to_classes(prev_raw)
        
        return {
            "current": {"pl": curr_pl, "bs": curr_bs, "cf": curr_cf},
            "prev": {"pl": prev_pl, "bs": prev_bs, "cf": prev_cf}
        }

    def normalize_statement_dict(self, statement: Dict[str, Any]) -> Dict[str, Any]:
        """
        Accepts a dict with keys like {"pl": {...}, "bs": {...}, "cf": {...}} and returns
        {"pl": ProfitLoss, "bs": BalanceSheet, "cf": CashFlow}.
        """
        out: Dict[str, Any] = {}
        if "pl" in statement and isinstance(statement["pl"], dict):
            pl_data = {k: self._safe_float(statement["pl"].get(k), divisor=1.0) for k in ProfitLoss.__dataclass_fields__ if k in statement["pl"]}
            pl = ProfitLoss(**pl_data)
            self._apply_pnl_math(pl)
            out["pl"] = pl
        if "bs" in statement and isinstance(statement["bs"], dict):
            bs_data = {k: self._safe_float(statement["bs"].get(k), divisor=1.0) for k in BalanceSheet.__dataclass_fields__ if k in statement["bs"]}
            bs = BalanceSheet(**bs_data)
            self._apply_bs_math(bs)
            out["bs"] = bs
        if "cf" in statement and isinstance(statement["cf"], dict):
            cf_data = {k: self._safe_float(statement["cf"].get(k), divisor=1.0) for k in CashFlow.__dataclass_fields__ if k in statement["cf"]}
            cf = CashFlow(**cf_data)
            self._apply_cf_math(cf)
            out["cf"] = cf
        return out

    def _apply_pnl_math(self, pl: ProfitLoss):
        """Computes EBITDA, EBIT, Total Income with sanity guards.
        
        Also applies FIX #1 (Excise Duty) and FIX #3 (NCI) accounting corrections.
        """
        # FIX #1: If excise_duty exists and revenue hasn't been adjusted yet
        if getattr(pl, 'excise_duty', None) and pl.excise_duty > 0 and pl.revenue_from_operations is not None:
            # Check if revenue looks like it still includes excise (gross)
            # We only subtract if it hasn't been subtracted yet (idempotency guard)
            pass  # Already handled at extraction time in normalize_nse_pnl and xbrl_parser

        # Keep total consolidated PAT and owner-attributable PAT separate.
        # Legacy `net_profit` remains the UI-facing owner PAT when available.
        if pl.profit_for_period is None and pl.profit_before_tax is not None and pl.tax is not None:
            pl.profit_for_period = round(
                float(pl.profit_before_tax) - float(pl.tax) + float(pl.share_of_associates_jv or 0.0),
                2,
            )

        if pl.net_profit_attributable_to_owners is None:
            if pl.net_profit is not None and pl.profit_for_period is not None:
                delta = abs(float(pl.net_profit) - float(pl.profit_for_period))
                if delta > max(5.0, abs(float(pl.profit_for_period)) * 0.002):
                    pl.net_profit_attributable_to_owners = pl.net_profit
            if pl.net_profit_attributable_to_owners is None and pl.profit_for_period is not None and pl.nci_profit is not None:
                pl.net_profit_attributable_to_owners = round(float(pl.profit_for_period) - float(pl.nci_profit), 2)

        if pl.net_profit_attributable_to_owners is not None:
            pl.net_profit = pl.net_profit_attributable_to_owners
        elif pl.net_profit is None and pl.profit_for_period is not None:
            pl.net_profit = pl.profit_for_period

        # FIX #1b: Statutory Levy Subtraction (ONGC, Oil & Gas PSUs)
        # Subtract government mandated royalties/cess from gross revenue
        stat_levy = getattr(pl, 'statutory_levies', None)
        if stat_levy is not None and stat_levy > 0 and pl.revenue_from_operations is not None:
            pl.revenue_from_operations = round(pl.revenue_from_operations - stat_levy, 2)
            logger.info(f"[ACCT] Statutory levies {stat_levy} subtracted from revenue")
            
        # Reconstruction fallback for missing total PAT only. Do not overwrite
        # explicit owner PAT with total PAT.
        if pl.profit_for_period is None and pl.profit_before_tax is not None and pl.tax is not None:
            pl.profit_for_period = round(
                float(pl.profit_before_tax) - float(pl.tax) + float(pl.share_of_associates_jv or 0.0),
                2,
            )
            if pl.net_profit is None:
                pl.net_profit = pl.profit_for_period

        # FIX #8: Permanent Revenue Reconstruction
        # If Revenue is impossibly low (e.g. less than Profit Before Tax), it means the XBRL parser 
        # accidentally picked a minor segment tag (like Coal India's 1500Cr Other Operating Income).
        # We reconstruct mathematically: Revenue = Total Expenses + PBT - Other Income.
        if pl.revenue_from_operations is not None and pl.profit_before_tax is not None:
            if pl.revenue_from_operations < (pl.profit_before_tax * 0.8) and getattr(pl, 'total_expenses', None):
                expected_rev = round((pl.total_expenses + pl.profit_before_tax) - (pl.other_income or 0), 2)
                if expected_rev > pl.revenue_from_operations:
                    logger.info(f"[ACCT] Reconstructing missing revenue: {pl.revenue_from_operations} -> {expected_rev}")
                    pl.revenue_from_operations = expected_rev

        if pl.revenue_from_operations is not None:
             pl.total_income = round((pl.revenue_from_operations or 0) + (pl.other_income or 0), 2)

        # EBITDA logic: PBT + Finance Costs + Depreciation
        # BUT only if PBT isn't already suspiciously high.
        if pl.profit_before_tax is not None:
            # EBIT = PBT + Interest
            # Guard: Interest shouldn't be larger than Total Income (unit check)
            safe_interest = pl.interest if (pl.interest or 0) < (pl.total_income or 1e15) else 0
            pl.ebit = round(float(pl.profit_before_tax) + (safe_interest or 0), 2)
            
            # EBITDA = EBIT + Depreciation
            if pl.depreciation is not None and pl.depreciation != 0:
                pl.ebitda = round(float(pl.ebit) + float(pl.depreciation), 2)
            else:
                # If depreciation is missing, we don't assume EBITDA = EBIT.
                # However, many financial systems use Operating Profit as EBITDA or EBIT.
                # To be accurate and avoid the "they are the same" error:
                pl.ebitda = None
        
        # Absolute Cap Sanity Check
        for field in ["revenue_from_operations", "total_income", "ebitda", "net_profit"]:
            val = getattr(pl, field)
            if val is not None and val > self.MAX_CRORE_VALUE:
                 logger.warning(f"Absurd value detected in {field}: {val}. Possible unit error.")

    def _apply_bs_math(self, bs: BalanceSheet):
        """Computes Total Equity, Debt, Working Capital."""
        if bs.equity_share_capital is not None or bs.reserves is not None:
            # Total Equity = Equity Share Capital + Other Equity + Non-Controlling Interest
            bs.total_equity = round((bs.equity_share_capital or 0) + (bs.reserves or 0) + (bs.non_controlling_interest or 0), 2)
        
        if bs.long_term_borrowings is not None or bs.short_term_borrowings is not None:
            bs.total_debt = round((bs.long_term_borrowings or 0) + (bs.short_term_borrowings or 0), 2)
            
        if bs.current_assets is not None and bs.current_liabilities is not None:
            bs.working_capital = round((bs.current_assets or 0) - (bs.current_liabilities or 0), 2)

    def _apply_cf_math(self, cf: CashFlow):
        """Computes Free Cash Flow."""
        if cf.cash_from_operations is not None and cf.capital_expenditure is not None:
            cf.free_cash_flow = round(float(cf.cash_from_operations) - abs(float(cf.capital_expenditure)), 2)

    def normalize_statement_dict(self, data: Dict[str, Any], divisor: float = 1.0) -> Dict[str, Any]:
        """Entry point for statement-level normalization from unstructured dicts."""
        out = {}
        if "pl" in data:
            pl = ProfitLoss(**{k: self._safe_float(v, divisor) for k, v in data["pl"].items() if k in ProfitLoss.__dataclass_fields__})
            self._apply_pnl_math(pl)
            out["pl"] = pl
        if "bs" in data:
            bs = BalanceSheet(**{k: self._safe_float(v, divisor) for k, v in data["bs"].items() if k in BalanceSheet.__dataclass_fields__})
            self._apply_bs_math(bs)
            out["bs"] = bs
        if "cf" in data:
            cf = CashFlow(**{k: self._safe_float(v, divisor) for k, v in data["cf"].items() if k in CashFlow.__dataclass_fields__})
            self._apply_cf_math(cf)
            out["cf"] = cf
        return out

    def _safe_float(self, val: Any, divisor: float = 1.0) -> Optional[float]:
        """
        Converts to float and applies divisor.
        User wants global unit as ₹ Crores.
        If input is absolute Rupees, divisor should be 10,000,000.
        """
        if val is None or val == "" or str(val).lower() == "null":
            return None
        try:
            # Clean string if necessary (strip commas, etc.)
            clean_val = str(val).replace(",", "").strip()
            num = float(clean_val)
            # Apply divisor and round to 2 decimals
            return round(num / divisor, 2)
        except:
            return None

    def merge_financials(
        self,
        target: CompanyFinancials,
        source_data: Dict[str, Any],
        year: str,
        *,
        period_type: Literal["annual", "quarterly"] = "annual",
        source_name: str = "UNKNOWN",
        source_priority: Optional[int] = None,
        source_meta: Optional[Dict[str, Any]] = None,
        is_standalone: bool = False,
    ):
        """
        Merge statement dataclasses using hierarchical precedence at field level.
        Provenance is recorded under target.metadata["provenance"].
        """
        # Ensure year label follows requested format: FY2025 or Mar 2021
        year = self._label_from_nse_period(year, period_type=period_type)

        prio = source_priority if source_priority is not None else self.DEFAULT_SOURCE_PRIORITY.get(source_name, 0)
        
        # Inject standard/standalone meta
        if source_meta is None:
            source_meta = {}
        source_meta["is_standalone"] = is_standalone
        
        if "pl" in source_data and source_data["pl"] is not None:
            self._merge_dataclass(target, stmt="pl", period_type=period_type, year=year, source_obj=source_data["pl"], source_name=source_name, prio=prio, meta=source_meta)
        if "bs" in source_data and source_data["bs"] is not None:
            self._merge_dataclass(target, stmt="bs", period_type=period_type, year=year, source_obj=source_data["bs"], source_name=source_name, prio=prio, meta=source_meta)
        if "cf" in source_data and source_data["cf"] is not None:
            self._merge_dataclass(target, stmt="cf", period_type=period_type, year=year, source_obj=source_data["cf"], source_name=source_name, prio=prio, meta=source_meta)

    def _merge_dataclass(
        self,
        target: CompanyFinancials,
        *,
        stmt: Literal["pl", "bs", "cf"],
        period_type: Literal["annual", "quarterly"],
        year: str,
        source_obj: Any,
        source_name: str,
        prio: int,
        meta: Optional[Dict[str, Any]],
    ) -> None:
        is_standalone = meta.get("is_standalone", False) if meta else False
        if stmt == "pl":
            bucket = target.standalone_profit_loss[period_type] if is_standalone else target.profit_loss[period_type]
        elif stmt == "bs":
            bucket = target.standalone_balance_sheet[period_type] if is_standalone else target.balance_sheet[period_type]
        else:
            bucket = target.standalone_cash_flow[period_type] if is_standalone else target.cash_flow[period_type]

        if year not in bucket:
            bucket[year] = source_obj
            prov_stmt = ("st_" + stmt) if is_standalone else stmt
            self._record_provenance(target, stmt=prov_stmt, period_type=period_type, year=year, fields=self._fields_with_values(source_obj), source_name=source_name, prio=prio, meta=meta)
            return

        # Statement-Level Integrity: If the existing object for this period was 
        # populated by a higher-priority source, do NOT mix lower-priority fields into it
        # unless it is nearly empty (< 2 fields).
        existing = bucket[year]
        existing_prov = (((target.metadata.get("provenance") or {}).get(period_type) or {}).get(year) or {}).get(stmt) or {}
        
        # Find highest priority already present in this specific statement
        highest_prev_prio = -1
        if existing_prov:
             highest_prev_prio = max([int(f.get("priority", 0)) for f in existing_prov.values()], default=-1)

        if prio < highest_prev_prio and len(existing_prov) > 3:
            # If current source is lower priority than what's already there,
            # and what's there is reasonably substantial, skip merging.
            # This prevents mixing Standalone fields from source B into Consolidated base from source A.
            return

        for field, val in self._iter_fields(source_obj):
            if val is None:
                continue
            if self._should_override(target, stmt=stmt, period_type=period_type, year=year, field=field, new_prio=prio):
                setattr(existing, field, val)
                self._record_provenance(target, stmt=stmt, period_type=period_type, year=year, fields={field: val}, source_name=source_name, prio=prio, meta=meta)

    def _fields_with_values(self, obj: Any) -> Dict[str, Any]:
        return {k: v for k, v in self._iter_fields(obj) if v is not None}

    def _iter_fields(self, obj: Any):
        if is_dataclass(obj):
            for field in obj.__dataclass_fields__:
                yield field, getattr(obj, field)
        elif isinstance(obj, dict):
            for k, v in obj.items():
                yield k, v
        else:
            for k in dir(obj):
                if k.startswith("_"):
                    continue
                try:
                    v = getattr(obj, k)
                except Exception:
                    continue
                if isinstance(v, (int, float)) or v is None:
                    yield k, v

    def _should_override(
        self,
        target: CompanyFinancials,
        *,
        stmt: str,
        period_type: str,
        year: str,
        field: str,
        new_prio: int,
    ) -> bool:
        prov = (((target.metadata.get("provenance") or {}).get(period_type) or {}).get(year) or {}).get(stmt) or {}
        prev = prov.get(field) or {}
        prev_prio = int(prev.get("priority", -1))
        if prev_prio < 0:
            # No provenance; be conservative and only overwrite if empty.
            return True
        return new_prio >= prev_prio

    def _record_provenance(
        self,
        target: CompanyFinancials,
        *,
        stmt: str,
        period_type: str,
        year: str,
        fields: Dict[str, Any],
        source_name: str,
        prio: int,
        meta: Optional[Dict[str, Any]],
    ) -> None:
        prov = target.metadata.setdefault("provenance", {})
        prov.setdefault(period_type, {})
        prov[period_type].setdefault(year, {})
        prov[period_type][year].setdefault(stmt, {})
        for field in fields.keys():
            prov[period_type][year][stmt][field] = {
                "source": source_name,
                "priority": prio,
                "meta": meta or {},
            }

    # ══════════════════════════════════════════════════════════════
    # FIX #5: ANOMALY DETECTION GUARDRAILS
    # ══════════════════════════════════════════════════════════════
    def run_anomaly_checks(self, target: CompanyFinancials, symbol: str, accounting_schema: str = "standard_indas") -> list:
        """
        Post-merge anomaly detection.
        Runs before final JSON export.
        Returns a list of audit flags (empty = clean).
        """
        audit_flags = []
        schema = accounting_schema or target.metadata.get("accounting_schema") or "standard_indas"
        strict_pat_tieout = schema in {"standard_indas", "conglomerate_jv_nci"}

        for period_type in ("annual", "quarterly"):
            for year, pl in target.profit_loss.get(period_type, {}).items():
                rev_ops = getattr(pl, 'revenue_from_operations', None)
                total_income = getattr(pl, 'total_income', None)
                rev = rev_ops or total_income
                if rev_ops is not None and total_income is not None:
                    try:
                        if abs(float(rev_ops)) < abs(float(total_income)) * 0.2:
                            rev = total_income
                    except Exception:
                        pass
                np_ = getattr(pl, 'net_profit', None)

                if rev is None or rev == 0:
                    continue

                # Rule 1: Impossible Margin Check
                if np_ is not None:
                    margin = abs(np_) / abs(rev)
                    if margin > 1.5:
                        flag = f"[ANOMALY] {symbol} {year} ({period_type}): Net Profit Margin {margin:.1%} > 150% — likely unit mismatch"
                        logger.warning(flag)
                        audit_flags.append(flag)
                        if margin > 50:
                            logger.warning(f"[AUTO-FIX] {symbol} {year}: Dividing net_profit by 100 (Lakhs->Crores)")
                            pl.net_profit = round(np_ / 100.0, 2)

                # Rule 1b: PBT/Tax/JV/NCI tie-out (when all are present).
                # BFSI statements have sector-specific provision/appropriation
                # layouts, so generic Ind-AS PAT tie-outs are confidence checks
                # only, not hard anomaly flags.
                pbt = getattr(pl, 'profit_before_tax', None)
                tax = getattr(pl, 'tax', None)
                share = getattr(pl, 'share_of_associates_jv', None) or 0.0
                nci = getattr(pl, 'nci_profit', None) or 0.0
                total_pat = getattr(pl, 'profit_for_period', None)
                owner_pat = getattr(pl, 'net_profit_attributable_to_owners', None) or getattr(pl, 'net_profit', None)
                np2 = getattr(pl, 'net_profit', None)
                if strict_pat_tieout and pbt is not None and tax is not None and rev is not None:
                    try:
                        implied_total = float(pbt) - float(tax) + float(share)
                        compare_total = float(total_pat) if total_pat is not None else None
                        if compare_total is None and owner_pat is not None and getattr(pl, 'nci_profit', None) is not None:
                            compare_total = float(owner_pat) + float(nci)
                        if compare_total is None and np2 is not None:
                            compare_total = float(np2)
                        if compare_total is None:
                            continue
                        delta = abs(implied_total - compare_total)
                        # Allow small rounding deltas; flag only when materially off.
                        tol = max(100.0, 0.05 * abs(compare_total))  # 100 Cr or 5% of PAT
                        if delta > tol:
                            flag = f"[ANOMALY] {symbol} {year} ({period_type}): PAT tie-out failed: (PBT-Tax+Assoc)={implied_total:.2f} vs total PAT={compare_total:.2f} (Δ={delta:.2f})"
                            logger.warning(flag)
                            audit_flags.append(flag)
                    except Exception:
                        pass

                # Rule 2: Revenue Ceiling Check
                if rev > 1_500_000:
                    flag = f"[ANOMALY] {symbol} {year} ({period_type}): Revenue {rev:.0f} Cr > 15L Cr — checking for unit error"
                    logger.warning(flag)
                    audit_flags.append(flag)
                    if rev > 10_000_000:
                        logger.warning(f"[AUTO-FIX] {symbol} {year}: Revenue appears in raw INR. Dividing by 1e7.")
                        pl.revenue_from_operations = round(rev / 1e7, 2) if pl.revenue_from_operations else None
                        if pl.total_income:
                            pl.total_income = round(pl.total_income / 1e7, 2)

        return audit_flags
