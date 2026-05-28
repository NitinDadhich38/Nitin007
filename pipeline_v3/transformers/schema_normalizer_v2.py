import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

DEFAULT_SOURCE_PRIORITY: Dict[str, int] = {
    "NSE_XBRL": 500,
    "NSE_API":  450,
    "PDF":      350,
}

MIN_TRUST_SCORE: int = 300

INCOME_STATEMENT_FIELDS: Tuple[str, ...] = (
    "revenue_from_operations", "other_income", "total_income", "cost_of_materials_consumed", 
    "changes_in_inventories", "employee_benefit_expense", "finance_costs", "depreciation_amortisation", 
    "other_expenses", "total_expenses", "profit_before_exceptional", "exceptional_items", "profit_before_tax", 
    "tax_expense_current", "tax_expense_deferred", "total_tax_expense", "share_of_associates_jv",
    "profit_for_period", "net_profit", "net_profit_attributable_to_owners", "nci_profit", "excise_duty", "statutory_levies",
    "other_comprehensive_income", "total_comprehensive_income", "eps_basic", "eps_diluted",
)

BALANCE_SHEET_FIELDS: Tuple[str, ...] = (
    "equity_share_capital", "other_equity", "total_equity", "non_current_borrowings", "current_borrowings", 
    "total_borrowings", "trade_payables", "other_current_liabilities", "total_current_liabilities", 
    "total_liabilities", "property_plant_equipment_net", "capital_work_in_progress", "goodwill", 
    "other_intangible_assets", "non_current_investments", "deferred_tax_assets", "total_non_current_assets", 
    "inventories", "trade_receivables", "cash_and_cash_equivalents", "bank_balances_other", 
    "current_investments", "other_current_assets", "total_current_assets", "total_assets",
)

CASH_FLOW_FIELDS: Tuple[str, ...] = (
    "cfo", "cfi", "cff", "net_change_in_cash", "capex", "free_cash_flow",
)

ALL_FINANCIAL_FIELDS: Tuple[str, ...] = INCOME_STATEMENT_FIELDS + BALANCE_SHEET_FIELDS + CASH_FLOW_FIELDS
QUARTERS_WITHOUT_BALANCE_SHEET = {"Q1", "Q3"}

_NSE_XBRL_ALIASES: Dict[str, str] = {
    "RevenueFromOperations": "revenue_from_operations",
    "OtherIncome": "other_income",
    "TotalRevenue": "total_income",
    "CostOfMaterialsConsumed": "cost_of_materials_consumed",
    "ChangesInInventoriesOfFinishedGoods": "changes_in_inventories",
    "EmployeeBenefitExpense": "employee_benefit_expense",
    "FinanceCosts": "finance_costs",
    "DepreciationDepletionAndAmortisation": "depreciation_amortisation",
    "OtherExpenses": "other_expenses",
    "TotalExpenses": "total_expenses",
    "ProfitBeforeExceptionalItemsAndTax": "profit_before_exceptional",
    "ExceptionalItems": "exceptional_items",
    "ProfitBeforeTax": "profit_before_tax",
    "CurrentTax": "tax_expense_current",
    "DeferredTax": "tax_expense_deferred",
    "TaxExpense": "total_tax_expense",
    "ProfitForThePeriod": "profit_for_period",
    "ProfitLossForPeriod": "profit_for_period",
    "ShareOfProfitLossOfAssociatesAndJointVenturesAccountedForUsingEquityMethod": "share_of_associates_jv",
    "ShareOfProfitLossOfAssociates": "share_of_associates_jv",
    "ProfitOrLossAttributableToNonControllingInterests": "nci_profit",
    "ProfitLossOfMinorityInterest": "nci_profit",
    "OtherComprehensiveIncome": "other_comprehensive_income",
    "TotalComprehensiveIncome": "total_comprehensive_income",
    "BasicEarningsLossPerShare": "eps_basic",
    "DilutedEarningsLossPerShare": "eps_diluted",
    "EquityShareCapital": "equity_share_capital",
    "OtherEquity": "other_equity",
    "TotalEquity": "total_equity",
    "LongTermBorrowings": "non_current_borrowings",
    "ShortTermBorrowings": "current_borrowings",
    "TradePayables": "trade_payables",
    "TotalAssets": "total_assets",
    "TotalCurrentAssets": "total_current_assets",
    "TotalNonCurrentAssets": "total_non_current_assets",
    "CashAndCashEquivalents": "cash_and_cash_equivalents",
    "Inventories": "inventories",
    "TradeReceivables": "trade_receivables",
    "NetCashFlowFromOperatingActivities": "cfo",
    "NetCashFlowFromInvestingActivities": "cfi",
    "NetCashFlowFromFinancingActivities": "cff",
    "NetIncreaseDecreaseInCashAndCashEquivalents": "net_change_in_cash",
    "PurchaseOfPropertyPlantAndEquipment": "capex",
    # FIX #1: Excise Duty tags
    "ExciseDuty": "excise_duty",
    "DutiesAndTaxes": "excise_duty",
    "ExciseDutyOnSalesOfGoods": "excise_duty",
    # FIX #3: NCI tags
    "ProfitLossAttributableToOwnersOfParent": "net_profit_attributable_to_owners",
    "ProfitForThePeriod": "profit_for_period",
    # Statutory levies (ONGC, Oil & Gas)
    "StatutoryLevies": "statutory_levies",
    "RoyaltyCessAndOtherProductionCharges": "statutory_levies",
    "RoyaltyAndCessExpense": "statutory_levies",
}

_NSE_API_ALIASES: Dict[str, str] = {
    "netSales": "revenue_from_operations",
    "totalRevenue": "total_income",
    "otherIncome": "other_income",
    "totalExpenditure": "total_expenses",
    "pbdt": "profit_before_exceptional",
    "pbt": "profit_before_tax",
    "tax": "total_tax_expense",
    "pat": "net_profit",
    "eps": "eps_basic",
    "dilEps": "eps_diluted",
}

_PDF_ALIASES: Dict[str, str] = {
    "revenue from operations": "revenue_from_operations",
    "net sales": "revenue_from_operations",
    "other income": "other_income",
    "total income": "total_income",
    "profit before tax": "profit_before_tax",
    "profit after tax": "net_profit",
    "pat": "net_profit",
    "basic eps": "eps_basic",
    "diluted eps": "eps_diluted",
}

ALIAS_MAP_BY_SOURCE = {
    "NSE_XBRL": _NSE_XBRL_ALIASES,
    "NSE_API":  _NSE_API_ALIASES,
    "PDF":      _PDF_ALIASES,
}

@dataclass
class SourcedValue:
    canonical_field:  str
    value:            Any
    source:           str
    priority:         int
    period:           Optional[str] = None
    unit:             str = "INR_CRORES"
    raw_key:          Optional[str] = None
    notes:            Optional[str] = None
    is_consolidated:  Optional[bool] = None  # FIX #2: Track consolidation status

    def __post_init__(self) -> None:
        if self.source not in DEFAULT_SOURCE_PRIORITY:
            raise ValueError(f"Unknown source '{self.source}'")

@dataclass
class NormalisedFinancials:
    symbol:     str
    period:     str
    period_type: str
    quarter:    Optional[str] = None

    revenue_from_operations: Optional[float] = None
    other_income: Optional[float] = None
    total_income: Optional[float] = None
    cost_of_materials_consumed: Optional[float] = None
    changes_in_inventories: Optional[float] = None
    employee_benefit_expense: Optional[float] = None
    finance_costs: Optional[float] = None
    depreciation_amortisation: Optional[float] = None
    other_expenses: Optional[float] = None
    total_expenses: Optional[float] = None
    profit_before_exceptional: Optional[float] = None
    exceptional_items: Optional[float] = None
    profit_before_tax: Optional[float] = None
    tax_expense_current: Optional[float] = None
    tax_expense_deferred: Optional[float] = None
    total_tax_expense: Optional[float] = None
    share_of_associates_jv: Optional[float] = None
    profit_for_period: Optional[float] = None
    net_profit: Optional[float] = None
    net_profit_attributable_to_owners: Optional[float] = None
    nci_profit: Optional[float] = None
    excise_duty: Optional[float] = None
    statutory_levies: Optional[float] = None
    other_comprehensive_income: Optional[float] = None
    total_comprehensive_income: Optional[float] = None
    eps_basic: Optional[float] = None
    eps_diluted: Optional[float] = None

    equity_share_capital: Optional[float] = None
    other_equity: Optional[float] = None
    total_equity: Optional[float] = None
    non_current_borrowings: Optional[float] = None
    current_borrowings: Optional[float] = None
    total_borrowings: Optional[float] = None
    trade_payables: Optional[float] = None
    other_current_liabilities: Optional[float] = None
    total_current_liabilities: Optional[float] = None
    total_liabilities: Optional[float] = None
    property_plant_equipment_net: Optional[float] = None
    capital_work_in_progress: Optional[float] = None
    goodwill: Optional[float] = None
    other_intangible_assets: Optional[float] = None
    non_current_investments: Optional[float] = None
    deferred_tax_assets: Optional[float] = None
    total_non_current_assets: Optional[float] = None
    inventories: Optional[float] = None
    trade_receivables: Optional[float] = None
    cash_and_cash_equivalents: Optional[float] = None
    bank_balances_other: Optional[float] = None
    current_investments: Optional[float] = None
    other_current_assets: Optional[float] = None
    total_current_assets: Optional[float] = None
    total_assets: Optional[float] = None

    cfo: Optional[float] = None
    cfi: Optional[float] = None
    cff: Optional[float] = None
    net_change_in_cash: Optional[float] = None
    capex: Optional[float] = None
    free_cash_flow: Optional[float] = None

    field_sources: Dict[str, str] = field(default_factory=dict)
    field_priorities: Dict[str, int] = field(default_factory=dict)
    sources_used: List[str] = field(default_factory=list)

    def has_core_metrics(self) -> bool:
        return self.revenue_from_operations is not None and self.net_profit is not None

    def to_dict(self) -> Dict[str, Any]:
        result = {"symbol": self.symbol, "period": self.period, "period_type": self.period_type, "quarter": self.quarter}
        for f in ALL_FINANCIAL_FIELDS:
            result[f] = getattr(self, f, None)
        result["_meta"] = {"field_sources": self.field_sources, "field_priorities": self.field_priorities, "sources_used": self.sources_used}
        return result

class SchemaNormaliser:
    def __init__(self, source_priority: Optional[Dict[str, int]] = None):
        self.priority = source_priority or DEFAULT_SOURCE_PRIORITY

    def normalise_alias(self, raw_key: str, source: str) -> Optional[str]:
        alias_map = ALIAS_MAP_BY_SOURCE.get(source, {})
        if raw_key in alias_map: return alias_map[raw_key]
        if raw_key in ALL_FINANCIAL_FIELDS: return raw_key
        if source == "PDF":
            lower = raw_key.lower().strip()
            for alias, canonical in alias_map.items():
                if alias.lower() == lower: return canonical
        return None

    def merge(self, symbol: str, period: str, period_type: str, sourced_values: List[SourcedValue], quarter: Optional[str] = None) -> NormalisedFinancials:
        result = NormalisedFinancials(symbol=symbol, period=period, period_type=period_type, quarter=quarter)
        best = {}

        for sv in sourced_values:
            if sv.source not in self.priority or sv.value is None or self.priority[sv.source] < MIN_TRUST_SCORE: continue
            src_priority = self.priority[sv.source]

            # FIX #2: Consolidated sources get +100 priority bonus
            effective_priority = src_priority
            if sv.is_consolidated is True:
                effective_priority += 100
            elif sv.is_consolidated is False:
                effective_priority -= 50  # Standalone penalty

            existing = best.get(sv.canonical_field)
            if existing is None or effective_priority > existing[0]:
                best[sv.canonical_field] = (effective_priority, sv.value, sv.source)

        sources_used = set()
        for canonical, (priority_score, value, source) in best.items():
            if hasattr(result, canonical):
                setattr(result, canonical, value)
                result.field_sources[canonical] = source
                result.field_priorities[canonical] = priority_score
                sources_used.add(source)

        result.sources_used = sorted(sources_used)

        # POST-MERGE ACCOUNTING CORRECTIONS
        self._apply_accounting_fixes(result)

        if quarter in QUARTERS_WITHOUT_BALANCE_SHEET:
            self._enforce_reg33_nulls(result, quarter)
        return result

    def _apply_accounting_fixes(self, result: NormalisedFinancials) -> None:
        """Post-merge accounting intelligence layer."""
        # FIX #1: Excise Duty Subtraction
        if result.excise_duty is not None and result.excise_duty > 0 and result.revenue_from_operations is not None:
            result.revenue_from_operations = round(result.revenue_from_operations - result.excise_duty, 2)
            logger.info(f"[ACCT-V2] Excise duty {result.excise_duty} subtracted from revenue for {result.symbol}")

        # FIX #1b: Statutory Levy Subtraction (ONGC, Oil & Gas)
        if getattr(result, 'statutory_levies', None) is not None and result.statutory_levies > 0 and result.revenue_from_operations is not None:
            result.revenue_from_operations = round(result.revenue_from_operations - result.statutory_levies, 2)
            logger.info(f"[ACCT-V2] Statutory levies {result.statutory_levies} subtracted for {result.symbol}")

        total_tax = getattr(result, 'total_tax_expense', None) or result.tax_expense_current
        if result.profit_for_period is None and result.profit_before_tax is not None and total_tax is not None:
            result.profit_for_period = round(result.profit_before_tax - total_tax + (result.share_of_associates_jv or 0), 2)

        if result.net_profit_attributable_to_owners is None and result.profit_for_period is not None and result.nci_profit is not None:
            result.net_profit_attributable_to_owners = round(result.profit_for_period - result.nci_profit, 2)

        if result.net_profit_attributable_to_owners is not None:
            result.net_profit = result.net_profit_attributable_to_owners
        elif result.net_profit is None and result.profit_for_period is not None:
            result.net_profit = result.profit_for_period
            
        # FIX #6: Reconstruction Fallback for missing/corrupted PAT
        if result.net_profit is None and result.profit_for_period is not None:
            result.net_profit = result.profit_for_period

        # FIX #8: Permanent Revenue Reconstruction
        if result.revenue_from_operations is not None and result.profit_before_tax is not None:
            if result.revenue_from_operations < (result.profit_before_tax * 0.8) and getattr(result, 'total_expenses', None):
                expected_rev = round((result.total_expenses + result.profit_before_tax) - (result.other_income or 0), 2)
                if expected_rev > result.revenue_from_operations:
                    logger.info(f"[ACCT-V2] Reconstructing missing revenue: {result.revenue_from_operations} -> {expected_rev}")
                    result.revenue_from_operations = expected_rev

    def _enforce_reg33_nulls(self, result: NormalisedFinancials, quarter: str) -> None:
        for f in BALANCE_SHEET_FIELDS + CASH_FLOW_FIELDS:
            if getattr(result, f, None) is not None:
                setattr(result, f, None)
                result.field_sources.pop(f, None)
                result.field_priorities.pop(f, None)

    def from_raw_dict(self, symbol: str, period: str, period_type: str, source: str, raw_data: Dict[str, Any], quarter: Optional[str] = None) -> List[SourcedValue]:
        if source not in self.priority: return []
        src_priority = self.priority[source]
        results = []

        for raw_key, value in raw_data.items():
            canonical = self.normalise_alias(raw_key, source)
            if canonical is None or value is None: continue
            try: numeric_value = float(value)
            except (TypeError, ValueError): continue
            results.append(SourcedValue(canonical_field=canonical, value=numeric_value, source=source, priority=src_priority, period=period, raw_key=raw_key))

        return results

default_normaliser = SchemaNormaliser()
