from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict

@dataclass
class ProfitLoss:
    gross_revenue_from_operations: Optional[float] = None
    revenue_from_operations: Optional[float] = None
    sales_screener_basis: Optional[float] = None
    sales_adjustment: Optional[float] = None
    other_income: Optional[float] = None
    total_income: Optional[float] = None
    operating_expenses: Optional[float] = None
    ebitda: Optional[float] = None
    ebitda_like_profit_before_interest_depreciation_tax: Optional[float] = None
    operating_profit: Optional[float] = None
    operating_profit_screener_basis: Optional[float] = None
    ebit: Optional[float] = None
    interest: Optional[float] = None
    depreciation: Optional[float] = None
    profit_before_tax: Optional[float] = None
    tax: Optional[float] = None
    share_of_associates_jv: Optional[float] = None
    profit_for_period: Optional[float] = None
    screener_net_profit: Optional[float] = None
    net_profit: Optional[float] = None
    net_profit_attributable_to_owners: Optional[float] = None
    pat_attributable_to_owners: Optional[float] = None
    minority_interest_profit: Optional[float] = None
    eps: Optional[float] = None
    diluted_eps: Optional[float] = None
    exceptional_items: Optional[float] = None
    excise_duty: Optional[float] = None
    nci_profit: Optional[float] = None
    statutory_levies: Optional[float] = None

@dataclass
class BalanceSheet:
    equity_share_capital: Optional[float] = None
    reserves: Optional[float] = None
    total_equity: Optional[float] = None
    total_debt: Optional[float] = None
    long_term_borrowings: Optional[float] = None
    short_term_borrowings: Optional[float] = None
    current_maturities_of_long_term_debt: Optional[float] = None
    lease_liabilities_current: Optional[float] = None
    lease_liabilities_non_current: Optional[float] = None
    debt_securities: Optional[float] = None
    subordinated_liabilities: Optional[float] = None
    deposits: Optional[float] = None
    deposits_for_banks: Optional[float] = None
    borrowings: Optional[float] = None
    screener_borrowings: Optional[float] = None
    total_liabilities: Optional[float] = None
    total_assets: Optional[float] = None
    cash_and_equivalents: Optional[float] = None
    investments: Optional[float] = None
    current_investments: Optional[float] = None
    non_current_investments: Optional[float] = None
    investments_in_associates_jv: Optional[float] = None
    other_financial_asset_investments: Optional[float] = None
    screener_investments: Optional[float] = None
    receivables: Optional[float] = None
    inventory: Optional[float] = None
    ppe: Optional[float] = None
    capital_work_in_progress: Optional[float] = None
    right_of_use_assets: Optional[float] = None
    intangible_assets: Optional[float] = None
    intangible_assets_under_development: Optional[float] = None
    screener_fixed_assets: Optional[float] = None
    current_assets: Optional[float] = None
    current_liabilities: Optional[float] = None
    non_controlling_interest: Optional[float] = None
    deferred_tax_assets: Optional[float] = None
    deferred_tax_liabilities: Optional[float] = None
    working_capital: Optional[float] = None
    book_value: Optional[float] = None
    shares_outstanding: Optional[float] = None

@dataclass
class CashFlow:
    cash_from_operations: Optional[float] = None
    capital_expenditure: Optional[float] = None
    cash_from_investing: Optional[float] = None
    cash_from_financing: Optional[float] = None
    dividends_paid: Optional[float] = None
    reported_net_cash_flow: Optional[float] = None
    computed_net_cash_flow: Optional[float] = None
    net_cash_flow: Optional[float] = None
    free_cash_flow: Optional[float] = None

@dataclass
class FinancialPeriod:
    annual: Dict[str, Any] = field(default_factory=dict)
    quarterly: Dict[str, Any] = field(default_factory=dict)

@dataclass
class CompanyFinancials:
    company_info: Dict[str, Any] = field(default_factory=dict)
    # Structure: { "annual": { "2025": ProfitLoss }, "quarterly": { "Q1-2025": ProfitLoss } }
    profit_loss: Dict[str, Dict[str, ProfitLoss]] = field(default_factory=lambda: {"annual": {}, "quarterly": {}})
    balance_sheet: Dict[str, Dict[str, BalanceSheet]] = field(default_factory=lambda: {"annual": {}, "quarterly": {}})
    cash_flow: Dict[str, Dict[str, CashFlow]] = field(default_factory=lambda: {"annual": {}, "quarterly": {}})
    standalone_profit_loss: Dict[str, Dict[str, ProfitLoss]] = field(default_factory=lambda: {"annual": {}, "quarterly": {}})
    standalone_balance_sheet: Dict[str, Dict[str, BalanceSheet]] = field(default_factory=lambda: {"annual": {}, "quarterly": {}})
    standalone_cash_flow: Dict[str, Dict[str, CashFlow]] = field(default_factory=lambda: {"annual": {}, "quarterly": {}})
    ratios: Dict[str, Dict[str, Dict[str, float]]] = field(default_factory=lambda: {"annual": {}, "quarterly": {}})
    growth: Dict[str, Dict[str, Dict[str, float]]] = field(default_factory=lambda: {"annual": {}, "quarterly": {}})
    metadata: Dict[str, Any] = field(default_factory=dict)
    insights: List[str] = field(default_factory=list)

def to_dict(obj):
    return asdict(obj)
