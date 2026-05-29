"""
XBRL tag mapping for both MCA (AOC-4) and SEBI (in-capmkt) taxonomies.

Keep this as data-only so it can be extended without touching parser logic.
We match using QName localname because namespaces vary by taxonomy version.

FIX #5: Added SEBI in-capmkt taxonomy localnames used by older filings (FY2023 and earlier)
        that use different tag naming conventions than MCA Ind-AS.
"""

# Canonical schema fields -> candidate XBRL element localnames (Ind-AS / IGAAP / SEBI variants).
MCA_LOCALNAME_MAP = {
    "pl": {
        "revenue_from_operations": [
            # --- Ind-AS Primary ---
            "RevenueFromOperations",
            "RevenueFromOperationsNet",
            "RevenueFromContractWithCustomer",
            "Revenue",
            # --- Banking / NBFC (NSE Banking taxonomy variants) ---
            "InterestEarned",
            "SegmentRevenueFromOperations",
            # --- L&T / Construction / Project companies ---
            "ContractRevenue",
            "RevenueFromContractsWithCustomers",
            "RevenueFromLongTermContracts",
            # --- IGAAP (Coal India, ONGC, PSU legacy filers) ---
            "SaleOfGoods",
            "SaleOfProducts",
            "SaleOfServices",
            "GrossSales",
            "TurnoverNetOfExcise",
            "RevenueFromOperationsGross",
            # --- SEBI in-capmkt variants ---
            "IncomeFromOperations",
            "RevenueFromOperationsOther",
            "TurnoverGrossIncome",
            "NetSalesTurnover",
            # --- Life insurance taxonomy ---
            "GrossPremiumIncome",
            "NetPremiumIncome",
        ],
        "other_income": [
            "OtherIncome",
            "OtherIncomeNet",
            # --- Banking / NBFC ---
            "OtherInterest",
        ],
        "total_income": [
            "TotalIncome",
            "TotalIncomeFromOperations",
            "Income",
            "TotalRevenueFromOperations",
            "ShareholdersAccountIncome",
            "IncomeUnderShareholdersAccount",
        ],
        "operating_expenses": [
            "TotalExpenses",
            "Expenses",
            "TotalExpenditure",
            "ExpensesOfManagement",
            "OperatingExpensesRelatedToInsuranceBusiness",
            "ShareholdersAccountExpenses",
        ],
        "interest": [
            "FinanceCosts",
            "FinanceCost",
            "InterestExpense",
            "Interest",
            "FinanceCostsForPeriod",
            # --- Banking / NBFC ---
            "InterestExpended",
        ],
        "depreciation": [
            "DepreciationAndAmortisationExpense",
            "DepreciationDepletionAndAmortisationExpense",
            "Depreciation",
            "Amortisation",
            "DepreciationAndAmortisation",
        ],
        "profit_before_tax": [
            "ProfitBeforeTax",
            "ProfitLossBeforeTax",
            "ProfitBeforeTaxFromContinuingOperations",
            "NetProfitLossBeforeTaxAndExceptionalItemsAfterShareOfProfitLossOfAssociates",
            # --- Banking / NBFC ---
            "ProfitLossFromOrdinaryActivitiesBeforeTax",
            "SegmentProfitBeforeTax",
            "SegmentProfitLossBeforeTaxAndFinanceCosts",
        ],
        # Discontinued ops (used to reconcile total PBT/Tax when PAT is total)
        "profit_before_tax_discontinued_ops": [
            "ProfitLossFromDiscontinuedOperationsBeforeTax",
            "ProfitLossBeforeTaxDiscontinuedOperations",
        ],
        "tax": [
            "TaxExpense",
            "CurrentTaxExpense",
            "IncomeTaxExpense",
            "TaxExpenseContinuingOperations",
            "TotalTaxExpenses",
            "IncomeTaxExpenseContinuingOperations",
            "CurrentAndDeferredTaxExpenseIncome",
            "ProvisionForTax",
            "ProvisionsForTaxes",
            "CurrentTax",
            "CurrentTaxes",
        ],
        "tax_discontinued_ops": [
            "TaxExpenseOfDiscontinuedOperations",
        ],
        "share_of_associates_jv": [
            "ShareOfProfitLossOfAssociatesAndJointVenturesAccountedForUsingEquityMethod",
            "ShareOfProfitLossOfAssociates",
            "ShareOfProfitLossOfJointVentures",
            "ShareOfProfitLossOfAssociatesAndJointVentures",
        ],
        "profit_for_period": [
            "ProfitLossForPeriod",
            "ProfitLoss",
            "ProfitLossForTheYear",
            "ProfitLossForThePeriod",
            "NetProfitLossAfterTaxes",
            "NetProfitLossForThePeriod",
            "ProfitLossFromContinuingOperations",
            "ProfitLossFromOrdinaryActivitiesAfterTax",
            "ProfitLossAfterTaxBeforeExtraordinaryItems",
            "ProfitLossAfterTaxAndExtraordinaryItems",
        ],
        "net_profit": [
            # Owner-attributable PAT variants. Keep `profit_for_period` separate for
            # total consolidated PAT so JV/NCI math can be validated explicitly.
            "ProfitLossAttributableToOwnersOfParent",
            "NetProfitLossAttributableToOwnerOfParent",
            "ProfitLossAttributableToOwnersOfParentEntity",
            "ShareOfProfitLossOfParent",
            "ProfitAttributableToOwnersOfParent",
            # --- Banking / NBFC ---
            "ProfitLossAfterTaxesMinorityInterestAndShareOfProfitLossOfAssociates",
            "ProfitLossAfterTaxAndExtraordinaryItems",
            "ProfitLossAfterTaxBeforeExtraordinaryItems",
        ],
        "net_profit_attributable_to_owners": [
            "ProfitLossAttributableToOwnersOfParent",
            "NetProfitLossAttributableToOwnerOfParent",
            "ProfitLossAttributableToOwnersOfParentEntity",
            "ShareOfProfitLossOfParent",
            "ProfitAttributableToOwnersOfParent",
        ],
        # NSE XBRL variants (continuing/discontinued split) used to build owner PAT when present.
        "net_profit_attrib_owners_continuing_ops": [
            "ProfitLossFromContinuingOperationsAttributableToOrdinaryEquityHoldersOfParentEntityIncludingDilutiveEffects",
        ],
        "net_profit_attrib_owners_discontinued_ops": [
            "ProfitLossFromDiscontinuedOperationsAttributableToOrdinaryEquityHoldersOfParentEntityIncludingDilutiveEffects",
        ],
        "nci_profit": [
            "ProfitLossAttributableToNoncontrollingInterests",
            "ProfitLossAttributableToNoncontrollingInterest",
            "NetProfitLossAttributableToNoncontrollingInterest",
            "ProfitOrLossAttributableToNonControllingInterests",
            "ProfitLossOfMinorityInterest",
            "MinorityInterestInProfitLoss",
        ],
        "eps": [
            "BasicEarningsLossPerShareFromContinuingOperations",
            "BasicEarningsLossPerShare",
            "EarningsPerShareBasic",
            "BasicEarningsPerShare",
            "BasicEPSAfterExtraordinaryItems",
            "BasicEPSBeforeExtraordinaryItems",
            "BasicAndDilutedEPSBeforeExtraordinaryItemsNetOfTaxExpenseForThePeriodNotToBeAnnualized",
            "BasicAndDilutedEPSAfterExtraordinaryItemsNetOfTaxExpenseForThePeriodNotToBeAnnualized",
        ],
        "diluted_eps": [
            "DilutedEarningsLossPerShareFromContinuingOperations",
            "DilutedEarningsLossPerShare",
            "EarningsPerShareDiluted",
            "DilutedEarningsPerShare",
            "DilutedEPSAfterExtraordinaryItems",
            "DilutedEPSBeforeExtraordinaryItems",
            "BasicAndDilutedEPSBeforeExtraordinaryItemsNetOfTaxExpenseForThePeriodNotToBeAnnualized",
            "BasicAndDilutedEPSAfterExtraordinaryItemsNetOfTaxExpenseForThePeriodNotToBeAnnualized",
        ],
        "exceptional_items": [
            "ExceptionalItems",
            "ExtraordinaryItems",
        ],
        "excise_duty": [
            "ExciseDuty",
            "DutiesAndTaxes",
            "ExciseDutyOnSalesOfGoods",
            "ExciseDutyExpense",
            "ExciseDutyOnDomesticSales",
            "StatutoryDuties",
            "GovernmentLevies",
            "TaxesOnRevenue",
        ],
        "statutory_levies": [
            "StatutoryLevies",
            "RoyaltyCessAndOtherProductionCharges",
            "RoyaltyAndCessExpense",
            "OilAndNaturalGasCessExpense",
            "ProductionSharingPayments",
            "RoyaltyExpense",
        ],
    },
    "bs": {
        "equity_share_capital": [
            "EquityShareCapital",
            "ShareCapital",
            "PaidUpEquityShareCapital",
            "PaidUpValueOfEquityShareCapital",
        ],
        "reserves": [
            "OtherEquity",
            "ReservesAndSurplus",
            "Reserves",
            "OtherEquityAttributableToOwnersOfParent",
        ],
        "total_equity": [
            "Equity",
            "TotalEquity",
            "TotalEquityAttributableToOwnersOfParent",
            "TotalShareholdersEquity",
            "NetWorth",
            "ShareholdersFunds",
        ],
        "non_controlling_interest": [
            "NonControllingInterest",
            "NonControllingInterests",
            "MinorityInterest",
        ],
        "long_term_borrowings": [
            "BorrowingsNonCurrent",
            "BorrowingsNoncurrent",
            "NonCurrentBorrowings",
            "NoncurrentBorrowings",
            "LongTermBorrowings",
            "NoncurrentBorrowings",
            "LongTermDebt",
            "NonCurrentLeaseLiabilities",
        ],
        "short_term_borrowings": [
            "BorrowingsCurrent",
            "CurrentBorrowings",
            "ShortTermBorrowings",
            "ShortTermDebt",
        ],
        "current_maturities_of_long_term_debt": [
            "CurrentMaturitiesOfLongTermBorrowings",
            "CurrentMaturitiesOfLongTermDebt",
            "CurrentMaturitiesOfFinanceLeaseObligations",
            "CurrentMaturitiesOfLeaseLiabilities",
        ],
        "lease_liabilities_current": [
            "LeaseLiabilitiesCurrent",
            "CurrentLeaseLiabilities",
        ],
        "lease_liabilities_non_current": [
            "LeaseLiabilitiesNonCurrent",
            "NonCurrentLeaseLiabilities",
        ],
        "debt_securities": [
            "DebtSecurities",
            "DebtSecuritiesIssued",
            "DebtSecuritiesBorrowings",
        ],
        "subordinated_liabilities": [
            "SubordinatedLiabilities",
            "SubordinatedDebt",
        ],
        "deposits": [
            "Deposits",
            "DepositsFromCustomers",
            "CustomerDeposits",
        ],
        "deposits_for_banks": [
            "Deposits",
            "DepositsFromCustomers",
            "CustomerDeposits",
        ],
        "borrowings": [
            "Borrowings",
            "TotalBorrowings",
            "TotalDebt",
        ],
        "total_debt": [
            "Borrowings",
            "TotalBorrowings",
            "TotalDebt",
        ],
        "total_liabilities": [
            "Liabilities",
            "TotalLiabilities",
            "TotalEquityAndLiabilities",
            "PolicyLiabilities",
            "LinkedLiabilities",
            "ProvisionForLinkedLiabilities",
            "CurrentLiabilitiesAndProvisions",
        ],
        "total_assets": [
            "Assets",
            "TotalAssets",
        ],
        "cash_and_equivalents": [
            "CashAndCashEquivalents",
            "CashAndCashEquivalentsAtCarryingValue",
            "CashAndBankBalances",
            "CashAndCashEquivalentsAtEndOfPeriod",
        ],
        "investments": [
            "Investments",
            "FinancialAssets",
        ],
        "current_investments": [
            "CurrentInvestments",
            "InvestmentsCurrent",
        ],
        "non_current_investments": [
            "InvestmentsNoncurrent",
            "NoncurrentInvestments",
            "NonCurrentInvestments",
        ],
        "investments_in_associates_jv": [
            "InvestmentsInAssociates",
            "InvestmentsInJointVentures",
            "InvestmentsAccountedForUsingEquityMethod",
            "InvestmentsInAssociatesAndJointVentures",
        ],
        "other_financial_asset_investments": [
            "OtherFinancialAssetsInvestments",
            "OtherInvestments",
            "InvestmentsShareholdersFund",
            "InvestmentsPolicyholdersFundExcludingLinkedAssets",
            "InvestmentsShareholders",
        ],
        "receivables": [
            "TradeReceivables",
            "Receivables",
            "TradeReceivablesCurrent",
        ],
        "inventory": [
            "Inventories",
            "Inventory",
        ],
        "ppe": [
            "PropertyPlantAndEquipment",
            "PropertyPlantAndEquipmentNet",
            "TangibleAssets",
            "FixedAssets",
        ],
        "capital_work_in_progress": [
            "CapitalWorkInProgress",
            "CapitalWorkInProgressPropertyPlantAndEquipment",
            "CapitalWorkInProgressAndIntangibleAssetsUnderDevelopment",
        ],
        "right_of_use_assets": [
            "RightOfUseAssets",
            "RightOfUseAsset",
            "RightOfUseAssetsNet",
        ],
        "intangible_assets": [
            "IntangibleAssets",
            "IntangibleAssetsOtherThanGoodwill",
            "GoodwillOnConsolidation",
            "Goodwill",
        ],
        "intangible_assets_under_development": [
            "IntangibleAssetsUnderDevelopment",
            "IntangibleAssetsUnderDevelopmentNet",
        ],
        "current_assets": [
            "CurrentAssets",
            "TotalCurrentAssets",
            "AdvancesAndOtherAssets",
        ],
        "current_liabilities": [
            "CurrentLiabilities",
            "TotalCurrentLiabilities",
        ],
        "deferred_tax_assets": [
            "DeferredTaxAssets",
            "DeferredTaxAssetsNet",
        ],
        "deferred_tax_liabilities": [
            "DeferredTaxLiabilities",
            "DeferredTaxLiabilitiesNet",
        ],
    },
    "cf": {
        "cash_from_operations": [
            "NetCashFlowsFromUsedInOperatingActivities",
            "NetCashFlowFromUsedInOperatingActivities",
            "NetCashFromOperatingActivities",
            "CashFlowsFromUsedInOperatingActivities",
            "NetCashGeneratedFromOperations",
        ],
        "capital_expenditure": [
            "PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities",
            "PaymentsToAcquirePropertyPlantAndEquipment",
            "PurchaseOfFixedAssets",
            "CapitalExpenditure",
            "PaymentsForPurchaseOfPropertyPlantAndEquipment",
        ],
        "cash_from_investing": [
            "NetCashFlowsFromUsedInInvestingActivities",
            "NetCashFlowFromUsedInInvestingActivities",
            "NetCashFromInvestingActivities",
            "CashFlowsFromUsedInInvestingActivities",
        ],
        "cash_from_financing": [
            "NetCashFlowsFromUsedInFinancingActivities",
            "NetCashFlowFromUsedInFinancingActivities",
            "NetCashFromFinancingActivities",
            "CashFlowsFromUsedInFinancingActivities",
        ],
        "dividends_paid": [
            "DividendsPaidClassifiedAsFinancingActivities",
            "DividendsPaid",
            "PaymentsOfDividends",
        ],
        "net_cash_flow": [
            "NetIncreaseDecreaseInCashAndCashEquivalents",
            "NetCashFlow",
            "IncreaseDecreaseInCashAndCashEquivalents",
        ],
    },
}
