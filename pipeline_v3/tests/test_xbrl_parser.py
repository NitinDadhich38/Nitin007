import unittest

from pipeline_v3.parsers.xbrl_parser import MCAXBRLInstanceParser


class TestXBRLParser(unittest.TestCase):
    def test_context_and_unit_conversion(self):
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance"
            xmlns:iso4217="http://www.xbrl.org/2003/iso4217"
            xmlns:mca="http://www.mca.gov.in/xbrit/2016-12-01/mca-ind-as">
  <xbrli:context id="C_FY2025_CONS">
    <xbrli:entity><xbrli:identifier scheme="test">X</xbrli:identifier></xbrli:entity>
    <xbrli:period>
      <xbrli:startDate>2024-04-01</xbrli:startDate>
      <xbrli:endDate>2025-03-31</xbrli:endDate>
    </xbrli:period>
    <xbrli:segment>Consolidated</xbrli:segment>
  </xbrli:context>

  <xbrli:context id="I_FY2025_CONS">
    <xbrli:entity><xbrli:identifier scheme="test">X</xbrli:identifier></xbrli:entity>
    <xbrli:period>
      <xbrli:instant>2025-03-31</xbrli:instant>
    </xbrli:period>
    <xbrli:segment>Consolidated</xbrli:segment>
  </xbrli:context>

  <xbrli:unit id="U_INR"><xbrli:measure>iso4217:INR</xbrli:measure></xbrli:unit>

  <mca:RevenueFromOperations contextRef="C_FY2025_CONS" unitRef="U_INR">10000000000</mca:RevenueFromOperations>
  <mca:ProfitLossForPeriod contextRef="C_FY2025_CONS" unitRef="U_INR">500000000</mca:ProfitLossForPeriod>
  <mca:Assets contextRef="I_FY2025_CONS" unitRef="U_INR">25000000000</mca:Assets>
</xbrli:xbrl>
"""
        p = MCAXBRLInstanceParser(target_unit="INR_CRORE", prefer_consolidated=True)
        parsed = p.parse_bytes(xml)
        stmts = parsed["statements"]

        self.assertIn("FY2025", stmts["pl"])
        self.assertAlmostEqual(stmts["pl"]["FY2025"]["revenue_from_operations"], 1000.0, places=2)
        self.assertAlmostEqual(stmts["pl"]["FY2025"]["net_profit"], 50.0, places=2)
        self.assertAlmostEqual(stmts["bs"]["FY2025"]["total_assets"], 2500.0, places=2)

    def test_quarterly_vs_annual_labeling(self):
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance"
            xmlns:iso4217="http://www.xbrl.org/2003/iso4217"
            xmlns:mca="http://www.mca.gov.in/xbrit/2016-12-01/mca-ind-as">
  <xbrli:context id="D_QTR_MAR2025">
    <xbrli:entity><xbrli:identifier scheme="test">X</xbrli:identifier></xbrli:entity>
    <xbrli:period>
      <xbrli:startDate>2025-01-01</xbrli:startDate>
      <xbrli:endDate>2025-03-31</xbrli:endDate>
    </xbrli:period>
  </xbrli:context>

  <xbrli:context id="D_FY2025">
    <xbrli:entity><xbrli:identifier scheme="test">X</xbrli:identifier></xbrli:entity>
    <xbrli:period>
      <xbrli:startDate>2024-04-01</xbrli:startDate>
      <xbrli:endDate>2025-03-31</xbrli:endDate>
    </xbrli:period>
  </xbrli:context>

  <xbrli:context id="I_2025_03_31">
    <xbrli:entity><xbrli:identifier scheme="test">X</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:instant>2025-03-31</xbrli:instant></xbrli:period>
  </xbrli:context>

  <xbrli:unit id="U_INR"><xbrli:measure>iso4217:INR</xbrli:measure></xbrli:unit>

  <mca:RevenueFromOperations contextRef="D_QTR_MAR2025" unitRef="U_INR">900000000</mca:RevenueFromOperations>
  <mca:RevenueFromOperations contextRef="D_FY2025" unitRef="U_INR">3600000000</mca:RevenueFromOperations>
  <mca:Assets contextRef="I_2025_03_31" unitRef="U_INR">1000000000</mca:Assets>
</xbrli:xbrl>
"""
        p = MCAXBRLInstanceParser(target_unit="INR_CRORE", prefer_consolidated=True)
        parsed = p.parse_bytes(xml)
        stmts = parsed["statements"]
        # Quarterly duration ending Mar 31 must be 'Mar 2025' (not FY2025)
        self.assertIn("Mar 2025", stmts["pl"])
        self.assertAlmostEqual(stmts["pl"]["Mar 2025"]["revenue_from_operations"], 90.0, places=2)
        # Annual duration ending Mar 31 must be FY label
        self.assertIn("FY2025", stmts["pl"])
        self.assertAlmostEqual(stmts["pl"]["FY2025"]["revenue_from_operations"], 360.0, places=2)
        # Instant context should be treated as FY2025 only because an annual duration exists
        self.assertIn("FY2025", stmts["bs"])
        self.assertAlmostEqual(stmts["bs"]["FY2025"]["total_assets"], 100.0, places=2)

    def test_discontinued_ops_reconciles_pbt_and_tax(self):
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance"
            xmlns:iso4217="http://www.xbrl.org/2003/iso4217"
            xmlns:mca="http://www.mca.gov.in/xbrit/2016-12-01/mca-ind-as">
  <xbrli:context id="D_FY2025">
    <xbrli:entity><xbrli:identifier scheme="test">X</xbrli:identifier></xbrli:entity>
    <xbrli:period>
      <xbrli:startDate>2024-04-01</xbrli:startDate>
      <xbrli:endDate>2025-03-31</xbrli:endDate>
    </xbrli:period>
  </xbrli:context>

  <xbrli:unit id="U_INR"><xbrli:measure>iso4217:INR</xbrli:measure></xbrli:unit>

  <mca:ProfitBeforeTax contextRef="D_FY2025" unitRef="U_INR">1000000000</mca:ProfitBeforeTax>
  <mca:TaxExpense contextRef="D_FY2025" unitRef="U_INR">200000000</mca:TaxExpense>
  <mca:ProfitLossForPeriod contextRef="D_FY2025" unitRef="U_INR">880000000</mca:ProfitLossForPeriod>
  <mca:ProfitLossFromDiscontinuedOperationsBeforeTax contextRef="D_FY2025" unitRef="U_INR">100000000</mca:ProfitLossFromDiscontinuedOperationsBeforeTax>
  <mca:TaxExpenseOfDiscontinuedOperations contextRef="D_FY2025" unitRef="U_INR">20000000</mca:TaxExpenseOfDiscontinuedOperations>
</xbrli:xbrl>
"""
        p = MCAXBRLInstanceParser(target_unit="INR_CRORE", prefer_consolidated=True)
        parsed = p.parse_bytes(xml)
        pl = parsed["statements"]["pl"]["FY2025"]
        self.assertAlmostEqual(pl["profit_before_tax"], 110.0, places=2)
        self.assertAlmostEqual(pl["tax"], 22.0, places=2)
        self.assertAlmostEqual(pl["net_profit"], 88.0, places=2)
        self.assertAlmostEqual(round(pl["profit_before_tax"] - pl["tax"], 2), pl["net_profit"], places=2)

    def test_annual_financial_results_prefers_fourd_full_year_context(self):
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance"
            xmlns:iso4217="http://www.xbrl.org/2003/iso4217"
            xmlns:mca="http://www.mca.gov.in/xbrit/2016-12-01/mca-ind-as">
  <xbrli:context id="OneD">
    <xbrli:entity><xbrli:identifier scheme="test">X</xbrli:identifier></xbrli:entity>
    <xbrli:period>
      <xbrli:startDate>2025-01-01</xbrli:startDate>
      <xbrli:endDate>2025-03-31</xbrli:endDate>
    </xbrli:period>
  </xbrli:context>
  <xbrli:context id="FourD">
    <xbrli:entity><xbrli:identifier scheme="test">X</xbrli:identifier></xbrli:entity>
    <xbrli:period>
      <xbrli:startDate>2025-01-01</xbrli:startDate>
      <xbrli:endDate>2025-03-31</xbrli:endDate>
    </xbrli:period>
  </xbrli:context>
  <xbrli:unit id="U_INR"><xbrli:measure>iso4217:INR</xbrli:measure></xbrli:unit>
  <mca:RevenueFromOperations contextRef="OneD" unitRef="U_INR">900000000</mca:RevenueFromOperations>
  <mca:RevenueFromOperations contextRef="FourD" unitRef="U_INR">3600000000</mca:RevenueFromOperations>
</xbrli:xbrl>
"""
        p = MCAXBRLInstanceParser(target_unit="INR_CRORE", prefer_consolidated=True)
        parsed = p.parse_bytes(xml, filing_period_type="annual")
        self.assertAlmostEqual(parsed["statements"]["pl"]["Mar 2025"]["revenue_from_operations"], 360.0, places=2)

    def test_associate_and_nci_math_keeps_total_and_owner_pat(self):
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance"
            xmlns:iso4217="http://www.xbrl.org/2003/iso4217"
            xmlns:mca="http://www.mca.gov.in/xbrit/2016-12-01/mca-ind-as">
  <xbrli:context id="D_FY2025">
    <xbrli:entity><xbrli:identifier scheme="test">X</xbrli:identifier></xbrli:entity>
    <xbrli:period>
      <xbrli:startDate>2024-04-01</xbrli:startDate>
      <xbrli:endDate>2025-03-31</xbrli:endDate>
    </xbrli:period>
  </xbrli:context>
  <xbrli:unit id="U_INR"><xbrli:measure>iso4217:INR</xbrli:measure></xbrli:unit>
  <mca:ProfitBeforeTax contextRef="D_FY2025" unitRef="U_INR">1000000000</mca:ProfitBeforeTax>
  <mca:TaxExpense contextRef="D_FY2025" unitRef="U_INR">200000000</mca:TaxExpense>
  <mca:ShareOfProfitLossOfAssociatesAndJointVenturesAccountedForUsingEquityMethod contextRef="D_FY2025" unitRef="U_INR">50000000</mca:ShareOfProfitLossOfAssociatesAndJointVenturesAccountedForUsingEquityMethod>
  <mca:ProfitOrLossAttributableToNonControllingInterests contextRef="D_FY2025" unitRef="U_INR">30000000</mca:ProfitOrLossAttributableToNonControllingInterests>
</xbrli:xbrl>
"""
        p = MCAXBRLInstanceParser(target_unit="INR_CRORE", prefer_consolidated=True)
        parsed = p.parse_bytes(xml)
        pl = parsed["statements"]["pl"]["FY2025"]
        self.assertAlmostEqual(pl["share_of_associates_jv"], 5.0, places=2)
        self.assertAlmostEqual(pl["nci_profit"], 3.0, places=2)
        self.assertAlmostEqual(pl["profit_for_period"], 85.0, places=2)
        self.assertAlmostEqual(pl["net_profit_attributable_to_owners"], 82.0, places=2)
        self.assertAlmostEqual(pl["net_profit"], 82.0, places=2)


if __name__ == "__main__":
    unittest.main()
