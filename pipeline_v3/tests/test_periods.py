import unittest
from datetime import date

from pipeline_v3.utils.periods import generate_active_periods


class TestActivePeriods(unittest.TestCase):
    def test_may_2026_includes_due_fy2026_quarters_and_fy2025(self):
        periods = generate_active_periods(lookback_years=3, today=date(2026, 5, 28))
        labels = [p["period"] for p in periods]

        self.assertIn("Q4FY26", labels)
        self.assertIn("Q3FY26", labels)
        self.assertIn("Q2FY26", labels)
        self.assertIn("Q1FY26", labels)
        self.assertIn("FY2025", labels)

    def test_skips_quarter_until_due_date_passes(self):
        periods = generate_active_periods(lookback_years=1, today=date(2026, 5, 1))
        labels = [p["period"] for p in periods]

        self.assertNotIn("Q4FY26", labels)
        self.assertIn("Q3FY26", labels)


if __name__ == "__main__":
    unittest.main()
