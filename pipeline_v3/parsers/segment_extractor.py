import logging
import re
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class SegmentExtractor:
    """
    Extracts business segment results from financial tables.
    Focuses on Revenue and EBIT/Profit per segment.
    """
    
    def __init__(self):
        # Keywords that indicate a segment row
        self.segment_triggers = ["segment revenue", "segment result", "segment assets", "segment liabilities"]
        # Keywords that indicate totals (to skip or use for validation)
        self.total_triggers = ["total segment", "consolidated total", "inter-segment"]
        # Common skip words (headers, notes)
        self.skip_words = ["(a)", "(b)", "(c)", "refer note", "particulars", "items"]

    def is_segment_table(self, table: List[List[Any]]) -> bool:
        """Checks if a table is likely a segment reporting table."""
        if not table or len(table) < 5:
            return False
            
        header_text = " ".join([str(c) for c in table[0] if c]).lower()
        if "segment" in header_text or "reporting" in header_text:
            return True
            
        # Check first column for common segment names if header is unclear
        sample_rows = " ".join([str(row[0]) for row in table[:10] if row and row[0]]).lower()
        if any(trigger in sample_rows for trigger in self.segment_triggers):
            return True
            
        return False

    def extract_segments(self, table: List[List[Any]], current_fy: str) -> Dict[str, float]:
        """
        Extracts segment revenue from a table.
        Returns a dict of {SegmentName: Revenue}.
        """
        segments = {}
        found_revenue_section = False
        
        # We need to find the right column for current FY
        # Usually, first column is Name, second or third is Current FY
        # Let's look for a row with "Segment Revenue" and start from there
        
        for row in table:
            if not row or not row[0]:
                continue
                
            label = str(row[0]).strip().lower()
            
            # Identify the start of the Revenue section
            if "segment revenue" in label:
                found_revenue_section = True
                continue
            
            # If we hit "Segment Result" or "Total", the Revenue section is over
            if "segment result" in label or "total segment" in label:
                found_revenue_section = False
            
            if found_revenue_section:
                # Skip sub-labels or noise
                if any(sw in label for sw in self.skip_words) or len(label) < 3:
                    continue
                
                # Extract the first valid number in the row
                nums = self._extract_numbers(row[1:])
                if nums:
                    clean_name = self._beautify_name(str(row[0]))
                    segments[clean_name] = nums[0] # Assuming first number is Current FY
                    
        return segments

    def _extract_numbers(self, cells: List[Any]) -> List[float]:
        nums = []
        for cell in cells:
            if not cell: continue
            s = str(cell).replace(",", "").replace("(", "-").replace(")", "").strip()
            # Clean non-numeric except . and -
            s = re.sub(r"[^0-9.\-]", "", s)
            try:
                if s and s != "-":
                    nums.append(float(s))
            except:
                pass
        return nums

    def _beautify_name(self, name: str) -> str:
        # Remove leading bullets, numbers, and extra spaces
        n = re.sub(r"^[a-z0-9\.\)\s-]*", "", name, flags=re.I).strip()
        # Capitalize words correctly
        return " ".join([w.capitalize() for w in n.split()])

if __name__ == "__main__":
    # Test sample
    test_table = [
        ["Segment Reporting", "Mar 2024", "Mar 2023"],
        ["1. Segment Revenue", "", ""],
        ["   (a) Oil & Chemicals", "5,00,000", "4,50,000"],
        ["   (b) Retail", "1,20,000", "1,00,000"],
        ["   (c) Digital Services", "80,000", "70,000"],
        ["Total Segment Revenue", "7,00,000", "6,20,000"]
    ]
    extractor = SegmentExtractor()
    if extractor.is_segment_table(test_table):
        res = extractor.extract_segments(test_table, "FY2024")
        print("Extracted Segments:", res)
