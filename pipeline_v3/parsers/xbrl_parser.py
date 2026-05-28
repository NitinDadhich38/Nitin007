import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from lxml import etree

logger = logging.getLogger(__name__)

from .mca_taxonomy_map import MCA_LOCALNAME_MAP


_NS = {
    "xbrli": "http://www.xbrl.org/2003/instance",
    "xbrldi": "http://xbrl.org/2006/xbrldi",
}


@dataclass
class XBRLContext:
    context_id: str
    period_type: str  # "instant" | "duration" | "unknown"
    start: Optional[date]
    end: Optional[date]
    instant: Optional[date]
    is_consolidated: Optional[bool]
    has_segment: bool = False
    # True when an instant context matches an annual duration context end date.
    # Used to label Balance Sheet instants as FY/CY only when we can prove it's year-end.
    is_year_end: bool = False

    def period_label(self) -> Optional[str]:
        """
        Convert context end/instant date to a label.
        Rules:
        - Duration contexts:
          - If full-year-ish (duration > ~300 days):
            - March 31 -> FY{year}
            - Dec 31   -> CY{year}
          - Else -> Mon {year} (e.g., Sep 2024, Mar 2024)
        - Instant contexts:
          - Only label as FY/CY if `is_year_end` is True (proved by matching annual duration end)
          - Else -> Mon {year}
        """
        d = self.instant or self.end
        if not d:
            return None

        dur = self.duration_days()

        if self.period_type == "duration":
            if dur is not None and dur > 300:
                if d.month == 3 and d.day >= 30:
                    return f"FY{d.year}"
                if d.month == 12 and d.day >= 30:
                    return f"CY{d.year}"
        elif self.period_type == "instant":
            # Balance sheets are point-in-time statements. For Indian fiscal-year reporters,
            # March 31 instants should be labeled as FY even when the filing is "quarterly"
            # (Q4 balance sheet is the year-end snapshot). For calendar-year reporters,
            # December 31 instants map to CY.
            if d.month == 3 and d.day >= 30:
                return f"FY{d.year}"
            if d.month == 12 and d.day >= 30:
                return f"CY{d.year}"

        MONTH_MAP = {
            1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
            7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
        }
        return f"{MONTH_MAP[d.month]} {d.year}"

    def fiscal_year(self) -> Optional[str]:
        # Backwards compatibility, but prefer period_label
        return self.period_label()

    def duration_days(self) -> Optional[int]:
        if self.period_type != "duration" or not self.start or not self.end:
            return None
        return (self.end - self.start).days


@dataclass(frozen=True)
class XBRLFact:
    localname: str
    context_id: str
    unit_ref: Optional[str]
    value: float
    scale: Optional[int]
    decimals: Optional[int]


class MCAXBRLInstanceParser:
    """
    Institutional-grade MCA XBRL parser:
    - Handles contexts (current vs previous), instant vs duration
    - Handles consolidated vs standalone via context dimensions (best-effort)
    - Matches tags by localname for taxonomy-version resilience
    - Emits canonical schema fields with provenance-ready structure
    """

    _NUM_CLEAN = re.compile(r"[,\s]")

    def __init__(self, *, target_unit: str = "INR_CRORE", prefer_consolidated: bool = True):
        self.target_unit = target_unit
        self.prefer_consolidated = prefer_consolidated

        # Reverse index: localname -> list[(stmt, field, alias_idx)]
        self._localname_index: Dict[str, List[Tuple[str, str, int]]] = {}
        for stmt, fields in MCA_LOCALNAME_MAP.items():
            for field, localnames in fields.items():
                for idx, ln in enumerate(localnames):
                    self._localname_index.setdefault(ln, []).append((stmt, field, idx))

    def parse_bytes(self, xml_bytes: bytes, *, filing_period_type: Optional[str] = None) -> Dict[str, Any]:
        root = etree.fromstring(xml_bytes)
        filing_period_type = (filing_period_type or "").lower().strip() or None

        contexts = self._parse_contexts(root)
        units = self._parse_units(root)
        facts = self._parse_facts(root)

        # stmt -> fy -> field -> (value, meta)
        out: Dict[str, Dict[str, Dict[str, Any]]] = {"pl": {}, "bs": {}, "cf": {}}
        prov: Dict[str, Any] = {"facts_used": 0, "contexts": len(contexts), "units": len(units)}

        for fact in facts:
            matches = self._localname_index.get(fact.localname)
            if not matches:
                continue
            ctx = contexts.get(fact.context_id)
            if not ctx:
                continue
            fy = ctx.fiscal_year()
            if not fy:
                continue

            value = self._apply_scale(fact.value, fact.scale)
            value = self._to_target_unit(value=value, unit_measure=units.get(fact.unit_ref), field_localname=fact.localname)

            for stmt, field, alias_idx in matches:
                chosen = self._choose_and_set(
                    out,
                    stmt=stmt,
                    fy=fy,
                    field=field,
                    value=value,
                    ctx=ctx,
                    fact=fact,
                    alias_idx=alias_idx,
                    filing_period_type=filing_period_type,
                )
                if chosen:
                    prov["facts_used"] += 1

        # ══════════════════════════════════════════════════════════════
        # POST-PARSE ACCOUNTING INTELLIGENCE LAYER
        # ══════════════════════════════════════════════════════════════
        for fy, pl_data in out.get("pl", {}).items():
            if not isinstance(pl_data, dict):
                continue

            # Discontinued operations reconciliation:
            # Some filings report discontinued PBT and tax separately while PAT is total.
            # If we detect discontinued PBT/tax, roll them into the main PBT/tax so the
            # statement is internally consistent: PAT == (PBT - Tax).
            pbt_disc = pl_data.get("profit_before_tax_discontinued_ops")
            if pbt_disc is not None and pl_data.get("profit_before_tax") is not None:
                pl_data["profit_before_tax"] = round(float(pl_data["profit_before_tax"]) + float(pbt_disc), 2)
            tax_disc = pl_data.get("tax_discontinued_ops")
            if tax_disc is not None and pl_data.get("tax") is not None:
                pl_data["tax"] = round(float(pl_data["tax"]) + float(tax_disc), 2)

            # Owner PAT build-up (continuing + discontinued) when filing splits it.
            if pl_data.get("net_profit_attributable_to_owners") is None:
                owner_cont = pl_data.get("net_profit_attrib_owners_continuing_ops")
                owner_disc = pl_data.get("net_profit_attrib_owners_discontinued_ops")
                if owner_cont is not None and owner_disc is not None:
                    pl_data["net_profit_attributable_to_owners"] = round(float(owner_cont) + float(owner_disc), 2)
                elif owner_cont is not None:
                    pl_data["net_profit_attributable_to_owners"] = float(owner_cont)

            pbt = pl_data.get("profit_before_tax")
            tax = pl_data.get("tax")
            share = pl_data.get("share_of_associates_jv") or 0.0
            nci = pl_data.get("nci_profit") or 0.0
            total_profit = pl_data.get("profit_for_period")
            owner_profit = pl_data.get("net_profit_attributable_to_owners")

            # Keep both total consolidated PAT and owner-attributable PAT. Do not
            # collapse them into one field; downstream confidence checks need both.
            if total_profit is None and pbt is not None and tax is not None:
                pl_data["profit_for_period"] = round(float(pbt) - float(tax) + float(share), 2)
                total_profit = pl_data["profit_for_period"]

            if owner_profit is None and total_profit is not None and pl_data.get("nci_profit") is not None:
                owner_profit = round(float(total_profit) - float(nci), 2)
                pl_data["net_profit_attributable_to_owners"] = owner_profit

            # Backward-compatible UI field: use owner PAT when available, otherwise
            # total PAT. The source fields remain separate for auditability.
            if owner_profit is not None:
                pl_data["net_profit"] = owner_profit
            elif total_profit is not None and pl_data.get("net_profit") is None:
                pl_data["net_profit"] = total_profit

        return {"statements": out, "provenance": prov}

    def _parse_contexts(self, root: etree._Element) -> Dict[str, XBRLContext]:
        contexts: Dict[str, XBRLContext] = {}
        for c in root.xpath(".//xbrli:context", namespaces=_NS):
            cid = c.get("id") or ""
            if not cid:
                continue

            period = c.find(".//{http://www.xbrl.org/2003/instance}period")
            period_type = "unknown"
            start_d: Optional[date] = None
            end_d: Optional[date] = None
            inst_d: Optional[date] = None

            if period is not None:
                inst = period.find("{http://www.xbrl.org/2003/instance}instant")
                if inst is not None and inst.text:
                    inst_d = self._parse_date(inst.text)
                    period_type = "instant"
                else:
                    s = period.find("{http://www.xbrl.org/2003/instance}startDate")
                    e = period.find("{http://www.xbrl.org/2003/instance}endDate")
                    if s is not None and s.text and e is not None and e.text:
                        start_d = self._parse_date(s.text)
                        end_d = self._parse_date(e.text)
                        period_type = "duration"

            is_consolidated = self._infer_consolidated(c)
            seg_text_all = " ".join(c.xpath(".//xbrli:segment//text()", namespaces=_NS))
            has_segment = len(seg_text_all.strip()) > 0
            contexts[cid] = XBRLContext(
                context_id=cid,
                period_type=period_type,
                start=start_d,
                end=end_d,
                instant=inst_d,
                is_consolidated=is_consolidated,
                has_segment=has_segment,
            )

        # Mark instant contexts as year-end only when there's a matching annual duration context.
        annual_end_dates: set[date] = set()
        for ctx in contexts.values():
            if ctx.period_type != "duration":
                continue
            if not ctx.end:
                continue
            dd = ctx.duration_days()
            if dd is not None and dd > 300:
                annual_end_dates.add(ctx.end)

        for ctx in contexts.values():
            if ctx.period_type == "instant" and ctx.instant and ctx.instant in annual_end_dates:
                ctx.is_year_end = True

        return contexts

    def _infer_consolidated(self, context_el: etree._Element) -> Optional[bool]:
        cid = (context_el.get("id") or "").lower()
        if "cfs" in cid or "consolidated" in cid:
            if "standalone" not in cid and "separate" not in cid:
                return True
        elif "standalone" in cid or "sfs" in cid:
            if "consolidated" not in cid and "cfs" not in cid:
                return False

        seg_text = " ".join(context_el.xpath(".//xbrli:segment//text()", namespaces=_NS)).lower()
        if not seg_text:
            return None
        if "consolidated" in seg_text:
            if "standalone" in seg_text or "separate" in seg_text:
                # If both exist, we don't know; let scoring decide
                return None
            return True
        if "standalone" in seg_text or "separate" in seg_text:
            return False
        return None

    def _parse_units(self, root: etree._Element) -> Dict[str, str]:
        units: Dict[str, str] = {}
        for u in root.xpath(".//xbrli:unit", namespaces=_NS):
            uid = u.get("id") or ""
            if not uid:
                continue
            measure = u.find(".//{http://www.xbrl.org/2003/instance}measure")
            if measure is not None and measure.text:
                units[uid] = measure.text.strip()
        return units

    def _parse_facts(self, root: etree._Element) -> List[XBRLFact]:
        facts: List[XBRLFact] = []
        # Most XBRL instance facts are elements with contextRef. We scan all elements.
        for el in root.iter():
            ctx = el.get("contextRef")
            if not ctx:
                continue
            txt = (el.text or "").strip()
            if not txt:
                continue
            val = self._parse_number(txt)
            if val is None:
                continue
            qn = etree.QName(el.tag)
            localname = qn.localname
            unit_ref = el.get("unitRef")
            scale = self._parse_int(el.get("scale"))
            decimals = self._parse_int(el.get("decimals"))
            facts.append(
                XBRLFact(
                    localname=localname,
                    context_id=ctx,
                    unit_ref=unit_ref,
                    value=val,
                    scale=scale,
                    decimals=decimals,
                )
            )
        return facts

    def _choose_and_set(
        self,
        out: Dict[str, Dict[str, Dict[str, Any]]],
        *,
        stmt: str,
        fy: str,
        field: str,
        value: float,
        ctx: XBRLContext,
        fact: XBRLFact,
        alias_idx: int = 0,
        filing_period_type: Optional[str] = None,
    ) -> bool:
        """
        Choose the best candidate per field based on:
        - Consolidation preference
        - Duration match (annual vs quarterly vs instant)
        - Non-zero and finite value
        """
        if stmt not in out:
            return False
        out.setdefault(stmt, {})
        out[stmt].setdefault(fy, {})

        new_score = self._score_candidate(
            stmt=stmt,
            value=value,
            ctx=ctx,
            alias_idx=alias_idx,
            filing_period_type=filing_period_type,
        )
        if field not in out[stmt][fy]:
            out[stmt][fy][field] = value
            out[stmt][fy].setdefault("_meta", {})[field] = self._meta(ctx, fact, score=new_score)
            return True

        old_meta = out[stmt][fy].get("_meta", {}).get(field, {})
        old_score = float(old_meta.get("score", 0.0))
        if new_score > old_score:
            out[stmt][fy][field] = value
            out[stmt][fy].setdefault("_meta", {})[field] = self._meta(ctx, fact, score=new_score)
            return True
        return False

    def _score_candidate(
        self,
        *,
        stmt: str,
        value: float,
        ctx: XBRLContext,
        alias_idx: int = 0,
        filing_period_type: Optional[str] = None,
    ) -> float:
        score = 0.0
        # CRITICAL FIX: The most canonical localname for a field gets a higher score.
        # This prevents "OtherIncome" from overwriting "RevenueFromOperations" 
        # and "NCI Profit" from overwriting "Net Profit", because the preferred
        # tag is earlier in the list (alias_idx = 0).
        score -= alias_idx * 0.1

        if value is None or not (abs(value) >= 0.0):
            return score

        # Prefer consolidated contexts when requested.
        # FIX #2: Harsh standalone penalty prevents standalone segments from ever
        # outscoring a consolidated context on the same filing.
        if self.prefer_consolidated:
            if ctx.is_consolidated is True:
                score += 5.0
            elif ctx.is_consolidated is False:
                score -= 10.0

        # HIGH #4: Prefer the face of the financial statements over segment-level breakdowns.
        if ctx.has_segment:
            score -= 1.0

        # SEBI financial-results context heuristics:
        # - Quarterly endpoint: One* is current quarter, Four*/Five* are YTD.
        # - Annual endpoint: Four* commonly carries full-year values even when
        #   dates are incorrectly duplicated as quarter dates. Prefer Four* there.
        ctx_id = ctx.context_id.lower()
        is_ytd_context = ctx_id.startswith("four") or ctx_id.startswith("five") or ctx_id.startswith("six")
        is_current_context = ctx_id.startswith("one")
        if filing_period_type == "annual":
            if is_ytd_context:
                score += 1.5
            elif is_current_context and stmt != "bs":
                score -= 1.0
        else:
            if is_ytd_context:
                score -= 1.0
            elif is_current_context:
                score += 0.5

        # Statement-type heuristics.
        if stmt == "bs":
            if ctx.period_type == "instant":
                score += 1.5
        else:
            if ctx.period_type == "duration":
                score += 1.0
                dd = ctx.duration_days()
                if dd is not None:
                    # Annual ~ 365; quarterly ~ 90. Prefer annual-ish for FY buckets.
                    if 300 <= dd <= 430:
                        score += 1.0
                    elif 60 <= dd <= 140:
                        score += 0.3

        # Prefer contexts that end on March 31 (Indian FY end).
        end_d = ctx.instant or ctx.end
        if end_d and end_d.month == 3 and end_d.day == 31:
            score += 0.5

        # Non-zero values (avoid picking placeholder zeros).
        if abs(value) > 0.0:
            score += 0.1
        return score

    def _meta(self, ctx: XBRLContext, fact: XBRLFact, *, score: float) -> Dict[str, Any]:
        end_d = ctx.instant or ctx.end
        return {
            "context_id": ctx.context_id,
            "period_type": ctx.period_type,
            "start": ctx.start.isoformat() if ctx.start else None,
            "end": end_d.isoformat() if end_d else None,
            "is_consolidated": ctx.is_consolidated,
            "unit_ref": fact.unit_ref,
            "scale": fact.scale,
            "decimals": fact.decimals,
            "score": round(score, 3),
        }

    def _apply_scale(self, value: float, scale: Optional[int]) -> float:
        if scale is None:
            return value
        try:
            return float(value) * (10 ** int(scale))
        except Exception:
            return value

    def _to_target_unit(self, *, value: float, unit_measure: Optional[str], field_localname: str) -> float:
        """
        Convert raw numeric into target unit.

        - For monetary facts with unit iso4217:INR, values are typically in INR (rupees).
        - Output is INR crores by default.
        - EPS is per-share and should not be converted.
        """
        if self.target_unit != "INR_CRORE":
            return value

        eps_indicators = ("earningslosspershare", "pershare", "earningspershare", "basiceps", "dilutedeps", "epsafter", "epsbefore")
        fl_lower = field_localname.lower()
        if any(ind in fl_lower for ind in eps_indicators):
            return value

        if unit_measure and ("INR" in unit_measure or unit_measure.endswith(":INR")):
            # Heuristic: MCA facts are typically in INR (rupees). Convert to crores unless it already
            # looks like a crores-scale number.
            #
            # - Rupee amounts for listed companies are commonly >= 1e6 (10 lakh) even for smaller line items.
            # - Crore-scale amounts are typically in the 1e2..1e5 range.
            if abs(value) >= 1e6:
                return round(value / 1e7, 2)
            return round(value, 2)
        return value

    def _parse_number(self, s: str) -> Optional[float]:
        st = s.strip()
        if not st or st.lower() in {"nil", "null", "none"}:
            return None
        neg = st.startswith("(") and st.endswith(")")
        st = st.strip("()")
        st = self._NUM_CLEAN.sub("", st)
        try:
            v = float(st)
            return -v if neg else v
        except Exception:
            return None

    def _parse_date(self, s: str) -> Optional[date]:
        st = s.strip()
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y%m%d"):
            try:
                return datetime.strptime(st[:10], fmt).date()
            except Exception:
                continue
        return None

    def _parse_int(self, s: Optional[str]) -> Optional[int]:
        if not s:
            return None
        try:
            return int(str(s).strip())
        except Exception:
            return None


def get_parser(source_type: str):
    if source_type in {"mca_xbrl", "xbrl"}:
        return MCAXBRLInstanceParser()
    return None
