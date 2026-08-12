"""bill_parser.py — Octopus electricity PDF bill → EMT import CSV (in-EMT).

Turns a set of Octopus **electricity** bills (PDF) into per-channel CSVs that
conform to EMT's CSV import contract (see docs/bill_to_csv_import_spec.md). The
parser is the fragile, format-churny part, so it is kept as a self-contained,
heavily self-checking module: every bill is reconciled against its own printed
totals and a mismatch is FLAGGED, never silently emitted.

Placement (per the user's design): this lives INSIDE EMT but strictly upstream of
the block-writing path. Its only job is to produce the same CSV a user would
hand-build; the existing CSV import then turns that CSV into blocks. So all the
billing-accurate machinery downstream is untouched.

Pipeline (three hard seams):
    PDF pages ──parse──▶ Bill (per-MPAN, per-period facts + HH tables)
              ──synthesise/transcribe──▶ CsvRows (per MPAN, per channel)
              ──(existing CSV import)──▶ blocks

pypdf is imported lazily so EMT runs without it; `pdf_support()` reports whether
the feature is available.

Scope of this module:
  * §0  MPAN scoping — import vs export supply numbers; warn on multi-import folders
  * §2a transcription — the per-day 48-slot HH tables (exact, IOG history)
  * §2b synthesis — flat / dual-rate periods with no HH pages (approximate)
  * §3  standing charge — "N days @ Xp/day", full daily on one row per day
  * §4  export — period kWh (or credit÷rate) at a flat outgoing rate
  * §5  reconciliation — per-day and per-period self-checks
It parses ELECTRICITY only; gas sections are ignored.
"""
from __future__ import annotations

import io
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

# Octopus PDFs trip pypdf's "Ignoring wrong pointing object N 0 (offset 0)" warning
# on nearly every object — benign (pypdf recovers and text extracts fine), but it
# floods the add-on log. Quiet it to ERROR; a genuine read failure still surfaces.
logging.getLogger("pypdf").setLevel(logging.ERROR)

TZ = ZoneInfo("Europe/London")


def _period_days(frm, to):
    """Inclusive list of dates frm..to, or [] if either bound is missing (a period
    header we couldn't fully parse — guards every date-arithmetic consumer)."""
    if not frm or not to or to < frm:
        return []
    return [frm + timedelta(days=i) for i in range((to - frm).days + 1)]

# Full names AND 3-letter abbreviations: Octopus uses full names on the HH day
# pages ("3rd July 2024") but abbreviations in the Charges-In-Detail period headers
# ("(3rd Jul 2024 - 2nd Aug 2024)"). Missing the abbreviations silently dropped the
# import/export PERIODS (standing charge + export), even while the HH pages parsed.
MONTHS = {}
for _i, _m in enumerate(
        ["January", "February", "March", "April", "May", "June", "July", "August",
         "September", "October", "November", "December"], 1):
    MONTHS[_m] = _i
    MONTHS[_m[:3]] = _i          # Jan, Feb, … Dec
MONTHS["Sept"] = 9               # common 4-letter variant

# ── Bill regexes (Dec-2024 Octopus layout; extend as new layouts appear) ──────
# HH day-page row: "00:00 - 00:30  6.67  3.59  23.928" (rate p/kWh, kWh, cost p)
_ROW_RE   = re.compile(r"(\d{2}):(\d{2})\s*-\s*(\d{2}):(\d{2})\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)")
_DATE_RE  = re.compile(r"(\d{1,2})(?:st|nd|rd|th)\s+([A-Z][a-z]+)\s+(\d{4})")
_TOTAL_RE = re.compile(r"Total consumption\s+([\d.]+)\s*kWh")
_STAND_RE = re.compile(r"(\d+)\s*days?\s*@\s*([\d.]+)\s*p/day")
_VAT_RE   = re.compile(r"VAT\s*@\s*([\d.]+)\s*%")
_MPAN_RE  = re.compile(r"\b(\d{13})\b")
# Charges-In-Detail tier row: "6.67p/kWh  1040.5 kWh  £69.367"
_TIER_RE  = re.compile(r"([\d.]+)\s*p/kWh\s+([\d.]+)\s*kWh\s+£([\d.]+)")
# Export line: "Energy Exported 118.1 kWh @ 15.00p/kWh £17.71"
_EXP_RE   = re.compile(r"Energy Exported\s+([\d.]+)\s*kWh\s*@\s*([\d.]+)\s*p/kWh\s+£([\d.]+)")
# Export FALLBACKS (bills vary): a "X kWh @ Yp/kWh" line without the "Energy
# Exported"/£ framing; a meter-reading value (kWh = last − first read); the
# outgoing unit rate from the tariff summary; and the credit total.
_EXP_KWH_RE  = re.compile(r"([\d.]+)\s*kWh\s*@\s*([\d.]+)\s*p/kWh")
_METERREAD_RE = re.compile(r"([\d][\d,]*\.?\d*)\s*(?:Smart\s+)?[Mm]eter\s+[Rr]eading")
_OUT_RATE_RE = re.compile(r"Outgoing[\s\S]{0,500}?Unit Rate\s*\n?\s*([\d.]+)\s*p")
_CREDIT_RE   = re.compile(r"(?:Total Electricity Credits|Subtotal of credits[^£\n]*)\s*\n?\s*£([\d.]+)")
# Period header: "... (3rd December 2024 - 31st December 2024)"
_PERIOD_RE = re.compile(
    r"\((\d{1,2})(?:st|nd|rd|th)\s+([A-Z][a-z]+)\s+(\d{4})\s*-\s*"
    r"(\d{1,2})(?:st|nd|rd|th)\s+([A-Z][a-z]+)\s+(\d{4})\)")


def pdf_support() -> bool:
    """True if pypdf is importable (the PDF feature is available)."""
    try:
        import pypdf  # noqa: F401
        return True
    except Exception:
        return False


# ── data model ───────────────────────────────────────────────────────────────
@dataclass
class HHSlot:
    d: date
    sh: int
    sm: int
    eh: int
    em: int
    rate_pre: float      # bill's printed (pre-VAT) rate p/kWh
    kwh: float
    cost_pre: float      # bill's printed (pre-VAT) cost p


@dataclass
class ImportPeriod:
    frm: date
    to: date
    tiers: list           # [(rate_pre_p, kwh, cost_pre_gbp)]
    total_kwh: Optional[float]
    standing_days: Optional[int]
    standing_pre_p: Optional[float]   # pence/day, pre-VAT


@dataclass
class ExportPeriod:
    frm: date
    to: date
    kwh: Optional[float]
    rate_pre_p: Optional[float]       # flat outgoing rate p/kWh (usually 0% VAT)
    credit_gbp: Optional[float]


@dataclass
class Bill:
    source: str = ""
    mpan_import: Optional[str] = None
    mpan_export: Optional[str] = None
    vat_import: float = 0.05
    vat_export: float = 0.0
    tariff_import: Optional[str] = None
    tariff_export: Optional[str] = None
    hh_days: list = field(default_factory=list)          # [ [HHSlot,...] per day ]
    import_periods: list = field(default_factory=list)   # [ImportPeriod]
    export_periods: list = field(default_factory=list)   # [ExportPeriod]
    warnings: list = field(default_factory=list)
    reconciliation: dict = field(default_factory=dict)

    @property
    def has_hh(self) -> bool:
        return bool(self.hh_days)


# ── extraction ───────────────────────────────────────────────────────────────
def _read_pages(src) -> list:
    """Return per-page text. `src` = path str or bytes/file-like."""
    from pypdf import PdfReader
    if isinstance(src, (bytes, bytearray)):
        rdr = PdfReader(io.BytesIO(src))
    else:
        rdr = PdfReader(src)
    return [(p.extract_text() or "") for p in rdr.pages]


def _mk_date(day: str, mon: str, yr: str) -> Optional[date]:
    m = MONTHS.get(mon)
    if not m:
        return None
    try:
        return date(int(yr), m, int(day))
    except ValueError:
        return None


def _iso_local(d: date, hh: int, mm: int, fold: int = 0) -> str:
    """Local ISO with the correct GMT/BST offset for the date.

    `fold` disambiguates the autumn clock-change: on the day the clocks go back
    (e.g. 29 Oct 2023) the 01:00 and 01:30 wall-clock slots occur twice. fold=0 is
    the first (BST, +01:00) occurrence; fold=1 is the second (GMT, +00:00). Without
    this both map to the same instant and one UTC half-hour is lost → a phantom gap."""
    return datetime(d.year, d.month, d.day, hh, mm, fold=fold, tzinfo=TZ).isoformat()


def _electricity_import_blocks(summary: str) -> list:
    """Slice the summary's 'Your Charges In Detail' into per-section text blocks,
    keeping only ELECTRICITY consumption (import) sections — not gas, not export."""
    # Sections are anchored on a supply/meter header; split on 'Supply number' and
    # 'Meter Point Reference' (gas). Keep electricity-import ones.
    parts = re.split(r"(?=Supply number|Meter Point Reference)", summary)
    out = []
    for p in parts:
        is_elec = ("Energy Charges for Meter" in p) or ("Total consumption" in p and "p/kWh" in p)
        # Discriminate on the export CHARGE line, not the word "Outgoing" — a consumption
        # block's tariff footer can mention the outgoing tariff and must NOT be dropped.
        # Case-insensitive so "Energy exported" / "Exported" variants still count.
        is_export = "exported" in p.lower()
        is_gas = ("Meter Point Reference" in p) or ("Units (m3)" in p) or ("p/thm" in p)
        if is_elec and not is_export and not is_gas:
            out.append(p)
    return out


def _electricity_export_blocks(summary: str) -> list:
    # Require the actual export CHARGE line ("Energy Exported"), not just the word
    # "Outgoing" (which also appears in the import block's tariff footer), so the
    # export supply block — and its MPAN — is identified, not a neighbour.
    parts = re.split(r"(?=Supply number|Meter Point Reference)", summary)
    return [p for p in parts if "exported" in p.lower()]


def _classify_mpans(summary: str) -> tuple:
    """(import_mpan, export_mpan). Import = a 13-digit MPAN in a consumption block;
    export = one in an Outgoing/Exported block. Returns (imp, exp)."""
    imp = exp = None
    for blk in _electricity_import_blocks(summary):
        m = _MPAN_RE.search(blk)
        if m:
            imp = m.group(1)
            break
    for blk in _electricity_export_blocks(summary):
        m = _MPAN_RE.search(blk)
        if m:
            exp = m.group(1)
            break
    return imp, exp


def _all_import_mpans(summary: str) -> set:
    """Every distinct ELECTRICITY IMPORT MPAN in the summary — >1 ⇒ a house move
    (a different supply), which must NOT be merged into one CSV (§0)."""
    found = set()
    for blk in _electricity_import_blocks(summary):
        m = _MPAN_RE.search(blk)
        if m:
            found.add(m.group(1))
    return found


def _parse_import_periods(summary: str) -> list:
    periods = []
    for blk in _electricity_import_blocks(summary):
        pm = _PERIOD_RE.search(blk)
        if not pm:
            continue
        frm = _mk_date(pm.group(1), pm.group(2), pm.group(3))
        to = _mk_date(pm.group(4), pm.group(5), pm.group(6))
        if not frm or not to:
            continue
        tiers = [(float(r), float(k), float(c)) for (r, k, c) in _TIER_RE.findall(blk)]
        tot = None
        tm = _TOTAL_RE.search(blk)
        if tm:
            tot = float(tm.group(1))
        sd = sp = None
        sm = _STAND_RE.search(blk)
        if sm:
            sd, sp = int(sm.group(1)), float(sm.group(2))
        periods.append(ImportPeriod(frm, to, tiers, tot, sd, sp))
    return periods


def _parse_export_periods(summary: str) -> list:
    out = []
    for blk in _electricity_export_blocks(summary):
        pm = _PERIOD_RE.search(blk)
        if not pm:
            continue
        frm = _mk_date(pm.group(1), pm.group(2), pm.group(3))
        to = _mk_date(pm.group(4), pm.group(5), pm.group(6))
        if not frm or not to:          # unparseable period header — skip, don't crash
            continue
        kwh = rate = credit = None
        em = _EXP_RE.search(blk)
        if em:
            kwh, rate, credit = float(em.group(1)), float(em.group(2)), float(em.group(3))
        # ── Fallbacks for bills whose export section is formatted differently ──
        # (e.g. the credit-only layout: meter reads + a rate in the tariff summary,
        #  but no "Energy Exported X kWh @ Yp/kWh £Z" line).
        if kwh is None or rate is None:
            km = _EXP_KWH_RE.search(blk)          # "X kWh @ Yp/kWh" anywhere in the block
            if km:
                if kwh is None:
                    kwh = float(km.group(1))
                if rate is None:
                    rate = float(km.group(2))
        if kwh is None:                            # derive from the two meter reads
            reads = []
            for x in _METERREAD_RE.findall(blk):
                try:
                    reads.append(float(x.replace(",", "")))
                except ValueError:
                    pass
            if len(reads) >= 2:
                kwh = round(max(reads) - min(reads), 3)
        if rate is None:                           # outgoing unit rate from the tariff summary
            rm = _OUT_RATE_RE.search(blk) or _OUT_RATE_RE.search(summary)
            if rm:
                rate = float(rm.group(1))
        if credit is None:
            cm = _CREDIT_RE.search(blk)
            if cm:
                credit = float(cm.group(1))
        out.append(ExportPeriod(frm, to, kwh, rate, credit))
    return out


def _tariff_name(summary: str, kind: str) -> Optional[str]:
    """Best-effort tariff name for import/export from the 'About Your Tariff' block."""
    if kind == "export":
        # The product line, e.g. "Outgoing Octopus 12M Fixed" — strip the trailing
        # "(date - date)" period and any "export" header word.
        m = re.search(r"Outgoing Octopus[^(\n]*", summary)
        if m:
            return m.group(0).strip()
        m = re.search(r"Outgoing[^(\n]*", summary)
        return m.group(0).replace("export", "").strip() if m else None
    m = re.search(r"Tariff Name\s*\n?\s*([A-Za-z0-9 ]+)", summary)
    return m.group(1).strip() if m else None


def _parse_hh_days(pages: list) -> list:
    """Transcribe the per-day 48-slot HH tables (import, IOG). Each page with a day
    header + ≥40 HH rows is one day. Returns [ [HHSlot,...], ... ]."""
    days = []
    for t in pages:
        rows = _ROW_RE.findall(t)
        dm = _DATE_RE.search(t)
        if not dm or len(rows) < 40:
            continue
        d = _mk_date(dm.group(1), dm.group(2), dm.group(3))
        if not d:
            continue
        slots = []
        for (sh, sm, eh, em, rt, kw, co) in rows:
            slots.append(HHSlot(d, int(sh), int(sm), int(eh), int(em),
                                float(rt), float(kw), float(co)))
        days.append(slots)
    return days


def _reconcile(bill: Bill) -> dict:
    """Per-day (2a) + per-period (Charges-In-Detail) self-checks. Populates
    bill.warnings with any mismatch and returns a structured report."""
    rep = {"days": [], "import_periods": [], "ok": True}
    # per-day: Σ HH kWh ≈ the page's Total consumption (page totals are 1-dp).
    day_total_index = {}
    for slots in bill.hh_days:
        d = slots[0].d
        got = round(sum(s.kwh for s in slots), 2)
        day_total_index[d] = got
    # We only have the page-printed totals via _parse_hh_days indirectly; recompute
    # from the raw pages is done in parse_bill (stored on warnings there).
    # per-period: Σ HH kWh in the period ≈ the period's total_kwh.
    for p in bill.import_periods:
        if p.total_kwh is None or not p.frm or not p.to:
            continue
        got = round(sum(v for (d, v) in day_total_index.items()
                        if p.frm <= d <= p.to), 1)
        diff = round(got - p.total_kwh, 1)
        ok = abs(diff) <= max(0.5, 0.01 * p.total_kwh)
        rep["import_periods"].append({
            "from": p.frm.isoformat(), "to": p.to.isoformat(),
            "parsed_kwh": got, "billed_kwh": p.total_kwh, "diff": diff, "ok": ok})
        if not ok:
            rep["ok"] = False
            bill.warnings.append(
                f"Import {p.frm}–{p.to}: parsed {got} kWh vs billed {p.total_kwh} kWh "
                f"(diff {diff}).")
    return rep


def parse_bill(src, *, source_name: str = "") -> Bill:
    """Parse ONE Octopus electricity bill PDF into a Bill. `src` = path or bytes.
    Never raises on a malformed bill — returns a Bill with warnings instead."""
    bill = Bill(source=source_name or (src if isinstance(src, str) else "bill.pdf"))
    try:
        pages = _read_pages(src)
    except Exception as e:  # pypdf missing / unreadable
        bill.warnings.append(f"Could not read PDF: {e}")
        return bill
    summary = "\n".join(pages[:8])

    vat = _VAT_RE.findall(summary)
    if vat:
        bill.vat_import = float(vat[0]) / 100.0
    bill.mpan_import, bill.mpan_export = _classify_mpans(summary)

    imps = _all_import_mpans(summary)
    if len(imps) > 1:
        bill.warnings.append(
            "This bill contains more than one electricity import MPAN "
            f"({', '.join(sorted(imps))}) — a house move. Split the PDFs so each "
            "file (and CSV) covers a single MPAN before importing.")

    bill.tariff_import = _tariff_name(summary, "import")
    bill.tariff_export = _tariff_name(summary, "export")
    bill.import_periods = _parse_import_periods(summary)
    bill.export_periods = _parse_export_periods(summary)
    # Diagnostic: an export supply section was present but we couldn't pull a usable
    # period + kWh/rate out of it — surface it instead of silently dropping export.
    if _electricity_export_blocks(summary) and not any(
            (p.kwh or (p.credit_gbp and p.rate_pre_p)) for p in bill.export_periods):
        bill.warnings.append(
            "An export (outgoing) section was found but its figures couldn't be read — "
            "no export CSV was produced for it. Send this bill so the parser can be widened.")
    bill.hh_days = _parse_hh_days(pages)

    # per-day reconciliation against each page's own printed Total consumption.
    day_recon = []
    for t in pages:
        rows = _ROW_RE.findall(t)
        dm = _DATE_RE.search(t)
        tm = _TOTAL_RE.search(t)
        if not dm or len(rows) < 40 or not tm:
            continue
        d = _mk_date(dm.group(1), dm.group(2), dm.group(3))
        got = round(sum(float(r[5]) for r in rows), 2)
        page_tot = float(tm.group(1))
        # The page prints its daily total to 1 dp while we sum 48 half-hours at 2 dp,
        # so a ≤~0.1 kWh difference is just rounding, not a transcription error. A real
        # error (a dropped/duplicated slot) is far larger, so this stays a tight gate.
        ok = abs(got - page_tot) <= 0.11
        day_recon.append({"date": d.isoformat() if d else "?", "parsed_kwh": got,
                          "page_kwh": page_tot, "ok": ok})
        if not ok:
            bill.warnings.append(f"Day {d}: parsed {got} kWh vs page {page_tot} kWh.")
    bill.reconciliation = _reconcile(bill)
    bill.reconciliation["days"] = day_recon
    bill.reconciliation["ok"] = (bill.reconciliation.get("ok", True)
                                 and all(x["ok"] for x in day_recon))
    return bill


# ── CSV generation (Bill → per-channel CSV rows) ─────────────────────────────
# EMT CSV contract (§1): Start, End, Consumption (kWh), Unit Rate (p/kWh),
# Estimated Cost Inc. Tax (p), Standing Charge Inc. Tax (p). Rate-first: emit the
# inc-VAT rate and leave Cost blank (EMT computes cost = rate/100 × kWh). Standing
# = the FULL daily inc-VAT charge on the first slot of each day, blank elsewhere.
CSV_HEADER = ["Start", "End", "Consumption (kWh)", "Unit Rate (p/kWh)",
              "Estimated Cost Inc. Tax (p)", "Standing Charge Inc. Tax (p)",
              # BL-23 (4.2 Slice D): the bill's pre-VAT figures, emitted so EMT stores
              # imp_cost_exc/imp_rate_exc directly (penny-accurate off the bill).
              "Unit Rate Exc. Tax (p/kWh)", "Standing Charge Exc. Tax (p)"]

# Default synthesis windows (local time) when a period has no HH pages. Off-peak /
# night window for a dual-rate import (IOG/Go family); daylight window for a flat
# export spread. Overridable per call.
_NIGHT_WINDOW = ("23:30", "05:30")   # off-peak (wraps midnight)
_DAYLIGHT = ("08:00", "16:00")


def _in_window(hhmm: str, win: tuple) -> bool:
    """Is local HH:MM inside [win0, win1)? Handles a window that wraps midnight."""
    a, b = win
    if a <= b:
        return a <= hhmm < b
    return hhmm >= a or hhmm < b        # wrap (e.g. 23:30 → 05:30)


def _day_slots(d: date, block_min: int = 30):
    """Yield (start_hhmm, HHSlot-times) for each block of a local day."""
    n = 24 * 60 // block_min
    for i in range(n):
        mins = i * block_min
        sh, sm = divmod(mins, 60)
        e = mins + block_min
        eh, em = divmod(e, 60)
        wrap = eh >= 24
        if wrap:
            eh -= 24
        yield f"{sh:02d}:{sm:02d}", (sh, sm, eh, em, wrap)


def _standing_inc_for_day(bill: Bill, d: date):
    """Full daily inc-VAT standing (pence) for a local day, from the covering import
    period. None if uncovered (leave the CSV cell blank)."""
    for p in bill.import_periods:
        if (p.standing_pre_p is not None and p.frm and p.to and p.frm <= d <= p.to):
            return round(p.standing_pre_p * (1 + bill.vat_import), 4)
    return None


def _standing_exc_for_day(bill: Bill, d: date):
    """Full daily EX-VAT standing (pence) — the bill's printed pre-VAT figure, no
    gross-up. None if uncovered. BL-23 (4.2 Slice D)."""
    for p in bill.import_periods:
        if (p.standing_pre_p is not None and p.frm and p.to and p.frm <= d <= p.to):
            return round(p.standing_pre_p, 4)
    return None


def _import_hh_rows(bill: Bill, rate_resolver=None) -> list:
    """Transcription (§2a): one CSV row per HH slot, exact. inc-VAT rate; standing
    once per day. rate_resolver(tariff, date_iso, off_peak) → inc-VAT p/kWh overrides
    the bill's grossed-up rate when the caller (EMT, API-connected) supplies it."""
    rows = []
    vat = bill.vat_import
    for slots in bill.hh_days:
        d = slots[0].d
        stand = _standing_inc_for_day(bill, d)
        stand_exc = _standing_exc_for_day(bill, d)   # BL-23: pre-VAT standing
        # Autumn clock-change: the page lists the repeated 01:00/01:30 hour twice, in
        # order (BST pair, then GMT pair). Wall-clock time therefore steps BACKWARD at
        # the fold; from that point on the slots are the second (GMT, fold=1) occurrence.
        fold = 0
        prev = None
        for i, s in enumerate(slots):
            cur = (s.sh, s.sm)
            if prev is not None and cur <= prev:
                fold = 1
            prev = cur
            start_d = s.d
            end_d = s.d + timedelta(days=1) if (s.eh == 0 and s.em == 0) else s.d
            off_peak = None                       # unknown label; use rate to infer
            rate_inc = None
            if rate_resolver:
                rate_inc = rate_resolver(bill.tariff_import,
                                         _iso_local(s.d, s.sh, s.sm, fold)[:19], off_peak)
            if rate_inc is None:
                rate_inc = round(s.rate_pre * (1 + vat), 4)
            rows.append({
                "Start": _iso_local(start_d, s.sh, s.sm, fold),
                "End": _iso_local(end_d, s.eh, s.em, fold),
                "Consumption (kWh)": s.kwh,
                "Unit Rate (p/kWh)": rate_inc,
                "Estimated Cost Inc. Tax (p)": "",
                "Standing Charge Inc. Tax (p)": (stand if i == 0 else ""),
                # BL-23: the bill's pre-VAT rate + standing (penny-accurate); rate-first,
                # so leave the exc Cost cell blank (EMT derives exc cost = rate_exc × kWh).
                "Unit Rate Exc. Tax (p/kWh)": round(s.rate_pre, 4),
                "Standing Charge Exc. Tax (p)": (stand_exc if i == 0 else ""),
            })
    return rows


def _import_synth_rows(bill: Bill, block_min: int = 30) -> list:
    """Synthesis (§2b) for import periods WITHOUT HH pages — distribute the Charges-
    In-Detail tier totals across the period's half-hours. Flat (one tier) → even
    across all blocks; dual-rate → night-tier kWh across the night window, day-tier
    across the rest. Per-tier rate is exact; only the intra-window shape is synthetic
    (flagged). Returns [] when the period already has HH transcription."""
    rows = []
    vat = bill.vat_import
    hh_dates = {s.d for day in bill.hh_days for s in day}
    for p in bill.import_periods:
        # Skip periods fully covered by HH transcription (or with an unparseable range).
        days = _period_days(p.frm, p.to)
        if not days or all(d in hh_dates for d in days):
            continue
        if not p.tiers:
            continue
        bill.warnings.append(
            f"Import {p.frm}–{p.to}: no half-hour pages — shape synthesised from the "
            f"period tier totals (period cost is exact; intra-day shape is approximate).")
        tiers = sorted(p.tiers, key=lambda t: t[0])       # cheapest first = night
        night_rate, night_kwh = tiers[0][0], tiers[0][1]
        day_rate, day_kwh = (tiers[-1][0], tiers[-1][1]) if len(tiers) > 1 else (None, 0.0)
        # Count slots in each window across the period.
        night_slots, day_slots_ = [], []
        for d in days:
            stand = _standing_inc_for_day(bill, d)
            for hhmm, (sh, sm, eh, em, wrap) in _day_slots(d, block_min):
                is_night = (len(tiers) > 1 and _in_window(hhmm, _NIGHT_WINDOW))
                (night_slots if is_night else day_slots_).append((d, sh, sm, eh, em, wrap, stand))
        def _emit(slot_list, total_kwh, rate_pre):
            if not slot_list or rate_pre is None:
                return
            per = total_kwh / len(slot_list)
            rate_inc = round(rate_pre * (1 + vat), 4)
            seen_day = set()
            for (d, sh, sm, eh, em, wrap, stand) in slot_list:
                end_d = d + timedelta(days=1) if wrap else d
                first = d not in seen_day
                seen_day.add(d)
                stand_exc = _standing_exc_for_day(bill, d)   # BL-23: pre-VAT standing
                rows.append({
                    "Start": _iso_local(d, sh, sm),
                    "End": _iso_local(end_d, eh, em),
                    "Consumption (kWh)": round(per, 4),
                    "Unit Rate (p/kWh)": rate_inc,
                    "Estimated Cost Inc. Tax (p)": "",
                    "Standing Charge Inc. Tax (p)": (stand if first else ""),
                    # BL-23: the tier's exact pre-VAT rate + standing (rate-first).
                    "Unit Rate Exc. Tax (p/kWh)": round(rate_pre, 4),
                    "Standing Charge Exc. Tax (p)": (stand_exc if (first and stand_exc is not None) else ""),
                })
        if len(tiers) > 1:
            _emit(night_slots, night_kwh, night_rate)
            _emit(day_slots_, day_kwh, day_rate)
        else:
            _emit(night_slots + day_slots_, night_kwh, night_rate)
    return rows


def _export_rows(bill: Bill, block_min: int = 30, daylight=_DAYLIGHT) -> list:
    """Export (§4): a flat outgoing period total spread evenly across the period's
    DAYLIGHT half-hours (roughly when a solar export occurs). Import and export are
    both GRID-BOUNDARY quantities and device attribution only ever splits IMPORT, so
    a half-hour may legitimately carry both — no import/export exclusivity is needed.
    Rate = the flat outgoing rate (export VAT is usually 0). Period total (and credit)
    are exact; only the intra-day shape is reconstructed."""
    rows = []
    for p in bill.export_periods:
        kwh = p.kwh
        rate = p.rate_pre_p
        if kwh is None and p.credit_gbp is not None and rate:
            kwh = p.credit_gbp / (rate / 100.0)       # back out kWh from credit ÷ rate
        if not kwh or not rate:
            bill.warnings.append(
                f"Export {p.frm}–{p.to}: no kWh and no rate to reconstruct — skipped.")
            continue
        days = _period_days(p.frm, p.to)
        if not days:
            continue
        bill.warnings.append(
            f"Export {p.frm}–{p.to}: {round(kwh,1)} kWh at {rate}p flat — daylight shape "
            f"reconstructed (period total is exact).")
        slots = []
        for d in days:
            for hhmm, (sh, sm, eh, em, wrap) in _day_slots(d, block_min):
                if _in_window(hhmm, daylight):
                    slots.append((d, sh, sm, eh, em, wrap))
        if not slots:
            continue
        per = kwh / len(slots)
        rate_inc = round(rate * (1 + bill.vat_export), 4)
        for (d, sh, sm, eh, em, wrap) in slots:
            end_d = d + timedelta(days=1) if wrap else d
            rows.append({
                "Start": _iso_local(d, sh, sm),
                "End": _iso_local(end_d, eh, em),
                "Consumption (kWh)": round(per, 5),
                "Unit Rate (p/kWh)": rate_inc,
                "Estimated Cost Inc. Tax (p)": "",
                "Standing Charge Inc. Tax (p)": "",     # outgoing has no standing charge
            })
    return rows


def build_csv_rows(bill: Bill, *, rate_resolver=None, block_min: int = 30) -> dict:
    """{'import': [rows], 'export': [rows]} — the two per-channel CSVs for this bill's
    MPAN(s). Import uses transcription where HH pages exist, synthesis otherwise."""
    imp = _import_hh_rows(bill, rate_resolver=rate_resolver)
    imp += _import_synth_rows(bill, block_min=block_min)
    imp.sort(key=lambda r: r["Start"])
    exp = _export_rows(bill, block_min=block_min)
    exp.sort(key=lambda r: r["Start"])
    return {"import": imp, "export": exp}


def rows_to_csv(rows: list) -> str:
    """Serialise CSV rows (dicts keyed by CSV_HEADER) to text."""
    import csv
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=CSV_HEADER)
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return buf.getvalue()
