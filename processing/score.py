"""
Parcel scoring logic — pure functions, no I/O.

Computes article_97_priority and ccr_priority for each parcel row dict.
Imported by processing/build.py.
"""

from collections import Counter, defaultdict

# ── Land use code sets ────────────────────────────────────────────────────────

A97_CONSERVATION_CODES = {
    "0130", "0131", "0170", "0370", "0310",
    "7160", "7170", "9320", "9820", "9460",
    "9500", "9510", "9520", "9530", "9540",
    "9560", "9570", "9580", "9590",
}
A97_WELLFIELD_CODES = {"9380", "9390"}
CCR_COMMON_CODES = {
    "1320", "0130", "0131", "0170", "0370",
    "0310", "1021", "1023", "9460",
}

PUBLIC_OWNER_CATEGORIES = {
    "Town of Dennis", "Water District", "Housing Authority",
    "Conservation Trust", "Conservation Land Trust", "Commonwealth",
}
TOWN_CLASS      = {"Town of Dennis"}
DISTRICT_CLASS  = {"Water District"}
AUTHORITY_CLASS = {"Housing Authority"}

CONSERVATION_OWNER_KEYWORDS = ["CONSERVATION", "LAND TRUST", "COMMONWEALTH",
                                "AUDUBON", "NATURE CONSERVANCY"]
HOA_OWNER_KEYWORDS          = ["ASSOCIATION", "HOMEOWNERS", "HOA",
                                "HOMEOWNER'S", "CONDOMINIUM", "CONDO"]
REALTY_TRUST_KEYWORDS       = ["REALTY TRUST"]


def compute_deed_book_stats(rows: list[dict]) -> tuple[dict, dict]:
    """Return ({(book, page): count}, {book: avg_acres})."""
    bp_counts: Counter = Counter()
    book_acres: defaultdict = defaultdict(list)
    for row in rows:
        b = str(row.get("book_last") or row.get("deed_book") or "").strip()
        p = str(row.get("page_last") or row.get("deed_page") or "").strip()
        try:
            acres = float(row.get("billing_acres") or row.get("lot_size_acres") or 0)
        except (ValueError, TypeError):
            acres = 0.0
        if b and p:
            bp_counts[(b, p)] += 1
        if b and acres > 0:
            book_acres[b].append(acres)
    return dict(bp_counts), {b: sum(v) / len(v) for b, v in book_acres.items() if v}


def score_parcel(
    row: dict,
    warrant: dict | None,
    book_page_counts: dict,
    book_avg_acres: dict,
    cx_pids: set,
) -> tuple[int, str, int, str]:
    """Return (a97_score, a97_reasons, ccr_score, ccr_reasons)."""
    owner_name     = str(row.get("owner_name") or "").upper()
    owner_cat      = str(row.get("owner_category") or "").strip()
    use_code       = str(row.get("use_code") or "").strip()
    prot_flag      = str(row.get("protection_flag") or "").strip()
    deed_book      = str(row.get("book_last") or row.get("deed_book") or "").strip()
    deed_page      = str(row.get("page_last") or row.get("deed_page") or "").strip()
    zoning         = str(row.get("zone_desc") or row.get("zoning") or "").strip().upper()
    farmland_class = str(row.get("farmland_class") or "").strip()

    try:
        acres = float(row.get("billing_acres") or row.get("lot_size_acres") or 0)
    except (ValueError, TypeError):
        acres = 0.0
    try:
        total_val = float(row.get("total_appr_value") or row.get("assessed_total_value") or 0)
    except (ValueError, TypeError):
        total_val = 0.0

    join_status     = (warrant or {}).get("join_status", "")
    cpa_funded      = str((warrant or {}).get("cpa_funded", "")) == "True"
    recorded_at_reg = str((warrant or {}).get("recorded_at_registry", "")) == "True"

    # ── Article 97 ────────────────────────────────────────────────────────────
    a97: list[tuple[int, str]] = []

    if owner_cat in TOWN_CLASS:
        a97.append((50, "A97-TOWN-OWNED"))
    if owner_cat in DISTRICT_CLASS:
        a97.append((50, "A97-WATER-DISTRICT"))
    if owner_cat in AUTHORITY_CLASS:
        a97.append((40, "A97-HOUSING-AUTHORITY"))
    if use_code in A97_CONSERVATION_CODES:
        a97.append((35, "A97-CONSERVATION-LANDUSE"))
    if prot_flag == "PROTECTED":
        a97.append((30, "A97-MASSGIS-PROTECTED"))
    if join_status == "MATCHED":
        a97.append((25, "A97-WARRANT-MATCHED"))
    if cpa_funded:
        a97.append((20, "A97-CPA-FUNDED"))
    if recorded_at_reg:
        a97.append((20, "A97-REGISTRY-RECORDED"))
    if any(kw in owner_name for kw in CONSERVATION_OWNER_KEYWORDS):
        a97.append((15, "A97-CONSERVATION-OWNER"))
    if use_code in A97_WELLFIELD_CODES:
        a97.append((15, "A97-ZONE1-WELLFIELD"))
    if farmland_class == "All areas are prime farmland":
        a97.append((15, "A97-PRIME-FARMLAND"))
    elif farmland_class in ("Farmland of unique importance", "Farmland of statewide importance"):
        a97.append((8, "A97-FARMLAND"))
    if acres > 0 and total_val > 0 and (total_val / acres) < 5000:
        a97.append((10, "A97-LOW-VALUE-RATIO"))
    if join_status == "NO_WARRANT_RECORD" and owner_cat in PUBLIC_OWNER_CATEGORIES:
        a97.append((-20, "A97-NO-WARRANT-RECORD"))
    if owner_cat not in PUBLIC_OWNER_CATEGORIES and prot_flag != "PROTECTED":
        a97.append((-30, "A97-PRIVATE-OWNER"))

    # ── CCR ───────────────────────────────────────────────────────────────────
    ccr: list[tuple[int, str]] = []

    is_hoa = any(kw in owner_name for kw in HOA_OWNER_KEYWORDS)
    if is_hoa:
        ccr.append((50, "CCR-HOA-OWNER"))
    if use_code in CCR_COMMON_CODES:
        ccr.append((40, "CCR-COMMON-LANDUSE"))
    if deed_book and deed_page:
        n = book_page_counts.get((deed_book, deed_page), 1)
        if n >= 4:
            ccr.append((35, "CCR-SHARED-DEED"))
    if 0.5 < acres < 5.0 and deed_book:
        avg = book_avg_acres.get(deed_book)
        if avg is not None and avg < 0.5:
            ccr.append((30, "CCR-SMALL-HIGH-DENSITY-CONTEXT"))
    if acres > 0 and total_val > 0 and owner_cat not in PUBLIC_OWNER_CATEGORIES and (total_val / acres) < 2000:
        ccr.append((25, "CCR-LOW-VALUE-RATIO"))
    if any(kw in owner_name for kw in REALTY_TRUST_KEYWORDS):
        if not any(kw in owner_name for kw in CONSERVATION_OWNER_KEYWORDS):
            ccr.append((20, "CCR-REALTY-TRUST-OWNER"))
    if prot_flag == "NEEDS REVIEW - Private conservation org":
        ccr.append((20, "CCR-MASSGIS-RESTRICTION"))
    if zoning.startswith("R") and owner_cat not in PUBLIC_OWNER_CATEGORIES:
        if "," not in owner_name or is_hoa:
            ccr.append((15, "CCR-RESIDENTIAL-ZONE"))
    if owner_cat in PUBLIC_OWNER_CATEGORIES:
        ccr.append((-20, "CCR-TOWN-OWNED"))

    is_individual = (
        "," in owner_name and not is_hoa
        and not any(kw in owner_name for kw in CONSERVATION_OWNER_KEYWORDS + REALTY_TRUST_KEYWORDS)
        and owner_cat not in PUBLIC_OWNER_CATEGORIES
    )
    if is_individual and not any(p > 0 for p, _ in ccr):
        ccr.append((-20, "CCR-INDIVIDUAL-OWNER-NO-SIGNALS"))

    a97.sort(key=lambda x: -x[0])
    ccr.sort(key=lambda x: -x[0])

    return (
        sum(p for p, _ in a97), ",".join(c for _, c in a97),
        sum(p for p, _ in ccr), ",".join(c for _, c in ccr),
    )
