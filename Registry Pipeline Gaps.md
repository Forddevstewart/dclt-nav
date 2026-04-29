Registry Pipeline: Gap Analysis
The pipeline has four strong passes — book/page lookup, name search, cross-reference expansion, and town sweep. But there are several places where documents slip through.

Gap 1 — Land Court parcels fall into a dead end (High)
enumerate.py:278-324 — process_tier1 routes any parcel with a non-empty deed_book to the Tier 1 book/page path. Land Court parcels (where deed_book starts with "LC") go here, but _bp_lookup_params uses the standard Registry params (WSIQTP=LR09AP), not the LC-specific form. When that lookup returns None, the parcel is written to cache as empty and never reaches Tier 2, where is_land_court=True would route it to the LC name search (WSIQTP=LC01LP).

Fix: In process_tier1, either skip rows where is_land_court == True so they fall to Tier 2 naturally, or add a fallback name search when Tier 1 returns None on an LC parcel.

Gap 2 — Tier 1 no-result parcels never get a name search (High)
Same location: when lookup_book_page() returns None (stale/wrong deed reference, subdivision, data error), the parcel is saved with an empty index and the run moves on. No name search under the owner is ever attempted as a fallback.

For DCLT's use case — finding restrictions on conservation-adjacent parcels — a deed reference being bad doesn't mean the owner hasn't recorded other instruments. The grantor/grantee search would still find them.

Fix: After a Tier 1 None result, queue the parcel's search_name_primary for a name search before writing the empty cache.

Gap 3 — Default date window cuts off pre-1970 history (Medium)
queue.py:159-165:


date_start = "1970"
if r["earliest_meeting_date"]:
    acq_year = ...
    date_start = str(max(acq_year - 2, 1742))
Parcels with no linked warrant get date_start = "1970". Historic conservation restrictions, deed restrictions, and easements from the 1940s–1960s are silently excluded. Given that Dennis has been recording land-use agreements since the mid-20th century, this is a significant window.

Fix: Change the default to "1742" (same as the full-queue fallback). The wider date range adds minimal load since most parcels don't have dense pre-1970 history.

Gap 4 — No instrument sweep for Tier 1 parcels after the deed is found (Medium)
Tier 1 finds the current deed (one document). That's the transfer. But the owner may have also recorded a conservation restriction, easement, or vote under their own name — separately from the deed itself. No name search is ever run for Tier 1 parcels.

The search_name_secondary field (the grantor from the found deed) is available in the queue row but is only used in process_tier2. Tier 1 never searches under either the current owner or the prior grantor.

Fix: After a successful Tier 1 book/page hit, run a name search for search_name_primary (and search_name_secondary if different) filtered by the document types of interest, and merge results into the parcel's index.

Gap 5 — Cross-reference expansion is one-level only within a single run (Low)
sweep.py:77-89 — collect_xref_targets() scans all currently-cached indexes to find unseen book/page pairs. But documents written by process_xrefs() during the same pipeline run aren't re-scanned — all_cached_indexes() reads the state at the start of that call. Xrefs-of-xrefs only get followed on the next pipeline run.

This isn't catastrophic because pipeline runs are meant to be repeated, but it means the first full enumeration is shallower than subsequent ones.

Fix: After process_xrefs completes, call collect_xref_targets() again and loop until no new targets remain (with a depth cap).

Gap 6 — No grantee sweeps for other conservation entities (Low)
The town sweep (sweep.py:48-50) covers "DENNIS TOWN" as both grantor and grantee. But instruments where the grantee is a conservation organization that holds restrictions on Dennis parcels — e.g., Cape Cod Land Bank, Barnstable County, Dennis Conservation Trust, Compact of Cape Cod Conservation Trusts — are only caught if they happen to appear as cross-references or show up in a parcel owner name search.

A narrow grantee sweep for these names (date-windowed, limited to town=DENN) would pull in the same class of documents the town sweep targets.

Summary table
Gap	Impact	Effort
LC parcels trapped in Tier 1	High — active data loss	Low
No name search fallback after Tier 1 miss	High — common for stale deed refs	Low
Date window defaults to 1970	Medium — decades of history missing	Trivial
No name search for Tier 1 parcels	Medium — related instruments missed	Medium
Xrefs only one level per run	Low — caught on re-runs	Medium
No conservation-entity grantee sweeps	Low — edge cases	Medium
The first three are the best return on effort — they're mostly one-line or small logic changes, and they directly affect the completeness of the output