# DCLT Navigator — Claude instructions

## Architecture

This repository is the **Presentation** application for DCLT Navigator.

Discovery and processing now live in the sibling project `dennis-discovery`.

Canonical architecture contract for both projects:
- [CONCEPTUAL_ARCHITECTURE.md](CONCEPTUAL_ARCHITECTURE.md)

**Two databases — never confuse them:**
- `reference.db` — produced and deployed by `dennis-discovery`. Read-only from this app.
- `transactions.db` — lives with this app and is written by user actions. WORM triggers enforce append-only on taggings and notes.

**Tags vs Layers:**
- Tag = user-authored decision, written to `transactions.db`. Defined in `app/dimensions.py`.
- Layer = external/derived/dynamic attribute, lives in `reference.db`. Never written by the app.

**Write path for tags:** `app/tags.py::apply_tag()` → applicability + transition checks via `dimensions.py` → INSERT into `transactions.db`. Reference DB is read-only throughout.

## Testing

Include tests with every code change. New API endpoints go in `app/test_routes.py`, new UI flows in `app/test_ui.py`, DB/schema logic in `app/test_adjudications.py`. Run the relevant test file locally before committing to confirm it passes.

## Commits

Follow the existing commit style: lowercase summary line, no period, body explains why not what. Co-author line required.
