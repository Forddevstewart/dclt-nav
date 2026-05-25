# DCLT Navigator App Runbook

## Scope

This repository runs the Flask presentation app and manages `transactions.db`.

`reference.db` and CivicTwin content are produced and deployed by the sibling project:

- `/Users/fordstewart/Projects/dennis-discovery`

Canonical architecture reference:

- [CONCEPTUAL_ARCHITECTURE.md](CONCEPTUAL_ARCHITECTURE.md)

## Data ownership

- `reference.db`: read-only in this app
- `transactions.db`: write path for tags, notes, campaigns, usage, and adjudications

## Local development

```bash
cd /Users/fordstewart/Projects/dclt-nav
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
flask run -p 5001
```

## Tests

```bash
cd /Users/fordstewart/Projects/dclt-nav
source .venv/bin/activate
pytest app/test_adjudications.py app/test_routes.py -v
pytest app/test_ui.py -v
```

## Deploy app code

```bash
git push origin main
```

GitHub Actions deploys code to VPS and restarts `dclt-nav`.

## Environment

- `REFERENCE_DATABASE`: absolute path to read-only reference DB
- `CIVICTWIN_ROOT`: CivicTwin root for local read-side file access (PDFs, GIS)
- `SECRET_KEY`: Flask secret key
- `DCLT_ENV`: `production` on VPS
