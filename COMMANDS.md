# DCLT Navigator App Commands

Working directory: `/Users/fordstewart/Projects/dclt-nav`

## Architecture contract

- [CONCEPTUAL_ARCHITECTURE.md](CONCEPTUAL_ARCHITECTURE.md)

## Start app

```bash
source .venv/bin/activate
flask run -p 5001
```

## Run tests

```bash
source .venv/bin/activate
pytest app/test_adjudications.py app/test_routes.py -v
pytest app/test_ui.py -v
```

## WSGI smoke run

```bash
source .venv/bin/activate
python3 wsgi.py
```

## Deploy app code

```bash
git push origin main
```

## Pipeline/data operations

All reference DB builds, CivicTwin updates, OCR, and data deploy tasks now run from:

- `/Users/fordstewart/Projects/dennis-discovery`
