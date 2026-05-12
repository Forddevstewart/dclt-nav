"""
Flask route and API endpoint tests.

Run: python3 -m pytest app/test_routes.py -sv
"""
import pytest
from conftest import SEED_PARCEL_ID, SEED_BOOK, SEED_PAGE


# ── HTML routes ───────────────────────────────────────────────────────────────

def test_index_loads(client):
    r = client.get("/")
    assert r.status_code == 200


def test_login_page_loads(client):
    r = client.get("/login")
    assert r.status_code == 200


# ── Auth ──────────────────────────────────────────────────────────────────────

def test_login_valid_credentials(client):
    r = client.post("/login", data={"username": "ford", "password": "ford"},
                    follow_redirects=True)
    assert r.status_code == 200


def test_login_invalid_credentials(client):
    r = client.post("/login", data={"username": "ford", "password": "wrong"})
    assert r.status_code == 200
    assert b"Invalid" in r.data


# ── /api/parcels ──────────────────────────────────────────────────────────────

def test_parcels_list_ok(client):
    r = client.get("/api/parcels")
    assert r.status_code == 200
    assert r.is_json
    data = r.get_json()
    assert isinstance(data, list)


def test_parcels_list_contains_seed(client):
    r = client.get("/api/parcels")
    ids = [p["parcel_id"] for p in r.get_json()]
    assert SEED_PARCEL_ID in ids


def test_parcel_detail_found(client):
    r = client.get(f"/api/parcels/{SEED_PARCEL_ID}")
    assert r.status_code == 200
    body = r.get_json()
    assert body["parcel"]["parcel_id"] == SEED_PARCEL_ID
    assert "documents" in body
    assert "tags" in body


def test_parcel_detail_not_found(client):
    r = client.get("/api/parcels/DOES_NOT_EXIST")
    assert r.status_code == 404


# ── /api/documents ────────────────────────────────────────────────────────────

def test_documents_list_ok(client):
    r = client.get("/api/documents")
    assert r.status_code == 200
    assert r.is_json
    assert isinstance(r.get_json(), list)


def test_document_detail_found(client):
    r = client.get(f"/api/documents/{SEED_BOOK}/{SEED_PAGE}")
    assert r.status_code == 200
    body = r.get_json()
    assert body["document"]["book"] == SEED_BOOK
    assert body["document"]["page"] == SEED_PAGE


def test_document_detail_not_found(client):
    r = client.get("/api/documents/0/0")
    assert r.status_code == 404


# ── /api/tags ─────────────────────────────────────────────────────────────────

def test_tags_list_ok(client):
    r = client.get("/api/tags")
    assert r.status_code == 200
    assert r.is_json
    assert isinstance(r.get_json(), list)


def test_tags_list_filter_by_entity(client):
    r = client.get("/api/tags?entity=parcel")
    assert r.status_code == 200
    tags = r.get_json()
    entities = {t["target_entity"] for t in tags}
    assert entities <= {"parcel", "any"}


# ── /api/tagging ──────────────────────────────────────────────────────────────

def test_apply_tag_requires_login(client):
    r = client.post("/api/tagging", json={
        "tag_id": 1, "state": "x", "target_type": "parcel", "target_id": SEED_PARCEL_ID,
    })
    assert r.status_code == 302


def test_apply_tag_with_auth(auth_client):
    tags = auth_client.get("/api/tags?entity=parcel").get_json()
    # CoverageDetermination is always applicable — pick it
    tag = next(t for t in tags if t["name"] == "CoverageDetermination")
    first_state = tag["states_csv"].split(",")[0]

    r = auth_client.post("/api/tagging", json={
        "tag_id":      tag["tag_id"],
        "state":       first_state,
        "target_type": "parcel",
        "target_id":   SEED_PARCEL_ID,
    })
    assert r.status_code == 200
    assert r.get_json()["ok"] is True
