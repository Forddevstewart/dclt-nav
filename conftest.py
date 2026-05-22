import sqlite3
import threading
import pytest
from werkzeug.serving import make_server
from app import create_app

SEED_PARCEL_ID = "P001"
SEED_BOOK = "33445"
SEED_PAGE = "100"


@pytest.fixture(scope="session")
def ref_db_path(tmp_path_factory):
    path = tmp_path_factory.mktemp("ref") / "reference.db"
    conn = sqlite3.connect(str(path))
    conn.executescript(f"""
        CREATE TABLE parcels (
            parcel_id             TEXT PRIMARY KEY,
            site_addr             TEXT,
            owner_name            TEXT,
            owner_category        TEXT,
            property_class        TEXT,
            use_code_norm         TEXT,
            use_code_desc         TEXT,
            totalapprvalue        REAL,
            billingacres          REAL,
            village               TEXT,
            is_public             INTEGER DEFAULT 0,
            condo_units           INTEGER,
            centroid_lat          REAL,
            parcel_class          TEXT    DEFAULT 'parcel',
            parcel_gisid_status   TEXT,
            parcel_massgis_status TEXT,
            parcel_adb_gisid      TEXT,
            join_status           TEXT    DEFAULT 'BOTH',
            locno                 TEXT,
            locst                 TEXT,
            backbone_source       TEXT
        );
        INSERT INTO parcels (parcel_id, site_addr, owner_name, join_status)
        VALUES ('{SEED_PARCEL_ID}', '1 MAIN ST', 'TEST OWNER', 'BOTH');

        CREATE TABLE registry_documents (
            book              TEXT,
            page              TEXT,
            parcel_id         TEXT,
            instrument_type   TEXT,
            recorded_date     TEXT,
            grantor           TEXT,
            grantee           TEXT,
            reverse_party     TEXT,
            relevance         TEXT,
            description       TEXT,
            lookup_method     TEXT,
            search_name       TEXT,
            address           TEXT,
            scan_cached       INTEGER DEFAULT 0,
            doc_amount        REAL,
            doc_rank          INTEGER DEFAULT 0,
            cross_refs        TEXT,
            image_id          TEXT,
            document_number   TEXT,
            doc_type_code     TEXT
        );
        INSERT INTO registry_documents
            (book, page, parcel_id, instrument_type, recorded_date, grantor, grantee, relevance)
        VALUES ('{SEED_BOOK}', '{SEED_PAGE}', '{SEED_PARCEL_ID}', 'DEED', '2020-01-01', 'SELLER, JOHN', 'BUYER, JANE', 'CCR');

        CREATE TABLE parcels_gis (
            parcel_id     TEXT PRIMARY KEY,
            wetlands_code TEXT,
            zone1_type    TEXT,
            zone2_id      TEXT,
            prihab_id     TEXT,
            esthab_id     TEXT,
            natcomm_id    TEXT,
            bm3_vp_id     TEXT,
            bm3_wc_id     TEXT,
            bm3_ch_id     TEXT,
            bm3_cnl_id    TEXT,
            os_site_name  TEXT,
            struct_count  INTEGER
        );
        CREATE TABLE layer_soils (parcel_id TEXT PRIMARY KEY);
    """)
    conn.commit()
    conn.close()
    return str(path)


@pytest.fixture
def app(ref_db_path, tmp_path):
    flask_app = create_app({
        "TESTING": True,
        "DATABASE": str(tmp_path / "transactions.db"),
        "REFERENCE_DATABASE": ref_db_path,
        "UPLOAD_DIR": str(tmp_path / "uploads"),
    })
    yield flask_app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def auth_client(app):
    c = app.test_client()
    with c:
        c.post("/login", data={"username": "ford", "password": "ford"})
    return c


# ── Playwright fixtures ───────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def session_app(ref_db_path, tmp_path_factory):
    """Session-scoped Flask app for Playwright live-server tests."""
    tx = tmp_path_factory.mktemp("tx_ui") / "transactions.db"
    uploads = tmp_path_factory.mktemp("uploads_ui")
    flask_app = create_app({
        "TESTING": True,
        "DATABASE": str(tx),
        "REFERENCE_DATABASE": ref_db_path,
        "UPLOAD_DIR": str(uploads),
    })
    yield flask_app


@pytest.fixture(scope="session")
def live_server_url(session_app):
    """Boot Flask on a random port; yield base URL; shut down after session."""
    server = make_server("127.0.0.1", 0, session_app)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever)
    t.daemon = True
    t.start()
    yield f"http://127.0.0.1:{port}"
    server.shutdown()


@pytest.fixture
def logged_in_page(page, live_server_url):
    """Playwright page already authenticated as ford."""
    page.goto(f"{live_server_url}/login")
    page.fill("#username", "ford")
    page.fill("#password", "ford")
    page.click("button[type='submit']")
    page.wait_for_url(f"{live_server_url}/")
    return page
