"""
Playwright end-to-end UI tests.

Run: python3 -m pytest app/test_ui.py -sv --headed   (local, headed)
     python3 -m pytest app/test_ui.py -sv             (headless)
"""
from conftest import SEED_PARCEL_ID, SEED_BOOK, SEED_PAGE


# ── Auth ──────────────────────────────────────────────────────────────────────

def test_login_success(page, live_server_url):
    page.goto(f"{live_server_url}/login")
    page.fill("#username", "ford")
    page.fill("#password", "ford")
    page.click("button[type='submit']")
    page.wait_for_url(f"{live_server_url}/")
    # Nav tabs only render once CURRENT_USER is set
    page.wait_for_selector(".nav-tab", state="visible")
    assert page.locator(".nav-tab", has_text="Parcels").is_visible()


def test_login_failure(page, live_server_url):
    page.goto(f"{live_server_url}/login")
    page.fill("#username", "ford")
    page.fill("#password", "wrong")
    page.click("button[type='submit']")
    page.wait_for_selector(".error", state="visible")
    assert "Invalid" in page.locator(".error").inner_text()


# ── Parcels tab ───────────────────────────────────────────────────────────────

def test_parcels_tab_loads_data(logged_in_page):
    logged_in_page.locator(".nav-tab", has_text="Parcels").click()
    logged_in_page.wait_for_selector(".pn-lrow", state="visible")
    assert logged_in_page.locator(".pn-lrow").count() >= 1


def test_parcel_count_label(logged_in_page):
    logged_in_page.locator(".nav-tab", has_text="Parcels").click()
    logged_in_page.wait_for_selector(".pn-lcount", state="visible")
    assert logged_in_page.locator(".pn-lcount").inner_text() == "1"


def test_parcel_search_filters(logged_in_page):
    logged_in_page.locator(".nav-tab", has_text="Parcels").click()
    logged_in_page.wait_for_selector(".pn-lrow", state="visible")
    logged_in_page.fill(".pn-sinput", "XXXXXXXXXXX")
    logged_in_page.wait_for_function("document.querySelector('.pn-lcount').innerText === '0'")
    assert logged_in_page.locator(".pn-lrow").count() == 0


def test_parcel_detail_opens(logged_in_page):
    logged_in_page.locator(".nav-tab", has_text="Parcels").click()
    logged_in_page.wait_for_selector(".pn-lrow", state="visible")
    logged_in_page.locator(".pn-lrow").first.click()
    logged_in_page.wait_for_selector(".pn-hid", state="visible")
    assert logged_in_page.locator(".pn-hid").inner_text() == SEED_PARCEL_ID


# ── Registry tab ──────────────────────────────────────────────────────────────

def test_registry_tab_loads_data(logged_in_page):
    logged_in_page.locator(".nav-tab", has_text="Registry").click()
    logged_in_page.wait_for_selector(".list-row", state="visible")
    assert logged_in_page.locator(".list-row").count() >= 1


def test_registry_row_shows_book_page(logged_in_page):
    logged_in_page.locator(".nav-tab", has_text="Registry").click()
    logged_in_page.wait_for_selector(".list-row", state="visible")
    sub = logged_in_page.locator(".row-sub").first.inner_text()
    assert SEED_BOOK in sub
    assert SEED_PAGE in sub
