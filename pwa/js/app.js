const RECENT_KEY = 'dclt_recent_parcels';
const MAX_RECENT = 10;

let allParcels = [];

async function init() {
  const res = await fetch('/api/pwa/me', { credentials: 'include' });
  if (!res.ok) { location.replace('/pwa/'); return; }

  const me = await res.json();
  document.getElementById('username-display').textContent = me.full_name || me.username;
  document.getElementById('logout-btn').addEventListener('click', logout);
  document.getElementById('search-input').addEventListener('input', onSearch);

  initLocation();
  await loadParcels();
  renderRecents();

  if ('serviceWorker' in navigator) navigator.serviceWorker.register('/pwa/sw.js');
}

// ── Location ────────────────────────────────────────────────────────────────

function initLocation() {
  const bar = document.getElementById('location-bar');
  if (!navigator.geolocation) {
    bar.textContent = 'Location not available in this browser.';
    return;
  }
  navigator.geolocation.getCurrentPosition(
    () => {
      bar.innerHTML = '&#x1F4CD; Location available — parcel detection coming soon.';
    },
    () => {
      bar.innerHTML =
        'Location not enabled. ' +
        '<a id="loc-help">How to enable &rsaquo;</a>';
      document.getElementById('loc-help').addEventListener('click', () => {
        alert(
          'To enable location:\n' +
          'iOS: Settings → Privacy → Location Services → Safari → While Using\n' +
          'Android: browser address bar → Site settings → Location → Allow'
        );
      });
    },
    { timeout: 8000 }
  );
}

// ── Parcel list ─────────────────────────────────────────────────────────────

async function loadParcels() {
  document.getElementById('results').innerHTML = '';
  try {
    const res = await fetch('/api/parcels', { credentials: 'include' });
    if (!res.ok) throw new Error('fetch failed');
    allParcels = await res.json();
  } catch {
    allParcels = [];
    showToast('Could not load parcel data.');
  }
}

// ── Search ───────────────────────────────────────────────────────────────────

function onSearch(e) {
  const q = e.target.value.trim().toLowerCase();
  const resultsEl = document.getElementById('results');
  const recentsEl = document.getElementById('recents-section');

  if (!q) {
    resultsEl.innerHTML = '';
    recentsEl.style.display = '';
    return;
  }

  recentsEl.style.display = 'none';

  const hits = allParcels
    .filter(p =>
      (p.parcel_id  || '').toLowerCase().includes(q) ||
      (p.site_addr  || '').toLowerCase().includes(q)
    )
    .slice(0, 50);

  if (hits.length) {
    resultsEl.innerHTML = hits.map(parcelCard).join('');
    resultsEl.querySelectorAll('.parcel-card').forEach(el =>
      el.addEventListener('click', () => openParcel(el.dataset.id, el.dataset.addr))
    );
  } else {
    resultsEl.innerHTML = '<p class="empty">No parcels match.</p>';
  }
}

// ── Recent parcels ───────────────────────────────────────────────────────────

function renderRecents() {
  const recents  = getRecents();
  const listEl   = document.getElementById('recents-list');
  const sectionEl = document.getElementById('recents-section');

  if (!recents.length) {
    sectionEl.style.display = 'none';
    return;
  }

  listEl.innerHTML = recents.map(parcelCard).join('');
  listEl.querySelectorAll('.parcel-card').forEach(el =>
    el.addEventListener('click', () => openParcel(el.dataset.id, el.dataset.addr))
  );
}

function getRecents() {
  try { return JSON.parse(localStorage.getItem(RECENT_KEY) || '[]'); }
  catch { return []; }
}

function addRecent(p) {
  const list = getRecents().filter(r => r.parcel_id !== p.parcel_id);
  list.unshift({ parcel_id: p.parcel_id, site_addr: p.site_addr, visited_at: new Date().toISOString() });
  localStorage.setItem(RECENT_KEY, JSON.stringify(list.slice(0, MAX_RECENT)));
}

// ── Navigation ───────────────────────────────────────────────────────────────

function openParcel(parcel_id, site_addr) {
  addRecent({ parcel_id, site_addr });
  location.href = `/pwa/parcel.html?id=${encodeURIComponent(parcel_id)}`;
}

// ── Auth ─────────────────────────────────────────────────────────────────────

async function logout() {
  await fetch('/logout', { credentials: 'include' }).catch(() => {});
  location.replace('/pwa/');
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function parcelCard(p) {
  return `<div class="parcel-card" data-id="${esc(p.parcel_id)}" data-addr="${esc(p.site_addr || '')}">
    <div class="parcel-addr">${esc(p.site_addr || '—')}</div>
    <div class="parcel-id">${esc(p.parcel_id)}</div>
  </div>`;
}

function showToast(msg) {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.classList.add('show');
  setTimeout(() => el.classList.remove('show'), 2500);
}

function esc(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

init();
