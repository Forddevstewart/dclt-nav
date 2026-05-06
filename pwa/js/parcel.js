const parcelId = new URLSearchParams(location.search).get('id');
let farmingTagId = null;
let selectedFile  = null;

async function init() {
  if (!parcelId) { location.replace('/pwa/app.html'); return; }

  const res = await fetch('/api/pwa/me', { credentials: 'include' });
  if (!res.ok) { location.replace('/pwa/'); return; }

  document.getElementById('back-btn').addEventListener('click', () => {
    history.length > 1 ? history.back() : location.replace('/pwa/app.html');
  });

  initFileInputs();
  initUploadBtn();

  await Promise.all([loadParcel(), loadFarmingTag(), loadUploads()]);

  if ('serviceWorker' in navigator) navigator.serviceWorker.register('/pwa/sw.js');
}

// ── Parcel info ──────────────────────────────────────────────────────────────

async function loadParcel() {
  try {
    const res  = await fetch('/api/parcels', { credentials: 'include' });
    const list = await res.json();
    const p    = list.find(x => x.parcel_id === parcelId);
    if (!p) { document.getElementById('parcel-title').textContent = parcelId; return; }

    document.title = `${p.site_addr || parcelId} — DCLT Field`;
    document.getElementById('parcel-title').textContent = p.site_addr || parcelId;
    const parts = [parcelId];
    if (p.owner_name)   parts.push(p.owner_name);
    if (p.billingacres) parts.push(`${p.billingacres} ac`);
    document.getElementById('parcel-meta').textContent = parts.join(' · ');
  } catch {
    document.getElementById('parcel-title').textContent = parcelId;
  }
}

// ── File selection ────────────────────────────────────────────────────────────

function initFileInputs() {
  const pairs = [
    ['btn-camera',  'input-camera'],
    ['btn-library', 'input-library'],
    ['btn-file',    'input-file'],
  ];

  for (const [btnId, inputId] of pairs) {
    document.getElementById(btnId).addEventListener('click', () =>
      document.getElementById(inputId).click()
    );
    document.getElementById(inputId).addEventListener('change', e => {
      const file = e.target.files[0];
      if (!file) return;
      selectedFile = file;
      const sel = document.getElementById('file-selected');
      sel.textContent = `${file.name}  (${(file.size / 1024).toFixed(1)} KB)`;
      sel.classList.remove('hidden');
      // clear sibling inputs so only one file is active
      for (const [, otherId] of pairs) {
        if (otherId !== inputId) document.getElementById(otherId).value = '';
      }
    });
  }
}

// ── Upload form ───────────────────────────────────────────────────────────────

function initUploadBtn() {
  document.getElementById('upload-btn').addEventListener('click', submitUpload);
}

async function submitUpload() {
  const noteText = document.getElementById('note-text').value.trim();
  const errEl    = document.getElementById('upload-error');
  const btn      = document.getElementById('upload-btn');
  errEl.classList.add('hidden');

  if (!selectedFile && !noteText) {
    errEl.textContent = 'Select a file or enter a note.';
    errEl.classList.remove('hidden');
    return;
  }

  const docType = selectedFile ? 'photo' : 'note';
  const form    = new FormData();
  form.append('parcel_id', parcelId);
  form.append('doc_type',  docType);
  if (selectedFile) form.append('file', selectedFile);
  if (noteText)     form.append('note_text', noteText);

  btn.disabled    = true;
  btn.textContent = 'Saving…';

  try {
    const res = await fetch('/api/uploads', {
      method: 'POST',
      credentials: 'include',
      body: form,
    });
    if (!res.ok) {
      const d = await res.json().catch(() => ({}));
      throw new Error(d.error || `Server error ${res.status}`);
    }
    showToast('Document saved.');
    document.getElementById('note-text').value = '';
    selectedFile = null;
    document.getElementById('file-selected').classList.add('hidden');
    for (const id of ['input-camera', 'input-library', 'input-file']) {
      document.getElementById(id).value = '';
    }
    await loadUploads();
  } catch (err) {
    errEl.textContent = err.message || 'Upload failed — please try again.';
    errEl.classList.remove('hidden');
  } finally {
    btn.disabled    = false;
    btn.textContent = 'Save Document';
  }
}

// ── Uploads list ──────────────────────────────────────────────────────────────

async function loadUploads() {
  const el = document.getElementById('uploads-list');
  try {
    const res   = await fetch(`/api/uploads/parcel/${encodeURIComponent(parcelId)}`, { credentials: 'include' });
    const items = await res.json();
    if (!items.length) {
      el.innerHTML = '<p class="empty">No documents yet.</p>';
      return;
    }
    el.innerHTML = items.map(uploadItem).join('');
  } catch {
    el.innerHTML = '<p class="empty">Failed to load documents.</p>';
  }
}

function uploadItem(u) {
  const date = new Date(u.created_at).toLocaleDateString('en-US', {
    month: 'short', day: 'numeric', year: 'numeric',
  });
  const isImage = u.mime_type && u.mime_type.startsWith('image/');
  const icon = u.doc_type === 'note' ? '&#x1F4DD;'
             : isImage               ? '&#x1F4F7;'
             :                        '&#x1F4CE;';
  const thumb = isImage && u.filename
    ? `<img class="upload-thumb" src="/api/uploads/${esc(u.upload_id)}/file" alt="photo" loading="lazy">`
    : `<div class="upload-thumb-icon">${icon}</div>`;
  const text = u.note_text ? `<div class="upload-item-text">${esc(u.note_text)}</div>` : '';
  return `
    <div class="upload-item">
      ${thumb}
      <div class="upload-item-body">
        <div class="upload-item-type">${esc(u.doc_type)}</div>
        ${text}
        <div class="upload-item-meta">${esc(u.username)} &middot; ${date}</div>
      </div>
    </div>`;
}

// ── FarmingDetermination tag ─────────────────────────────────────────────────

async function loadFarmingTag() {
  const el = document.getElementById('farming-form');
  try {
    const res  = await fetch(`/api/tagging/parcel/${encodeURIComponent(parcelId)}`, { credentials: 'include' });
    const data = await res.json();
    const tag  = data.tags.find(t => t.name === 'FarmingDetermination');
    if (!tag) {
      el.innerHTML = '<p class="empty">FarmingDetermination not configured.</p>';
      return;
    }
    farmingTagId = tag.tag_id;
    const current = data.current[String(tag.tag_id)];
    const state   = current ? current.state : 'Unconfirmed';
    renderFarmingPicker(el, tag.states_csv.split(','), state);
  } catch {
    el.innerHTML = '<p class="empty">Failed to load determination.</p>';
  }
}

function renderFarmingPicker(el, states, currentState) {
  el.innerHTML = `
    <div class="tag-states">
      ${states.map(s => `
        <label class="tag-state-option${s === currentState ? ' selected' : ''}">
          <input type="radio" name="farming" value="${esc(s)}"${s === currentState ? ' checked' : ''}>
          ${esc(s)}
        </label>`).join('')}
    </div>
    <button class="btn-primary" id="save-tag-btn">Save Determination</button>
    <p id="tag-msg" class="tag-saved-msg hidden"></p>`;

  el.querySelectorAll('.tag-state-option').forEach(label => {
    label.querySelector('input').addEventListener('change', () => {
      el.querySelectorAll('.tag-state-option').forEach(l => l.classList.remove('selected'));
      label.classList.add('selected');
    });
  });

  document.getElementById('save-tag-btn').addEventListener('click', saveTag);
}

async function saveTag() {
  const btn     = document.getElementById('save-tag-btn');
  const msgEl   = document.getElementById('tag-msg');
  const checked = document.querySelector('input[name="farming"]:checked');
  if (!checked || !farmingTagId) return;

  btn.disabled    = true;
  btn.textContent = 'Saving…';
  msgEl.classList.add('hidden');

  try {
    const res = await fetch('/api/tagging', {
      method: 'POST',
      credentials: 'include',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        tag_id:      farmingTagId,
        state:       checked.value,
        target_type: 'parcel',
        target_id:   parcelId,
      }),
    });
    if (!res.ok) {
      const d = await res.json().catch(() => ({}));
      throw new Error(d.error || `Error ${res.status}`);
    }
    msgEl.textContent = 'Saved.';
    msgEl.classList.remove('hidden');
    showToast('Determination saved.');
  } catch (err) {
    msgEl.textContent = err.message;
    msgEl.classList.remove('hidden');
  } finally {
    btn.disabled    = false;
    btn.textContent = 'Save Determination';
  }
}

// ── Helpers ────────────────────────────────────────────────────────────────────

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
