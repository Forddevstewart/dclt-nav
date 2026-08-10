// ── Parcel and campaign Leaflet map managers ──────────────────────────────────

const _WMS = 'https://gis-prod.digital.mass.gov/geoserver/wms';

// Maps parcels_gis column key → WMS config. Only layers with a WMS equivalent.
const _GIS_WMS = {
  zone1_type:   { label: 'Zone 1 WHP',              wmsLayer: 'massgis:GISDATA.ZONE1_POLY_DISSOLVE' },
  zone2_id:     { label: 'Zone 2 WHP',              wmsLayer: 'massgis:GISDATA.ZONE2_POLY_DISSOLVE' },
  prihab_id:    { label: 'Priority Habitat',         wmsLayer: 'massgis:GISDATA.PRIHAB_POLY' },
  esthab_id:    { label: 'Estimated Habitat',        wmsLayer: 'massgis:GISDATA.ESTHAB_POLY' },
  natcomm_id:   { label: 'Natural Community',        wmsLayer: 'massgis:GISDATA.NATCOMM_POLY' },
  bm3_vp_id:    { label: 'BioMap3 Vernal Pool',      wmsLayer: 'massgis:GISDATA.BM3_CH_VERNAL_POOLS_CORE' },
  bm3_wc_id:    { label: 'BioMap3 Wetland Corridor', wmsLayer: 'massgis:GISDATA.BM3_LOCAL_WETLANDS' },
  os_site_name: { label: 'Open Space',               wmsLayer: 'massgis:GISDATA.OPENSPACE_POLY' },
  wetlands_code:{ label: 'Wetlands',                 wmsLayer: 'massgis:GISDATA.WETLANDSDEP_POLY',
                  styles: 'GISDATA.WETLANDSDEP_POLY::General_Categories_Max_24000' },
  cvp_id:       { label: 'Certified Vernal Pool',    wmsLayer: 'massgis:GISDATA.CVP_PT' },
};

// ── Parcel detail map ─────────────────────────────────────────────────────────

let _lmap        = null;
let _lLayer      = null;
let _lWmsControl = null;
let _lWmsLayers  = {};

function _initParcelMap() {
  if (_lmap) return;
  _lmap = L.map('parcel-map', { zoomControl: true });
  L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 20,
    attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
  }).addTo(_lmap);
  new ResizeObserver(() => _lmap.invalidateSize()).observe(document.getElementById('parcel-map'));
}

function _clearWmsLayers() {
  Object.values(_lWmsLayers).forEach(l => _lmap.removeLayer(l));
  _lWmsLayers = {};
  if (_lWmsControl) { _lWmsControl.remove(); _lWmsControl = null; }
}

async function _updateParcelMap(parcelId, centroidLat, centroidLon, gis) {
  _initParcelMap();
  // Wait for layout before measuring container so invalidateSize sees real dimensions.
  await new Promise(r => requestAnimationFrame(r));
  _lmap.invalidateSize();

  if (_lLayer) { _lmap.removeLayer(_lLayer); _lLayer = null; }
  _clearWmsLayers();

  try {
    const resp = await fetch('/api/parcels/' + encodeURIComponent(parcelId) + '/geometry');
    if (resp.ok) {
      const feature = await resp.json();
      _lLayer = L.geoJSON(feature, {
        style: { color: '#2563eb', weight: 2, fillColor: '#3b82f6', fillOpacity: 0.2 },
      }).addTo(_lmap);
      _lmap.fitBounds(_lLayer.getBounds(), { maxZoom: 18, padding: [20, 20] });
    }
  } catch(_) {}

  if (!_lLayer && centroidLat && centroidLon) {
    _lLayer = L.marker([centroidLat, centroidLon]).addTo(_lmap);
    _lmap.setView([centroidLat, centroidLon], 17);
  }

  if (!_lLayer) {
    _lmap.setView([41.7352, -70.1939], 13);
  }

  const overlays = {};
  for (const [key, cfg] of Object.entries(_GIS_WMS)) {
    const val = gis && gis[key];
    if (val === null || val === undefined || val === '' || val === 0) continue;
    const opts = {
      layers: cfg.wmsLayer, format: 'image/png',
      transparent: true, version: '1.1.1', attribution: 'MassGIS',
    };
    if (cfg.styles) opts.styles = cfg.styles;
    const layer = L.tileLayer.wms(_WMS, opts);
    _lWmsLayers[key] = layer;
    overlays[cfg.label] = layer;
  }
  if (Object.keys(overlays).length > 0) {
    _lWmsControl = L.control.layers(null, overlays, { collapsed: false }).addTo(_lmap);
  }
}

// ── Campaign work-queue map ───────────────────────────────────────────────────

let _cmap        = null;
let _cLayer      = null;
let _cWmsControl = null;
let _cWmsLayers  = {};

function _initCampaignMap() {
  if (_cmap) return;
  _cmap = L.map('campaign-map', { zoomControl: true });
  L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 20,
    attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
  }).addTo(_cmap);
  new ResizeObserver(() => _cmap && _cmap.invalidateSize()).observe(
    document.getElementById('campaign-map')
  );
}

function _clearCwWmsLayers() {
  Object.values(_cWmsLayers).forEach(l => _cmap.removeLayer(l));
  _cWmsLayers = {};
  if (_cWmsControl) { _cWmsControl.remove(); _cWmsControl = null; }
}

async function _updateCampaignMap(parcelId, centroidLat, centroidLon, gis) {
  _initCampaignMap();
  await new Promise(r => requestAnimationFrame(r));
  _cmap.invalidateSize();
  if (_cLayer) { _cmap.removeLayer(_cLayer); _cLayer = null; }
  _clearCwWmsLayers();
  try {
    const resp = await fetch('/api/parcels/' + encodeURIComponent(parcelId) + '/geometry');
    if (resp.ok) {
      const feature = await resp.json();
      _cLayer = L.geoJSON(feature, {
        style: { color: '#2563eb', weight: 2, fillColor: '#3b82f6', fillOpacity: 0.2 },
      }).addTo(_cmap);
      _cmap.fitBounds(_cLayer.getBounds(), { maxZoom: 18, padding: [20, 20] });
    }
  } catch(_) {}
  if (!_cLayer && centroidLat && centroidLon) {
    _cLayer = L.marker([centroidLat, centroidLon]).addTo(_cmap);
    _cmap.setView([centroidLat, centroidLon], 17);
  }
  if (!_cLayer) { _cmap.setView([41.7352, -70.1939], 13); }
  const overlays = {};
  for (const [key, cfg] of Object.entries(_GIS_WMS)) {
    const val = gis && gis[key];
    if (val === null || val === undefined || val === '' || val === 0) continue;
    const opts = { layers: cfg.wmsLayer, format: 'image/png', transparent: true, version: '1.1.1', attribution: 'MassGIS' };
    if (cfg.styles) opts.styles = cfg.styles;
    const layer = L.tileLayer.wms(_WMS, opts);
    _cWmsLayers[key] = layer;
    overlays[cfg.label] = layer;
  }
  if (Object.keys(overlays).length > 0) {
    _cWmsControl = L.control.layers(null, overlays, { collapsed: false }).addTo(_cmap);
  }
}
