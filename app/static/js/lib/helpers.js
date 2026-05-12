// ── Pure helpers (no `this`, no DOM) ─────────────────────────────────────────

function pnDecisionStatus(tag, pd) {
  if (!pd || !pd.tags) return 'undecided';
  const info = pd.tags[tag.name];
  if (info && info.applicable === false) return 'na';
  const state = info ? info.state : null;
  const def   = tag.states_csv.split(',')[0];
  return (state != null && state !== def) ? 'decided' : 'undecided';
}

function pnParcelInfoRows(parcel) {
  if (!parcel) return [];
  const f = v => v !== null && v !== undefined && String(v).trim() !== '';
  const cur = v => f(v) ? String(v) : null;
  const usd = v => f(v) ? '$' + Number(v).toLocaleString() : null;
  const ac  = v => f(v) ? Number(v).toFixed(2) + ' ac' : null;
  return [
    { k:'Owner',    v:cur(parcel.owner_name),     src:'Assessor' },
    { k:'Category', v:cur(parcel.owner_category), src:'Assessor' },
    { k:'Use Code', v:parcel.use_code_norm?(parcel.use_code_norm+(parcel.use_code_desc?' — '+parcel.use_code_desc:'')):null, src:'Assessor' },
    { k:'Zone',     v:cur(parcel.zonedesc),        src:'Assessor' },
    { k:'Village',  v:cur(parcel.village),         src:'Assessor' },
    { k:'Acres',    v:ac(parcel.billingacres),     src:'Assessor', mono:true },
    { k:'Value',    v:usd(parcel.totalapprvalue),  src:'Assessor', mono:true },
  ].filter(r=>r.v!==null);
}

function pnIdentityRows(parcel) {
  if (!parcel) return [];
  const f = v => v !== null && v !== undefined && String(v).trim() !== '';
  const cur = v => f(v) ? String(v) : null;
  const gs = parcel.parcel_gisid_status;
  const ms = parcel.parcel_massgis_status;
  return [
    { k:'Identity State',    v:cur(parcel.identity_state),   src:'GIS / ADB' },
    { k:'Parcel Class',      v:(parcel.parcel_class && parcel.parcel_class !== 'standard') ? cur(parcel.parcel_class) : null, src:'Derived' },
    { k:'GIS ID Status',     v:(gs && gs !== 'matches') ? cur(gs) : null, src:'Derived' },
    { k:'MassGIS Status',    v:(ms && ms !== 'ok')      ? cur(ms) : null, src:'Derived' },
    { k:'ADB GIS ID',        v:(parcel.parcel_adb_gisid && parcel.parcel_adb_gisid !== '') ? cur(parcel.parcel_adb_gisid) : null, src:'Assessor', mono:true },
    { k:'Deed Ref.',         v:(parcel.booklast&&parcel.pagelast)?'Bk '+parcel.booklast+' Pg '+parcel.pagelast:null, src:'Assessor', mono:true },
    { k:'Public',            v:parcel.is_public?'Yes':(parcel.is_public===0?'No':null), src:'Assessor' },
    { k:'Condo Units',       v:(parcel.condo_units>0)?String(parcel.condo_units):null, src:'Assessor' },
  ].filter(r=>r.v!==null);
}

function pnConservationRows(parcel, gis) {
  if (!parcel) return [];
  const f = v => v !== null && v !== undefined && String(v).trim() !== '';
  const ac = v => f(v) ? Number(v).toFixed(2)+' ac' : null;
  return [
    { k:'Open Space',       v:ac(parcel.os_acres),        src:'MassGIS', mono:true },
    { k:'BioMap Core',      v:ac(parcel.bm3_core_acres),  src:'MassGIS', mono:true },
    { k:'BioMap CNL',       v:ac(parcel.bm3_cnl_acres),   src:'MassGIS', mono:true },
    { k:'BioMap Wetland',   v:ac(parcel.bm3_local_acres), src:'MassGIS', mono:true },
    { k:'Zone 2 WHP',       v:ac(parcel.zone2_acres),     src:'MassDEP', mono:true },
    { k:'Zone 1 WHP',       v:(gis&&f(gis.zone1_type))?gis.zone1_type:null, src:'MassDEP' },
    { k:'Priority Habitat', v:parcel.phrs_present!=null?(parcel.phrs_present?'Present':'Not detected'):null, src:'NHESP' },
    { k:'Vernal Pool',      v:parcel.vp_present!=null?(parcel.vp_present?'Present':'Not detected'):null,     src:'NHESP' },
    { k:'Wetlands',         v:(gis&&f(gis.wetlands_code))?gis.wetlands_code:null, src:'MassDEP' },
    { k:'Natural Community',v:(gis&&f(gis.natcomm_name))?gis.natcomm_name:null,   src:'NHESP' },
  ].filter(r=>r.v!==null);
}

function pnSuitabilityRows(parcel) {
  if (!parcel) return [];
  const f = v => v !== null && v !== undefined && String(v).trim() !== '';
  const cur = v => f(v) ? String(v) : null;
  const pct = v => v!=null ? (v*100).toFixed(1)+'%' : null;
  return [
    { k:'Coverage Estimate',   v:cur(parcel.coverage_estimate),       src:'Derived' },
    { k:'Coverage Ratio',      v:pct(parcel.coverage_ratio),          src:'Derived', mono:true },
    { k:'Farming Suitability', v:cur(parcel.farming_suitability),     src:'NRCS SSURGO' },
    { k:'Acq. Suitability',    v:cur(parcel.acquisition_suitability), src:'Scoring' },
    { k:'Undeveloped SC',      v:parcel.is_undeveloped_state_code?'Yes':null, src:'Assessor' },
  ].filter(r=>r.v!==null);
}

function pnAttrGroups(parcel, gis) {
  if (!parcel) return [];
  const f   = v => v !== null && v !== undefined && String(v).trim() !== '';
  const cur = v => f(v) ? String(v) : null;
  const usd = v => f(v) ? '$' + Number(v).toLocaleString() : null;
  const ac  = v => f(v) ? Number(v).toFixed(2) + ' ac' : null;
  const pct = v => v != null ? (v * 100).toFixed(1) + '%' : null;
  const groups = [
    { id:'identity', title:'Identity', rows: (() => {
      const gs = parcel.parcel_gisid_status;
      const ms = parcel.parcel_massgis_status;
      return [
        { k:'Owner',          v:cur(parcel.owner_name),     src:'Assessor' },
        { k:'Owner Category', v:cur(parcel.owner_category), src:'Assessor' },
        { k:'Village',        v:cur(parcel.village),        src:'Assessor' },
        { k:'Zone',           v:cur(parcel.zonedesc),       src:'Assessor' },
        { k:'Use Code',       v:parcel.use_code_norm?(parcel.use_code_norm+(parcel.use_code_desc?' — '+parcel.use_code_desc:'')):null, src:'Assessor' },
        { k:'Identity State', v:cur(parcel.identity_state), src:'GIS / ADB' },
        { k:'Parcel Class',   v:(parcel.parcel_class && parcel.parcel_class !== 'standard') ? cur(parcel.parcel_class) : null, src:'Derived' },
        { k:'GIS ID Status',  v:(gs && gs !== 'matches') ? cur(gs) : null, src:'Derived' },
        { k:'MassGIS Status', v:(ms && ms !== 'ok')      ? cur(ms) : null, src:'Derived' },
        { k:'ADB GIS ID',     v:(parcel.parcel_adb_gisid && parcel.parcel_adb_gisid !== '') ? cur(parcel.parcel_adb_gisid) : null, src:'Assessor', mono:true },
      ].filter(r=>r.v!==null);
    })() },
    { id:'valuation', title:'Valuation', rows: [
      { k:'Appraised Value', v:usd(parcel.totalapprvalue), src:'Assessor', mono:true },
      { k:'Billing Acres',   v:ac(parcel.billingacres),    src:'Assessor', mono:true },
      { k:'Property Class',  v:cur(parcel.property_class), src:'Assessor' },
      { k:'Condo Units',     v:(parcel.condo_units>0)?String(parcel.condo_units):null, src:'Assessor' },
    ].filter(r=>r.v!==null) },
    { id:'conservation', title:'Conservation', rows: [
      { k:'Open Space',      v:ac(parcel.os_acres),        src:'MassGIS', mono:true },
      { k:'BioMap Core',     v:ac(parcel.bm3_core_acres),  src:'MassGIS', mono:true },
      { k:'BioMap CNL',      v:ac(parcel.bm3_cnl_acres),   src:'MassGIS', mono:true },
      { k:'BioMap Wetland',  v:ac(parcel.bm3_local_acres), src:'MassGIS', mono:true },
      { k:'Zone 2 WHP',      v:ac(parcel.zone2_acres),     src:'MassGIS', mono:true },
      { k:'Priority Habitat',v:parcel.phrs_present!=null?(parcel.phrs_present?'Present':'Not detected'):null, src:'NHESP' },
      { k:'Vernal Pool',     v:parcel.vp_present!=null?(parcel.vp_present?'Present':'Not detected'):null,     src:'NHESP' },
      { k:'Wetlands',        v:(gis&&f(gis.wetlands_code))?gis.wetlands_code:null, src:'MassDEP' },
    ].filter(r=>r.v!==null) },
    { id:'suitability', title:'Suitability', rows: [
      { k:'Coverage Estimate',  v:cur(parcel.coverage_estimate),       src:'Derived' },
      { k:'Coverage Ratio',     v:pct(parcel.coverage_ratio),          src:'Derived', mono:true },
      { k:'Farming Suitability',v:cur(parcel.farming_suitability),     src:'NRCS SSURGO' },
      { k:'Acq. Suitability',   v:cur(parcel.acquisition_suitability), src:'Scoring' },
    ].filter(r=>r.v!==null) },
  ];
  return groups.filter(g=>g.rows.length>0);
}

function pnTagDots(p) {
  return [
    { t:'Coverage',    on:p.coverage_estimate&&!['Missing values','Developed',null].includes(p.coverage_estimate), attn:false },
    { t:'Identity',    on:false, attn:p.identity_state&&p.identity_state!=='OK' },
    { t:'Farming',     on:p.farming_suitability&&p.farming_suitability!=='Not suitable', attn:false },
    { t:'Acquisition', on:['Possible','Likely'].includes(p.acquisition_suitability), attn:false },
  ];
}

function coverageEstimate(p) {
  if (!p || p.coverage_status == null) return null;
  if (p.coverage_status === 'no_acreage' || p.coverage_status === 'no_structure') return 'Missing values';
  if (p.coverage_ratio != null && p.coverage_ratio > 0.9) return 'Out of range';
  if (p.coverage_ratio != null && p.coverage_ratio < 0.1) return 'Underdeveloped';
  return 'Developed';
}

function parcelAttrs(p) {
  const cur  = v => (v !== null && v !== undefined && v !== '') ? v : null;
  const usd  = v => v ? '$' + Number(v).toLocaleString() : null;
  const ac   = v => v ? Number(v).toFixed(2) + ' ac' : null;
  const covPct = p.coverage_ratio != null ? (p.coverage_ratio * 100).toFixed(1) + '%' : null;
  return [
    ['Owner',          cur(p.owner_name)],
    ['Owner Category', cur(p.owner_category)],
    ['Village',        cur(p.village)],
    ['Use Code',       p.use_code_norm ? p.use_code_norm + (p.use_code_desc ? ' — ' + p.use_code_desc : '') : null],
    ['Zone',           cur(p.zonedesc)],
    ['IdentityState',  (p.identity_state && p.identity_state !== 'OK') ? p.identity_state : null],
    ['Appraised Value',usd(p.totalapprvalue)],
    ['Billing Acres',  ac(p.billingacres)],
    ['CoverageEstimate', coverageEstimate(p)],
    ['Coverage',         covPct],
    ['Farming Suitability',     (p.farming_suitability     && p.farming_suitability     !== 'Not suitable') ? p.farming_suitability     : null],
    ['Acq. Suitability',        (p.acquisition_suitability && p.acquisition_suitability !== 'Not suitable') ? p.acquisition_suitability : null],
    ['Condo Units',        (p.condo_units > 0) ? p.condo_units : null],
  ].filter(([,v]) => v !== null);
}

function conservationLayers(p) {
  if (!p || p.acquisition_suitability == null) return null;
  const ac   = v => v != null ? Number(v).toFixed(2) + ' ac' : '—';
  const pres = v => v == null  ? '—' : v ? 'Present' : 'Not detected';
  return [
    ['Protected Open Space', ac(p.os_acres)],
    ['BioMap Core Habitat',  ac(p.bm3_core_acres)],
    ['BioMap CNL',           ac(p.bm3_cnl_acres)],
    ['BioMap Local',         ac(p.bm3_local_acres)],
    ['Priority Habitat',     pres(p.phrs_present)],
    ['Zone II WHP',          ac(p.zone2_acres)],
    ['Vernal Pool',          pres(p.vp_present)],
  ];
}

function parcelTagStates(tag, parcel) {
  if (tag.name === 'IdentityResolution' && parcel) {
    const is = parcel.identity_state;
    if (is === 'GIS-only') return ['Unconfirmed', 'ADB Add', 'GIS Remove'];
    if (is === 'ADB-only') return ['Unconfirmed', 'GIS Add', 'ADB Remove'];
  }
  return tag.states_csv.split(',');
}

function gisGroups(gis, soil) {
  if (!gis) return [];
  const f  = v => v !== null && v !== undefined && v !== '';
  const ac = v => (v != null && v !== '') ? Number(v).toFixed(2) + ' ac' : null;
  const farmland = [];
  if (soil) {
    if (soil.prime)     farmland.push(['Prime Farmland',             'Yes']);
    if (soil.statewide) farmland.push(['Farmland of Statewide Imp.','Yes']);
    if (soil.unique)    farmland.push(['Farmland of Unique Imp.',   'Yes']);
    if (soil.not_prime) farmland.push(['Not Prime Farmland',        'Yes']);
  }
  return [
    { key:'zone1',   label:'Zone 1 WHP',              present:f(gis.zone1_type),    fields:[['Type',gis.zone1_type],['Site',gis.zone1_site],['Supplier',gis.zone1_supplier],['Buffer ft',gis.zone1_ft],['PWS ID',gis.zone1_pws_id]] },
    { key:'zone2',   label:'Zone 2 WHP',              present:f(gis.zone2_id),       fields:[['ID',gis.zone2_id],['Supplier',gis.zone2_supplier],['Acres',ac(gis.zone2_acres)],['PWS ID',gis.zone2_pws_id]] },
    { key:'prihab',  label:'Priority Habitat',        present:f(gis.prihab_id),     fields:[['ID',gis.prihab_id],['Version',gis.prihab_version]] },
    { key:'esthab',  label:'Est. Habitat',            present:f(gis.esthab_id),     fields:[['ID',gis.esthab_id],['Version',gis.esthab_version]] },
    { key:'natcomm', label:'Nat. Community',          present:f(gis.natcomm_id),    fields:[['Name',gis.natcomm_name],['Rank',gis.natcomm_rank],['Community',gis.natcomm_community],['Description',gis.natcomm_description]] },
    { key:'bm3vp',   label:'BioMap3 VP',              present:f(gis.bm3_vp_id),    fields:[['ID',gis.bm3_vp_id],['Acres',ac(gis.bm3_vp_acres)]] },
    { key:'bm3wc',   label:'BioMap3 Wetland',         present:f(gis.bm3_wc_id),    fields:[['ID',gis.bm3_wc_id],['Acres',ac(gis.bm3_wc_acres)],['Integrity',gis.bm3_wc_integrity],['Resilience',gis.bm3_wc_resilience]] },
    { key:'bm3ch',   label:'BioMap3 Core',            present:f(gis.bm3_ch_id),    fields:[['ID',gis.bm3_ch_id],['Acres',ac(gis.bm3_ch_acres)],['Town Acres',ac(gis.bm3_ch_town_acres)]] },
    { key:'bm3cnl',  label:'BioMap3 CNL',             present:f(gis.bm3_cnl_id),   fields:[['ID',gis.bm3_cnl_id],['Acres',ac(gis.bm3_cnl_acres)],['Town Acres',ac(gis.bm3_cnl_town_acres)]] },
    { key:'os',      label:'Open Space',              present:f(gis.os_site_name),  fields:[['Site',gis.os_site_name],['Alt Name',gis.os_alt_name],['Owner',gis.os_owner],['Type',gis.os_type],['Purpose',gis.os_purpose],['Access',gis.os_public_access],['Protection',gis.os_protection_level],['Acres',ac(gis.os_acres)],['Manager',gis.os_manager],['Comments',gis.os_comments]] },
    { key:'wetlands',label:'Wetlands',                present:f(gis.wetlands_code), fields:[['Code',gis.wetlands_code],['Description',gis.wetlands_val_desc],['Poly Code',gis.wetlands_poly_code],['Acres',ac(gis.wetlands_acres)]] },
    { key:'struct',  label:'Structures',              present:(gis.struct_count||0)>0, fields:[['Count',gis.struct_count],['Total Sq Ft',gis.struct_total_sqft?Number(gis.struct_total_sqft).toLocaleString():null],['Has Archived',gis.struct_has_archived?'Yes':'No']] },
    { key:'soil',    label:'Soil',                    present:f(gis.soil_name),     fields:[['Name',gis.soil_name],['Component',gis.soil_component],['Drainage',gis.soil_drainage_class],['Farmland Class',gis.soil_farmland_class],['Hydric Rating',gis.soil_hydric_rating],['Hydro Group',gis.soil_hydro_group],['Slope',gis.soil_slope],['Depth to WTbl',gis.soil_depth_to_water_table],['Flooding',gis.soil_flooding],['Ponding',gis.soil_ponding],['Septic',gis.soil_septic],...farmland] },
  ];
}

function gisGroupsSorted(gis, soil) {
  return gisGroups(gis, soil).slice().sort((a, b) => a.label.localeCompare(b.label));
}
