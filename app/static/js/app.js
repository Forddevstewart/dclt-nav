    // ── Layer attribute filter chips (read from External/Derived Layer columns) ─
    // values: available picker options. 1 = boolean "Present"; otherwise exact column values.
    // attr_id links each chip to layer_attributes for display_group lookup.
    // col/key is the field name in the API response (may differ from DB col_name).
    const _PARCEL_LAYER_CHIPS = [
      {attr_id: 'identity.identity_state',          label: 'Identity State',          col: 'identity_state',            values: ['OK','ADB-only','GIS-only']},
      {attr_id: 'gis.zone1_type',                   label: 'Zone 1 WHP',              col: 'has_zone1',                 values: [1]},
      {attr_id: 'gis.zone2_id',                     label: 'Zone 2 WHP',              col: 'has_zone2',                 values: [1]},
      {attr_id: 'gis.prihab_id',                    label: 'Priority Habitat',        col: 'has_prihab',                values: [1]},
      {attr_id: 'gis.esthab_id',                    label: 'Est. Habitat',            col: 'has_esthab',                values: [1]},
      {attr_id: 'gis.natcomm_id',                   label: 'Nat. Community',          col: 'has_natcomm',               values: [1]},
      {attr_id: 'gis.bm3_ch_id',                    label: 'BioMap3',                 col: 'has_bm3',                   values: [1]},
      {attr_id: 'gis.os_site_name',                 label: 'Open Space',              col: 'has_openspace',             values: [1]},
      {attr_id: 'gis.wetlands_code',                label: 'Wetlands',                col: 'has_wetlands',              values: [1]},
      {attr_id: 'for_sale.for_sale',                label: 'For Sale',                col: 'for_sale',                  values: [1]},
      {attr_id: 'usc.is_undeveloped_state_code',    label: 'Undeveloped SC',          col: 'is_undeveloped_state_code', values: [1]},
      {attr_id: 'farming.farming_suitability',      label: 'Farming Suitability',     col: 'farming_suitability',       values: ['Not suitable','Possible','Suitable']},
      {attr_id: 'acquisition.acquisition_suitability', label: 'Acquisition Suitability', col: 'acquisition_suitability', values: ['Not suitable','Possible','Likely']},
      {attr_id: null, display_group: 'Coverage',   label: 'Coverage Estimate',       col: 'coverage_estimate',         values: ['Missing values','Out of range','Underdeveloped','Developed']},
    ];

    const _DOC_LAYER_CHIPS = [
      {attr_id: 'ocr.kw_conservation_restriction',              label: 'Conservation Restriction', key: 'kw_conservation_restriction'},
      {attr_id: 'ocr.kw_article_97',                            label: 'Article 97',               key: 'kw_article_97'},
      {attr_id: 'ocr.kw_deed_restriction',                      label: 'Deed Restriction',         key: 'kw_deed_restriction'},
      {attr_id: 'ocr.kw_chapter_61',                            label: 'Chapter 61',               key: 'kw_chapter_61'},
      {attr_id: 'ocr.kw_agricultural_preservation_restriction', label: 'Ag. Pres. Restriction',    key: 'kw_agricultural_preservation_restriction'},
      {attr_id: 'ocr.kw_perpetual_restriction',                 label: 'Perpetual Restriction',    key: 'kw_perpetual_restriction'},
      {attr_id: 'ocr.kw_ccr',                                   label: 'CC&R',                     key: 'kw_ccr'},
    ];

    // ── Keyword metadata (priority order, for detail view score bars) ─────────
    const KW_ORDER = [
      { key:'conservation_restriction',             label:'Conservation Restriction', abbr:'CR',  cls:'cr'  },
      { key:'article_97',                           label:'Article 97',               abbr:'A97', cls:'a97' },
      { key:'deed_restriction',                     label:'Deed Restriction',         abbr:'DR',  cls:'dr'  },
      { key:'chapter_61',                           label:'Chapter 61',               abbr:'C61', cls:'c61' },
      { key:'agricultural_preservation_restriction',label:'Ag. Preservation',         abbr:'APR', cls:'apr' },
      { key:'perpetual_restriction',                label:'Perpetual Restriction',    abbr:'PR',  cls:'pr'  },
      { key:'ccr',                                  label:'CC&R',                     abbr:'CCR', cls:'ccr' },
    ];
    const KW_THRESHOLD = 0.4;

    // ── Pure helpers (no `this`) ──────────────────────────────────────────────
    // ── Parcel Navigator helpers ──────────────────────────────────────────────

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

    // ── Parcel map (Leaflet, managed outside Alpine reactivity) ──────────────
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
      bm3_ch_id:    { label: 'BioMap3 Core Habitat',     wmsLayer: 'massgis:GISDATA.BM3_CORE_HABITAT' },
      bm3_cnl_id:   { label: 'BioMap3 CNL',              wmsLayer: 'massgis:GISDATA.BM3_CRITICAL_NATURAL_LANDSCAPE' },
      os_site_name: { label: 'Open Space',               wmsLayer: 'massgis:GISDATA.OPENSPACE_POLY' },
      wetlands_code:{ label: 'Wetlands',                 wmsLayer: 'massgis:GISDATA.WETLANDSDEP_POLY',
                      styles: 'GISDATA.WETLANDSDEP_POLY::General_Categories_Max_24000' },
    };

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
      // Wait for the browser to finish layout before measuring the container,
      // so invalidateSize() sees the real dimensions before fitBounds() runs.
      await new Promise(r => requestAnimationFrame(r));
      _lmap.invalidateSize();

      if (_lLayer) { _lmap.removeLayer(_lLayer); _lLayer = null; }
      _clearWmsLayers();

      // parcel polygon
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

      // fallback: centroid pin
      if (!_lLayer && centroidLat && centroidLon) {
        _lLayer = L.marker([centroidLat, centroidLon]).addTo(_lmap);
        _lmap.setView([centroidLat, centroidLon], 17);
      }

      // no location at all — reset to Dennis town view
      if (!_lLayer) {
        _lmap.setView([41.7352, -70.1939], 13);
      }

      // WMS layers present on this parcel, listed but unchecked
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

    // ── Campaign work-queue map (separate Leaflet instance from parcel map) ──────
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
      // WMS overlays for GIS layers present on this parcel
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

    // ── Alpine component ──────────────────────────────────────────────────────
    function app() {
      return {
        // nav
        activeTab: 'overview',
        KW_ORDER,

        // attribute registry (loaded from /api/meta/attributes)
        attrRegistry: [],
        attrById:     {},

        // parcel navigator UI state
        pnExpandMode:    null,    // 'map' | 'doc' | null — which panel fills center+right
        pnCampaignsOpen: true,    // decisions/campaigns filter accordion
        pnAttrsOpen:     false,   // attribute filter accordion

        // campaigns
        campaignProgress:      null,
        campaignView:          'inventory',  // 'inventory' | 'work'
        selectedCampaignTag:   null,
        activeCampaignId:      null,
        activeCampaign:        null,
        adminCampaigns:        [],
        adminCampaignsLoading: false,
        campaignMetaByDim:     {},   // dimension → {campaign_id, label, scope, color}
        campaignMetaLoaded:    false,

        // campaign work queue (identity)
        cwQueue:          [],
        cwQueueLoading:   false,
        cwIdx:            0,
        cwDecisions:      {},     // parcelId → state string
        cwFilter:         'all',  // 'all' | 'adb_only' | 'gis_only'
        cwFilterQuery:    '',
        cwParcelDetail:   null,
        cwDetailLoading:  false,
        cwDetailError:    null,
        cwChipOpen:       false,
        cwIdentityDecisions: [
          { key: 'ADB Add',    hint: 'Add owner record to assessor database' },
          { key: 'ADB Remove', hint: 'Retire stale assessor record' },
          { key: 'GIS Add',    hint: 'Add parcel polygon to GIS layer' },
          { key: 'GIS Remove', hint: 'Retire spurious GIS polygon' },
        ],

        // overview
        overviewData:    null,
        overviewLoading: false,

        // identity reports
        reportsData:    null,
        reportsLoading: false,

        // assets sub-tab
        assetsSubtab: 'reference',

        // parcels
        allParcels:          [],
        parcelsLoading:      true,
        parcelSearch:        '',
        parcelSort:          'site_addr',
        parcelSortDir:       'asc',
        parcelPage:          1,
        parcelPageSize:      50,
        selectedParcelId:    null,
        parcelDetail:        null,
        parcelDetailLoading: false,
        parcelDetailError:   null,
        parcelSelectedDoc:     null,
        parcelPdfUrl:          null,
        parcelSelectedTownDoc: null,
        parcelUploads:         [],
        parcelSelectedUpload:  null,
        parcelTagFilters:    {},   // tagId → [selectedStates]
        parcelTagFoldMaps:   {},   // tagId → {parcelId: state}
        parcelLayerFilters:  {},   // col   → [selectedValues]
        filterChipPickerOpen: null,

        // registry
        allRegistry:       [],
        regLoading:        false,
        docSearch:         '',
        docTypeFilters:    [],   // [instrumentType, …]
        docTagFilters:     {},   // tagId → [selectedStates]
        docTagFoldMaps:    {},   // tagId → {docId: state}
        docLayerFilters:   {},   // key   → [bucket label, …]
        docSort:           'recorded_date',
        docSortDir:        'desc',
        regPage:           1,
        regPageSize:       50,
        docSelectedBP:     null,
        docDetail:         null,
        docDetailLoading:  false,
        docDetailError:    null,
        docPdfUrl:         null,



        // admin — users
        adminUsers:        [],
        adminUsersLoading: false,
        adminSelected:     null,
        adminAction:       null,
        adminPw:            { password:'', confirm:'', saving:false, msg:'', err:'' },
        adminName:          { full_name:'', saving:false, msg:'', err:'' },
        adminRole:          { role:'user', saving:false, msg:'', err:'' },
        adminDeleteConfirm: false,
        adminNewUser:       { username:'', full_name:'', password:'', role:'user', saving:false, msg:'', err:'' },

        // admin — tags
        adminSection:    'tags',
        allAdminTags:    [],
        adminTagsLoading:false,
        selectedTag:     null,
        tagEditForm:     { isNew:false, name:'', states:[], statesRaw:'', newState:'', deprecated:false, displayOrder:0, saving:false, deleting:false, err:'', msg:'' },
        tagConfirmDialog:null,

        // town
        townSection:         'town_doc_links',
        townDocList:         [],
        townDocListLoading:  false,
        townCommitteeFilter: '',
        townStatusFilter:    'candidate',
        townDocPage:         1,
        townDocPageSize:     50,
        selectedTownDocId:   null,
        townDocDetail:       null,
        townDetailLoading:   false,
        townLinkPending:     {},
        townPdfUrl:          null,
        townOcrOpen:         false,
        townAdjRequest:      null,   // set by x-if outer div @click; watched in init()
        pickerQuery:            '',
        townDocsOverview:       null,
        parcelTownDocs:         null,

        // user-facing tags
        allTags:       [],
        tagsLoading:   false,
        parcelTagState:{},
        docTagState:   {},
        tagPickerOpen: null,
        tagPending:    {},

        // admin — usage
        usageLog:     [],
        usageLoading: false,
        usageSearch:  '',
        usageSort:    'seq',
        usageSortDir: 'desc',
        usageCols: [
          { key:'ts',         label:'Timestamp' },
          { key:'username',   label:'User'      },
          { key:'session_id', label:'Session'   },
          { key:'event_type', label:'Event'     },
          { key:'api_call',   label:'API Call'  },
          { key:'details',    label:'Details'   },
          { key:'ip',         label:'IP'        },
          { key:'user_agent', label:'Agent'     },
        ],

        // ── Computed — usage ────────────────────────────────────────────────
        get filteredUsage() {
          const q = this.usageSearch.trim().toLowerCase();
          const KEYS = ['username','session_id','event_type','api_call','details','ip','user_agent'];
          let list = q
            ? this.usageLog.filter(r => KEYS.some(k => (r[k]||'').toLowerCase().includes(q)))
            : this.usageLog;
          const f = this.usageSort, dir = this.usageSortDir==='asc' ? 1 : -1;
          return [...list].sort((a,b) =>
            dir * String(a[f]??'').localeCompare(String(b[f]??''), undefined, {numeric:true})
          );
        },

        // ── Computed — tag filter options ───────────────────────────────────
        get allDocTagOptions() {
          return this.allTags.filter(t => t.target_entity === 'document' || t.target_entity === 'any');
        },
        get allParcelTagOptions() {
          return this.allTags.filter(t => t.target_entity === 'parcel' || t.target_entity === 'any');
        },
        get campaignSortedTags() {
          return [...this.allTags].sort((a, b) => {
            const oa = this.campaignMetaByDim[a.name]?.display_order ?? 9999;
            const ob = this.campaignMetaByDim[b.name]?.display_order ?? 9999;
            return oa - ob;
          });
        },

        // ── Computed — attribute chip groups ───────────────────────────────
        get parcelLayerChipGroups() {
          const groups = new Map();
          for (const chip of _PARCEL_LAYER_CHIPS) {
            const grp = (chip.attr_id ? this.attrById[chip.attr_id]?.display_group : null)
                     || chip.display_group || 'Other';
            if (!groups.has(grp)) groups.set(grp, { display_group: grp, chips: [] });
            groups.get(grp).chips.push(chip);
          }
          return [...groups.values()];
        },
        get docLayerChipGroups() {
          const groups = new Map();
          for (const chip of _DOC_LAYER_CHIPS) {
            const grp = (chip.attr_id ? this.attrById[chip.attr_id]?.display_group : null)
                     || 'Legal Restrictions';
            if (!groups.has(grp)) groups.set(grp, { display_group: grp, chips: [] });
            groups.get(grp).chips.push(chip);
          }
          return [...groups.values()];
        },

        // ── Computed — parcels ──────────────────────────────────────────────
        get filteredParcels() {
          let list = this.allParcels;
          // Layer attribute filters: col → [selectedValues]
          for (const [col, selected] of Object.entries(this.parcelLayerFilters)) {
            if (!selected || !selected.length) continue;
            list = list.filter(p => selected.some(s => s === 1 ? p[col] : p[col] === s));
          }
          // Tag state filters: tagId → [selectedStates]
          for (const [tagId, states] of Object.entries(this.parcelTagFilters)) {
            if (!states || !states.length) continue;
            const foldMap = this.parcelTagFoldMaps[tagId] || {};
            const tag = this.allTags.find(t => t.tag_id == tagId);
            const defaultState = tag ? tag.states_csv.split(',')[0] : 'Unconfirmed';
            list = list.filter(p => {
              const fold = Object.prototype.hasOwnProperty.call(foldMap, p.parcel_id)
                ? foldMap[p.parcel_id]
                : (tag && tag.name === 'IdentityResolution' && p.identity_state === 'OK') ? null : defaultState;
              return states.includes(fold);
            });
          }
          const q = this.parcelSearch.trim().toLowerCase();
          if (q) {
            list = list.filter(p =>
              (p.site_addr  ||'').toLowerCase().includes(q) ||
              (p.owner_name ||'').toLowerCase().includes(q) ||
              (p.parcel_id  ||'').toLowerCase().includes(q));
          }
          const f = this.parcelSort, d = this.parcelSortDir==='asc'?1:-1;
          return [...list].sort((a,b)=>{
            const av=a[f], bv=b[f];
            const na=parseFloat(av), nb=parseFloat(bv);
            if(!isNaN(na)&&!isNaN(nb)) return d*(na-nb);
            return d*String(av??'').localeCompare(String(bv??''),undefined,{numeric:true});
          });
        },
        get parcelFilteredCount() { return this.filteredParcels.length; },
        get parcelTotalPages()    { return Math.max(1,Math.ceil(this.filteredParcels.length/this.parcelPageSize)); },
        get pagedParcels()        { const s=(this.parcelPage-1)*this.parcelPageSize; return this.filteredParcels.slice(s,s+this.parcelPageSize); },

        // ── Computed — campaign work queue ──────────────────────────────────
        get cwFilteredQueue() {
          let list = this.cwQueue;
          if (this.cwFilter === 'adb_only')  list = list.filter(p => p.identity_state === 'ADB-only');
          if (this.cwFilter === 'gis_only')  list = list.filter(p => p.identity_state === 'GIS-only');
          // 'mismatch' filter reserved for future name-mismatch state; shows all for now
          const q = this.cwFilterQuery.trim().toLowerCase();
          if (q) list = list.filter(p =>
            (p.parcel_id  ||'').toLowerCase().includes(q) ||
            (p.owner_name ||'').toLowerCase().includes(q) ||
            (p.site_addr  ||'').toLowerCase().includes(q)
          );
          return list;
        },
        get cwCurrentParcel() { return this.cwFilteredQueue[this.cwIdx] || null; },

        // ── Computed — registry ─────────────────────────────────────────────
        get docTypes() {
          return [...new Set(this.allRegistry.map(d=>d.instrument_type||'').filter(Boolean))]
            .filter(t => /^[A-Z][a-z]/.test(t))
            .sort();
        },
        get filteredRegistry() {
          const q = this.docSearch.trim().toLowerCase();
          let list = this.allRegistry;
          if (q) list = list.filter(d =>
            (d.grantor||'').toLowerCase().includes(q) ||
            (d.grantee||'').toLowerCase().includes(q) ||
            (d.address||'').toLowerCase().includes(q) ||
            (d.parcel_id||'').toLowerCase().includes(q) ||
            (d.book+'').includes(q) ||
            (d.page+'').includes(q));
          if (this.docTypeFilters.length) list = list.filter(d=>this.docTypeFilters.includes(d.instrument_type||''));
          // Layer attribute filters: key → [selected buckets]
          for (const [key, buckets] of Object.entries(this.docLayerFilters)) {
            if (!buckets || !buckets.length) continue;
            list = list.filter(d => {
              const s = d[key] || 0;
              const b = s === 0 ? '0%' : s < 0.2 ? '0–20%' : s < 0.4 ? '20–40%' : s < 0.6 ? '40–60%' : s < 0.8 ? '60–80%' : '80–100%';
              return buckets.includes(b);
            });
          }
          // Tag state filters: tagId → [selectedStates]
          for (const [tagId, states] of Object.entries(this.docTagFilters)) {
            if (!states || !states.length) continue;
            const foldMap = this.docTagFoldMaps[tagId] || {};
            const tag = this.allTags.find(t => t.tag_id == tagId);
            const defaultState = tag ? tag.states_csv.split(',')[0] : 'Unconfirmed';
            list = list.filter(d => {
              const id = d.book + '/' + d.page;
              const fold = Object.prototype.hasOwnProperty.call(foldMap, id)
                ? foldMap[id]
                : defaultState;
              return states.includes(fold);
            });
          }
          const f=this.docSort, dir=this.docSortDir==='asc'?1:-1;
          return [...list].sort((a,b)=>{
            const av=a[f], bv=b[f];
            const na=parseFloat(av), nb=parseFloat(bv);
            if(!isNaN(na)&&!isNaN(nb)) return dir*(na-nb);
            return dir*String(av??'').localeCompare(String(bv??''),undefined,{numeric:true});
          });
        },
        get regFilteredCount() { return this.filteredRegistry.length; },
        get regTotalPages()    { return Math.max(1,Math.ceil(this.filteredRegistry.length/this.regPageSize)); },
        get pagedRegistry()    { const s=(this.regPage-1)*this.regPageSize; return this.filteredRegistry.slice(s,s+this.regPageSize); },

        // ── Computed — town ─────────────────────────────────────────────────
        get townDocCommittees() {
          return [...new Set(this.townDocList.map(d=>d.committee))].sort();
        },
        get filteredTownDocs() {
          let list = this.townDocList;
          if (this.townCommitteeFilter)
            list = list.filter(d => d.committee === this.townCommitteeFilter);
          if (this.townStatusFilter)
            list = list.filter(d => d['n_'+this.townStatusFilter] > 0);
          return list;
        },
        get townDocTotalPages() { return Math.max(1, Math.ceil(this.filteredTownDocs.length / this.townDocPageSize)); },
        get pagedTownDocs() {
          const s = (this.townDocPage-1)*this.townDocPageSize;
          return this.filteredTownDocs.slice(s, s+this.townDocPageSize);
        },
        get visibleTownLinks() {
          if (!this.townDocDetail) return [];
          const order = {candidate: 0, confirmed: 1, rejected: 2};
          // Show all links sorted: pending first, then adjudicated (dimmed in UI).
          // townStatusFilter controls which *docs* appear in the left list, not which
          // links show here — items must stay visible so buttons can remain highlighted.
          return [...this.townDocDetail.links].sort((a, b) => (order[a.status]??0) - (order[b.status]??0));
        },
        get pickerSuggestions() {
          const q = this.pickerQuery.trim().toLowerCase();
          if (!q) return [];
          return this.allParcels
            .filter(p =>
              (p.site_addr||'').toLowerCase().includes(q) ||
              (p.parcel_id||'').toLowerCase().includes(q) ||
              (p.owner_name||'').toLowerCase().includes(q)
            )
            .slice(0, 10);
        },

        // ── Export URLs ─────────────────────────────────────────────────────
        get parcelExportUrl() {
          const p = new URLSearchParams();
          if (this.parcelSearch.trim()) p.set('q', this.parcelSearch.trim());
          return '/exports/parcels.csv' + (p.size ? '?' + p : '');
        },
        get docExportUrl() {
          const p = new URLSearchParams();
          if (this.docSearch.trim()) p.set('q', this.docSearch.trim());
          if (this.docTypeFilters.length) p.set('type', this.docTypeFilters.join(','));
          return '/exports/documents.csv' + (p.size ? '?' + p : '');
        },
        get usageExportUrl() {
          const p = new URLSearchParams();
          if (this.usageSearch.trim()) p.set('q', this.usageSearch.trim());
          return '/exports/usage.csv' + (p.size ? '?' + p : '');
        },

        // ── Init ────────────────────────────────────────────────────────────
        async init() {
          this.$watch('parcelSearch',        ()=>{ this.parcelPage=1; });
          this.$watch('parcelSort',      ()=>{ this.parcelPage=1; });
          this.$watch('parcelSortDir',   ()=>{ this.parcelPage=1; });
          this.$watch('docSearch',   ()=>{ this.regPage=1; });
          this.$watch('docSort',     ()=>{ this.regPage=1; });
          this.$watch('docSortDir',      ()=>{ this.regPage=1; });
          // when user closes a doc, the map re-appears — tell Leaflet to recalculate size
          this.$watch('parcelSelectedDoc', (val) => {
            if (!val && _lmap) this.$nextTick(() => _lmap.invalidateSize());
          });
          // Town adjudication: the outer x-if div's @click sets townAdjRequest
          // to "action:linkId" via a simple DOM expression (only pattern that works
          // in Alpine x-if scope). This watcher runs in the component scope where
          // updateLinkStatus / deleteTownLink are properly accessible.
          this.$watch('townAdjRequest', val => {
            if (!val || typeof val !== 'string') return;
            const colon = val.indexOf(':');
            if (colon < 0) return;
            const action   = val.slice(0, colon);
            const parcelId = val.slice(colon + 1);
            if (!action || !parcelId) return;
            // Look up link_id by parcel_id — avoids any dynamic binding on buttons
            const link = this.townDocDetail?.links.find(l => l.parcel_id === parcelId);
            if (!link) return;
            if (action === 'del') this.deleteTownLink(link.link_id);
            else                  this.updateLinkStatus(link.link_id, action);
            this.townAdjRequest = null;
          });
          this.loadAttrRegistry();
          document.addEventListener('keydown', e => { if (e.key==='Escape' && this.pnExpandMode) this.pnExpandMode=null; });
          // Campaign work queue keyboard shortcuts
          document.addEventListener('keydown', e => {
            if (this.activeTab !== 'campaigns' || this.campaignView !== 'work') return;
            const tag = document.activeElement?.tagName;
            if (tag === 'INPUT' || tag === 'TEXTAREA') return;
            if (e.key === 'Escape')                    { this.cwChipOpen = false; return; }
            if (e.key === 's' || e.key === 'S')        { e.preventDefault(); this.cwSkip(); return; }
            if (e.key === 'Enter')                     { e.preventDefault(); this.cwSaveAndNext(); return; }
            if (e.key === 'j' || e.key === 'J' || e.key === 'ArrowDown') { e.preventDefault(); this.cwAdvance(); return; }
            if (e.key === 'k' || e.key === 'K' || e.key === 'ArrowUp') {
              e.preventDefault();
              if (this.cwIdx > 0) this.cwSelectIdx(this.cwIdx - 1);
              return;
            }
            if (['1','2','3','4'].includes(e.key) && this.cwParcelDetail) {
              e.preventDefault();
              const d = this.cwIdentityDecisions[parseInt(e.key) - 1];
              if (d) {
                const pid = this.cwParcelDetail.parcel.parcel_id;
                this.cwDecisions = {...this.cwDecisions, [pid]: d.key};
                this.cwChipOpen = false;
              }
            }
          });
          // Reload parcel detail when filter query changes
          this.$watch('cwFilterQuery', () => {
            if (this.campaignView !== 'work') return;
            this.cwIdx = 0;
            const q = this.cwFilteredQueue;
            if (q.length > 0) this.loadCwParcelDetail(q[0]);
            else this.cwParcelDetail = null;
          });
          this.$nextTick(() => {
            this.updateParcelPageSize();
            window.addEventListener('resize', () => { this.updateParcelPageSize(); });
          });
          if (CURRENT_USER) {
            this.loadParcels();
            this.loadTags();
          }
        },

        // ── Tab switch ───────────────────────────────────────────────────────
        switchTab(tab) {
          if (!CURRENT_USER && tab !== 'overview') return;
          this.activeTab = tab;
          this.tagPickerOpen = null;
          if (tab==='campaigns') { this.campaignView = 'inventory'; if (CURRENT_USER) { if (!this.campaignProgress) this.loadCampaignProgress(); if (!this.campaignMetaLoaded) this.loadCampaignMeta(); } }
          if (tab==='parcels') this.$nextTick(() => this.updateParcelPageSize());
          if (tab==='assets' && !this.overviewData && !this.overviewLoading) this.loadOverview();
          if (tab==='assets' && this.assetsSubtab === 'reports') this.loadReports();
          if (tab==='registry' && this.allRegistry.length===0) this.loadRegistry();
          if (tab==='town' && this.townDocList.length===0) this.loadTownDocs();
          if (tab==='admin') { this.loadAdminUsers(); this.loadUsage(); this.adminSection = 'users'; }
          if (tab==='parcels' && _lmap) {
            setTimeout(() => {
              _lmap.invalidateSize();
              if (_lLayer) {
                try {
                  if (_lLayer.getBounds) _lmap.fitBounds(_lLayer.getBounds(), { maxZoom: 18, padding: [20, 20] });
                  else if (_lLayer.getLatLng) _lmap.setView(_lLayer.getLatLng(), 17);
                } catch(_) {}
              }
            }, 50);
          }
        },

        // ── Loaders ──────────────────────────────────────────────────────────
        async loadReports() {
          this.reportsLoading = true;
          try {
            const r = await fetch('/api/reports/identity');
            if (r.ok) this.reportsData = await r.json();
          } catch(e) { console.error('reports:', e); }
          finally { this.reportsLoading = false; }
        },

        async loadOverview() {
          this.overviewLoading = true;
          try {
            const [ovR, tdR] = await Promise.all([
              fetch('/api/overview'),
              fetch('/api/town-docs/overview'),
            ]);
            if (!ovR.ok) throw new Error(ovR.status);
            this.overviewData    = await ovR.json();
            this.townDocsOverview = tdR.ok ? await tdR.json() : null;
          } catch(e) { console.error('overview:', e); }
          finally { this.overviewLoading = false; }
        },

        updateParcelPageSize() {
          const el = document.getElementById('pn-list-scroll');
          if (!el || el.clientHeight === 0) return;
          const rowH = 46; // pn-lrow height (padding 7+7 + two text lines ~15+14 + border 1)
          const size = Math.max(10, Math.floor(el.clientHeight / rowH));
          if (size !== this.parcelPageSize) {
            this.parcelPageSize = size;
            this.parcelPage = Math.min(this.parcelPage, this.parcelTotalPages || 1);
          }
        },

        pnShowMap() {
          this.parcelSelectedDoc     = null;
          this.parcelPdfUrl          = null;
          this.parcelSelectedTownDoc = null;
          this.parcelSelectedUpload  = null;
        },

        async loadAttrRegistry() {
          try {
            const r = await fetch('/api/meta/attributes');
            if (!r.ok) return;
            const groups = await r.json();
            const byId = {};
            for (const g of groups) {
              for (const a of g.attributes) byId[a.attr_id] = a;
            }
            this.attrById     = byId;
            this.attrRegistry = groups;
          } catch(e) { console.error('attr registry:', e); }
        },

        async loadCampaignProgress() {
          try {
            const r = await fetch('/api/campaigns/progress');
            if (r.ok) this.campaignProgress = await r.json();
          } catch(e) { console.error('campaign progress:', e); }
        },

        async loadCampaignMeta() {
          try {
            const r = await fetch('/api/campaigns/meta');
            if (r.ok) { this.campaignMetaByDim = await r.json(); this.campaignMetaLoaded = true; }
          } catch(e) { console.error('campaign meta:', e); }
        },

        // ── Campaign work queue ──────────────────────────────────────────────
        async launchCampaignWork(tag) {
          this.selectedCampaignTag = tag;
          this.campaignView        = 'work';
          this.cwIdx               = 0;
          this.cwDecisions         = {};
          this.cwChipOpen          = false;
          this.cwParcelDetail      = null;
          this.cwFilter            = 'all';
          this.cwFilterQuery       = '';
          await this.loadCwQueue();
        },

        async loadCwQueue() {
          this.cwQueueLoading = true;
          try {
            if (this.allTags.length === 0) await this.loadTags();
            if (this.allParcels.length === 0) {
              const r = await fetch('/api/parcels');
              if (r.ok) this.allParcels = (await r.json()).map(p => ({...p, coverage_estimate: coverageEstimate(p)}));
            }
            this.cwQueue = this.allParcels.filter(p => p.identity_state !== 'OK');
            // Pre-populate decisions from the existing fold state
            const idTag = this.allTags.find(t => t.name === 'IdentityResolution');
            if (idTag) {
              try {
                const r = await fetch('/api/folds/parcel?tag_id=' + idTag.tag_id);
                if (r.ok) {
                  const foldMap = await r.json();
                  const dec = {};
                  for (const [pid, state] of Object.entries(foldMap)) {
                    if (state && state !== 'Unconfirmed') dec[pid] = state;
                  }
                  this.cwDecisions = dec;
                }
              } catch(_) {}
            }
            if (this.cwFilteredQueue.length > 0) {
              await this.loadCwParcelDetail(this.cwFilteredQueue[0]);
            }
          } finally { this.cwQueueLoading = false; }
        },

        async loadCwParcelDetail(parcel) {
          if (!parcel) return;
          this.cwParcelDetail  = null;
          this.cwDetailLoading = true;
          this.cwDetailError   = null;
          this.cwChipOpen      = false;
          try {
            const r = await fetch('/api/parcels/' + encodeURIComponent(parcel.parcel_id));
            if (!r.ok) throw new Error(r.status);
            this.cwParcelDetail = await r.json();
            // Pick up any existing tag state not yet in cwDecisions
            const tagInfo = this.cwParcelDetail.tags?.['IdentityResolution'];
            if (tagInfo?.state && tagInfo.state !== 'Unconfirmed' && !this.cwDecisions[parcel.parcel_id]) {
              this.cwDecisions = {...this.cwDecisions, [parcel.parcel_id]: tagInfo.state};
            }
            this.$nextTick(() => _updateCampaignMap(
              parcel.parcel_id,
              this.cwParcelDetail.parcel.centroid_lat,
              this.cwParcelDetail.parcel.centroid_lon,
              this.cwParcelDetail.gis,
            ));
          } catch(e) { this.cwDetailError = e.message; }
          finally    { this.cwDetailLoading = false; }
        },

        async cwSelectIdx(i) {
          if (i < 0 || i >= this.cwFilteredQueue.length) return;
          this.cwIdx = i;
          await this.loadCwParcelDetail(this.cwFilteredQueue[i]);
        },

        async cwSaveAndNext() {
          const parcel   = this.cwCurrentParcel;
          const decision = parcel ? this.cwDecisions[parcel.parcel_id] : null;
          if (!parcel || !decision) return;
          const idTag = this.allTags.find(t => t.name === 'IdentityResolution');
          if (!idTag) return;
          await this.applyTag('parcel', parcel.parcel_id, idTag.tag_id, decision);
          this.loadCampaignProgress();
          await this.cwAdvance();
        },

        cwSkip() { this.cwAdvance(); },

        async cwAdvance() {
          if (this.cwIdx < this.cwFilteredQueue.length - 1) {
            await this.cwSelectIdx(this.cwIdx + 1);
          }
        },

        async cwSetFilter(filter) {
          this.cwFilter = filter;
          this.cwIdx = 0;
          const q = this.cwFilteredQueue;
          if (q.length > 0) await this.loadCwParcelDetail(q[0]);
          else this.cwParcelDetail = null;
        },

        cwSuggestion(parcel) {
          if (!parcel) return null;
          if (parcel.identity_state === 'ADB-only') return 'GIS Add';
          if (parcel.identity_state === 'GIS-only') return 'ADB Add';
          return null;
        },

        cwBasicInfoRows(parcel) {
          if (!parcel) return [];
          return [
            { k: 'Use Code', v: parcel.use_code_desc || parcel.use_code_norm || '—' },
            { k: 'Village',  v: parcel.village || '—' },
            { k: 'Acres',    v: parcel.billingacres != null ? Number(parcel.billingacres).toFixed(2) + ' ac' : '—' },
            { k: 'Public',   v: parcel.is_public ? 'Yes' : 'No' },
            { k: 'Class',    v: parcel.property_class || '—' },
          ];
        },

        cwOpenFullParcel(parcelId) {
          const p = this.allParcels.find(x => x.parcel_id === parcelId);
          this.switchTab('parcels');
          if (p) this.$nextTick(() => this.selectParcel(p));
        },

        cwClearDecision(parcelId) {
          const next = {...this.cwDecisions};
          delete next[parcelId];
          this.cwDecisions = next;
        },

        launchDimension(tag) {
          this.selectedCampaignTag = tag;
          this.campaignView = 'work';
        },

        async loadAdminCampaigns() {
          this.adminCampaignsLoading = true;
          try {
            const r = await fetch('/api/campaigns/all');
            this.adminCampaigns = r.ok ? await r.json() : [];
          } catch(e) { this.adminCampaigns = []; }
          finally { this.adminCampaignsLoading = false; }
        },

        async setCampaignStatus(campaign_id, status) {
          await fetch('/api/campaigns/' + encodeURIComponent(campaign_id) + '/status', {
            method: 'PATCH', headers: {'Content-Type':'application/json'},
            body: JSON.stringify({status}),
          });
          await this.loadAdminCampaigns();
        },

        async shiftCampaignPriority(campaign_id, direction) {
          await fetch('/api/campaigns/' + encodeURIComponent(campaign_id) + '/priority', {
            method: 'PATCH', headers: {'Content-Type':'application/json'},
            body: JSON.stringify({direction}),
          });
          await Promise.all([this.loadAdminCampaigns(), this.loadCampaignMeta()]);
        },

        async loadParcels() {
          try {
            const r = await fetch('/api/parcels');
            if (!r.ok) throw new Error(r.status);
            this.allParcels = (await r.json()).map(p => ({ ...p, coverage_estimate: coverageEstimate(p) }));
            const def = this.allParcels.find(p => p.parcel_id === '392-8');
            if (def) this.selectParcel(def);
          } finally { this.parcelsLoading = false; }
        },

        async loadRegistry() {
          this.regLoading = true;
          try {
            const r = await fetch('/api/documents');
            if (!r.ok) throw new Error(r.status);
            this.allRegistry = await r.json();
            if (!this.docSelectedBP) {
              const def = this.allRegistry.find(d => d.book === '35807' && d.page === '166');
              if (def) this.selectDoc(def);
            }
          } catch(e) { console.error('registry:', e); }
          finally { this.regLoading = false; }
        },

        // ── Actions — parcels ────────────────────────────────────────────────
        async selectParcel(p) {
          if (this.selectedParcelId===p.parcel_id) return;
          this.selectedParcelId      = p.parcel_id;
          this.parcelDetail          = null;
          this.parcelSelectedDoc     = null;
          this.parcelPdfUrl          = null;
          this.parcelSelectedTownDoc = null;
          this.parcelUploads         = [];
          this.parcelSelectedUpload  = null;
          this.parcelDetailError  = null;
          this.parcelDetailLoading= true;
          this.parcelTagState     = {};
          this.parcelTownDocs     = null;
          this.tagPickerOpen      = null;
          try {
            const [detailResp, tdResp, uploadsResp] = await Promise.all([
              fetch('/api/parcels/'+encodeURIComponent(p.parcel_id)),
              fetch('/api/parcels/'+encodeURIComponent(p.parcel_id)+'/town-docs'),
              CURRENT_USER ? fetch('/api/uploads/parcel/'+encodeURIComponent(p.parcel_id)) : Promise.resolve(null),
            ]);
            if (!detailResp.ok) throw new Error('Server returned '+detailResp.status);
            this.parcelDetail   = await detailResp.json();
            this.parcelTownDocs = tdResp.ok ? await tdResp.json() : [];
            this.parcelUploads  = (uploadsResp && uploadsResp.ok) ? await uploadsResp.json() : [];
            // Auto-load first available document into viewer
            const _docs = this.parcelDetail.documents || [];
            if (_docs.length > 0) {
              this.selectParcelDoc(_docs[0]);
            } else if (this.parcelUploads.length > 0) {
              this.selectParcelUpload(this.parcelUploads[0]);
            }
            this.parcelTagState = {};
            for (const info of Object.values(this.parcelDetail.tags || {})) {
              if (info && info.state !== null) this.parcelTagState[info.tag_id] = info.state;
            }
            const parcel = this.parcelDetail.parcel;
            this.$nextTick(() => _updateParcelMap(
              p.parcel_id,
              parcel.centroid_lat,
              parcel.centroid_lon,
              this.parcelDetail.gis,
            ));
          } catch(e) { this.parcelDetailError = e.message; }
          finally    { this.parcelDetailLoading = false; }
        },

        selectParcelDoc(doc) {
          this.parcelSelectedDoc     = doc;
          this.parcelSelectedTownDoc = null;
          this.parcelSelectedUpload  = null;
          this.parcelPdfUrl = doc.scan_cached
            ? '/api/documents/'+doc.book+'/'+doc.page+'/pdf'
            : null;
        },

        selectParcelTownDoc(td) {
          this.parcelSelectedTownDoc = td;
          this.parcelSelectedDoc     = null;
          this.parcelSelectedUpload  = null;
          this.parcelPdfUrl          = null;
        },

        selectParcelUpload(u) {
          this.parcelSelectedUpload  = u;
          this.parcelSelectedDoc     = null;
          this.parcelSelectedTownDoc = null;
          this.parcelPdfUrl          = null;
        },

        // ── Actions — documents ──────────────────────────────────────────────
        async selectDoc(d) {
          if (this.docSelectedBP && this.docSelectedBP.book===d.book && this.docSelectedBP.page===d.page) return;
          this.docSelectedBP    = {book:d.book, page:d.page};
          this.docDetail        = null;
          this.docPdfUrl        = null;
          this.docDetailError   = null;
          this.docDetailLoading = true;
          this.docTagState      = {};
          this.tagPickerOpen    = null;
          const tid = d.book + '/' + d.page;
          try {
            const [docResp, tagResp] = await Promise.all([
              fetch('/api/documents/'+d.book+'/'+d.page),
              CURRENT_USER ? fetch('/api/tagging/document/'+tid) : Promise.resolve(null),
            ]);
            if (!docResp.ok) throw new Error('Server returned '+docResp.status);
            this.docDetail = await docResp.json();
            this.docPdfUrl = this.docDetail.document.scan_cached
              ? '/api/documents/'+d.book+'/'+d.page+'/pdf'
              : null;
            if (tagResp && tagResp.ok) {
              const td = await tagResp.json();
              this.allTags    = td.tags;
              this.docTagState = {};
              for (const [k, v] of Object.entries(td.current)) {
                if (v !== null && v.state !== null) this.docTagState[k] = v.state;
              }
            }
          } catch(e) { this.docDetailError = e.message; }
          finally    { this.docDetailLoading = false; }
        },

        // ── Filter chip picker ───────────────────────────────────────────────

        toggleFilterChipPicker(key) {
          this.filterChipPickerOpen = this.filterChipPickerOpen === key ? null : key;
        },

        // Parcel layer attribute filter
        parcelLayerChipLabel(chip) {
          const sel = this.parcelLayerFilters[chip.col];
          if (sel && sel.length) {
            const labels = sel.map(v => v === 1 ? 'Present' : String(v));
            return chip.label + ': ' + labels.join(', ');
          }
          return chip.label;
        },

        toggleParcelLayerValue(col, val) {
          const cur = this.parcelLayerFilters[col] || [];
          const next = cur.includes(val) ? cur.filter(v => v !== val) : [...cur, val];
          this.parcelLayerFilters = {...this.parcelLayerFilters, [col]: next};
          this.parcelPage = 1;
        },

        clearParcelLayerFilter(col) {
          const f = {...this.parcelLayerFilters};
          delete f[col];
          this.parcelLayerFilters = f;
          this.parcelPage = 1;
        },

        // Parcel tag state filter
        parcelTagChipLabel(tag) {
          const sel = this.parcelTagFilters[tag.tag_id];
          if (sel && sel.length) return tag.name + ': ' + sel.join(', ');
          return tag.name;
        },

        async toggleParcelTagState(tagId, state) {
          const cur = this.parcelTagFilters[tagId] || [];
          const next = cur.includes(state) ? cur.filter(s => s !== state) : [...cur, state];
          this.parcelTagFilters = {...this.parcelTagFilters, [tagId]: next};
          this.parcelPage = 1;
          if (next.length > 0 && !this.parcelTagFoldMaps[tagId]) {
            await this.fetchParcelTagFoldMap(tagId);
          }
        },

        clearParcelTagFilter(tagId) {
          const f = {...this.parcelTagFilters};
          delete f[tagId];
          this.parcelTagFilters = f;
          this.parcelPage = 1;
        },

        async fetchParcelTagFoldMap(tagId) {
          try {
            const r = await fetch('/api/folds/parcel?tag_id=' + tagId);
            if (r.ok) this.parcelTagFoldMaps = {...this.parcelTagFoldMaps, [tagId]: await r.json()};
          } catch(e) {}
        },

        // Instrument type chip
        docTypeChipLabel() {
          if (!this.docTypeFilters.length) return 'Instrument Type';
          if (this.docTypeFilters.length === 1) return this.docTypeFilters[0];
          return 'Type: ' + this.docTypeFilters.length + ' selected';
        },
        toggleDocType(type) {
          const cur = this.docTypeFilters;
          this.docTypeFilters = cur.includes(type) ? cur.filter(t => t !== type) : [...cur, type];
          this.regPage = 1;
        },
        clearDocTypeFilter() {
          this.docTypeFilters = [];
          this.regPage = 1;
        },

        // Doc layer attribute filter (bucket multi-select)
        docLayerChipLabel(chip) {
          const sel = this.docLayerFilters[chip.key];
          if (sel && sel.length) return chip.label + ': ' + sel.join(', ');
          return chip.label;
        },
        toggleDocLayerBucket(key, bucket) {
          const cur = this.docLayerFilters[key] || [];
          const next = cur.includes(bucket) ? cur.filter(b => b !== bucket) : [...cur, bucket];
          this.docLayerFilters = {...this.docLayerFilters, [key]: next};
          this.regPage = 1;
        },
        clearDocLayerFilter(key) {
          const f = {...this.docLayerFilters};
          delete f[key];
          this.docLayerFilters = f;
          this.regPage = 1;
        },

        // Doc tag state filter
        docTagChipLabel(tag) {
          const sel = this.docTagFilters[tag.tag_id];
          if (sel && sel.length) return tag.name + ': ' + sel.join(', ');
          return tag.name;
        },

        async toggleDocTagState(tagId, state) {
          const cur = this.docTagFilters[tagId] || [];
          const next = cur.includes(state) ? cur.filter(s => s !== state) : [...cur, state];
          this.docTagFilters = {...this.docTagFilters, [tagId]: next};
          this.regPage = 1;
          if (next.length > 0 && !this.docTagFoldMaps[tagId]) {
            await this.fetchDocTagFoldMap(tagId);
          }
        },

        clearDocTagFilter(tagId) {
          const f = {...this.docTagFilters};
          delete f[tagId];
          this.docTagFilters = f;
          this.regPage = 1;
        },

        async fetchDocTagFoldMap(tagId) {
          try {
            const r = await fetch('/api/folds/document?tag_id=' + tagId);
            if (r.ok) this.docTagFoldMaps = {...this.docTagFoldMaps, [tagId]: await r.json()};
          } catch(e) {}
        },

        // ── Town ─────────────────────────────────────────────────────────────
        async loadTownDocs() {
          this.townDocListLoading = true;
          try {
            const r = await fetch('/api/town-docs');
            this.townDocList = r.ok ? await r.json() : [];
          } catch(e) { this.townDocList = []; }
          finally { this.townDocListLoading = false; }
        },

        async selectTownDoc(doc) {
          if (this.selectedTownDocId === doc.doc_id) return;
          this.selectedTownDocId = doc.doc_id;
          this.townDocDetail     = null;
          this.townPdfUrl        = null;
          this.townOcrOpen       = false;
          this.townDetailLoading = true;
          this.pickerQuery          = '';
          // encode each path segment individually so slashes are preserved in the URL
          const encId = doc.doc_id.split('/').map(encodeURIComponent).join('/');
          try {
            const r = await fetch('/api/town-docs/'+encId);
            if (!r.ok) throw new Error(r.status);
            this.townDocDetail = await r.json();
            this.townPdfUrl    = '/api/town-docs/pdf/'+encId;
          } catch(e) { console.error('town doc:', e); }
          finally { this.townDetailLoading = false; }
        },

        async updateLinkStatus(linkId, status) {
          // Optimistic: mutate the reactive link object immediately so button highlights
          if (this.townDocDetail) {
            const lk = this.townDocDetail.links.find(l => l.link_id === linkId);
            if (lk) lk.status = status;
          }
          this.townLinkPending = {...this.townLinkPending, [linkId]: true};
          try {
            const r = await fetch('/api/hygiene/links/'+linkId, {
              method: 'PATCH',
              headers: {'Content-Type':'application/json'},
              body: JSON.stringify({status}),
            });
            if (!r.ok) throw new Error(r.status);
            await this._reloadTownDetail();
          } catch(e) {
            console.error('update link:', e);
            await this._reloadTownDetail(); // revert optimistic on error
          }
          finally { this.townLinkPending = {...this.townLinkPending, [linkId]: false}; }
        },

        async _reloadTownDetail() {
          if (!this.selectedTownDocId) return;
          const encId = this.selectedTownDocId.split('/').map(encodeURIComponent).join('/');
          const r = await fetch('/api/town-docs/'+encId);
          if (!r.ok) return;
          this.townDocDetail = await r.json();
          // Sync left-panel count badges for this doc
          const docId = this.selectedTownDocId;
          const idx   = this.townDocList.findIndex(d => d.doc_id === docId);
          if (idx >= 0 && this.townDocDetail) {
            const links = this.townDocDetail.links;
            const updated = {
              ...this.townDocList[idx],
              n_candidate: links.filter(l => l.status === 'candidate').length,
              n_confirmed: links.filter(l => l.status === 'confirmed').length,
              n_rejected:  links.filter(l => l.status === 'rejected').length,
            };
            const next = [...this.townDocList];
            next[idx] = updated;
            this.townDocList = next;
          }
        },

        async addManualLink(parcel) {
          if (!this.townDocDetail) return;
          const doc = this.townDocDetail.doc;
          this.pickerQuery = '';
          try {
            const r = await fetch('/api/hygiene/links', {
              method: 'POST',
              headers: {'Content-Type':'application/json'},
              body: JSON.stringify({doc_id: doc.doc_id, parcel_id: parcel.parcel_id, source_type: doc.source_type}),
            });
            if (!r.ok) throw new Error(r.status);
            await this._reloadTownDetail();
          } catch(e) { console.error('add link:', e); }
        },

        async deleteTownLink(linkId) {
          this.townLinkPending = {...this.townLinkPending, [linkId]: true};
          try {
            const r = await fetch('/api/hygiene/links/'+linkId, {method:'DELETE'});
            if (!r.ok) throw new Error(r.status);
            await this._reloadTownDetail();
          } catch(e) { console.error('delete link:', e); }
          finally { this.townLinkPending = {...this.townLinkPending, [linkId]: false}; }
        },

        // ── Tags (user-facing) ────────────────────────────────────────────────
        async loadTags() {
          this.tagsLoading = true;
          try {
            const r = await fetch('/api/tags');
            this.allTags = r.ok ? await r.json() : [];
          } catch(e) { this.allTags = []; }
          finally { this.tagsLoading = false; }
        },


        toggleTagPicker(ns, tagId) {
          const key = ns + ':' + tagId;
          this.tagPickerOpen = this.tagPickerOpen === key ? null : key;
        },

        async applyTag(targetType, targetId, tagId, state) {
          this.tagPickerOpen = null;
          const stateMap  = targetType === 'parcel' ? 'parcelTagState' : 'docTagState';
          const prior     = this[stateMap][tagId];
          const priorFold = (targetType === 'parcel' && this.parcelTagFoldMaps[tagId])
            ? this.parcelTagFoldMaps[tagId][targetId]
            : undefined;
          // Optimistic update — patch both the per-parcel display state and the filter fold map
          this.tagPending = {...this.tagPending, [tagId]: true};
          const next = {...this[stateMap]};
          if (state === null) { delete next[tagId]; } else { next[tagId] = state; }
          this[stateMap] = next;
          if (targetType === 'parcel' && this.parcelTagFoldMaps[tagId] !== undefined) {
            const fm = {...this.parcelTagFoldMaps[tagId]};
            if (state === null) { delete fm[targetId]; } else { fm[targetId] = state; }
            this.parcelTagFoldMaps = {...this.parcelTagFoldMaps, [tagId]: fm};
          }
          try {
            const r = await fetch('/api/tagging', {
              method: 'POST',
              headers: {'Content-Type':'application/json'},
              body: JSON.stringify({tag_id: tagId, state, target_type: targetType, target_id: targetId}),
            });
            if (!r.ok) throw new Error(r.status);
          } catch(e) {
            // revert both
            const reverted = {...this[stateMap]};
            if (prior == null) { delete reverted[tagId]; } else { reverted[tagId] = prior; }
            this[stateMap] = reverted;
            if (targetType === 'parcel' && this.parcelTagFoldMaps[tagId] !== undefined) {
              const fm = {...this.parcelTagFoldMaps[tagId]};
              if (priorFold === undefined) { delete fm[targetId]; } else { fm[targetId] = priorFold; }
              this.parcelTagFoldMaps = {...this.parcelTagFoldMaps, [tagId]: fm};
            }
            console.error('tag apply failed:', e);
          } finally {
            this.tagPending = {...this.tagPending, [tagId]: false};
          }
        },

        // ── Tags (admin) ──────────────────────────────────────────────────────
        async switchAdminSection(section) {
          this.adminSection = section;
          this.selectedTag  = null;
          this.tagEditForm  = { isNew:false, name:'', states:[], statesRaw:'', newState:'', deprecated:false, displayOrder:0, saving:false, deleting:false, err:'', msg:'' };
          if (section === 'tags')      await this.loadAdminTags();
          if (section === 'campaigns') await this.loadAdminCampaigns();
        },

        async loadAdminTags() {
          this.adminTagsLoading = true;
          try {
            const r = await fetch('/api/admin/tags');
            this.allAdminTags = r.ok ? await r.json() : [];
          } catch(e) { this.allAdminTags = []; }
          finally { this.adminTagsLoading = false; }
        },

        selectTag(t) {
          this.selectedTag = t;
          this.tagEditForm = {
            isNew:        false,
            name:         t.name,
            states:       t.states_csv.split(','),
            statesRaw:    t.states_csv,
            newState:     '',
            deprecated:   !!t.deprecated_at,
            displayOrder: t.display_order,
            saving:       false,
            deleting:     false,
            err:          '',
            msg:          '',
          };
        },

        startNewTag() {
          this.selectedTag  = null;
          this.tagEditForm  = { isNew:true, name:'', states:[], statesRaw:'', newState:'', deprecated:false, displayOrder:0, saving:false, deleting:false, err:'', msg:'' };
        },

        addTagState() {
          const s = this.tagEditForm.newState.trim();
          if (!s) return;
          if (this.tagEditForm.states.includes(s)) { this.tagEditForm.err = `State '${s}' already exists`; return; }
          this.tagEditForm.states = [...this.tagEditForm.states, s];
          this.tagEditForm.newState = '';
          this.tagEditForm.err = '';
        },

        removeTagState(i) {
          const next = [...this.tagEditForm.states];
          next.splice(i, 1);
          this.tagEditForm.states = next;
        },

        moveTagState(i, dir) {
          const next = [...this.tagEditForm.states];
          const j = i + dir;
          if (j < 0 || j >= next.length) return;
          [next[i], next[j]] = [next[j], next[i]];
          this.tagEditForm.states = next;
        },

        tagUsageLine(t) {
          if (!t.usage || Object.keys(t.usage).length === 0) return '';
          const states = t.states_csv.split(',');
          const parts = states
            .filter(s => t.usage[s] > 0)
            .map(s => `${t.usage[s]} ${s.slice(0,6)}`);
          return parts.join(' · ');
        },

        campaignMeta(tag) {
          if (!tag) return null;
          return this.campaignMetaByDim[tag.name] || null;
        },

        async saveNewTag() {
          const name   = this.tagEditForm.name.trim();
          const states = this.tagEditForm.statesRaw.split(',').map(s=>s.trim()).filter(Boolean);
          if (!name)          { this.tagEditForm.err = 'Name required'; return; }
          if (!states.length) { this.tagEditForm.err = 'At least one state required'; return; }
          this.tagEditForm.saving = true; this.tagEditForm.err = '';
          try {
            const r = await fetch('/api/admin/tags', {
              method: 'POST', headers: {'Content-Type':'application/json'},
              body: JSON.stringify({name, states_csv: states.join(','), display_order: 0}),
            });
            const d = await r.json();
            if (!r.ok) { this.tagEditForm.err = d.error || 'Error'; return; }
            await this.loadAdminTags();
            await this.loadTags();
            const created = this.allAdminTags.find(t => t.tag_id === d.tag_id);
            if (created) this.selectTag(created);
          } catch(e) { this.tagEditForm.err = 'Network error'; }
          finally    { this.tagEditForm.saving = false; }
        },

        async saveTag(confirmed = false) {
          if (!this.selectedTag) return;
          if (!this.tagEditForm.states.length) { this.tagEditForm.err = 'At least one state required'; return; }
          this.tagEditForm.saving = true; this.tagEditForm.err = ''; this.tagEditForm.msg = '';
          try {
            const payload = {
              name:          this.tagEditForm.name.trim(),
              states_csv:    this.tagEditForm.states.join(','),
              display_order: this.tagEditForm.displayOrder,
              deprecated:    this.tagEditForm.deprecated,
              confirm:       confirmed,
            };
            const r = await fetch('/api/admin/tags/' + this.selectedTag.tag_id, {
              method: 'PATCH', headers: {'Content-Type':'application/json'},
              body: JSON.stringify(payload),
            });
            const d = await r.json();
            if (r.status === 409 && d.needs_confirm) {
              this.tagEditForm.saving = false;
              const lines = d.removed_states.map(x =>
                x.n_affected > 0
                  ? `${x.n_affected} node${x.n_affected!==1?'s':''} with "${x.state}" → reset to "${x.default_state}"`
                  : `No nodes are currently in state "${x.state}"`
              ).join('\n');
              this.tagConfirmDialog = {
                title:   'Confirm state removal',
                body:    lines,
                confirm: () => { this.tagConfirmDialog = null; this.saveTag(true); },
              };
              return;
            }
            if (!r.ok) { this.tagEditForm.err = d.error || 'Error'; return; }
            this.tagEditForm.msg = 'Saved.';
            await this.loadAdminTags();
            await this.loadTags();
            const updated = this.allAdminTags.find(t => t.tag_id === this.selectedTag.tag_id);
            if (updated) { this.selectedTag = updated; }
          } catch(e) { this.tagEditForm.err = 'Network error'; }
          finally    { this.tagEditForm.saving = false; }
        },

        async confirmDeleteTag() {
          if (!this.selectedTag) return;
          this.tagEditForm.deleting = true; this.tagEditForm.err = '';
          try {
            // Dry-run: confirm:false to get n_affected from server
            const r = await fetch('/api/admin/tags/' + this.selectedTag.tag_id, {
              method: 'DELETE', headers: {'Content-Type':'application/json'},
              body: JSON.stringify({confirm: false}),
            });
            const d = await r.json();
            if (r.status === 409 && d.needs_confirm) {
              const n = d.n_affected;
              this.tagConfirmDialog = {
                title:   `Delete tag "${d.tag_name}"?`,
                body:    n > 0
                  ? `There are ${n} node${n!==1?'s':''} tagged "${d.tag_name}". Removing this tag will untag all of them. This cannot be undone.`
                  : `Remove tag "${d.tag_name}"? This cannot be undone.`,
                confirm: () => { this.tagConfirmDialog = null; this.executeDeleteTag(); },
              };
              return;
            }
            // n_affected was 0 — server deleted immediately
            if (!r.ok) { this.tagEditForm.err = d.error || 'Error'; return; }
            this._afterTagDeleted();
          } catch(e) { this.tagEditForm.err = 'Network error'; }
          finally    { this.tagEditForm.deleting = false; }
        },

        async executeDeleteTag() {
          if (!this.selectedTag) return;
          this.tagEditForm.deleting = true; this.tagEditForm.err = '';
          try {
            const r = await fetch('/api/admin/tags/' + this.selectedTag.tag_id, {
              method: 'DELETE', headers: {'Content-Type':'application/json'},
              body: JSON.stringify({confirm: true}),
            });
            const d = await r.json();
            if (!r.ok) { this.tagEditForm.err = d.error || 'Error'; return; }
            this._afterTagDeleted();
          } catch(e) { this.tagEditForm.err = 'Network error'; }
          finally    { this.tagEditForm.deleting = false; }
        },

        async _afterTagDeleted() {
          this.selectedTag = null;
          this.tagEditForm = { isNew:false, name:'', states:[], statesRaw:'', newState:'', deprecated:false, displayOrder:0, saving:false, deleting:false, err:'', msg:'' };
          await this.loadAdminTags();
          await this.loadTags();
        },

        // ── Admin ─────────────────────────────────────────────────────────────
        async loadUsage() {
          this.usageLoading = true;
          try {
            const r = await fetch('/api/admin/usage');
            this.usageLog = r.ok ? await r.json() : [];
          } catch(e) { this.usageLog = []; }
          finally { this.usageLoading = false; }
        },

        sortUsage(col) {
          if (this.usageSort === col) {
            this.usageSortDir = this.usageSortDir==='asc' ? 'desc' : 'asc';
          } else {
            this.usageSort = col;
            this.usageSortDir = 'desc';
          }
        },

        async loadAdminUsers() {
          this.adminUsersLoading = true;
          try {
            const r = await fetch('/api/admin/users');
            this.adminUsers = r.ok ? await r.json() : [];
          } catch(e) { this.adminUsers = []; }
          finally { this.adminUsersLoading = false; }
        },

        selectAdminUser(u) {
          this.adminSelected = u;
          this.adminAction   = 'change_pw';
          this.adminPw       = { password:'', confirm:'', saving:false, msg:'', err:'' };
          this.adminName     = { full_name: u.full_name || u.username, saving:false, msg:'', err:'' };
          this.adminRole     = { role: u.role, saving:false, msg:'', err:'' };
          this.adminDeleteConfirm = false;
        },

        async changeAdminFullName() {
          if (!this.adminSelected) return;
          if (!this.adminName.full_name.trim()) { this.adminName.err = 'Full name required'; return; }
          this.adminName.saving = true; this.adminName.err = ''; this.adminName.msg = '';
          try {
            const r = await fetch('/api/admin/users/'+this.adminSelected.id, {
              method: 'PATCH', headers: {'Content-Type':'application/json'},
              body: JSON.stringify({full_name: this.adminName.full_name.trim()}),
            });
            if (!r.ok) { const d = await r.json(); this.adminName.err = d.error||'Error'; return; }
            this.adminSelected.full_name = this.adminName.full_name.trim();
            this.adminName.msg = 'Name updated.';
            await this.loadAdminUsers();
          } catch(e) { this.adminName.err = 'Network error'; }
          finally { this.adminName.saving = false; }
        },

        canChangePassword(userId) {
          if (!CURRENT_USER) return false;
          if (CURRENT_USER.role === 'admin') return true;
          return CURRENT_USER.role === 'user' && CURRENT_USER.id === userId;
        },

        async changeAdminPassword() {
          if (!this.adminSelected) return;
          if (!this.adminPw.password)                       { this.adminPw.err = 'Password required'; return; }
          if (this.adminPw.password !== this.adminPw.confirm) { this.adminPw.err = 'Passwords do not match'; return; }
          this.adminPw.saving = true; this.adminPw.err = ''; this.adminPw.msg = '';
          try {
            const r = await fetch('/api/admin/users/'+this.adminSelected.id+'/password', {
              method: 'POST', headers: {'Content-Type':'application/json'},
              body: JSON.stringify({password: this.adminPw.password}),
            });
            if (!r.ok) { const d = await r.json(); this.adminPw.err = d.error||'Error'; return; }
            this.adminPw.msg = 'Password updated.';
            this.adminPw.password = ''; this.adminPw.confirm = '';
          } catch(e) { this.adminPw.err = 'Network error'; }
          finally { this.adminPw.saving = false; }
        },

        async addAdminUser() {
          if (!this.adminNewUser.username) { this.adminNewUser.err = 'Username required'; return; }
          if (!this.adminNewUser.full_name) { this.adminNewUser.err = 'Full name required'; return; }
          if (!this.adminNewUser.password) { this.adminNewUser.err = 'Password required'; return; }
          this.adminNewUser.saving = true; this.adminNewUser.err = ''; this.adminNewUser.msg = '';
          try {
            const r = await fetch('/api/admin/users', {
              method: 'POST', headers: {'Content-Type':'application/json'},
              body: JSON.stringify({username: this.adminNewUser.username, full_name: this.adminNewUser.full_name, password: this.adminNewUser.password, role: this.adminNewUser.role}),
            });
            if (!r.ok) { const d = await r.json(); this.adminNewUser.err = d.error||'Error'; return; }
            this.adminNewUser.msg = 'User added.';
            this.adminNewUser.username = ''; this.adminNewUser.full_name = ''; this.adminNewUser.password = ''; this.adminNewUser.role = 'user';
            await this.loadAdminUsers();
          } catch(e) { this.adminNewUser.err = 'Network error'; }
          finally { this.adminNewUser.saving = false; }
        },

        async changeAdminRole() {
          if (!this.adminSelected) return;
          this.adminRole.saving = true; this.adminRole.err = ''; this.adminRole.msg = '';
          try {
            const r = await fetch('/api/admin/users/'+this.adminSelected.id, {
              method: 'PATCH', headers: {'Content-Type':'application/json'},
              body: JSON.stringify({role: this.adminRole.role}),
            });
            if (!r.ok) { const d = await r.json(); this.adminRole.err = d.error||'Error'; return; }
            this.adminSelected.role = this.adminRole.role;
            this.adminRole.msg = 'Role updated.';
            await this.loadAdminUsers();
          } catch(e) { this.adminRole.err = 'Network error'; }
          finally { this.adminRole.saving = false; }
        },

        async deleteAdminUser() {
          if (!this.adminSelected) return;
          if (!this.adminDeleteConfirm) { this.adminDeleteConfirm = true; return; }
          try {
            const r = await fetch('/api/admin/users/'+this.adminSelected.id, { method: 'DELETE' });
            if (!r.ok) { const d = await r.json(); alert(d.error||'Error deleting user'); return; }
            this.adminSelected = null;
            this.adminAction   = null;
            this.adminDeleteConfirm = false;
            await this.loadAdminUsers();
          } catch(e) { alert('Network error'); }
        },

        // ── Display helpers ──────────────────────────────────────────────────
        parcelAttrs,
        gisGroups,

        propClassBadge(cls) {
          return {
            'Residential':              'badge-blue',
            'Agricultural / Open Space':'badge-green',
            'Municipal':                'badge-teal',
            'Exempt / Non-profit':      'badge-purple',
          }[cls] || 'badge-gray';
        },

        instrBadge(type) {
          if (!type) return 'badge-gray';
          if (type==='Deed')                return 'badge-blue';
          if (type==='Mortgage')            return 'badge-orange';
          if (type==='Notice')              return 'badge-gray';
          if (type==='Certificate')         return 'badge-teal';
          if (type==='Declaration Of Trust')return 'badge-purple';
          if (type==='Court Order')         return 'badge-amber';
          return 'badge-gray';
        },

        docBadges(d) {
          return KW_ORDER
            .filter(kw => (d['kw_'+kw.key]||0) > KW_THRESHOLD)
            .map(kw => ({
              key:   kw.key,
              abbr:  kw.abbr,
              score: (d['kw_'+kw.key]||0).toFixed(2),
              cls:   'kw-'+kw.cls,
            }));
        },
      };
    }
