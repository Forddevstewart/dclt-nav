// ── Layer attribute filter chips ──────────────────────────────────────────────
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

// ── Keyword metadata (priority order, for detail view score bars) ─────────────
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
