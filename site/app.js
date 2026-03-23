(async function () {
  'use strict';

  var DATA_PATH = 'data';

  // ── Utilities ──

  async function fetchJSON(filename) {
    try {
      var resp = await fetch(DATA_PATH + '/' + filename);
      if (!resp.ok) return null;
      var text = await resp.text();
      var clean = text
        .replace(/\bNaN\b/g, 'null')
        .replace(/\b-Infinity\b/g, 'null')
        .replace(/\bInfinity\b/g, 'null');
      return JSON.parse(clean);
    } catch (e) {
      console.warn('Failed to load ' + filename + ':', e);
      return null;
    }
  }

  function $(sel, ctx) { return (ctx || document).querySelector(sel); }
  function $$(sel, ctx) { return Array.from((ctx || document).querySelectorAll(sel)); }

  function el(tag, attrs, children) {
    var e = document.createElement(tag);
    attrs = attrs || {};
    children = children || [];
    for (var k in attrs) {
      if (!attrs.hasOwnProperty(k)) continue;
      if (k === 'class') e.className = attrs[k];
      else if (k === 'text') e.textContent = attrs[k];
      else if (k === 'html') e.innerHTML = attrs[k];
      else e.setAttribute(k, attrs[k]);
    }
    for (var i = 0; i < children.length; i++) {
      var c = children[i];
      if (typeof c === 'string') e.appendChild(document.createTextNode(c));
      else if (c) e.appendChild(c);
    }
    return e;
  }

  // ── Formatters ──

  function dash() { return '\u2014'; }

  function fmtBn(v) {
    if (v == null) return dash();
    if (v < 0) return '-$' + Math.abs(v).toFixed(1) + 'B';
    return '$' + v.toFixed(1) + 'B';
  }

  function fmtUsdBn(v) {
    if (v == null) return dash();
    var bn = v / 1e9;
    if (bn < 0) return '-$' + Math.abs(bn).toFixed(1) + 'B';
    return '$' + bn.toFixed(1) + 'B';
  }

  function fmtPct(v) {
    if (v == null) return dash();
    return (v * 100).toFixed(1) + '%';
  }

  function fmtNum(v, d) {
    if (v == null) return dash();
    return v.toFixed(d != null ? d : 2);
  }

  function fmtSci(v, d) {
    v = toNumber(v);
    if (v == null) return dash();
    d = d != null ? d : 3;
    if (v === 0) return '0.000';
    if (Math.abs(v) < 0.001) return v.toExponential(d);
    if (Math.abs(v) > 1e6) return v.toExponential(d);
    return v.toFixed(d);
  }

  function fmtPval(v) {
    if (v == null) return dash();
    if (v < 0.001) return '<0.001';
    return v.toFixed(3);
  }

  function fmtDirection(v) {
    if (!v) return dash();
    return v.replace(/_/g, ' ');
  }

  function fmtSnake(v) {
    if (!v) return dash();
    return v.replace(/_/g, ' ');
  }

  var LABEL_MAP = {
    user_note_table_2: 'Manual seed',
    user_note_table_2_adjusted: 'Manual seed (adj.)',
    user_note_table_3_forecast: 'Manual forecast',
    note_estimate: 'Seed estimate',
    note_estimate_adjusted: 'Seed est. (adj.)',
    note_forecast: 'Seed forecast',
    exact_official: 'Official',
    exact_official_numeric: 'Official',
    exact_official_net: 'Official net',
    headline: 'Headline',
    supporting: 'Supporting',
    descriptive_only: 'Descriptive Only',
    causal_pilot_only: 'Causal Pilot Only',
    summary_ready: 'Summary',
    headline_ready: 'Headline',
    supporting_ready: 'Supporting Ready',
    provisional_supporting: 'Provisional Supporting',
    not_started: 'Not started',
    manual_official_capture: 'Official capture',
    official_matched: 'Matched',
    date_proxy: 'Date Proxy',
    derived_event_ledger: 'Derived Event Ledger',
    derived_shock_crosswalk: 'Derived Shock Crosswalk',
    derived_qra_usability: 'Derived Usability',
    derived_qra_robustness: 'Derived Robustness',
    derived_absorption_bridge: 'Derived Absorption Bridge',
    hybrid_exact_nonbill_net_plus_qt_proxy: 'Exact net + QT proxy',
    fallback_gross_coupon_proxy_plus_qt_proxy: 'Gross coupon + QT proxy',
    headline_exact_net_with_labeled_fallbacks: 'Exact net',
    headline_hybrid_exact_with_labeled_fallbacks: 'Hybrid exact',
    csv_canonical: 'CSV',
    json_canonical: 'JSON',
    json_repaired: 'JSON (repaired)',
    official_sec_archive: 'SEC archive'
  };

  function fmtLabel(v) {
    if (!v) return dash();
    return LABEL_MAP[v] || v.replace(/_/g, ' ');
  }

  function signClass(v) {
    if (v == null) return '';
    if (v > 0) return 'positive';
    if (v < 0) return 'negative';
    return '';
  }

  function stars(pval) {
    if (pval == null) return '';
    if (pval < 0.001) return '***';
    if (pval < 0.01) return '**';
    if (pval < 0.05) return '*';
    return '';
  }

  function statusBadge(tier) {
    var labels = {
      headline_ready: 'Headline Ready',
      summary_ready: 'Summary Ready',
      supporting_ready: 'Supporting Ready',
      provisional_supporting: 'Provisional Supporting',
      not_started: 'Not Started'
    };
    return el('span', {
      class: 'badge badge-' + tier,
      text: labels[tier] || tier
    });
  }

  function relTime(isoStr) {
    if (!isoStr) return 'Never';
    var d = new Date(isoStr);
    var now = new Date();
    var diffH = Math.floor((now - d) / 3600000);
    if (diffH < 1) return 'Just now';
    if (diffH < 24) return diffH + 'h ago';
    var diffD = Math.floor(diffH / 24);
    if (diffD < 7) return diffD + 'd ago';
    return d.toISOString().slice(0, 10);
  }

  function fmtTimestamp(v) {
    if (!v) return 'Never';
    return v.slice(0, 16).replace('T', ' ') + ' UTC';
  }

  function checkMark(v) { return v ? '\u2713' : '\u2717'; }

  function toNumber(value) {
    if (value === null || value === undefined || value === '') return null;
    var n = Number(value);
    return Number.isFinite(n) ? n : null;
  }

  function quarterCompare(a, b) {
    return String(a).localeCompare(String(b));
  }

  function quarterRank(q) {
    var match = String(q || '').match(/^(\d{4})Q([1-4])$/);
    if (!match) return null;
    return (parseInt(match[1], 10) * 4) + parseInt(match[2], 10);
  }

  function formatQuarterSpans(quarters) {
    if (!quarters.length) return '';
    var spans = [];
    var current = { start: quarters[0], end: quarters[0], rank: quarterRank(quarters[0]) };
    for (var i = 1; i < quarters.length; i++) {
      var q = quarters[i];
      var rank = quarterRank(q);
      if (current.rank != null && rank != null && rank === current.rank + 1) {
        current.end = q;
        current.rank = rank;
      } else {
        spans.push(current);
        current = { start: q, end: q, rank: rank };
      }
    }
    spans.push(current);
    return spans.map(function (span) {
      return span.start === span.end ? span.start : span.start + ' through ' + span.end;
    }).join(', ');
  }

  function sortRowsByQuarter(rows) {
    return rows.slice().sort(function (a, b) {
      return quarterCompare(a.quarter, b.quarter);
    });
  }

  function hasSeedAtiColumns(rows) {
    return !!(rows && rows.length && (
      Object.prototype.hasOwnProperty.call(rows[0], 'seed_source') ||
      Object.prototype.hasOwnProperty.call(rows[0], 'seed_quality')
    ));
  }

  function normalizeAtiRow(row, opts) {
    opts = opts || {};
    return {
      quarter: row.quarter,
      financing_need_bn: toNumber(row.financing_need_bn),
      net_bills_bn: toNumber(row.net_bills_bn),
      bill_share: toNumber(row.bill_share),
      missing_coupons_15_bn: toNumber(row.missing_coupons_15_bn),
      missing_coupons_18_bn: toNumber(row.missing_coupons_18_bn),
      missing_coupons_20_bn: toNumber(row.missing_coupons_20_bn),
      ati_baseline_bn: toNumber(row.ati_baseline_bn),
      readiness_tier: row.readiness_tier || opts.readiness_tier || '',
      public_role: row.public_role || opts.public_role || ''
    };
  }

  function buildOfficialAtiFromCapture(captureRows) {
    var rows = [];
    for (var i = 0; i < captureRows.length; i++) {
      var row = captureRows[i];
      var financingNeed = toNumber(row.total_financing_need_bn);
      var netBills = toNumber(row.net_bill_issuance_bn);
      if (financingNeed == null || netBills == null) continue;
      rows.push({
        quarter: row.quarter,
        financing_need_bn: financingNeed,
        net_bills_bn: netBills,
        bill_share: financingNeed === 0 ? null : netBills / financingNeed,
        missing_coupons_15_bn: netBills - financingNeed * 0.15,
        missing_coupons_18_bn: netBills - financingNeed * 0.18,
        missing_coupons_20_bn: netBills - financingNeed * 0.20,
        ati_baseline_bn: netBills - financingNeed * 0.18,
        readiness_tier: row.readiness_tier || 'headline_ready',
        public_role: 'headline'
      });
    }
    return sortRowsByQuarter(rows);
  }

  function buildOfficialAtiFromQuarterTable(rows) {
    var official = [];
    for (var i = 0; i < rows.length; i++) {
      var row = rows[i];
      official.push(normalizeAtiRow(row, { readiness_tier: 'headline_ready', public_role: 'headline' }));
    }
    return sortRowsByQuarter(official);
  }

  function buildSeedForecastRows(seedRows, officialQuarters) {
    var officialMap = {};
    for (var i = 0; i < officialQuarters.length; i++) {
      officialMap[String(officialQuarters[i])] = true;
    }

    var rows = [];
    for (var j = 0; j < seedRows.length; j++) {
      var row = seedRows[j];
      if (officialMap[String(row.quarter)]) continue;
      rows.push({
        quarter: row.quarter,
        financing_need_bn: toNumber(row.financing_need_bn),
        net_bills_bn: toNumber(row.net_bills_bn),
        bill_share: toNumber(row.bill_share),
        missing_coupons_15_bn: toNumber(row.missing_coupons_15_bn),
        missing_coupons_18_bn: toNumber(row.missing_coupons_18_bn),
        missing_coupons_20_bn: toNumber(row.missing_coupons_20_bn),
        ati_baseline_bn: toNumber(row.ati_baseline_bn),
        seed_source: row.seed_source || row.source_label || row.comparison_status || 'Seed / forecast',
        seed_quality: row.seed_quality || row.comparison_status || '',
        public_role: 'supporting'
      });
    }
    return sortRowsByQuarter(rows);
  }

  function normalizePublicRole(value) {
    return value === 'headline' ? 'headline' : 'supporting';
  }

  function publicRoleForDataset(dataset, fallback) {
    var headline = {
      official_ati: true,
      plumbing: true,
      duration: true
    };
    if (headline[dataset]) return 'headline';
    if (fallback === 'supporting') return 'supporting';
    if (fallback === 'headline') return 'supporting';
    return 'supporting';
  }

  function rolePill(role) {
    var normalized = normalizePublicRole(role);
    return el('span', {
      class: 'role-pill role-' + normalized,
      text: normalized === 'headline' ? 'Headline' : 'Supporting'
    });
  }

  function formatCoverage(rows) {
    if (!rows || !rows.length) return 'Official exact coverage unavailable.';
    var sorted = rows
      .map(function (row) { return String(row.quarter); })
      .filter(function (q) { return quarterRank(q) != null; })
      .sort(function (a, b) { return quarterRank(a) - quarterRank(b); });
    return 'Current exact official quarter coverage includes ' + formatQuarterSpans(sorted) + ' (' + sorted.length + ' quarters).';
  }

  function robustnessNote(rows) {
    if (!rows || !rows.length) return 'Robustness evidence unavailable.';
    var allSingle = rows.every(function (row) { return toNumber(row.n_events) === 1; });
    if (allSingle) {
      return 'Robustness rows are all n=1. Treat this grid as descriptive single-event summary work, not inferential evidence.';
    }
    var counts = rows
      .map(function (row) { return toNumber(row.n_events); })
      .filter(function (value) { return value != null; });
    var minN = Math.min.apply(Math, counts);
    var maxN = Math.max.apply(Math, counts);
    return 'Robustness sample sizes range from n=' + minN + ' to n=' + maxN + '. Results remain descriptive unless the sample expands further.';
  }

  function normalizeCausalClaimsRow(row) {
    return {
      subject: row.claim_id || row.dataset || row.artifact || row.surface || row.event_id || row.claim_name || '',
      claim_scope: row.claim_scope || row.scope || row.public_role || '',
      readiness_tier: row.readiness_tier || row.review_maturity || row.status || '',
      public_role: row.public_role || '',
      headline_ready: row.headline_ready != null ? row.headline_ready : (row.is_headline_ready != null ? row.is_headline_ready : null),
      causal_pilot_ready: row.causal_pilot_ready != null ? row.causal_pilot_ready : (row.causal_eligible != null ? row.causal_eligible : null),
      source_quality: row.source_quality || '',
      reason: row.boundary_reason || row.claim_scope_reason || row.reason || row.notes || '',
      updated: row.last_regenerated_utc || row.generated_at_utc || row.updated_at_utc || ''
    };
  }

  function benchmarkDispositionLabel(value) {
    var v = String(value || '').trim();
    var labels = {
      tier_a_causal_pilot_ready: 'Tier A / pilot ready',
      reviewed_surprise_ready_not_tier_a: 'Pre-release, not Tier A',
      reviewed_contaminated_context_only: 'Context only',
      reviewed_contaminated_exclude: 'Excluded',
      post_release_invalid: 'Blocked',
      external_timing_unverified: 'Blocked',
      same_release_placeholder: 'Blocked',
      benchmark_verification_incomplete: 'Blocked',
      pending_contamination_review: 'Pending',
      review_pending: 'Pending'
    };
    return labels[v] || fmtLabel(v);
  }

  function normalizeBenchmarkEvidenceRow(row) {
    return {
      release_component_id: row.release_component_id || '',
      event_id: row.event_id || '',
      quarter: row.quarter || '',
      quality_tier: row.quality_tier || '',
      benchmark_timing_status: row.benchmark_timing_status || '',
      external_benchmark_ready: row.external_benchmark_ready,
      expectation_status: row.expectation_status || '',
      contamination_status: row.contamination_status || '',
      terminal_disposition: row.terminal_disposition || '',
      claim_scope: row.claim_scope || '',
      benchmark_source_family: row.benchmark_source_family || ''
    };
  }

  // ── Table Builder ──

  function buildTable(columns, rows, opts) {
    opts = opts || {};
    var wrap = el('div', { class: 'table-wrap' });
    var table = el('table', opts.tableClass ? { class: opts.tableClass } : {});

    var thead = el('thead');
    var hrow = el('tr');
    for (var c = 0; c < columns.length; c++) {
      var col = columns[c];
      hrow.appendChild(el('th', {
        text: col.label,
        class: col.numeric ? 'num' : ''
      }));
    }
    thead.appendChild(hrow);
    table.appendChild(thead);

    var tbody = el('tbody');
    for (var r = 0; r < rows.length; r++) {
      var row = rows[r];
      var tr = el('tr');
      if (opts.metaRows && opts.metaRows.indexOf(r) >= 0) tr.className = 'meta-row';
      for (var ci = 0; ci < columns.length; ci++) {
        var col2 = columns[ci];
        var raw = row[col2.key];
        var formatted = col2.format ? col2.format(raw, row) : (raw != null ? raw : dash());
        var cls = [];
        if (col2.numeric) cls.push('num');
        if (col2.colorSign && raw != null) {
          var sc = signClass(raw);
          if (sc) cls.push(sc);
        }
        var td = el('td', { class: cls.join(' ') });
        if (formatted instanceof HTMLElement) {
          td.appendChild(formatted);
        } else {
          td.textContent = String(formatted);
        }
        tr.appendChild(td);
      }
      tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    wrap.appendChild(table);
    return wrap;
  }

  function sectionError(container, msg) {
    container.appendChild(el('p', { class: 'error-msg', text: msg }));
  }

  // ── Section Renderers ──

  async function renderOverview() {
    var container = $('#overview-content');
    var results = await Promise.all([
      fetchJSON('dataset_status.json'),
      fetchJSON('official_qra_capture.json'),
      fetchJSON('ati_quarter_table.json')
    ]);
    var data = results[0], capture = results[1], ati = results[2];
    if (!data) { sectionError(container, 'Dataset status unavailable.'); return; }

    var officialRows = [];
    if (ati && ati.rows && ati.rows.length && !hasSeedAtiColumns(ati.rows)) {
      officialRows = buildOfficialAtiFromQuarterTable(ati.rows);
    } else if (capture && capture.rows) {
      officialRows = buildOfficialAtiFromCapture(capture.rows);
    }

    var coverageCopy = $('#overview-coverage');
    if (coverageCopy) {
      coverageCopy.textContent = officialRows.length
        ? formatCoverage(officialRows)
        : 'Official exact coverage unavailable.';
    }

    container.appendChild(el('div', { class: 'card' }, [
      el('div', { class: 'card-title', text: 'Current Coverage' }),
      el('div', { class: 'card-value', text: officialRows.length ? (officialRows[0].quarter + ' to ' + officialRows[officialRows.length - 1].quarter) : 'Unavailable' }),
      el('div', { class: 'card-meta', text: formatCoverage(officialRows) })
    ]));

    var grid = el('div', { class: 'card-grid' });
    for (var i = 0; i < data.rows.length; i++) {
      var row = data.rows[i];
      var publicRole = publicRoleForDataset(row.dataset, row.public_role);
      var name = row.dataset.replace(/^extension_/, '').replace(/_/g, ' ');
      grid.appendChild(el('div', { class: 'card' }, [
        el('div', { class: 'card-title', text: name }),
        el('div', { class: 'card-value' }, [statusBadge(row.readiness_tier)]),
        el('div', { class: 'card-meta' }, [el('span', { class: 'card-inline-label', text: 'Public role: ' }), rolePill(publicRole)]),
        el('div', { class: 'card-meta', text: 'Source: ' + fmtSnake(row.source_quality) }),
        row.review_maturity ? el('div', { class: 'card-meta', text: 'Review: ' + fmtLabel(row.review_maturity) }) : null,
        el('div', { class: 'card-meta', text: 'Updated: ' + relTime(row.last_regenerated_utc) })
      ]));
    }
    container.appendChild(grid);
  }

  async function renderCapture() {
    var container = $('#capture-content');
    var index = await fetchJSON('index.json');
    var results = await Promise.all([
      fetchJSON('official_qra_capture.json'),
      fetchJSON('ati_quarter_table.json'),
      fetchJSON('ati_seed_vs_official.json')
    ]);
    var capture = results[0], ati = results[1], seedVs = results[2];
    var atiSeedStyle = !!(ati && ati.rows && ati.rows.length && hasSeedAtiColumns(ati.rows));
    var forecast = null;
    if (!atiSeedStyle && index && index.artifacts && index.artifacts.indexOf('ati_seed_forecast_table.json') >= 0) {
      forecast = await fetchJSON('ati_seed_forecast_table.json');
    }

    var officialAtiRows = [];
    if (ati && ati.rows && ati.rows.length && !hasSeedAtiColumns(ati.rows)) {
      officialAtiRows = buildOfficialAtiFromQuarterTable(ati.rows);
    } else if (capture && capture.rows) {
      officialAtiRows = buildOfficialAtiFromCapture(capture.rows);
    }

    var seedForecastRows = [];
    if (forecast && forecast.rows && forecast.rows.length) {
      seedForecastRows = buildSeedForecastRows(forecast.rows, officialAtiRows.map(function (row) { return row.quarter; }));
    } else if (atiSeedStyle && ati && ati.rows && ati.rows.length) {
      seedForecastRows = buildSeedForecastRows(ati.rows, officialAtiRows.map(function (row) { return row.quarter; }));
    }

    if (capture) {
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Official Quarterly Refunding Capture' }));
      container.appendChild(el('p', { class: 'card-meta', text: formatCoverage(officialAtiRows) }));
      container.appendChild(buildTable([
        { key: 'quarter', label: 'Quarter' },
        { key: 'qra_release_date', label: 'Release' },
        { key: 'total_financing_need_bn', label: 'Need ($B)', numeric: true, format: fmtBn },
        { key: 'net_bill_issuance_bn', label: 'Net Bills ($B)', numeric: true, format: fmtBn, colorSign: true },
        { key: 'readiness_tier', label: 'Status', format: function (v) { return statusBadge(v); } }
      ], capture.rows));
    }

    if (officialAtiRows.length) {
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Treasury Maturity Composition' }));
      container.appendChild(el('p', { class: 'section-desc',
        text: 'Headline official quarter panel. Coupon shortfall is measured relative to an 18% bill baseline, and higher values mean more bill-heavy financing.'
      }));
      container.appendChild(buildTable([
        { key: 'quarter', label: 'Quarter' },
        { key: 'financing_need_bn', label: 'Need ($B)', numeric: true, format: fmtBn },
        { key: 'net_bills_bn', label: 'Net Bills ($B)', numeric: true, format: fmtBn },
        { key: 'bill_share', label: 'Bill Share', numeric: true, format: fmtPct },
        { key: 'missing_coupons_15_bn', label: 'Shortfall @15% ($B)', numeric: true, format: fmtBn, colorSign: true },
        { key: 'ati_baseline_bn', label: 'Shortfall ($B)', numeric: true, format: fmtBn, colorSign: true },
        { key: 'missing_coupons_20_bn', label: 'Shortfall @20% ($B)', numeric: true, format: fmtBn, colorSign: true },
        { key: 'readiness_tier', label: 'Status', format: function (v) { return statusBadge(v); } }
      ], officialAtiRows));
    }

    if (seedForecastRows.length) {
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Seed / Forecast Rows' }));
      container.appendChild(el('p', { class: 'section-desc',
        text: 'Non-headline context only. These rows remain seed or forecast values and should not be read as the official quarter panel.'
      }));
      container.appendChild(buildTable([
        { key: 'quarter', label: 'Quarter' },
        { key: 'financing_need_bn', label: 'Need ($B)', numeric: true, format: fmtBn },
        { key: 'net_bills_bn', label: 'Net Bills ($B)', numeric: true, format: fmtBn },
        { key: 'bill_share', label: 'Bill Share', numeric: true, format: fmtPct },
        { key: 'ati_baseline_bn', label: 'Shortfall ($B)', numeric: true, format: fmtBn, colorSign: true },
        { key: 'seed_source', label: 'Source', format: fmtLabel },
        { key: 'seed_quality', label: 'Seed Quality', format: fmtLabel }
      ], seedForecastRows));
    }

    if (seedVs) {
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Seed vs. Official Comparison' }));
      container.appendChild(buildTable([
        { key: 'quarter', label: 'Quarter' },
        { key: 'ati_seed_bn', label: 'Seed Shortfall ($B)', numeric: true, format: fmtBn },
        { key: 'ati_official_bn', label: 'Official Shortfall ($B)', numeric: true, format: fmtBn },
        { key: 'ati_diff_official_minus_seed', label: 'Diff ($B)', numeric: true, format: fmtBn, colorSign: true },
        { key: 'comparison_status', label: 'Status', format: fmtLabel }
      ], seedVs.rows));
    }

    if (!capture && !officialAtiRows.length && !seedForecastRows.length && !seedVs) {
      sectionError(container, 'QRA capture data unavailable.');
    }
  }

  async function renderEvents() {
    var container = $('#events-content');
    var results = await Promise.all([
      fetchJSON('qra_event_summary.json'),
      fetchJSON('qra_event_table.json'),
      fetchJSON('qra_event_robustness.json')
    ]);
    var summary = results[0], events = results[1], robustness = results[2];

    if (summary) {
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Market Reactions by QRA Direction' }));
      container.appendChild(el('p', { class: 'section-desc',
        text: 'Mean changes in yields (percentage points), S&P 500 (points), term premium, and VIX around QRA announcements, grouped by expected policy direction. d1 = 1-day window, d3 = 3-day window.'
      }));

      var metrics = [
        { key: 'DGS10', label: '10Y Yield' },
        { key: 'DGS2', label: '2Y Yield' },
        { key: 'DGS30', label: '30Y Yield' },
        { key: 'slope_10y_2y', label: '10Y\u20132Y Slope' },
        { key: 'THREEFYTP10', label: 'Term Premium' },
        { key: 'SP500', label: 'S&P 500' },
        { key: 'VIXCLS', label: 'VIX' }
      ];
      var directions = summary.rows.map(function (r) { return r.expected_direction; });

      var columns = [
        { key: 'metric', label: 'Metric' },
        { key: 'window', label: 'Window' }
      ];
      for (var di = 0; di < directions.length; di++) {
        (function (d) {
          columns.push({
            key: d,
            label: fmtDirection(d),
            numeric: true,
            colorSign: true,
            format: function (v) { return v == null ? dash() : fmtNum(v, 3); }
          });
        })(directions[di]);
      }

      var rows = [];
      for (var mi = 0; mi < metrics.length; mi++) {
        for (var wi = 0; wi < 2; wi++) {
          var w = wi === 0 ? 'd1' : 'd3';
          var row = { metric: metrics[mi].label, window: wi === 0 ? '1-day' : '3-day' };
          for (var si = 0; si < summary.rows.length; si++) {
            var sr = summary.rows[si];
            row[sr.expected_direction] = sr[metrics[mi].key + '_' + w];
          }
          rows.push(row);
        }
      }
      container.appendChild(buildTable(columns, rows));
    }

    if (events) {
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Individual QRA Events' }));
      var officialEvents = events.rows.filter(function (r) {
        return r.event_date_type === 'official_release_date';
      });
      var displayEvents = officialEvents.length > 0 ? officialEvents : events.rows.slice(0, 10);

      container.appendChild(buildTable([
        { key: 'event_label', label: 'Event' },
        { key: 'event_date_aligned', label: 'Date' },
        { key: 'expected_direction', label: 'Direction', format: fmtDirection },
        { key: 'DGS10_d1', label: '10Y d1', numeric: true, colorSign: true, format: function (v) { return fmtNum(v, 3); } },
        { key: 'DGS10_d3', label: '10Y d3', numeric: true, colorSign: true, format: function (v) { return fmtNum(v, 3); } },
        { key: 'THREEFYTP10_d1', label: 'TP d1', numeric: true, colorSign: true, format: function (v) { return fmtNum(v, 4); } },
        { key: 'SP500_d1', label: 'S&P d1', numeric: true, colorSign: true, format: function (v) { return fmtNum(v, 1); } },
        { key: 'VIXCLS_d1', label: 'VIX d1', numeric: true, colorSign: true, format: function (v) { return fmtNum(v, 2); } }
      ], displayEvents));
    }

    if (robustness) {
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Robustness: All Events by Date Type' }));
      var robRows = robustness.rows.filter(function (r) { return r.sample_variant === 'all_events'; });
      if (robRows.length) {
        container.appendChild(el('p', { class: 'supporting-note', text: robustnessNote(robRows) }));
        container.appendChild(buildTable([
          { key: 'event_date_type', label: 'Date Type', format: fmtSnake },
          { key: 'expected_direction', label: 'Direction', format: fmtDirection },
          { key: 'n_events', label: 'N', numeric: true },
          { key: 'DGS10_d1', label: '10Y d1', numeric: true, colorSign: true, format: function (v) { return fmtNum(v, 3); } },
          { key: 'DGS10_d3', label: '10Y d3', numeric: true, colorSign: true, format: function (v) { return fmtNum(v, 3); } },
          { key: 'THREEFYTP10_d3', label: 'TP d3', numeric: true, colorSign: true, format: function (v) { return fmtNum(v, 4); } }
        ], robRows));
      }
    }

    if (robustness && !robustness.rows.filter(function (r) { return r.sample_variant === 'all_events'; }).length) {
      container.appendChild(el('p', { class: 'supporting-note', text: robustnessNote(robustness.rows) }));
    }

    if (!summary && !events) {
      sectionError(container, 'Event study data unavailable.');
    }
  }

  async function renderMethods() {
    var container = $('#methods-content');
    var results = await Promise.all([
      fetchJSON('qra_event_registry_v2.json'),
      fetchJSON('qra_shock_crosswalk_v1.json'),
      fetchJSON('treatment_comparison_table.json'),
      fetchJSON('event_usability_table.json'),
      fetchJSON('leave_one_event_out_table.json'),
      fetchJSON('auction_absorption_table.json'),
      fetchJSON('qra_event_shock_summary.json'),
      fetchJSON('qra_event_elasticity.json'),
      fetchJSON('qra_event_robustness.json'),
      fetchJSON('dataset_status.json')
    ]);
    var registry = results[0], crosswalk = results[1], treatmentComparison = results[2], usability = results[3], leaveOne = results[4], absorption = results[5];
    var shockSummary = results[6], elasticity = results[7], robustness = results[8], dsStatus = results[9];

    if (registry || shockSummary) {
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Event Ledger' }));
      var ledgerRows = registry && registry.rows && registry.rows.length ? registry.rows : (shockSummary ? shockSummary.rows : []);
      if (ledgerRows.length) {
        var normalizedLedger = ledgerRows.map(function (row) {
          return {
            event_id: row.event_id,
            quarter: row.quarter,
            release_timestamp_et: row.release_timestamp_et,
            release_timestamp_kind: row.release_timestamp_kind || row.release_timestamp_precision || '',
            release_bundle_type: row.release_bundle_type || row.timing_quality || '',
            policy_statement_url: row.policy_statement_url,
            financing_estimates_url: row.financing_estimates_url,
            headline_eligibility_reason: row.headline_eligibility_reason || row.usable_for_headline_reason || '',
            treatment_version_id: row.treatment_version_id || row.canonical_shock_id || row.shock_construction || ''
          };
        });
        container.appendChild(el('p', { class: 'section-desc',
          text: 'Reviewed event rows with bundle type, treatment version, and the current headline eligibility reason. Exact timestamps are surfaced when available; otherwise the current release uses a date proxy.'
        }));
        container.appendChild(buildTable([
          { key: 'event_id', label: 'Event' },
          { key: 'quarter', label: 'Quarter' },
          { key: 'release_timestamp_et', label: 'Release ET', format: fmtTimestamp },
          { key: 'release_timestamp_kind', label: 'Timestamp Kind', format: fmtLabel },
          { key: 'release_bundle_type', label: 'Bundle', format: fmtLabel },
          { key: 'headline_eligibility_reason', label: 'Eligibility', format: fmtLabel },
          { key: 'treatment_version_id', label: 'Treatment', format: fmtSnake }
        ], normalizedLedger.slice(0, 12)));
      }
    }

    if (crosswalk || elasticity) {
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Shock Crosswalk' }));
      var crossRows = crosswalk && crosswalk.rows && crosswalk.rows.length ? crosswalk.rows : (elasticity ? elasticity.rows : []);
      if (crossRows.length) {
        container.appendChild(buildTable([
          { key: 'event_id', label: 'Event' },
          { key: 'event_date_type', label: 'Date Type', format: fmtSnake },
          { key: 'canonical_shock_id', label: 'Canonical Shock', format: fmtSnake },
          { key: 'shock_bn', label: 'Shock ($B)', numeric: true, colorSign: true, format: fmtBn },
          { key: 'schedule_diff_10y_eq_bn', label: '10Y-eq ($B)', numeric: true, colorSign: true, format: fmtBn },
          { key: 'schedule_diff_dynamic_10y_eq_bn', label: 'Dyn. 10Y-eq ($B)', numeric: true, colorSign: true, format: fmtBn },
          { key: 'shock_review_status', label: 'Review', format: fmtLabel }
        ], crossRows.slice(0, 12)));
      }
    }

    if (treatmentComparison) {
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Treatment Comparison' }));
      var comparisonRows = treatmentComparison.rows && treatmentComparison.rows.length ? treatmentComparison.rows : [];
      if (comparisonRows.length) {
        var normalizedComparison = comparisonRows.map(function (row) {
          return {
            event_date_type: row.event_date_type || 'official_release_date',
            series: row.series,
            window: row.window,
            treatment_variant: row.treatment_variant,
            comparison_family_label: row.comparison_family_label || row.comparison_family || '',
            n_events: row.n_events,
            n_headline_eligible_events: row.n_headline_eligible_events,
            mean_elasticity_value: row.mean_elasticity_value,
            delta_vs_family_reference_mean_elasticity_value: row.delta_vs_family_reference_mean_elasticity_value,
            bp_family_spread_elasticity_value: row.bp_family_spread_elasticity_value,
            headline_recommendation_status: row.headline_recommendation_status,
            primary_treatment_reason: row.primary_treatment_reason || '',
          };
        });
        container.appendChild(el('p', { class: 'section-desc',
          text: 'Canonical shock stays the headline contract. Fixed 10Y-equivalent, dynamic 10Y-equivalent, and DV01 variants are published as comparison diagnostics by series and window.'
        }));
        container.appendChild(buildTable([
          { key: 'event_date_type', label: 'Date Type', format: fmtSnake },
          { key: 'series', label: 'Series' },
          { key: 'window', label: 'Window' },
          { key: 'treatment_variant', label: 'Treatment', format: fmtSnake },
          { key: 'comparison_family_label', label: 'Family', format: fmtLabel },
          { key: 'n_events', label: 'Events', numeric: true },
          { key: 'n_headline_eligible_events', label: 'Headline Eligible', numeric: true },
          { key: 'mean_elasticity_value', label: 'Mean', numeric: true, colorSign: true, format: function (v) { return fmtNum(v, 3); } },
          { key: 'delta_vs_family_reference_mean_elasticity_value', label: 'Delta vs Family Ref', numeric: true, colorSign: true, format: function (v) { return fmtNum(v, 3); } },
          { key: 'bp_family_spread_elasticity_value', label: 'BP Family Spread', numeric: true, colorSign: true, format: function (v) { return fmtNum(v, 3); } },
          { key: 'headline_recommendation_status', label: 'Status', format: fmtLabel }
        ], normalizedComparison.slice(0, 16)));
        var primaryReason = normalizedComparison[0] && normalizedComparison[0].primary_treatment_reason;
        if (primaryReason) {
          container.appendChild(el('p', { class: 'supporting-note', text: primaryReason }));
        }
      }
    }

    if (usability || robustness) {
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Event Usability' }));
      var usabilityRows = usability && usability.rows && usability.rows.length ? usability.rows : [];
      if (usabilityRows.length) {
        var normalizedUsability = usabilityRows.map(function (row) {
          var count = row.n_rows != null ? row.n_rows : (row.n_events != null ? row.n_events : row.event_count);
          return {
            event_date_type: row.event_date_type,
            headline_bucket: row.headline_bucket,
            classification_review_status: row.classification_review_status,
            shock_review_status: row.shock_review_status,
            overlap_severity: row.overlap_severity,
            usable_for_headline: row.usable_for_headline,
            n_rows: count,
            n_events: row.n_events != null ? row.n_events : count
          };
        });
        container.appendChild(buildTable([
          { key: 'event_date_type', label: 'Date Type', format: fmtSnake },
          { key: 'headline_bucket', label: 'Bucket', format: fmtDirection },
          { key: 'classification_review_status', label: 'Class Review', format: fmtLabel },
          { key: 'shock_review_status', label: 'Shock Review', format: fmtLabel },
          { key: 'overlap_severity', label: 'Overlap', format: fmtLabel },
          { key: 'usable_for_headline', label: 'Headline?', format: checkMark },
          { key: 'n_rows', label: 'Rows', numeric: true },
          { key: 'n_events', label: 'Events', numeric: true }
        ], normalizedUsability.slice(0, 12)));
      } else if (robustness && robustness.rows && robustness.rows.length) {
        container.appendChild(el('p', { class: 'supporting-note',
          text: 'Event usability counts are not yet published in this snapshot; the event robustness grid remains available as a descriptive fallback.'
        }));
        container.appendChild(buildTable([
          { key: 'sample_variant', label: 'Sample' },
          { key: 'event_date_type', label: 'Date Type', format: fmtSnake },
          { key: 'headline_bucket', label: 'Bucket', format: fmtDirection },
          { key: 'n_events', label: 'Events', numeric: true }
        ], robustness.rows.slice(0, 12)));
      }
    }

    container.appendChild(el('h3', { class: 'section-subtitle', text: 'Absorption Bridge' }));
    if (absorption && absorption.rows && absorption.rows.length) {
      var normalizedAbsorption = absorption.rows.map(function (row) {
        return {
          qra_event_id: row.qra_event_id,
          quarter: row.quarter,
          auction_date: row.auction_date || row.date || row.as_of_date || '',
          security_family: row.security_family,
          investor_class: row.investor_class,
          measure: row.measure,
          value: row.value,
          units: row.units,
          source_family: row.source_family || row.view_type || '',
          provenance_summary: row.provenance_summary || row.source_quality || ''
        };
      });
      container.appendChild(el('p', { class: 'section-desc',
        text: 'Quarter-keyed bridge from QRA event rows into Treasury investor allotments and Treasury coupon/TIPS dealer subsets.'
      }));
      container.appendChild(buildTable([
        { key: 'qra_event_id', label: 'Event' },
        { key: 'quarter', label: 'Quarter' },
        { key: 'auction_date', label: 'Auction Date' },
        { key: 'security_family', label: 'Security', format: fmtSnake },
        { key: 'investor_class', label: 'Investor Class', format: fmtSnake },
        { key: 'measure', label: 'Measure', format: fmtSnake },
        { key: 'value', label: 'Value', numeric: true, format: function (v) { return v != null ? fmtNum(v, 2) : dash(); } },
        { key: 'source_family', label: 'Source', format: fmtSnake }
      ], normalizedAbsorption.slice(0, 12)));
    }

    if (leaveOne && leaveOne.rows && leaveOne.rows.length) {
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Leave-One-Out Robustness' }));
      var normalizedLeaveOne = leaveOne.rows.map(function (row) {
        return {
          event_id: row.event_id || row.dropped_event_id || '',
          event_date_type: row.event_date_type || '',
          series: row.series,
          window: row.window,
          leave_one_out_coefficient: row.leave_one_out_coefficient != null ? row.leave_one_out_coefficient : (row.mean_elasticity != null ? row.mean_elasticity : row.mean_elasticity_value),
          leave_one_out_std_err: row.leave_one_out_std_err != null ? row.leave_one_out_std_err : row.std_err,
          leave_one_out_delta: row.leave_one_out_delta != null ? row.leave_one_out_delta : '',
          n_observations: row.n_observations != null ? row.n_observations : row.n_events
        };
      });
      container.appendChild(buildTable([
        { key: 'event_id', label: 'Event' },
        { key: 'event_date_type', label: 'Date Type', format: fmtSnake },
        { key: 'series', label: 'Series' },
        { key: 'window', label: 'Window' },
        { key: 'leave_one_out_coefficient', label: 'Coef', numeric: true, format: fmtSci },
        { key: 'leave_one_out_std_err', label: 'SE', numeric: true, format: fmtSci },
        { key: 'leave_one_out_delta', label: 'Delta', numeric: true, format: fmtSci },
        { key: 'n_observations', label: 'N', numeric: true }
      ], normalizedLeaveOne.slice(0, 12)));
    } else {
      container.appendChild(el('p', { class: 'supporting-note',
        text: 'Leave-one-out results are not yet published in this snapshot; the table is wired for future output files.'
      }));
    }

    if (dsStatus) {
      var qraRow = dsStatus.rows.find(function (row) { return row.dataset === 'qra_event_elasticity'; });
      if (qraRow) {
        container.appendChild(el('p', { class: 'supporting-note',
          text: 'QRA elasticity is currently marked as ' + fmtLabel(qraRow.review_maturity || qraRow.readiness_tier) + ' with source quality ' + fmtSnake(qraRow.source_quality) + '.'
        }));
      }
    }
  }

  async function renderPlumbing() {
    var container = $('#plumbing-content');
    var results = await Promise.all([
      fetchJSON('plumbing_regression_summary.json'),
      fetchJSON('plumbing_robustness.json')
    ]);
    var regSummary = results[0], robustness = results[1];

    if (regSummary) {
      var byDV = {};
      for (var i = 0; i < regSummary.rows.length; i++) {
        var row = regSummary.rows[i];
        if (!byDV[row.dependent_variable]) byDV[row.dependent_variable] = [];
        byDV[row.dependent_variable].push(row);
      }

      var dvKeys = Object.keys(byDV);
      var dvLabels = {
        delta_wlrral: '\u0394 ON RRP',
        delta_wresbal: '\u0394 Reserves'
      };
      var terms = byDV[dvKeys[0]].map(function (r) { return r.term; });
      var termLabels = {
        const: 'Constant',
        bill_net_exact: 'Bill net issuance',
        nonbill_net_exact: 'Non-bill net issuance',
        delta_wdtgal: '\u0394 TGA',
        qt_proxy: 'QT proxy'
      };

      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Regression Results' }));

      var meta = regSummary.rows[0];
      if (meta && meta.notes) {
        container.appendChild(el('p', { class: 'section-desc', text: meta.notes }));
      }

      // Build regression-style table
      var wrap = el('div', { class: 'table-wrap' });
      var table = el('table', { class: 'reg-table' });

      var thead = el('thead');
      var hrow = el('tr');
      hrow.appendChild(el('th', { text: '' }));
      for (var d = 0; d < dvKeys.length; d++) {
        hrow.appendChild(el('th', { class: 'num', text: dvLabels[dvKeys[d]] || dvKeys[d] }));
      }
      thead.appendChild(hrow);
      table.appendChild(thead);

      var tbody = el('tbody');
      for (var t = 0; t < terms.length; t++) {
        var tr = el('tr');
        tr.appendChild(el('td', { text: termLabels[terms[t]] || fmtSnake(terms[t]) }));
        for (var dv = 0; dv < dvKeys.length; dv++) {
          var match = byDV[dvKeys[dv]].find(function (r) { return r.term === terms[t]; });
          var td = el('td', { class: 'num coef-cell' });
          if (match) {
            var s = stars(match.p_value);
            td.appendChild(el('div', { class: 'coef', text: fmtSci(match.coef) + (s ? ' ' + s : '') }));
            td.appendChild(el('div', { class: 'se', text: '(' + fmtSci(match.std_err) + ')' }));
          } else {
            td.textContent = dash();
          }
          tr.appendChild(td);
        }
        tbody.appendChild(tr);
      }

      // N and R-squared rows
      var trN = el('tr', { class: 'meta-row' });
      trN.appendChild(el('td', { text: 'N' }));
      for (var d2 = 0; d2 < dvKeys.length; d2++) {
        trN.appendChild(el('td', { class: 'num', text: String(byDV[dvKeys[d2]][0].nobs || dash()) }));
      }
      tbody.appendChild(trN);

      var trR2 = el('tr', { class: 'meta-row' });
      trR2.appendChild(el('td', { text: 'R\u00b2' }));
      for (var d3 = 0; d3 < dvKeys.length; d3++) {
        trR2.appendChild(el('td', { class: 'num', text: fmtNum(byDV[dvKeys[d3]][0].rsquared, 4) }));
      }
      tbody.appendChild(trR2);

      table.appendChild(tbody);
      wrap.appendChild(table);
      container.appendChild(wrap);

      container.appendChild(el('p', { class: 'card-meta',
        text: 'Significance: *** p<0.001, ** p<0.01, * p<0.05. Standard errors in parentheses. Units: ' + (meta ? meta.proxy_units : '') + '.'
      }));
    }

    if (robustness) {
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Robustness Checks' }));
      container.appendChild(buildTable([
        { key: 'dependent_variable', label: 'DV', format: function (v) { return (dvLabels || {})[v] || fmtSnake(v); } },
        { key: 'term', label: 'Variable', format: function (v) { return (termLabels || {})[v] || fmtSnake(v); } },
        { key: 'coef', label: 'Coef', numeric: true, format: function (v) { return fmtSci(v); } },
        { key: 'p_value', label: 'p-value', numeric: true, format: fmtPval },
        { key: 'series_role', label: 'Role' },
        { key: 'bill_proxy_source_quality', label: 'Bill Source', format: fmtLabel }
      ], robustness.rows.slice(0, 20)));
    }

    if (!regSummary) {
      sectionError(container, 'Plumbing regression data unavailable.');
    }
  }

  async function renderDuration() {
    var container = $('#duration-content');
    var results = await Promise.all([
      fetchJSON('duration_supply_summary.json'),
      fetchJSON('duration_supply_comparison.json')
    ]);
    var summary = results[0], comparison = results[1];

    if (summary) {
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Weekly Public Duration Supply' }));

      var meta = summary.rows[0];
      if (meta && meta.notes) {
        container.appendChild(el('p', { class: 'section-desc', text: meta.notes }));
      }

      var sorted = summary.rows.slice().sort(function (a, b) {
        return a.date < b.date ? 1 : a.date > b.date ? -1 : 0;
      });
      var recent = sorted.slice(0, 12).reverse();

      container.appendChild(buildTable([
        { key: 'date', label: 'Week' },
        { key: 'headline_public_duration_supply', label: 'Headline ($B)', numeric: true, format: fmtUsdBn, colorSign: true },
        { key: 'provisional_public_duration_supply', label: 'Provisional ($B)', numeric: true, format: fmtUsdBn },
        { key: 'buybacks_accepted', label: 'Buybacks ($B)', numeric: true, format: fmtUsdBn },
        { key: 'headline_source_quality', label: 'Source', format: fmtLabel }
      ], recent));

      container.appendChild(el('p', { class: 'card-meta',
        text: 'Showing most recent ' + recent.length + ' of ' + summary.rows.length + ' weeks. Sign: ' + (meta ? meta.sign_convention : '')
      }));
    }

    if (comparison) {
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Duration Construction Comparison' }));
      // Pivot: one row per construction, showing its label and characteristics
      var seen = {};
      var constructions = [];
      for (var ci = 0; ci < comparison.rows.length; ci++) {
        var cr = comparison.rows[ci];
        if (!seen[cr.construction_id]) {
          seen[cr.construction_id] = true;
          constructions.push({
            label: cr.treasury_proxy_label || fmtSnake(cr.construction_id),
            family: cr.construction_family,
            frn: cr.includes_frn ? 'Yes' : 'No',
            tips: cr.includes_tips ? 'Yes' : 'No',
            qt: cr.includes_qt_proxy ? 'Yes' : 'No',
            buybacks: cr.subtracts_buybacks ? 'Yes' : 'No',
            quality: fmtLabel(cr.source_quality)
          });
        }
      }
      container.appendChild(buildTable([
        { key: 'label', label: 'Construction' },
        { key: 'family', label: 'Role' },
        { key: 'frn', label: 'FRN' },
        { key: 'tips', label: 'TIPS' },
        { key: 'qt', label: 'QT' },
        { key: 'buybacks', label: 'Buybacks' },
        { key: 'quality', label: 'Source' }
      ], constructions));
    }

    if (!summary) {
      sectionError(container, 'Duration supply data unavailable.');
    }
  }

  async function renderExtensions() {
    var container = $('#extensions-content');
    var results = await Promise.all([
      fetchJSON('extension_status.json'),
      fetchJSON('investor_allotments_summary.json'),
      fetchJSON('primary_dealer_summary.json'),
      fetchJSON('sec_nmfp_summary.json')
    ]);
    var extStatus = results[0], investor = results[1], dealer = results[2], nmfp = results[3];

    // Status cards
    if (extStatus) {
      container.appendChild(el('p', { class: 'section-desc',
        text: 'These modules are included as supporting status/context surfaces. They are not the main empirical claim of the current public release.'
      }));
      var grid = el('div', { class: 'card-grid' });
      for (var i = 0; i < extStatus.rows.length; i++) {
        var row = extStatus.rows[i];
        var name = row.extension.replace(/_/g, ' ');
        var publicRole = publicRoleForDataset('extension_' + row.extension, row.public_role);
        grid.appendChild(el('div', { class: 'card' }, [
          el('div', { class: 'card-title', text: name }),
          el('div', { class: 'card-value' }, [statusBadge(row.readiness_tier)]),
          el('div', { class: 'card-meta' }, [el('span', { class: 'card-inline-label', text: 'Public role: ' }), rolePill(publicRole)]),
          el('div', { class: 'card-meta', text: row.panel_exists ? row.panel_rows.toLocaleString() + ' panel rows' : 'No panel data' }),
          el('div', { class: 'card-meta', text: 'Backend: ' + fmtSnake(row.backend_status) })
        ]));
      }
      container.appendChild(grid);
    }

    // Investor Allotments
    if (investor) {
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Investor Allotments' }));
      var coverage = investor.rows.filter(function (r) { return r.summary_type === 'coverage'; });
      if (coverage.length) {
        container.appendChild(buildTable([
          { key: 'security_family', label: 'Security', format: fmtSnake },
          { key: 'measure', label: 'Measure', format: fmtSnake },
          { key: 'value', label: 'Value', numeric: true, format: function (v) { return v != null ? v.toLocaleString() : dash(); } },
          { key: 'units', label: 'Units' },
          { key: 'as_of_date', label: 'As Of' }
        ], coverage));
      }
    }

    // Primary Dealer
    if (dealer) {
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Primary Dealer' }));
      var available = dealer.rows.filter(function (r) { return r.summary_type === 'available_series'; });
      if (available.length) {
        container.appendChild(buildTable([
          { key: 'dataset_type', label: 'Dataset', format: fmtSnake },
          { key: 'value', label: 'Series Count', numeric: true, format: function (v) { return v != null ? v.toLocaleString() : dash(); } },
          { key: 'frequency', label: 'Frequency' },
          { key: 'source_quality', label: 'Source', format: fmtLabel },
          { key: 'as_of_date', label: 'As Of' }
        ], available));
      }
    }

    // SEC N-MFP
    if (nmfp) {
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'SEC N-MFP' }));
      // Aggregate archive status: count distinct periods, count those fully available
      var archiveRows = nmfp.rows.filter(function (r) { return r.summary_type === 'archive_status'; });
      var periodMap = {};
      for (var ai = 0; ai < archiveRows.length; ai++) {
        var ar = archiveRows[ai];
        if (!periodMap[ar.period_label]) periodMap[ar.period_label] = { total: 0, ok: 0 };
        periodMap[ar.period_label].total++;
        if (ar.value === 1) periodMap[ar.period_label].ok++;
      }
      var periods = Object.keys(periodMap);
      var fullyOk = periods.filter(function (p) { return periodMap[p].ok === periodMap[p].total; }).length;

      container.appendChild(el('p', { class: 'section-desc',
        text: nmfp.rows.length + ' summary rows. ' + periods.length + ' quarterly archives tracked, ' + fullyOk + ' fully available.'
      }));

      // Show a compact period summary
      var periodRows = periods.sort().slice(-10).map(function (p) {
        return {
          period: p,
          checks: periodMap[p].total,
          passed: periodMap[p].ok,
          status: periodMap[p].ok === periodMap[p].total ? 'complete' : 'partial'
        };
      });
      container.appendChild(buildTable([
        { key: 'period', label: 'Period' },
        { key: 'checks', label: 'Checks', numeric: true },
        { key: 'passed', label: 'Passed', numeric: true },
        { key: 'status', label: 'Status', format: function (v) { return v === 'complete' ? '\u2713 Complete' : '\u26a0 Partial'; } }
      ], periodRows));
      container.appendChild(el('p', { class: 'card-meta', text: 'Showing most recent 10 of ' + periods.length + ' quarterly archives.' }));
    }

    // TIC placeholder
    container.appendChild(el('h3', { class: 'section-subtitle', text: 'Treasury International Capital (TIC)' }));
    container.appendChild(el('div', { class: 'card' }, [
      el('div', { class: 'card-value' }, [statusBadge('not_started')]),
      el('div', { class: 'card-meta', text: 'TIC data integration is planned for a later phase and is out of scope for the current public release.' })
    ]));
  }

  async function renderProvenance() {
    var container = $('#provenance-content');
    var results = await Promise.all([
      fetchJSON('dataset_status.json'),
      fetchJSON('extension_status.json'),
      fetchJSON('series_metadata_catalog.json'),
      fetchJSON('data_sources_summary.json'),
      fetchJSON('causal_claims_status.json'),
      fetchJSON('qra_causal_claims_status.json'),
      fetchJSON('qra_benchmark_evidence_registry.json'),
      fetchJSON('official_capture_readiness.json'),
      fetchJSON('official_capture_completion.json'),
      fetchJSON('official_capture_backfill_queue.json')
    ]);
    var dsStatus = results[0], extStatus = results[1], catalog = results[2], sources = results[3], causalClaims = results[4] || results[5], benchmarkEvidence = results[6], captureReadiness = results[7], captureCompletion = results[8], captureBackfillQueue = results[9];

    if (dsStatus) {
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Dataset Readiness' }));
      container.appendChild(buildTable([
        { key: 'dataset', label: 'Dataset', format: fmtSnake },
        { key: 'readiness_tier', label: 'Readiness', format: function (v) { return statusBadge(v); } },
        { key: 'review_maturity', label: 'Review Maturity', format: function (v) { return v ? statusBadge(v) : dash(); } },
        { key: 'public_role', label: 'Public Role', format: function (v, row) { return rolePill(publicRoleForDataset(row.dataset, v)); } },
        { key: 'source_quality', label: 'Source Quality', format: fmtLabel },
        { key: 'headline_ready', label: 'Headline', format: function (v, row) { return publicRoleForDataset(row.dataset, row.public_role) === 'supporting' ? 'No' : checkMark(v); } },
        { key: 'fallback_only', label: 'Fallback Only', format: function (v) { return v ? 'Yes' : 'No'; } },
        { key: 'last_regenerated_utc', label: 'Last Regenerated', format: fmtTimestamp }
      ], dsStatus.rows));
    }

    if (causalClaims && causalClaims.rows && causalClaims.rows.length) {
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Causal Claims Status' }));
      container.appendChild(el('p', { class: 'section-desc',
        text: 'Claim scope is the public boundary: descriptive_only rows are context, causal_pilot_only rows stay inside the pilot, and headline rows are the only headline claim.'
      }));
      var normalizedClaims = causalClaims.rows.map(normalizeCausalClaimsRow);
      container.appendChild(buildTable([
        { key: 'subject', label: 'Claim' },
        { key: 'claim_scope', label: 'Scope', format: fmtLabel },
        { key: 'readiness_tier', label: 'Readiness', format: function (v) { return v ? statusBadge(v) : dash(); } },
        { key: 'public_role', label: 'Role', format: function (v) { return v ? fmtLabel(v) : dash(); } },
        { key: 'headline_ready', label: 'Headline', format: function (v) { return v == null ? dash() : checkMark(v); } },
        { key: 'causal_pilot_ready', label: 'Pilot', format: function (v) { return v == null ? dash() : checkMark(v); } },
        { key: 'source_quality', label: 'Source', format: fmtLabel },
        { key: 'reason', label: 'Reason' },
        { key: 'updated', label: 'Updated', format: fmtTimestamp }
      ], normalizedClaims.slice(0, 16)));
    }

    if (benchmarkEvidence && benchmarkEvidence.rows && benchmarkEvidence.rows.length) {
      var currentSampleEvidence = benchmarkEvidence.rows
        .map(normalizeBenchmarkEvidenceRow)
        .filter(function (row) {
          return quarterRank(row.quarter) != null && quarterRank(row.quarter) >= quarterRank('2022Q3');
        })
        .sort(function (a, b) {
          var q = quarterCompare(a.quarter, b.quarter);
          if (q !== 0) return q;
          return quarterCompare(a.release_component_id, b.release_component_id);
        });
      if (currentSampleEvidence.length) {
        container.appendChild(el('h3', { class: 'section-subtitle', text: 'Benchmark Evidence' }));
        container.appendChild(el('p', {
          class: 'section-desc',
          text: 'Current-sample financing evidence is split by terminal disposition: Tier A rows are pilot-ready, blocked rows still fail benchmark timing, and context-only rows remain publishable only as context.'
        }));
        container.appendChild(buildTable([
          { key: 'release_component_id', label: 'Component' },
          { key: 'quarter', label: 'Quarter' },
          { key: 'quality_tier', label: 'Tier', format: fmtLabel },
          { key: 'benchmark_timing_status', label: 'Timing', format: fmtLabel },
          { key: 'external_benchmark_ready', label: 'Benchmark Ready', format: checkMark },
          { key: 'expectation_status', label: 'Expectation', format: fmtLabel },
          { key: 'contamination_status', label: 'Contamination', format: fmtLabel },
          { key: 'terminal_disposition', label: 'Outcome', format: benchmarkDispositionLabel },
          { key: 'claim_scope', label: 'Scope', format: fmtLabel }
        ], currentSampleEvidence));
      }
    }

    if (captureReadiness) {
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Official Capture Readiness' }));
      container.appendChild(buildTable([
        { key: 'quarter', label: 'Quarter' },
        { key: 'readiness_tier', label: 'Readiness', format: function (v) { return statusBadge(v); } },
        { key: 'source_quality', label: 'Source Quality', format: fmtLabel },
        { key: 'headline_ready', label: 'Headline', format: checkMark },
        { key: 'fallback_only', label: 'Fallback', format: checkMark },
        { key: 'missing_critical_fields', label: 'Missing Fields', format: function (v) { return v || dash(); } }
      ], captureReadiness.rows.slice(0, 16)));
    }

    if (captureCompletion) {
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Official Capture Completion' }));
      container.appendChild(buildTable([
        { key: 'quarter', label: 'Quarter' },
        { key: 'completion_tier', label: 'Completion', format: fmtLabel },
        { key: 'qa_status', label: 'QA Status', format: fmtLabel },
        { key: 'is_headline_ready', label: 'Headline', format: checkMark },
        { key: 'uses_seed_source', label: 'Uses Seed', format: checkMark }
      ], captureCompletion.rows.slice(0, 16)));
    }

    if (captureBackfillQueue) {
      var pendingRows = captureBackfillQueue.rows.filter(function (row) {
        return !row.numeric_official_capture_ready;
      });
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Official Capture Backfill Queue' }));
      if (!pendingRows.length) {
        container.appendChild(el('p', {
          class: 'section-desc',
          text: 'No remaining official-capture quarter backlog. Every published quarter is currently numeric-official ready.'
        }));
      } else {
        container.appendChild(buildTable([
          { key: 'quarter', label: 'Quarter' },
          { key: 'source_quality', label: 'Source Quality', format: fmtLabel },
          { key: 'readiness_tier', label: 'Readiness', format: function (v) { return statusBadge(v); } },
          { key: 'missing_numeric_fields', label: 'Missing Numeric Fields', format: function (v) { return v || dash(); } },
          { key: 'next_action', label: 'Next Action', format: fmtLabel },
          { key: 'financing_provenance_ready', label: 'Financing', format: checkMark },
          { key: 'refunding_statement_provenance_ready', label: 'Statement', format: checkMark },
          { key: 'auction_reconstruction_ready', label: 'Auction', format: checkMark }
        ], pendingRows));
      }
    }

    if (extStatus) {
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Extension Backend Status' }));
      container.appendChild(buildTable([
        { key: 'extension', label: 'Extension', format: fmtSnake },
        { key: 'readiness_tier', label: 'Readiness', format: function (v) { return statusBadge(v); } },
        { key: 'public_role', label: 'Public Role', format: function (v, row) { return rolePill(publicRoleForDataset('extension_' + row.extension, v)); } },
        { key: 'raw_dir_exists', label: 'Raw', format: checkMark },
        { key: 'manifest_exists', label: 'Manifest', format: checkMark },
        { key: 'processed_exists', label: 'Processed', format: checkMark },
        { key: 'panel_exists', label: 'Panel', format: checkMark },
        { key: 'panel_rows', label: 'Rows', numeric: true, format: function (v) { return v != null ? v.toLocaleString() : dash(); } },
        { key: 'publish_exists', label: 'Published', format: checkMark }
      ], extStatus.rows));
    }

    if (catalog) {
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Series Metadata Catalog' }));
      container.appendChild(el('p', { class: 'section-desc', text: 'Every published series with its units, sign convention, quality tier, and public role.' }));
      container.appendChild(buildTable([
        { key: 'dataset', label: 'Dataset' },
        { key: 'series_id', label: 'Series', format: fmtSnake },
        { key: 'frequency', label: 'Frequency' },
        { key: 'value_units', label: 'Units' },
        { key: 'sign_convention', label: 'Sign Convention' },
        { key: 'source_quality', label: 'Quality', format: fmtLabel },
        { key: 'public_role', label: 'Public Role', format: function (v, row) { return rolePill(publicRoleForDataset(row.dataset, v || row.series_role)); } }
      ], catalog.rows));
    }

    if (sources) {
      container.appendChild(el('h3', { class: 'section-subtitle', text: 'Data Sources' }));
      container.appendChild(buildTable([
        { key: 'source_family', label: 'Source', format: fmtSnake },
        { key: 'raw_dir_exists', label: 'Raw Dir', format: checkMark },
        { key: 'file_count', label: 'Files', numeric: true },
        { key: 'manifest_exists', label: 'Manifest', format: checkMark },
        { key: 'downloads_exists', label: 'Downloads Log', format: checkMark }
      ], sources.rows));
    }
  }

  // ── Navigation ──

  function setupNav() {
    var sections = $$('section[id]');
    var navLinks = $$('nav a');

    for (var i = 0; i < navLinks.length; i++) {
      (function (link) {
        link.addEventListener('click', function (e) {
          e.preventDefault();
          var id = link.getAttribute('href').slice(1);
          var target = document.getElementById(id);
          if (target) {
            var navH = $('nav').offsetHeight;
            window.scrollTo({ top: target.offsetTop - navH - 8, behavior: 'smooth' });
          }
        });
      })(navLinks[i]);
    }

    var observer = new IntersectionObserver(function (entries) {
      for (var j = 0; j < entries.length; j++) {
        if (entries[j].isIntersecting) {
          var id = entries[j].target.id;
          for (var k = 0; k < navLinks.length; k++) {
            navLinks[k].classList.toggle('active', navLinks[k].getAttribute('href') === '#' + id);
          }
        }
      }
    }, { rootMargin: '-20% 0px -70% 0px' });

    for (var s = 0; s < sections.length; s++) {
      observer.observe(sections[s]);
    }
  }

  // ── Init ──

  async function init() {
    await Promise.all([
      renderOverview(),
      renderCapture(),
      renderEvents(),
      renderMethods(),
      renderPlumbing(),
      renderDuration(),
      renderExtensions(),
      renderProvenance()
    ]);
    setupNav();

    var loaders = $$('.loading');
    for (var i = 0; i < loaders.length; i++) {
      loaders[i].remove();
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
