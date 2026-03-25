(async function () {
  'use strict';

  var DATA_PATH = 'data';

  // ── Utilities ──

  async function fetchJSON(filename) {
    try {
      var resp = await fetch(DATA_PATH + '/' + filename);
      if (!resp.ok) return null;
      var text = await resp.text();
      if (/\bNaN\b|\bInfinity\b/.test(text)) {
        console.error('Artifact ' + filename + ' contains NaN or Infinity — backend should emit valid JSON. Attempting parse with repair.');
        text = text
          .replace(/\bNaN\b/g, 'null')
          .replace(/\b-Infinity\b/g, 'null')
          .replace(/\bInfinity\b/g, 'null');
      }
      return JSON.parse(text);
    } catch (e) {
      console.error('Failed to parse ' + filename + ':', e);
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
    supporting_provisional: 'Supporting Provisional',
    provisional_supporting: 'Provisional Supporting',
    not_started: 'Not Started',
    supporting_anchor: 'Supporting Anchor',
    supporting_context: 'Supporting Context',
    credibility_anchor: 'Credibility Anchor',
    context: 'Context',
    baseline_summary: 'Baseline Summary',
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
      supporting_provisional: 'Supporting Provisional',
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
      return span.start === span.end ? span.start : span.start + '\u2013' + span.end;
    }).join(', ');
  }

  function sortRowsByQuarter(rows) {
    return rows.slice().sort(function (a, b) {
      return String(a.quarter).localeCompare(String(b.quarter));
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
      official.push(normalizeAtiRow(rows[i], { readiness_tier: 'headline_ready', public_role: 'headline' }));
    }
    return sortRowsByQuarter(official);
  }

  function normalizePublicRole(value) {
    return value === 'headline' ? 'headline' : 'supporting';
  }

  function publicRoleForDataset(dataset) {
    var headline = { official_ati: true, plumbing: true, duration: true };
    return headline[dataset] ? 'headline' : 'supporting';
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
    return formatQuarterSpans(sorted) + ' (' + sorted.length + ' quarters)';
  }

  function benchmarkDispositionLabel(value) {
    var v = String(value || '').trim();
    var labels = {
      tier_a_causal_pilot_ready: 'Tier A / pilot ready',
      reviewed_surprise_ready_not_tier_a: 'Pre-release, not Tier A',
      reviewed_contaminated_context_only: 'Context only',
      reviewed_contaminated_exclude: 'Excluded',
      blocked_source_family_exhausted: 'Blocked / exhausted',
      blocked_open_candidate: 'Blocked / open',
      post_release_invalid: 'Blocked',
      external_timing_unverified: 'Blocked',
      same_release_placeholder: 'Blocked',
      benchmark_verification_incomplete: 'Blocked',
      pending_contamination_review: 'Pending',
      review_pending: 'Pending'
    };
    return labels[v] || fmtLabel(v);
  }

  // ── Table Builder ──

  function buildTable(columns, rows, opts) {
    opts = opts || {};
    var wrap = el('div', { class: 'table-wrap' });
    var table = el('table', opts.tableClass ? { class: opts.tableClass } : {});
    var thead = el('thead');
    var hrow = el('tr');
    for (var c = 0; c < columns.length; c++) {
      hrow.appendChild(el('th', {
        text: columns[c].label,
        class: columns[c].numeric ? 'num' : ''
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
        var col = columns[ci];
        var raw = row[col.key];
        var formatted = col.format ? col.format(raw, row) : (raw != null ? raw : dash());
        var cls = [];
        if (col.numeric) cls.push('num');
        if (col.colorSign && raw != null) {
          var sc = signClass(raw);
          if (sc) cls.push(sc);
        }
        var td = el('td', { class: cls.join(' ') });
        if (formatted instanceof HTMLElement) td.appendChild(formatted);
        else td.textContent = String(formatted);
        tr.appendChild(td);
      }
      tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    wrap.appendChild(table);
    return wrap;
  }

  function buildDisclosure(label, content) {
    var details = el('details');
    var summary = el('summary', { text: label });
    details.appendChild(summary);
    if (content instanceof HTMLElement) {
      details.appendChild(content);
    }
    return details;
  }

  function sectionError(container, msg) {
    container.appendChild(el('p', { class: 'error-msg', text: msg }));
  }

  // ── Shared Tooltip ──

  var _tooltip = null;
  function getTooltip() {
    if (!_tooltip) {
      _tooltip = el('div', { class: 'chart-tooltip' });
      document.body.appendChild(_tooltip);
    }
    return _tooltip;
  }

  function showTooltip(e, labelText, valueText) {
    var tip = getTooltip();
    tip.innerHTML = '';
    tip.appendChild(el('div', { class: 'chart-tooltip-label', text: labelText }));
    tip.appendChild(document.createTextNode(valueText));
    tip.classList.add('visible');
    positionTooltip(e);
  }

  function positionTooltip(e) {
    var tip = getTooltip();
    var tx = e.clientX + 14;
    var ty = e.clientY - 10;
    if (tx + tip.offsetWidth + 8 > window.innerWidth) tx = e.clientX - tip.offsetWidth - 14;
    if (ty + tip.offsetHeight + 8 > window.innerHeight) ty = e.clientY - tip.offsetHeight - 10;
    tip.style.left = tx + 'px';
    tip.style.top = ty + 'px';
  }

  function hideTooltip() {
    var tip = getTooltip();
    tip.classList.remove('visible');
  }

  // ── SVG helpers (DOM-based) ──

  var SVG_NS = 'http://www.w3.org/2000/svg';
  function svgEl(tag, attrs) {
    var e = document.createElementNS(SVG_NS, tag);
    if (attrs) {
      for (var k in attrs) {
        if (attrs.hasOwnProperty(k)) e.setAttribute(k, attrs[k]);
      }
    }
    return e;
  }

  // ── Interactive Bar Chart ──

  function buildBarChart(data, opts) {
    opts = opts || {};
    var w = opts.width || 1060;
    var h = opts.height || 280;
    var padL = opts.padLeft || 60;
    var padR = opts.padRight || 20;
    var padT = opts.padTop || 20;
    var padB = opts.padBottom || 50;
    var chartW = w - padL - padR;
    var chartH = h - padT - padB;

    var container = el('div', { class: 'chart-container' });

    // Header with optional controls
    var header = el('div', { class: 'chart-header' });
    var headerText = el('div', { class: 'chart-header-text' });
    if (opts.title) headerText.appendChild(el('div', { class: 'chart-title', text: opts.title }));
    if (opts.caption) headerText.appendChild(el('div', { class: 'chart-caption', text: opts.caption }));
    header.appendChild(headerText);

    if (opts.controls) {
      var controls = el('div', { class: 'chart-controls' });
      header.appendChild(controls);
    }
    container.appendChild(header);

    function render(chartData) {
      // Remove old SVG
      var oldSvg = container.querySelector('svg');
      if (oldSvg) oldSvg.remove();

      var vals = chartData.map(function (d) { return d.value; });
      var maxV = Math.max.apply(null, vals.concat([0]));
      var minV = Math.min.apply(null, vals.concat([0]));
      var range = maxV - minV || 1;
      var zeroY = padT + chartH * (maxV / range);
      var barW = Math.max(2, Math.min(14, (chartW / chartData.length) - 2));
      var gap = (chartW - barW * chartData.length) / (chartData.length + 1);

      var svg = svgEl('svg', { viewBox: '0 0 ' + w + ' ' + h, role: 'img', 'aria-label': opts.ariaLabel || 'Bar chart' });

      // Invisible hit area for mouse tracking
      var hitArea = svgEl('rect', { x: padL, y: padT, width: chartW, height: chartH, fill: 'transparent' });
      svg.appendChild(hitArea);

      // Grid lines
      var nTicks = 5;
      for (var t = 0; t <= nTicks; t++) {
        var tickVal = minV + (range * t / nTicks);
        var tickY = padT + chartH - (chartH * (tickVal - minV) / range);
        svg.appendChild(svgEl('line', { x1: padL, y1: tickY.toFixed(1), x2: w - padR, y2: tickY.toFixed(1), stroke: 'var(--color-border)', 'stroke-width': '0.5' }));
        var tickText = document.createElementNS(SVG_NS, 'text');
        tickText.setAttribute('x', padL - 8);
        tickText.setAttribute('y', (tickY + 4).toFixed(1));
        tickText.setAttribute('text-anchor', 'end');
        tickText.setAttribute('fill', 'var(--color-text-secondary)');
        tickText.setAttribute('font-size', '10');
        tickText.setAttribute('font-family', 'var(--font-mono)');
        tickText.textContent = tickVal.toFixed(0);
        svg.appendChild(tickText);
      }

      // Zero line
      svg.appendChild(svgEl('line', { x1: padL, y1: zeroY.toFixed(1), x2: w - padR, y2: zeroY.toFixed(1), stroke: 'var(--color-text-muted)', 'stroke-width': '1', 'stroke-dasharray': '4,3' }));

      // Hover highlight bar (invisible until hover)
      var highlightBar = svgEl('rect', { x: 0, y: padT, width: 0, height: chartH, fill: 'var(--color-accent)', opacity: '0.06', 'pointer-events': 'none' });
      svg.appendChild(highlightBar);

      // Bars
      var bars = [];
      for (var i = 0; i < chartData.length; i++) {
        var d = chartData[i];
        var x = padL + gap + i * (barW + gap);
        var val = d.value;
        var barH = Math.abs(val / range) * chartH;
        var y = val >= 0 ? zeroY - barH : zeroY;
        var color = val >= 0 ? 'var(--color-positive)' : 'var(--color-negative)';
        var bar = svgEl('rect', { x: x.toFixed(1), y: y.toFixed(1), width: barW.toFixed(1), height: Math.max(1, barH).toFixed(1), fill: color, rx: '1', opacity: '0.85' });
        bar._idx = i;
        bar._cx = x + barW / 2;
        bars.push(bar);
        svg.appendChild(bar);
      }

      // X-axis labels
      var labelEvery = Math.max(1, Math.ceil(chartData.length / 16));
      for (var li = 0; li < chartData.length; li++) {
        if (li % labelEvery === 0) {
          var lx = padL + gap + li * (barW + gap) + barW / 2;
          var label = document.createElementNS(SVG_NS, 'text');
          label.setAttribute('x', lx.toFixed(1));
          label.setAttribute('y', (h - padB + 14).toFixed(1));
          label.setAttribute('text-anchor', 'middle');
          label.setAttribute('fill', 'var(--color-text-secondary)');
          label.setAttribute('font-size', '9');
          label.setAttribute('font-family', 'var(--font-sans)');
          label.setAttribute('transform', 'rotate(-45,' + lx.toFixed(1) + ',' + (h - padB + 14).toFixed(1) + ')');
          label.textContent = chartData[li].label;
          svg.appendChild(label);
        }
      }

      container.appendChild(svg);

      // Interaction: hover to show tooltip + highlight
      function findNearest(e) {
        var rect = svg.getBoundingClientRect();
        var mx = (e.clientX - rect.left) / rect.width * w;
        var bestIdx = -1;
        var bestDist = Infinity;
        for (var bi = 0; bi < bars.length; bi++) {
          var dist = Math.abs(mx - bars[bi]._cx);
          if (dist < bestDist) { bestDist = dist; bestIdx = bi; }
        }
        return bestIdx;
      }

      svg.addEventListener('mousemove', function (e) {
        var idx = findNearest(e);
        if (idx < 0) return;
        var d = chartData[idx];
        var bx = parseFloat(bars[idx].getAttribute('x'));
        highlightBar.setAttribute('x', (bx - 2).toFixed(1));
        highlightBar.setAttribute('width', (barW + 4).toFixed(1));
        bars.forEach(function (b, bi) { b.setAttribute('opacity', bi === idx ? '1' : '0.55'); });
        showTooltip(e, d.label, '$' + d.value.toFixed(1) + 'B');
      });

      svg.addEventListener('mouseleave', function () {
        highlightBar.setAttribute('width', '0');
        bars.forEach(function (b) { b.setAttribute('opacity', '0.85'); });
        hideTooltip();
      });
    }

    render(data);

    // Expose re-render for toggle controls
    container._render = render;
    return container;
  }

  // ── Interactive Line Chart ──

  function buildLineChart(data, opts) {
    opts = opts || {};
    var w = opts.width || 1060;
    var h = opts.height || 260;
    var padL = opts.padLeft || 80;
    var padR = opts.padRight || 20;
    var padT = opts.padTop || 20;
    var padB = opts.padBottom || 40;
    var chartW = w - padL - padR;
    var chartH = h - padT - padB;

    var container = el('div', { class: 'chart-container' });
    var header = el('div', { class: 'chart-header' });
    var headerText = el('div', { class: 'chart-header-text' });
    if (opts.title) headerText.appendChild(el('div', { class: 'chart-title', text: opts.title }));
    if (opts.caption) headerText.appendChild(el('div', { class: 'chart-caption', text: opts.caption }));
    header.appendChild(headerText);
    container.appendChild(header);

    var validData = data.filter(function (d) { return d.value != null; });
    if (!validData.length) {
      container.appendChild(el('p', { class: 'card-meta', text: 'No data available for chart.' }));
      return container;
    }

    var vals = validData.map(function (d) { return d.value; });
    var maxV = Math.max.apply(null, vals);
    var minV = Math.min.apply(null, vals);
    var range = maxV - minV || 1;

    var svg = svgEl('svg', { viewBox: '0 0 ' + w + ' ' + h, role: 'img', 'aria-label': opts.ariaLabel || 'Line chart' });

    // Grid
    var nTicks = 5;
    for (var t = 0; t <= nTicks; t++) {
      var tickVal = minV + (range * t / nTicks);
      var tickY = padT + chartH - (chartH * t / nTicks);
      svg.appendChild(svgEl('line', { x1: padL, y1: tickY.toFixed(1), x2: w - padR, y2: tickY.toFixed(1), stroke: 'var(--color-border)', 'stroke-width': '0.5' }));
      var tickText = document.createElementNS(SVG_NS, 'text');
      tickText.setAttribute('x', padL - 8);
      tickText.setAttribute('y', (tickY + 4).toFixed(1));
      tickText.setAttribute('text-anchor', 'end');
      tickText.setAttribute('fill', 'var(--color-text-secondary)');
      tickText.setAttribute('font-size', '10');
      tickText.setAttribute('font-family', 'var(--font-mono)');
      var tickLabel = Math.abs(tickVal) >= 1e9 ? '$' + (tickVal / 1e9).toFixed(0) + 'B' : (Math.abs(tickVal) >= 1e6 ? '$' + (tickVal / 1e6).toFixed(0) + 'M' : '$' + tickVal.toFixed(0));
      tickText.textContent = tickLabel;
      svg.appendChild(tickText);
    }

    // Build point coordinates
    var pointCoords = [];
    for (var i = 0; i < validData.length; i++) {
      var px = padL + (chartW * i / (validData.length - 1));
      var py = padT + chartH - (chartH * (validData[i].value - minV) / range);
      pointCoords.push({ x: px, y: py, data: validData[i] });
    }

    // Area fill under line
    if (pointCoords.length > 1) {
      var areaPath = 'M' + pointCoords[0].x.toFixed(1) + ',' + (padT + chartH);
      for (var ai = 0; ai < pointCoords.length; ai++) {
        areaPath += ' L' + pointCoords[ai].x.toFixed(1) + ',' + pointCoords[ai].y.toFixed(1);
      }
      areaPath += ' L' + pointCoords[pointCoords.length - 1].x.toFixed(1) + ',' + (padT + chartH) + ' Z';
      svg.appendChild(svgEl('path', { d: areaPath, fill: 'var(--color-accent)', opacity: '0.08' }));
    }

    // Line path
    if (pointCoords.length > 1) {
      var pathD = 'M' + pointCoords.map(function (p) { return p.x.toFixed(1) + ',' + p.y.toFixed(1); }).join(' L');
      svg.appendChild(svgEl('path', { d: pathD, fill: 'none', stroke: 'var(--color-accent)', 'stroke-width': '2', 'stroke-linejoin': 'round' }));
    }

    // X-axis labels
    var labelEvery = Math.max(1, Math.ceil(validData.length / 10));
    for (var j = 0; j < validData.length; j += labelEvery) {
      var lx = padL + (chartW * j / (validData.length - 1));
      var lbl = document.createElementNS(SVG_NS, 'text');
      lbl.setAttribute('x', lx.toFixed(1));
      lbl.setAttribute('y', (h - 6).toFixed(1));
      lbl.setAttribute('text-anchor', 'middle');
      lbl.setAttribute('fill', 'var(--color-text-secondary)');
      lbl.setAttribute('font-size', '9');
      lbl.setAttribute('font-family', 'var(--font-sans)');
      lbl.textContent = validData[j].label;
      svg.appendChild(lbl);
    }

    // Crosshair + hover dot (interactive)
    var crossV = svgEl('line', { x1: 0, y1: padT, x2: 0, y2: padT + chartH, class: 'chart-crosshair', 'stroke-width': '1', stroke: 'var(--color-text-muted)', 'stroke-dasharray': '3,3', 'pointer-events': 'none', opacity: '0' });
    var crossH = svgEl('line', { x1: padL, y1: 0, x2: w - padR, y2: 0, class: 'chart-crosshair', 'stroke-width': '1', stroke: 'var(--color-text-muted)', 'stroke-dasharray': '3,3', 'pointer-events': 'none', opacity: '0' });
    var hoverDot = svgEl('circle', { cx: 0, cy: 0, r: '4.5', fill: 'var(--color-accent)', stroke: 'var(--color-bg)', 'stroke-width': '2', class: 'chart-hover-dot', 'pointer-events': 'none', opacity: '0' });
    svg.appendChild(crossV);
    svg.appendChild(crossH);
    svg.appendChild(hoverDot);

    // Hit area
    var hitArea = svgEl('rect', { x: padL, y: padT, width: chartW, height: chartH, fill: 'transparent' });
    svg.appendChild(hitArea);

    svg.addEventListener('mousemove', function (e) {
      var rect = svg.getBoundingClientRect();
      var mx = (e.clientX - rect.left) / rect.width * w;
      // Find nearest point
      var bestIdx = 0;
      var bestDist = Infinity;
      for (var bi = 0; bi < pointCoords.length; bi++) {
        var dist = Math.abs(mx - pointCoords[bi].x);
        if (dist < bestDist) { bestDist = dist; bestIdx = bi; }
      }
      var pt = pointCoords[bestIdx];
      crossV.setAttribute('x1', pt.x.toFixed(1));
      crossV.setAttribute('x2', pt.x.toFixed(1));
      crossV.setAttribute('opacity', '1');
      crossH.setAttribute('y1', pt.y.toFixed(1));
      crossH.setAttribute('y2', pt.y.toFixed(1));
      crossH.setAttribute('opacity', '1');
      hoverDot.setAttribute('cx', pt.x.toFixed(1));
      hoverDot.setAttribute('cy', pt.y.toFixed(1));
      hoverDot.setAttribute('opacity', '1');
      showTooltip(e, pt.data.label, fmtUsdBn(pt.data.value));
    });

    svg.addEventListener('mouseleave', function () {
      crossV.setAttribute('opacity', '0');
      crossH.setAttribute('opacity', '0');
      hoverDot.setAttribute('opacity', '0');
      hideTooltip();
    });

    container.appendChild(svg);
    return container;
  }

  // ── Dual-Series Overlay Chart (standardized) ──

  function buildOverlayChart(data, opts) {
    // data: [{date, left, right}]  opts: {leftLabel, rightLabel, title, caption, leftColor, rightColor}
    opts = opts || {};
    var w = opts.width || 1060;
    var h = opts.height || 300;
    var padL = 55, padR = 55, padT = 24, padB = 44;
    var chartW = w - padL - padR;
    var chartH = h - padT - padB;

    // Filter to rows where at least one series has data
    var valid = data.filter(function (d) { return d.left != null || d.right != null; });
    if (!valid.length) return el('p', { class: 'card-meta', text: 'No overlay data available.' });

    // Standardize both series (z-score)
    function zScore(arr) {
      var vals = arr.filter(function (v) { return v != null; });
      if (!vals.length) return arr.map(function () { return null; });
      var mean = vals.reduce(function (a, b) { return a + b; }, 0) / vals.length;
      var sd = Math.sqrt(vals.reduce(function (a, b) { return a + (b - mean) * (b - mean); }, 0) / vals.length) || 1;
      return arr.map(function (v) { return v != null ? (v - mean) / sd : null; });
    }

    var leftZ = zScore(valid.map(function (d) { return d.left; }));
    var rightZ = zScore(valid.map(function (d) { return d.right; }));

    var allZ = leftZ.concat(rightZ).filter(function (v) { return v != null; });
    var minV = Math.min.apply(null, allZ);
    var maxV = Math.max.apply(null, allZ);
    var pad = (maxV - minV) * 0.08 || 0.5;
    minV -= pad; maxV += pad;
    var range = maxV - minV;

    function xPos(i) { return padL + chartW * i / (valid.length - 1); }
    function yPos(v) { return padT + chartH - chartH * (v - minV) / range; }

    var leftColor = opts.leftColor || 'var(--color-primary)';
    var rightColor = opts.rightColor || '#dc2626';

    var container = el('div', { class: 'chart-container' });
    var header = el('div', { class: 'chart-header' });
    var headerText = el('div', { class: 'chart-header-text' });
    if (opts.title) headerText.appendChild(el('div', { class: 'chart-title', text: opts.title }));
    if (opts.caption) headerText.appendChild(el('div', { class: 'chart-caption', text: opts.caption }));
    header.appendChild(headerText);

    // Legend
    var legend = el('div', { class: 'chart-controls' });
    legend.appendChild(el('span', { class: 'chart-btn active', style: 'border-left: 3px solid ' + leftColor + '; cursor: default;', text: opts.leftLabel || 'Left' }));
    legend.appendChild(el('span', { class: 'chart-btn active', style: 'border-left: 3px solid ' + rightColor + '; cursor: default;', text: opts.rightLabel || 'Right' }));
    header.appendChild(legend);
    container.appendChild(header);

    var svg = svgEl('svg', { viewBox: '0 0 ' + w + ' ' + h, role: 'img', 'aria-label': opts.ariaLabel || 'Overlay chart' });

    // Y grid + labels
    var nTicks = 5;
    for (var t = 0; t <= nTicks; t++) {
      var tv = minV + range * t / nTicks;
      var ty = yPos(tv);
      svg.appendChild(svgEl('line', { x1: padL, y1: ty.toFixed(1), x2: w - padR, y2: ty.toFixed(1), stroke: 'var(--color-border)', 'stroke-width': '0.5' }));
      var yt = document.createElementNS(SVG_NS, 'text');
      yt.setAttribute('x', String(padL - 8)); yt.setAttribute('y', (ty + 4).toFixed(1));
      yt.setAttribute('text-anchor', 'end'); yt.setAttribute('fill', 'var(--color-text-secondary)');
      yt.setAttribute('font-size', '10'); yt.setAttribute('font-family', 'var(--font-mono)');
      yt.textContent = tv.toFixed(1) + '\u03C3';
      svg.appendChild(yt);
    }

    // Zero line
    svg.appendChild(svgEl('line', { x1: padL, y1: yPos(0).toFixed(1), x2: w - padR, y2: yPos(0).toFixed(1), stroke: 'var(--color-text-muted)', 'stroke-width': '1', 'stroke-dasharray': '4,3' }));

    // X axis dates
    var labelEvery = Math.max(1, Math.ceil(valid.length / 8));
    for (var li = 0; li < valid.length; li += labelEvery) {
      var lbl = document.createElementNS(SVG_NS, 'text');
      lbl.setAttribute('x', xPos(li).toFixed(1)); lbl.setAttribute('y', String(h - 8));
      lbl.setAttribute('text-anchor', 'middle'); lbl.setAttribute('fill', 'var(--color-text-secondary)');
      lbl.setAttribute('font-size', '9'); lbl.setAttribute('font-family', 'var(--font-sans)');
      lbl.textContent = valid[li].date.slice(0, 7);
      svg.appendChild(lbl);
    }

    // Draw left series
    var leftPath = '';
    var leftPoints = [];
    for (var i = 0; i < valid.length; i++) {
      if (leftZ[i] == null) continue;
      var px = xPos(i), py = yPos(leftZ[i]);
      leftPath += (leftPath ? ' L' : 'M') + px.toFixed(1) + ',' + py.toFixed(1);
      leftPoints.push({ idx: i, x: px, y: py, raw: valid[i].left, z: leftZ[i] });
    }
    if (leftPath) svg.appendChild(svgEl('path', { d: leftPath, fill: 'none', stroke: leftColor, 'stroke-width': '1.8' }));

    // Draw right series
    var rightPath = '';
    var rightPoints = [];
    for (var j = 0; j < valid.length; j++) {
      if (rightZ[j] == null) continue;
      var qx = xPos(j), qy = yPos(rightZ[j]);
      rightPath += (rightPath ? ' L' : 'M') + qx.toFixed(1) + ',' + qy.toFixed(1);
      rightPoints.push({ idx: j, x: qx, y: qy, raw: valid[j].right, z: rightZ[j] });
    }
    if (rightPath) svg.appendChild(svgEl('path', { d: rightPath, fill: 'none', stroke: rightColor, 'stroke-width': '1.8' }));

    // Crosshair + dots
    var crossV = svgEl('line', { x1: 0, y1: padT, x2: 0, y2: padT + chartH, stroke: 'var(--color-text-muted)', 'stroke-width': '1', 'stroke-dasharray': '3,3', 'pointer-events': 'none', opacity: '0' });
    var dotL = svgEl('circle', { cx: 0, cy: 0, r: '4.5', fill: leftColor, stroke: 'var(--color-bg)', 'stroke-width': '2', 'pointer-events': 'none', opacity: '0' });
    var dotR = svgEl('circle', { cx: 0, cy: 0, r: '4.5', fill: rightColor, stroke: 'var(--color-bg)', 'stroke-width': '2', 'pointer-events': 'none', opacity: '0' });
    svg.appendChild(crossV);
    svg.appendChild(dotL);
    svg.appendChild(dotR);

    // Hit area
    var hitArea = svgEl('rect', { x: padL, y: padT, width: chartW, height: chartH, fill: 'transparent' });
    svg.appendChild(hitArea);

    hitArea.addEventListener('mousemove', function (e) {
      var rect = svg.getBoundingClientRect();
      var mx = (e.clientX - rect.left) / rect.width * w;
      var bestI = 0, bestDist = Infinity;
      for (var bi = 0; bi < valid.length; bi++) {
        var d = Math.abs(mx - xPos(bi));
        if (d < bestDist) { bestDist = d; bestI = bi; }
      }
      var cx = xPos(bestI);
      crossV.setAttribute('x1', cx.toFixed(1)); crossV.setAttribute('x2', cx.toFixed(1));
      crossV.setAttribute('opacity', '1');

      // Find nearest points in each series at this index
      var lp = leftPoints.find(function (p) { return p.idx === bestI; });
      var rp = rightPoints.find(function (p) { return p.idx === bestI; });
      if (lp) { dotL.setAttribute('cx', lp.x.toFixed(1)); dotL.setAttribute('cy', lp.y.toFixed(1)); dotL.setAttribute('opacity', '1'); }
      else { dotL.setAttribute('opacity', '0'); }
      if (rp) { dotR.setAttribute('cx', rp.x.toFixed(1)); dotR.setAttribute('cy', rp.y.toFixed(1)); dotR.setAttribute('opacity', '1'); }
      else { dotR.setAttribute('opacity', '0'); }

      var tipParts = [valid[bestI].date.slice(0, 7)];
      if (lp) tipParts.push((opts.leftLabel || 'Left') + ': ' + fmtNum(lp.raw, 1) + ' (' + fmtNum(lp.z, 2) + '\u03C3)');
      if (rp) tipParts.push((opts.rightLabel || 'Right') + ': ' + fmtNum(rp.raw, 2) + ' (' + fmtNum(rp.z, 2) + '\u03C3)');
      showTooltip(e, valid[bestI].date.slice(0, 7), tipParts.slice(1).join(' | '));
    });

    hitArea.addEventListener('mouseleave', function () {
      crossV.setAttribute('opacity', '0');
      dotL.setAttribute('opacity', '0');
      dotR.setAttribute('opacity', '0');
      hideTooltip();
    });

    container.appendChild(svg);
    return container;
  }

  // ── Pricing Label ──

  function pricingLabel(type, text) {
    return el('span', { class: 'pricing-label pricing-label-' + type, text: text });
  }

  // ══════════════════════════════════════
  // SECTION RENDERERS
  // ══════════════════════════════════════

  // ── Hero Strip ──

  async function renderHeroStrip() {
    var results = await Promise.all([
      fetchJSON('index.json'),
      fetchJSON('ati_quarter_table.json'),
      fetchJSON('official_qra_capture.json'),
      fetchJSON('dataset_status.json')
    ]);
    var index = results[0], ati = results[1], capture = results[2], dsStatus = results[3];

    // Coverage chip
    var coverageChip = $('#hero-chip-coverage');
    var officialRows = [];
    if (ati && ati.rows && !hasSeedAtiColumns(ati.rows)) {
      officialRows = buildOfficialAtiFromQuarterTable(ati.rows);
    } else if (capture && capture.rows) {
      officialRows = buildOfficialAtiFromCapture(capture.rows);
    }
    if (officialRows.length) {
      coverageChip.textContent = 'Coverage: ' + officialRows[0].quarter + '\u2013' + officialRows[officialRows.length - 1].quarter + ' (' + officialRows.length + ' quarters)';
    } else {
      coverageChip.textContent = 'Coverage: loading\u2026';
    }

    // Artifact count chip
    var artifactsChip = $('#hero-chip-artifacts');
    if (index) {
      artifactsChip.textContent = index.artifact_count + ' published artifacts';
    } else {
      artifactsChip.textContent = 'Artifacts: unavailable';
    }

    // Updated chip
    var updatedChip = $('#hero-chip-updated');
    if (index && index.generated_at_utc) {
      updatedChip.textContent = 'Published: ' + relTime(index.generated_at_utc);
    } else if (dsStatus && dsStatus.rows && dsStatus.rows.length) {
      var latest = dsStatus.rows
        .map(function (r) { return r.last_regenerated_utc; })
        .filter(Boolean)
        .sort()
        .pop();
      updatedChip.textContent = 'Updated: ' + relTime(latest);
    }

    // Add headline status chips (use public_role, not pipeline readiness flag)
    if (dsStatus && dsStatus.rows) {
      var strip = $('#hero-strip');
      var headlineCount = dsStatus.rows.filter(function (r) { return r.public_role === 'headline'; }).length;
      strip.appendChild(el('span', { class: 'hero-chip', text: headlineCount + ' headline datasets' }));
    }
  }

  // ── Evidence Boundary ──

  async function renderEvidence() {
    var container = $('#evidence-content');
    var results = await Promise.all([
      fetchJSON('causal_claims_status.json'),
      fetchJSON('event_design_status.json'),
      fetchJSON('dataset_status.json')
    ]);
    var causalClaims = results[0], eventDesign = results[1], dsStatus = results[2];

    // Evidence Ladder: 3 lanes
    var ladder = el('div', { class: 'evidence-ladder' });

    // Lane 1: Headline Measurement
    var lane1 = el('div', { class: 'evidence-lane evidence-lane-headline' });
    lane1.appendChild(el('div', { class: 'evidence-lane-label', text: 'Headline' }));
    lane1.appendChild(el('h4', { text: 'Measurement & Mechanism' }));
    var lane1Desc = 'Official maturity composition, plumbing regressions, and public duration supply. ';
    if (dsStatus && dsStatus.rows) {
      var headlineDatasets = dsStatus.rows.filter(function (r) { return r.public_role === 'headline'; });
      lane1Desc += headlineDatasets.length + ' datasets at headline public role.';
    }
    lane1.appendChild(el('p', { text: lane1Desc }));
    ladder.appendChild(lane1);

    // Lane 2: Supporting Reduced-Form Pricing
    var lane2 = el('div', { class: 'evidence-lane evidence-lane-supporting' });
    lane2.appendChild(el('div', { class: 'evidence-lane-label', text: 'Supporting' }));
    lane2.appendChild(el('h4', { text: 'Reduced-Form Pricing' }));
    lane2.appendChild(el('p', { text: 'Release-level +63bd design is the credibility anchor. Monthly carry-forward flow spec provides supporting context. Both remain supporting/provisional.' }));
    ladder.appendChild(lane2);

    // Lane 3: Bounded Causal Pilot
    var lane3 = el('div', { class: 'evidence-lane evidence-lane-causal' });
    lane3.appendChild(el('div', { class: 'evidence-lane-label', text: 'Bounded' }));
    lane3.appendChild(el('h4', { text: 'Causal Pilot' }));
    if (causalClaims && causalClaims.rows && causalClaims.rows.length) {
      var cc = causalClaims.rows[0];
      lane3.appendChild(el('p', { text: cc.current_sample_financing_component_count + ' financing components, ' + cc.tier_a_count + ' Tier A, ' + cc.benchmark_ready_count + ' benchmark-ready. Narrower and supporting, not headline.' }));
    } else {
      lane3.appendChild(el('p', { text: 'A narrow post-2022Q3 financing pilot. Not a settled full-sample causal estimate.' }));
    }
    ladder.appendChild(lane3);
    container.appendChild(ladder);

    // Can / Cannot Claim panels
    if (causalClaims && causalClaims.rows && causalClaims.rows.length) {
      var claim = causalClaims.rows[0];
      var panels = el('div', { class: 'claim-panels' });

      var canPanel = el('div', { class: 'claim-panel claim-panel-can' });
      canPanel.appendChild(el('div', { class: 'claim-panel-label', text: 'What the current evidence supports' }));
      canPanel.appendChild(el('p', { text: claim.can_claim || 'Claim boundary loading\u2026' }));
      panels.appendChild(canPanel);

      var cannotPanel = el('div', { class: 'claim-panel claim-panel-cannot' });
      cannotPanel.appendChild(el('div', { class: 'claim-panel-label', text: 'What the current evidence does not establish' }));
      cannotPanel.appendChild(el('p', { text: claim.cannot_claim || 'Claim boundary loading\u2026' }));
      panels.appendChild(cannotPanel);

      container.appendChild(panels);

      if (claim.boundary_reason) {
        container.appendChild(el('p', { class: 'card-meta', text: 'Boundary detail: ' + claim.boundary_reason }));
      }
      if (claim.last_regenerated_utc) {
        container.appendChild(el('p', { class: 'card-meta', text: 'Last updated: ' + relTime(claim.last_regenerated_utc) }));
      }
    }

    // Event design status counts
    if (eventDesign && eventDesign.rows) {
      var countMap = {};
      for (var i = 0; i < eventDesign.rows.length; i++) {
        countMap[eventDesign.rows[i].metric] = eventDesign.rows[i].value;
      }

      var chips = el('div', { class: 'summary-row' });
      var chipDefs = [
        ['release_component_count', 'Release Components'],
        ['tier_a_count', 'Tier A'],
        ['current_sample_financing_component_count', 'Current-Sample Financing'],
        ['current_sample_financing_pre_release_external_count', 'Pre-Release Benchmarks'],
        ['current_sample_financing_source_family_exhausted_count', 'Source-Exhausted'],
        ['current_sample_financing_open_candidate_count', 'Open Candidates']
      ];
      for (var j = 0; j < chipDefs.length; j++) {
        var val = countMap[chipDefs[j][0]];
        if (val != null) {
          chips.appendChild(el('span', { class: 'summary-chip' }, [
            el('strong', { text: String(val) }),
            document.createTextNode(' ' + chipDefs[j][1])
          ]));
        }
      }
      container.appendChild(chips);
    }
  }

  // ── Key Findings ──

  async function renderFindings() {
    var container = $('#findings-content');
    var results = await Promise.all([
      fetchJSON('dataset_status.json'),
      fetchJSON('ati_quarter_table.json'),
      fetchJSON('official_qra_capture.json'),
      fetchJSON('causal_claims_status.json'),
      fetchJSON('pricing_spec_registry.json')
    ]);
    var dsStatus = results[0], ati = results[1], capture = results[2], causal = results[3], specReg = results[4];

    var grid = el('div', { class: 'findings-grid' });

    // Coverage window
    var officialRows = [];
    if (ati && ati.rows && !hasSeedAtiColumns(ati.rows)) {
      officialRows = buildOfficialAtiFromQuarterTable(ati.rows);
    } else if (capture && capture.rows) {
      officialRows = buildOfficialAtiFromCapture(capture.rows);
    }
    if (officialRows.length) {
      grid.appendChild(el('div', { class: 'finding-card' }, [
        el('div', { class: 'finding-card-title', text: 'Official Coverage' }),
        el('div', { class: 'finding-card-value', text: officialRows.length.toString() }),
        el('div', { class: 'finding-card-meta', text: officialRows[0].quarter + '\u2013' + officialRows[officialRows.length - 1].quarter })
      ]));
    }

    // Maturity composition status
    if (dsStatus) {
      var atiRow = dsStatus.rows.find(function (r) { return r.dataset === 'official_ati'; });
      if (atiRow) {
        grid.appendChild(el('div', { class: 'finding-card' }, [
          el('div', { class: 'finding-card-title', text: 'Maturity Composition' }),
          el('div', { class: 'finding-card-value' }, [statusBadge(atiRow.readiness_tier)]),
          el('div', { class: 'finding-card-meta', text: 'Updated ' + relTime(atiRow.last_regenerated_utc) })
        ]));
      }

      var plumbRow = dsStatus.rows.find(function (r) { return r.dataset === 'plumbing'; });
      if (plumbRow) {
        grid.appendChild(el('div', { class: 'finding-card' }, [
          el('div', { class: 'finding-card-title', text: 'Plumbing Mechanism' }),
          el('div', { class: 'finding-card-value' }, [statusBadge(plumbRow.readiness_tier)]),
          el('div', { class: 'finding-card-meta', text: 'Updated ' + relTime(plumbRow.last_regenerated_utc) })
        ]));
      }

      var durRow = dsStatus.rows.find(function (r) { return r.dataset === 'duration'; });
      if (durRow) {
        grid.appendChild(el('div', { class: 'finding-card' }, [
          el('div', { class: 'finding-card-title', text: 'Duration Supply' }),
          el('div', { class: 'finding-card-value' }, [statusBadge(durRow.readiness_tier)]),
          el('div', { class: 'finding-card-meta', text: 'Updated ' + relTime(durRow.last_regenerated_utc) })
        ]));
      }

      var pricingRow = dsStatus.rows.find(function (r) { return r.dataset === 'pricing'; });
      if (pricingRow) {
        grid.appendChild(el('div', { class: 'finding-card' }, [
          el('div', { class: 'finding-card-title', text: 'Pricing Layer' }),
          el('div', { class: 'finding-card-value' }, [statusBadge(pricingRow.readiness_tier)]),
          el('div', { class: 'finding-card-meta', text: 'Reduced-form, supporting' })
        ]));
      }
    }

    // Causal pilot status
    if (causal && causal.rows && causal.rows.length) {
      grid.appendChild(el('div', { class: 'finding-card' }, [
        el('div', { class: 'finding-card-title', text: 'Causal Pilot' }),
        el('div', { class: 'finding-card-value', text: causal.rows[0].tier_a_count + ' Tier A' }),
        el('div', { class: 'finding-card-meta', text: 'Bounded, supporting lane' })
      ]));
    }

    container.appendChild(grid);
  }

  // ── Treasury Maturity Composition ──

  async function renderComposition() {
    var container = $('#composition-content');
    var results = await Promise.all([
      fetchJSON('ati_quarter_table.json'),
      fetchJSON('official_qra_capture.json'),
      fetchJSON('index.json')
    ]);
    var ati = results[0], capture = results[1], index = results[2];

    var officialRows = [];
    if (ati && ati.rows && !hasSeedAtiColumns(ati.rows)) {
      officialRows = buildOfficialAtiFromQuarterTable(ati.rows);
    } else if (capture && capture.rows) {
      officialRows = buildOfficialAtiFromCapture(capture.rows);
    }

    if (!officialRows.length) {
      sectionError(container, 'Maturity composition data unavailable.');
      return;
    }

    // Summary chips
    var latest = officialRows[officialRows.length - 1];
    var summaryRow = el('div', { class: 'summary-row' });
    summaryRow.appendChild(el('span', { class: 'summary-chip' }, [
      document.createTextNode('Latest: '),
      el('strong', { text: latest.quarter })
    ]));
    summaryRow.appendChild(el('span', { class: 'summary-chip' }, [
      document.createTextNode('Maturity-Tilt Flow: '),
      el('strong', { text: fmtBn(latest.ati_baseline_bn) })
    ]));
    summaryRow.appendChild(el('span', { class: 'summary-chip' }, [
      document.createTextNode('Bill share: '),
      el('strong', { text: fmtPct(latest.bill_share) })
    ]));
    summaryRow.appendChild(el('span', { class: 'summary-chip' }, [
      document.createTextNode('Coverage: '),
      el('strong', { text: officialRows.length + ' quarters' })
    ]));
    container.appendChild(summaryRow);

    // Bar chart with baseline toggle
    var baselines = {
      '15%': function (r) { return r.missing_coupons_15_bn || 0; },
      '18%': function (r) { return r.ati_baseline_bn || 0; },
      '20%': function (r) { return r.missing_coupons_20_bn || 0; }
    };
    var activeBaseline = '18%';

    function makeChartData(key) {
      return officialRows.map(function (r) {
        return { label: r.quarter, value: baselines[key](r) };
      });
    }

    var chartContainer = buildBarChart(makeChartData('18%'), {
      title: 'Maturity-Tilt Flow by Quarter',
      caption: 'Coupon shortfall relative to bill-share baseline ($bn). Positive = more bill-heavy financing.',
      ariaLabel: 'Bar chart showing Maturity-Tilt Flow by quarter',
      controls: true
    });
    container.appendChild(chartContainer);

    // Add baseline toggle buttons
    var controlsDiv = chartContainer.querySelector('.chart-controls');
    if (controlsDiv) {
      ['15%', '18%', '20%'].forEach(function (key) {
        var btn = el('button', { class: 'chart-btn' + (key === activeBaseline ? ' active' : ''), text: key + ' baseline' });
        btn.addEventListener('click', function () {
          activeBaseline = key;
          controlsDiv.querySelectorAll('.chart-btn').forEach(function (b) { b.classList.remove('active'); });
          btn.classList.add('active');
          chartContainer._render(makeChartData(key));
          // Update caption
          var cap = chartContainer.querySelector('.chart-caption');
          if (cap) cap.textContent = 'Coupon shortfall relative to ' + key + ' bill-share baseline ($bn). Positive = more bill-heavy financing.';
        });
        controlsDiv.appendChild(btn);
      });
    }

    // Official quarter table behind disclosure
    var tableContent = buildTable([
      { key: 'quarter', label: 'Quarter' },
      { key: 'financing_need_bn', label: 'Need ($B)', numeric: true, format: fmtBn },
      { key: 'net_bills_bn', label: 'Net Bills ($B)', numeric: true, format: fmtBn },
      { key: 'bill_share', label: 'Bill Share', numeric: true, format: fmtPct },
      { key: 'missing_coupons_15_bn', label: '@15% ($B)', numeric: true, format: fmtBn, colorSign: true },
      { key: 'ati_baseline_bn', label: '@18% ($B)', numeric: true, format: fmtBn, colorSign: true },
      { key: 'missing_coupons_20_bn', label: '@20% ($B)', numeric: true, format: fmtBn, colorSign: true }
    ], officialRows);
    container.appendChild(buildDisclosure('View underlying quarter table (' + officialRows.length + ' rows)', tableContent));

    // Seed/forecast rows if available
    if (index && index.artifacts && index.artifacts.indexOf('ati_seed_forecast_table.json') >= 0) {
      var forecast = await fetchJSON('ati_seed_forecast_table.json');
      if (forecast && forecast.rows && forecast.rows.length) {
        var officialQMap = {};
        for (var i = 0; i < officialRows.length; i++) officialQMap[officialRows[i].quarter] = true;
        var seedRows = [];
        for (var j = 0; j < forecast.rows.length; j++) {
          if (!officialQMap[forecast.rows[j].quarter]) {
            seedRows.push(normalizeAtiRow(forecast.rows[j], { public_role: 'supporting' }));
          }
        }
        if (seedRows.length) {
          var seedTable = buildTable([
            { key: 'quarter', label: 'Quarter' },
            { key: 'ati_baseline_bn', label: '@18% ($B)', numeric: true, format: fmtBn, colorSign: true },
            { key: 'bill_share', label: 'Bill Share', numeric: true, format: fmtPct }
          ], sortRowsByQuarter(seedRows));
          container.appendChild(buildDisclosure('Seed / forecast rows (non-headline context)', seedTable));
        }
      }
    }

    // Raw data link
    container.appendChild(el('p', { class: 'card-meta' }, [
      document.createTextNode('Raw data: '),
      el('a', { href: 'data/ati_quarter_table.json', target: '_blank', text: 'JSON' }),
      document.createTextNode(' / '),
      el('a', { href: 'data/ati_quarter_table.csv', target: '_blank', text: 'CSV' })
    ]));
  }

  // ── Mechanism: Plumbing + Duration ──

  async function renderMechanism() {
    var container = $('#mechanism-content');
    var results = await Promise.all([
      fetchJSON('plumbing_regression_summary.json'),
      fetchJSON('plumbing_robustness.json'),
      fetchJSON('duration_supply_summary.json'),
      fetchJSON('duration_supply_comparison.json')
    ]);
    var plumbReg = results[0], plumbRob = results[1], durSummary = results[2], durComparison = results[3];

    // ── Plumbing ──
    container.appendChild(el('h3', { class: 'section-subtitle', text: 'Plumbing: Bills, Reserves & ON RRP' }));
    container.appendChild(el('p', { class: 'section-desc', text: 'Weekly regressions testing whether bill-heavy financing is associated with larger declines in ON RRP and smaller declines in reserve balances, consistent with a substitution channel where bills absorb money-market liquidity.' }));

    if (plumbReg && plumbReg.rows) {
      var byDV = {};
      for (var i = 0; i < plumbReg.rows.length; i++) {
        var row = plumbReg.rows[i];
        if (!byDV[row.dependent_variable]) byDV[row.dependent_variable] = [];
        byDV[row.dependent_variable].push(row);
      }

      var dvLabels = { delta_wlrral: '\u0394 ON RRP', delta_wresbal: '\u0394 Reserves' };
      var termLabels = {
        const: 'Constant',
        bill_net_exact: 'Bill net issuance',
        nonbill_net_exact: 'Non-bill net issuance',
        delta_wdtgal: '\u0394 TGA',
        qt_proxy: 'QT proxy'
      };

      // Coefficient panels (visual-first)
      var coefPanel = el('div', { class: 'coef-panel' });
      var dvKeys = Object.keys(byDV);

      for (var d = 0; d < dvKeys.length; d++) {
        var dvKey = dvKeys[d];
        var block = el('div', { class: 'coef-panel-block' });
        block.appendChild(el('div', { class: 'coef-panel-title', text: dvLabels[dvKey] || dvKey }));

        var terms = byDV[dvKey];
        var maxCoef = 0;
        for (var ti = 0; ti < terms.length; ti++) {
          if (terms[ti].term === 'const') continue;
          var absCoef = Math.abs(toNumber(terms[ti].t_stat) || 0);
          if (absCoef > maxCoef) maxCoef = absCoef;
        }

        for (var t = 0; t < terms.length; t++) {
          if (terms[t].term === 'const') continue;
          var coefRow = el('div', { class: 'coef-row' });
          coefRow.appendChild(el('span', { class: 'coef-label', text: termLabels[terms[t].term] || fmtSnake(terms[t].term) }));
          coefRow.appendChild(el('span', { class: 'coef-value', text: fmtSci(terms[t].coef) }));
          coefRow.appendChild(el('span', { class: 'coef-stars', text: stars(terms[t].p_value) }));

          var barWrap = el('div', { class: 'coef-bar-wrap' });
          var tStat = toNumber(terms[t].t_stat) || 0;
          var barWidth = maxCoef > 0 ? Math.abs(tStat) / maxCoef * 50 : 0;
          var bar = el('div', { class: 'coef-bar ' + (tStat < 0 ? 'negative' : 'positive') });
          bar.style.width = barWidth.toFixed(1) + '%';
          barWrap.appendChild(bar);
          coefRow.appendChild(barWrap);

          block.appendChild(coefRow);
        }

        var metaRow = byDV[dvKey][0];
        block.appendChild(el('p', { class: 'card-meta', text: 'N=' + (metaRow.nobs || dash()) + ' | R\u00b2=' + fmtNum(metaRow.rsquared, 4) }));
        coefPanel.appendChild(block);
      }
      container.appendChild(coefPanel);
      container.appendChild(el('p', { class: 'card-meta', text: 'Significance: *** p<0.001, ** p<0.01, * p<0.05. Units: ' + ((plumbReg.rows[0] || {}).proxy_units || 'USD notional') + '. Bars show t-statistic magnitude.' }));

      // Full regression table behind disclosure
      var dvKeysList = Object.keys(byDV);
      var allTerms = byDV[dvKeysList[0]].map(function (r) { return r.term; });
      var wrap = el('div', { class: 'table-wrap' });
      var table = el('table', { class: 'reg-table' });
      var thead = el('thead');
      var hrow = el('tr');
      hrow.appendChild(el('th', { text: '' }));
      for (var dk = 0; dk < dvKeysList.length; dk++) {
        hrow.appendChild(el('th', { class: 'num', text: dvLabels[dvKeysList[dk]] || dvKeysList[dk] }));
      }
      thead.appendChild(hrow);
      table.appendChild(thead);
      var tbody = el('tbody');
      for (var at = 0; at < allTerms.length; at++) {
        var tr = el('tr');
        tr.appendChild(el('td', { text: termLabels[allTerms[at]] || fmtSnake(allTerms[at]) }));
        for (var dv = 0; dv < dvKeysList.length; dv++) {
          var match = byDV[dvKeysList[dv]].find(function (r) { return r.term === allTerms[at]; });
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
      var trN = el('tr', { class: 'meta-row' });
      trN.appendChild(el('td', { text: 'N' }));
      for (var dn = 0; dn < dvKeysList.length; dn++) {
        trN.appendChild(el('td', { class: 'num', text: String(byDV[dvKeysList[dn]][0].nobs || dash()) }));
      }
      tbody.appendChild(trN);
      var trR2 = el('tr', { class: 'meta-row' });
      trR2.appendChild(el('td', { text: 'R\u00b2' }));
      for (var dr = 0; dr < dvKeysList.length; dr++) {
        trR2.appendChild(el('td', { class: 'num', text: fmtNum(byDV[dvKeysList[dr]][0].rsquared, 4) }));
      }
      tbody.appendChild(trR2);
      table.appendChild(tbody);
      wrap.appendChild(table);
      container.appendChild(buildDisclosure('View full plumbing regression table', wrap));

      // Robustness behind disclosure
      if (plumbRob && plumbRob.rows) {
        var robTable = buildTable([
          { key: 'dependent_variable', label: 'DV', format: function (v) { return dvLabels[v] || fmtSnake(v); } },
          { key: 'term', label: 'Variable', format: function (v) { return termLabels[v] || fmtSnake(v); } },
          { key: 'coef', label: 'Coef', numeric: true, format: function (v) { return fmtSci(v); } },
          { key: 'p_value', label: 'p-value', numeric: true, format: fmtPval },
          { key: 'series_role', label: 'Role' },
          { key: 'bill_proxy_source_quality', label: 'Bill Source', format: fmtLabel }
        ], plumbRob.rows.slice(0, 20));
        container.appendChild(buildDisclosure('View plumbing robustness checks', robTable));
      }
    } else {
      sectionError(container, 'Plumbing regression data unavailable.');
    }

    // ── Duration ──
    container.appendChild(el('h3', { class: 'section-subtitle', text: 'Public Duration Supply' }));
    container.appendChild(el('p', { class: 'section-desc', text: 'A joint measure of Treasury non-bill issuance, Fed QT, and buybacks. Higher values indicate more duration supply to the public. The headline measure uses exact net issuance; the provisional construction uses gross coupon flows as a fallback.' }));

    if (durSummary && durSummary.rows && durSummary.rows.length) {
      // Filter out future-dated rows, sort by date descending, take recent
      var todayStr = new Date().toISOString().slice(0, 10);
      var sorted = durSummary.rows
        .filter(function (r) { return r.date <= todayStr; })
        .sort(function (a, b) {
          return a.date < b.date ? 1 : a.date > b.date ? -1 : 0;
        });
      var recent = sorted.reverse();

      // Line chart with headline/provisional toggle
      var durSeries = {
        headline: function (r) { return toNumber(r.headline_public_duration_supply); },
        provisional: function (r) { return toNumber(r.provisional_public_duration_supply); }
      };
      var activeDurSeries = 'headline';

      function makeDurData(key) {
        return recent.map(function (r) {
          return { label: r.date, value: durSeries[key](r) };
        });
      }

      var durChartWrap = el('div', { class: 'chart-container' });

      // Header with toggle
      var durHeader = el('div', { class: 'chart-header' });
      var durHeaderText = el('div', { class: 'chart-header-text' });
      var durWeekCount = recent.length;
      durHeaderText.appendChild(el('div', { class: 'chart-title', text: 'Public Duration Supply (Recent ' + durWeekCount + ' Weeks)' }));
      var qtMissing = recent.some(function (r) { return r.qt_proxy_is_zero_filled === true; });
      var durCaptionText = 'Weekly USD. Higher = more duration supply to private hands. Headline = exact non-bill net + QT proxy \u2212 buybacks.';
      if (qtMissing) durCaptionText += ' Note: QT proxy is unavailable for some weeks and zero-filled in the headline construction.';
      var durCaptionEl = el('div', { class: 'chart-caption', text: durCaptionText });
      durHeaderText.appendChild(durCaptionEl);
      durHeader.appendChild(durHeaderText);
      var durControls = el('div', { class: 'chart-controls' });
      ['headline', 'provisional'].forEach(function (key) {
        var btn = el('button', { class: 'chart-btn' + (key === activeDurSeries ? ' active' : ''), text: key.charAt(0).toUpperCase() + key.slice(1) });
        btn.addEventListener('click', function () {
          activeDurSeries = key;
          durControls.querySelectorAll('.chart-btn').forEach(function (b) { b.classList.remove('active'); });
          btn.classList.add('active');
          // Re-render: build a fresh line chart and transplant its SVG
          var freshChart = buildLineChart(makeDurData(key), {
            ariaLabel: 'Line chart of ' + key + ' public duration supply'
          });
          var oldSvg = durChartWrap.querySelector('svg');
          var newSvg = freshChart.querySelector('svg');
          if (oldSvg && newSvg) durChartWrap.replaceChild(newSvg, oldSvg);
          else if (newSvg) durChartWrap.appendChild(newSvg);
          durCaptionEl.textContent = key === 'headline'
            ? 'Weekly USD. Higher = more duration supply to private hands. Headline = exact non-bill net + QT proxy - buybacks.'
            : 'Weekly USD. Provisional = gross coupon proxy + QT proxy - buybacks (fallback construction).';
        });
        durControls.appendChild(btn);
      });
      durHeader.appendChild(durControls);
      durChartWrap.appendChild(durHeader);

      // Initial SVG
      var initDurChart = buildLineChart(makeDurData('headline'), {
        ariaLabel: 'Line chart of headline public duration supply'
      });
      var initSvg = initDurChart.querySelector('svg');
      if (initSvg) durChartWrap.appendChild(initSvg);
      container.appendChild(durChartWrap);

      // Recent table behind disclosure
      var recentTable = buildTable([
        { key: 'date', label: 'Week' },
        { key: 'headline_public_duration_supply', label: 'Headline ($B)', numeric: true, format: fmtUsdBn, colorSign: true },
        { key: 'provisional_public_duration_supply', label: 'Provisional ($B)', numeric: true, format: fmtUsdBn },
        { key: 'buybacks_accepted', label: 'Buybacks ($B)', numeric: true, format: fmtUsdBn },
        { key: 'headline_source_quality', label: 'Source', format: fmtLabel }
      ], sorted.slice(0, 24).reverse());
      container.appendChild(buildDisclosure('View recent duration supply table (24 weeks)', recentTable));

      container.appendChild(el('p', { class: 'card-meta', text: 'Showing ' + recent.length + ' of ' + durSummary.rows.length + ' total weeks.' }));

      // Duration comparison behind disclosure
      if (durComparison && durComparison.rows) {
        var seen = {};
        var constructions = [];
        for (var ci = 0; ci < durComparison.rows.length; ci++) {
          var cr = durComparison.rows[ci];
          if (!seen[cr.construction_id]) {
            seen[cr.construction_id] = true;
            constructions.push({
              label: cr.treasury_proxy_label || fmtSnake(cr.construction_id),
              family: cr.construction_family,
              qt: cr.includes_qt_proxy ? 'Yes' : 'No',
              buybacks: cr.subtracts_buybacks ? 'Yes' : 'No',
              quality: fmtLabel(cr.source_quality)
            });
          }
        }
        var compTable = buildTable([
          { key: 'label', label: 'Construction' },
          { key: 'family', label: 'Role' },
          { key: 'qt', label: 'QT' },
          { key: 'buybacks', label: 'Buybacks' },
          { key: 'quality', label: 'Source' }
        ], constructions);
        container.appendChild(buildDisclosure('View duration construction comparison', compTable));
      }
    } else {
      sectionError(container, 'Duration supply data unavailable.');
    }

    // Raw data links
    container.appendChild(el('p', { class: 'card-meta' }, [
      document.createTextNode('Raw data: '),
      el('a', { href: 'data/plumbing_regression_summary.json', target: '_blank', text: 'Plumbing JSON' }),
      document.createTextNode(' / '),
      el('a', { href: 'data/duration_supply_summary.json', target: '_blank', text: 'Duration JSON' })
    ]));
  }

  // ── Interactive Dot-Whisker Chart ──

  function buildDotWhiskerChart(items, opts) {
    // items: [{label, coef, se, pval, roleClass}]
    opts = opts || {};
    var w = opts.width || 1060;
    var rowH = 44;
    var padL = opts.padLeft || 280;
    var padR = opts.padRight || 80;
    var padT = 20;
    var h = padT + items.length * rowH + 20;
    var chartW = w - padL - padR;

    // Compute range from coef +/- 1.96*SE (or just coef if no SE)
    var hideWhiskers = opts.hideWhiskers || false;
    var allVals = [];
    items.forEach(function (it) {
      if (it.se > 0) {
        allVals.push(it.coef - 1.96 * it.se, it.coef + 1.96 * it.se);
      } else {
        allVals.push(it.coef);
      }
    });
    var minV = Math.min.apply(null, allVals.concat([0]));
    var maxV = Math.max.apply(null, allVals.concat([0]));
    var pad = (maxV - minV) * 0.1 || 1;
    minV -= pad; maxV += pad;
    var range = maxV - minV;
    function xPos(v) { return padL + chartW * (v - minV) / range; }

    var container = el('div', { class: 'chart-container' });
    var header = el('div', { class: 'chart-header' });
    var headerText = el('div', { class: 'chart-header-text' });
    if (opts.title) headerText.appendChild(el('div', { class: 'chart-title', text: opts.title }));
    if (opts.caption) headerText.appendChild(el('div', { class: 'chart-caption', text: opts.caption }));
    header.appendChild(headerText);
    container.appendChild(header);

    var svg = svgEl('svg', { viewBox: '0 0 ' + w + ' ' + h, role: 'img', 'aria-label': opts.ariaLabel || 'Coefficient chart' });

    // Zero line
    var zx = xPos(0);
    svg.appendChild(svgEl('line', { x1: zx.toFixed(1), y1: String(padT), x2: zx.toFixed(1), y2: String(h - 20), stroke: 'var(--color-text-muted)', 'stroke-width': '1', 'stroke-dasharray': '4,3' }));

    // Axis ticks
    var nTicks = 6;
    for (var t = 0; t <= nTicks; t++) {
      var tv = minV + range * t / nTicks;
      var tx = xPos(tv);
      var tickText = document.createElementNS(SVG_NS, 'text');
      tickText.setAttribute('x', tx.toFixed(1));
      tickText.setAttribute('y', String(h - 4));
      tickText.setAttribute('text-anchor', 'middle');
      tickText.setAttribute('fill', 'var(--color-text-secondary)');
      tickText.setAttribute('font-size', '9');
      tickText.setAttribute('font-family', 'var(--font-mono)');
      tickText.textContent = tv.toFixed(1);
      svg.appendChild(tickText);
    }

    // Rows
    items.forEach(function (it, i) {
      var cy = padT + i * rowH + rowH / 2;
      var lo = it.coef - 1.96 * it.se;
      var hi = it.coef + 1.96 * it.se;

      // Hover band
      var band = svgEl('rect', { x: '0', y: String(cy - rowH / 2), width: String(w), height: String(rowH), fill: 'transparent' });
      svg.appendChild(band);

      // Whisker line + caps (only when SE is published)
      if (!hideWhiskers && it.se > 0) {
        svg.appendChild(svgEl('line', { x1: xPos(lo).toFixed(1), y1: String(cy), x2: xPos(hi).toFixed(1), y2: String(cy), stroke: 'var(--color-accent)', 'stroke-width': '2', opacity: '0.5' }));
        svg.appendChild(svgEl('line', { x1: xPos(lo).toFixed(1), y1: String(cy - 5), x2: xPos(lo).toFixed(1), y2: String(cy + 5), stroke: 'var(--color-accent)', 'stroke-width': '1.5', opacity: '0.5' }));
        svg.appendChild(svgEl('line', { x1: xPos(hi).toFixed(1), y1: String(cy - 5), x2: xPos(hi).toFixed(1), y2: String(cy + 5), stroke: 'var(--color-accent)', 'stroke-width': '1.5', opacity: '0.5' }));
      }

      // Dot
      var dotColor = it.pval < 0.05 ? 'var(--color-accent)' : 'var(--color-text-muted)';
      var dot = svgEl('circle', { cx: xPos(it.coef).toFixed(1), cy: String(cy), r: '5', fill: dotColor, stroke: 'var(--color-bg)', 'stroke-width': '1.5' });
      svg.appendChild(dot);

      // Row label
      var rowLabel = document.createElementNS(SVG_NS, 'text');
      rowLabel.setAttribute('x', String(padL - 10));
      rowLabel.setAttribute('y', String(cy + 4));
      rowLabel.setAttribute('text-anchor', 'end');
      rowLabel.setAttribute('fill', 'var(--color-text)');
      rowLabel.setAttribute('font-size', '11');
      rowLabel.setAttribute('font-family', 'var(--font-sans)');
      rowLabel.textContent = it.label;
      svg.appendChild(rowLabel);

      // Value label on right
      var valLabel = document.createElementNS(SVG_NS, 'text');
      valLabel.setAttribute('x', String(w - padR + 10));
      valLabel.setAttribute('y', String(cy + 4));
      valLabel.setAttribute('text-anchor', 'start');
      valLabel.setAttribute('fill', 'var(--color-text-secondary)');
      valLabel.setAttribute('font-size', '10');
      valLabel.setAttribute('font-family', 'var(--font-mono)');
      valLabel.textContent = fmtNum(it.coef, 2) + stars(it.pval);
      svg.appendChild(valLabel);

      // Row divider
      if (i < items.length - 1) {
        svg.appendChild(svgEl('line', { x1: String(padL), y1: String(cy + rowH / 2), x2: String(w - padR), y2: String(cy + rowH / 2), stroke: 'var(--color-border)', 'stroke-width': '0.5' }));
      }

      // Interaction
      band.addEventListener('mousemove', function (e) {
        var detail = fmtNum(it.coef, 3) + ' bp/$100bn';
        if (it.se > 0) detail += '  (SE ' + fmtNum(it.se, 3) + ', p=' + fmtPval(it.pval) + ')';
        else detail += '  (p=' + fmtPval(it.pval) + ')';
        showTooltip(e, it.label, detail);
        dot.setAttribute('r', '7');
      });
      band.addEventListener('mouseleave', function () {
        hideTooltip();
        dot.setAttribute('r', '5');
      });
    });

    container.appendChild(svg);
    return container;
  }

  // ── Interactive Horizon Profile Chart ──

  function buildHorizonChart(points, opts) {
    // points: [{horizon, coef, se, pval}] per outcome, grouped by outcome
    // opts.series: [{key, label, color}]
    opts = opts || {};
    var w = opts.width || 1060;
    var h = opts.height || 280;
    var padL = 70, padR = 20, padT = 20, padB = 50;
    var chartW = w - padL - padR;
    var chartH = h - padT - padB;

    var allCoefs = [];
    var seriesList = opts.series || [];
    seriesList.forEach(function (s) {
      (points[s.key] || []).forEach(function (p) {
        allCoefs.push(p.coef - 1.96 * p.se, p.coef + 1.96 * p.se);
      });
    });
    if (!allCoefs.length) return el('p', { class: 'card-meta', text: 'No horizon data.' });
    var minV = Math.min.apply(null, allCoefs.concat([0]));
    var maxV = Math.max.apply(null, allCoefs.concat([0]));
    var pad = (maxV - minV) * 0.12 || 1;
    minV -= pad; maxV += pad;
    var range = maxV - minV;

    var horizons = (points[seriesList[0].key] || []).map(function (p) { return p.horizon; });
    function xPos(i) { return padL + chartW * i / (horizons.length - 1); }
    function yPos(v) { return padT + chartH - chartH * (v - minV) / range; }

    var container = el('div', { class: 'chart-container' });
    var header = el('div', { class: 'chart-header' });
    var headerText = el('div', { class: 'chart-header-text' });
    if (opts.title) headerText.appendChild(el('div', { class: 'chart-title', text: opts.title }));
    if (opts.caption) headerText.appendChild(el('div', { class: 'chart-caption', text: opts.caption }));
    header.appendChild(headerText);
    container.appendChild(header);

    var svg = svgEl('svg', { viewBox: '0 0 ' + w + ' ' + h, role: 'img', 'aria-label': opts.ariaLabel || 'Horizon profile' });

    // Grid + Y axis
    var nTicks = 5;
    for (var t = 0; t <= nTicks; t++) {
      var tv = minV + range * t / nTicks;
      var ty = yPos(tv);
      svg.appendChild(svgEl('line', { x1: padL, y1: ty.toFixed(1), x2: w - padR, y2: ty.toFixed(1), stroke: 'var(--color-border)', 'stroke-width': '0.5' }));
      var yt = document.createElementNS(SVG_NS, 'text');
      yt.setAttribute('x', String(padL - 8)); yt.setAttribute('y', (ty + 4).toFixed(1));
      yt.setAttribute('text-anchor', 'end'); yt.setAttribute('fill', 'var(--color-text-secondary)');
      yt.setAttribute('font-size', '10'); yt.setAttribute('font-family', 'var(--font-mono)');
      yt.textContent = tv.toFixed(1);
      svg.appendChild(yt);
    }

    // Zero line
    svg.appendChild(svgEl('line', { x1: padL, y1: yPos(0).toFixed(1), x2: w - padR, y2: yPos(0).toFixed(1), stroke: 'var(--color-text-muted)', 'stroke-width': '1', 'stroke-dasharray': '4,3' }));

    // X axis labels
    horizons.forEach(function (hz, i) {
      var lbl = document.createElementNS(SVG_NS, 'text');
      lbl.setAttribute('x', xPos(i).toFixed(1)); lbl.setAttribute('y', String(h - padB + 20));
      lbl.setAttribute('text-anchor', 'middle'); lbl.setAttribute('fill', 'var(--color-text-secondary)');
      lbl.setAttribute('font-size', '11'); lbl.setAttribute('font-family', 'var(--font-sans)');
      lbl.textContent = '+' + hz + 'bd';
      svg.appendChild(lbl);
    });

    // Draw each series
    var allDots = [];
    seriesList.forEach(function (s) {
      var data = points[s.key] || [];
      if (!data.length) return;

      // CI band
      var bandUp = '', bandDown = '';
      data.forEach(function (p, i) {
        var x = xPos(i);
        bandUp += (i === 0 ? 'M' : 'L') + x.toFixed(1) + ',' + yPos(p.coef + 1.96 * p.se).toFixed(1);
      });
      for (var bi = data.length - 1; bi >= 0; bi--) {
        bandDown += 'L' + xPos(bi).toFixed(1) + ',' + yPos(data[bi].coef - 1.96 * data[bi].se).toFixed(1);
      }
      svg.appendChild(svgEl('path', { d: bandUp + bandDown + 'Z', fill: s.color, opacity: '0.1' }));

      // Line
      var lineD = data.map(function (p, i) { return (i === 0 ? 'M' : 'L') + xPos(i).toFixed(1) + ',' + yPos(p.coef).toFixed(1); }).join('');
      svg.appendChild(svgEl('path', { d: lineD, fill: 'none', stroke: s.color, 'stroke-width': '2' }));

      // Dots
      data.forEach(function (p, i) {
        var dot = svgEl('circle', { cx: xPos(i).toFixed(1), cy: yPos(p.coef).toFixed(1), r: '4', fill: s.color, stroke: 'var(--color-bg)', 'stroke-width': '1.5' });
        allDots.push({ el: dot, data: p, series: s, idx: i });
        svg.appendChild(dot);
      });
    });

    // Crosshair
    var crossV = svgEl('line', { x1: 0, y1: padT, x2: 0, y2: padT + chartH, stroke: 'var(--color-text-muted)', 'stroke-width': '1', 'stroke-dasharray': '3,3', 'pointer-events': 'none', opacity: '0' });
    svg.appendChild(crossV);

    // Hit area
    var hitArea = svgEl('rect', { x: padL, y: padT, width: chartW, height: chartH, fill: 'transparent' });
    svg.appendChild(hitArea);

    hitArea.addEventListener('mousemove', function (e) {
      var rect = svg.getBoundingClientRect();
      var mx = (e.clientX - rect.left) / rect.width * w;
      var bestI = 0, bestDist = Infinity;
      horizons.forEach(function (_, i) {
        var d = Math.abs(mx - xPos(i));
        if (d < bestDist) { bestDist = d; bestI = i; }
      });
      crossV.setAttribute('x1', xPos(bestI).toFixed(1));
      crossV.setAttribute('x2', xPos(bestI).toFixed(1));
      crossV.setAttribute('opacity', '1');
      var tipLines = '+' + horizons[bestI] + ' business days';
      seriesList.forEach(function (s) {
        var d = (points[s.key] || [])[bestI];
        if (d) tipLines += '\n' + s.label + ': ' + fmtNum(d.coef, 3) + ' (p=' + fmtPval(d.pval) + ')';
      });
      showTooltip(e, '+' + horizons[bestI] + 'bd', seriesList.map(function (s) {
        var d = (points[s.key] || [])[bestI];
        return d ? s.label + ': ' + fmtNum(d.coef, 3) + ' ' + stars(d.pval) : '';
      }).filter(Boolean).join(' | '));
      allDots.forEach(function (dd) { dd.el.setAttribute('r', dd.idx === bestI ? '6' : '3'); });
    });

    hitArea.addEventListener('mouseleave', function () {
      crossV.setAttribute('opacity', '0');
      hideTooltip();
      allDots.forEach(function (dd) { dd.el.setAttribute('r', '4'); });
    });

    container.appendChild(svg);
    return container;
  }

  // ── Pricing ──

  async function renderPricing() {
    var container = $('#pricing-content');
    var results = await Promise.all([
      fetchJSON('pricing_spec_registry.json'),
      fetchJSON('pricing_regression_summary.json'),
      fetchJSON('pricing_scenario_translation.json'),
      fetchJSON('pricing_subsample_grid.json'),
      fetchJSON('pricing_regression_robustness.json'),
      fetchJSON('pricing_release_flow_leave_one_out.json'),
      fetchJSON('dataset_status.json')
    ]);
    var specRegistry = results[0], summary = results[1], scenarios = results[2], subsample = results[3], robustness = results[4], leaveOneOut = results[5], dsStatus = results[6];

    if (!summary || !summary.rows || !summary.rows.length) {
      sectionError(container, 'Pricing regression data unavailable.');
      return;
    }

    container.appendChild(el('div', { class: 'interpretation-note' }, [
      el('strong', { text: 'Interpretation boundary: ' }),
      document.createTextNode('Supporting/provisional reduced-form evidence. The release-level +63bd design is the credibility anchor; the monthly flow spec provides additional context. Neither is a settled causal claim.')
    ]));

    // ── Build role map from spec registry ──
    var registryRoleMap = {};
    if (specRegistry && specRegistry.rows) {
      specRegistry.rows.forEach(function (sr) {
        if (!registryRoleMap[sr.spec_id]) {
          registryRoleMap[sr.spec_id] = sr.public_claim_role || sr.pipeline_anchor_role || 'supporting';
        }
      });
    }

    // Map artifact roles to display labels
    var claimRoleDisplayLabels = {
      supporting_anchor: 'anchor',
      credibility_anchor: 'anchor',
      supporting_context: 'context',
      context: 'context',
      supporting: 'supporting'
    };

    // ── Extract primary predictor rows ──
    var primaryRows = summary.rows
      .filter(function (row) { return row.term_role === 'primary_predictor' && row.outcome_role === 'headline'; })
      .sort(function (a, b) { return String(a.model_id).localeCompare(String(b.model_id)) || String(a.dependent_variable).localeCompare(String(b.dependent_variable)); });

    // ── 1) Interactive Coefficient Comparison ──
    var modelDisplayOrder = ['monthly_flow_baseline', 'release_flow_baseline_63bd', 'monthly_stock_baseline', 'weekly_duration_baseline'];
    var modelLabels = {
      monthly_flow_baseline: 'Monthly Flow',
      release_flow_baseline_63bd: 'Release +63bd',
      monthly_stock_baseline: 'Monthly Stock',
      weekly_duration_baseline: 'Weekly Duration'
    };
    var roleLabels = {};
    modelDisplayOrder.forEach(function (mid) {
      var artRole = registryRoleMap[mid] || 'supporting';
      roleLabels[mid] = claimRoleDisplayLabels[artRole] || 'supporting';
    });

    var coefItems = [];
    modelDisplayOrder.forEach(function (mid) {
      primaryRows.filter(function (r) { return r.model_id === mid; }).forEach(function (r) {
        var outcomeShort = r.dependent_variable === 'THREEFYTP10' ? 'TP' : '10Y';
        coefItems.push({
          label: (modelLabels[mid] || fmtSnake(mid)) + ' \u2192 ' + outcomeShort,
          coef: r.coef,
          se: r.std_err,
          pval: r.p_value,
          roleClass: roleLabels[mid] || 'supporting'
        });
      });
    });

    if (coefItems.length) {
      container.appendChild(buildDotWhiskerChart(coefItems, {
        title: 'Coefficient Comparison',
        caption: 'Coefficients in bp per $100bn with 95% CI. Filled dots = significant at p<0.05. Roles from spec registry: Release +63bd is the credibility anchor; Monthly Flow provides supporting context.',
        ariaLabel: 'Dot-whisker chart comparing pricing coefficients across specifications'
      }));
    }

    // ── 2) Interactive Horizon Profile ──
    var horizonOrder = [1, 5, 10, 21, 42, 63];
    var horizonModelMap = { 1: 'release_flow_horizon_1bd', 5: 'release_flow_horizon_5bd', 10: 'release_flow_horizon_10bd', 21: 'release_flow_horizon_21bd', 42: 'release_flow_horizon_42bd', 63: 'release_flow_baseline_63bd' };
    var horizonPoints = { tp: [], yield: [] };

    horizonOrder.forEach(function (hz) {
      var mid = horizonModelMap[hz];
      var rows = primaryRows.filter(function (r) { return r.model_id === mid; });
      rows.forEach(function (r) {
        var key = r.dependent_variable === 'THREEFYTP10' ? 'tp' : 'yield';
        horizonPoints[key].push({ horizon: hz, coef: r.coef, se: r.std_err, pval: r.p_value });
      });
    });

    if (horizonPoints.tp.length || horizonPoints.yield.length) {
      container.appendChild(buildHorizonChart(horizonPoints, {
        title: 'Release-Flow Horizon Profile',
        caption: 'How the Maturity-Tilt Flow coefficient evolves from +1 to +63 business days after each QRA release. CI bands show 95% intervals.',
        ariaLabel: 'Interactive horizon profile showing coefficient evolution across post-release windows',
        series: [
          { key: 'tp', label: 'Term Premium', color: 'var(--color-accent)' },
          { key: 'yield', label: '10Y Yield', color: 'var(--color-positive)' }
        ]
      }));
    }

    // ── 3) Key result cards: monthly flow + release anchor side-by-side ──
    var monthlyFlowRows = primaryRows.filter(function (r) { return r.model_id === 'monthly_flow_baseline'; });
    var releaseAnchorRows = primaryRows.filter(function (r) { return r.public_claim_role === 'supporting_anchor' || r.pipeline_anchor_role === 'credibility_anchor'; });

    var resultCards = el('div', { class: 'card-grid card-grid-3' });
    monthlyFlowRows.forEach(function (mr) {
      var outcomeShort = mr.dependent_variable === 'THREEFYTP10' ? 'Term Premium' : '10Y Yield';
      var mfRole = claimRoleDisplayLabels[registryRoleMap['monthly_flow_baseline']] || 'context';
      resultCards.appendChild(el('div', { class: 'card' }, [
        el('div', { class: 'card-title' }, [pricingLabel(mfRole, fmtLabel(registryRoleMap['monthly_flow_baseline'] || 'context')), document.createTextNode(' ' + outcomeShort)]),
        el('div', { class: 'card-value', text: fmtNum(mr.coef, 3) + ' bp' }),
        el('div', { class: 'card-meta', text: 'Monthly flow | p=' + fmtPval(mr.p_value) + ' ' + stars(mr.p_value) + ' | N=' + mr.nobs })
      ]));
    });
    releaseAnchorRows.forEach(function (ar) {
      var outcomeShort = ar.dependent_variable === 'THREEFYTP10' ? 'Term Premium' : '10Y Yield';
      var raRole = claimRoleDisplayLabels[registryRoleMap['release_flow_baseline_63bd']] || 'anchor';
      resultCards.appendChild(el('div', { class: 'card' }, [
        el('div', { class: 'card-title' }, [pricingLabel(raRole, fmtLabel(registryRoleMap['release_flow_baseline_63bd'] || 'supporting_anchor')), document.createTextNode(' ' + outcomeShort)]),
        el('div', { class: 'card-value', text: fmtNum(ar.coef, 3) + ' bp' }),
        el('div', { class: 'card-meta', text: 'Release +63bd | p=' + fmtPval(ar.p_value) + ' ' + stars(ar.p_value) + ' | N=' + ar.effective_shock_count })
      ]));
    });
    container.appendChild(resultCards);

    // ── 4) Scenario translations (interactive) ──
    if (scenarios && scenarios.rows && scenarios.rows.length) {
      container.appendChild(el('h3', { class: 'section-subtitle' }, [
        document.createTextNode('Scenario Translations '),
        pricingLabel('illustrative', 'Illustrative')
      ]));

      // Build interactive horizontal chart from scenario data (point estimates only — no published SE)
      var scenItems = scenarios.rows.map(function (r) {
        var outShort = r.dependent_variable === 'THREEFYTP10' ? 'TP' : '10Y';
        var scenShort = (r.scenario_label || '').replace(/^Plus /, '+').replace(/ translation via .*/, '');
        return {
          label: scenShort + ' \u2192 ' + outShort,
          coef: r.implied_bp_change,
          se: 0,
          pval: r.p_value,
          roleClass: r.scenario_role === 'illustrative_only' ? 'illustrative' : 'supporting'
        };
      });
      container.appendChild(buildDotWhiskerChart(scenItems, {
        title: 'Scenario Translations',
        caption: 'Implied basis-point changes from illustrative what-if exercises. Point estimates only — no published uncertainty. Large p-values on stock-based scenarios indicate high source-model uncertainty. Not headline estimates.',
        ariaLabel: 'Interactive scenario translation chart',
        padLeft: 300,
        hideWhiskers: true
      }));
    }

    // ── Correlation overlays (interactive, from monthly panel) ──
    var overlayPanel = await fetchJSON('pricing_overlay_panel.json');
    if (overlayPanel && overlayPanel.rows && overlayPanel.rows.length) {
      container.appendChild(el('h3', { class: 'section-subtitle' }, [
        document.createTextNode('Time-Series Context '),
        pricingLabel('supporting', 'Correlation Only')
      ]));

      // Maturity-Tilt Flow vs 10Y Yield
      var flowOverlayData = overlayPanel.rows
        .filter(function (r) { return r.ati_baseline_bn != null && r.DGS10 != null; })
        .map(function (r) { return { date: r.date, left: r.ati_baseline_bn, right: r.DGS10 }; });
      if (flowOverlayData.length) {
        container.appendChild(buildOverlayChart(flowOverlayData, {
          title: 'Maturity-Tilt Flow vs 10Y Yield',
          caption: 'Standardized monthly overlay from 2009 onward. Compares timing, not absolute levels. Correlation only \u2014 does not establish causation.',
          leftLabel: 'Maturity-Tilt Flow',
          rightLabel: '10Y Yield',
          leftColor: 'var(--color-primary-light)',
          rightColor: '#dc2626',
          ariaLabel: 'Interactive overlay of Maturity-Tilt Flow and 10Y yield over time'
        }));
      }

      // Excess Bills Stock vs Term Premium
      var stockOverlayData = overlayPanel.rows
        .filter(function (r) { return r.stock_excess_bills_bn != null && r.THREEFYTP10 != null; })
        .map(function (r) { return { date: r.date, left: r.stock_excess_bills_bn, right: r.THREEFYTP10 }; });
      if (stockOverlayData.length) {
        container.appendChild(buildOverlayChart(stockOverlayData, {
          title: 'Excess Bills Stock vs 10Y Term Premium',
          caption: 'Standardized monthly overlay from 2009 onward. Compares timing, not absolute levels. Correlation only \u2014 does not establish causation.',
          leftLabel: 'Excess Bills Stock',
          rightLabel: 'Term Premium',
          leftColor: 'var(--color-primary-light)',
          rightColor: '#dc2626',
          ariaLabel: 'Interactive overlay of Excess Bills Stock and 10Y term premium over time'
        }));
      }
    }

    // ── 5) All detailed tables behind a single group of disclosures ──
    var otherContextRows = primaryRows.filter(function (r) { return r.model_id !== 'monthly_flow_baseline' && r.public_claim_role !== 'supporting_anchor' && r.pipeline_anchor_role !== 'credibility_anchor'; });
    if (otherContextRows.length) {
      container.appendChild(buildDisclosure('Other context specs (' + otherContextRows.length + ')', buildTable([
        { key: 'model_id', label: 'Model', format: fmtSnake },
        { key: 'dependent_label', label: 'Outcome' },
        { key: 'coef', label: 'Coef', numeric: true, format: function (v) { return fmtNum(v, 3); } },
        { key: 'p_value', label: 'p', numeric: true, format: fmtPval },
        { key: 'effective_shock_count', label: 'N', numeric: true }
      ], otherContextRows)));
    }
    if (specRegistry && specRegistry.rows) {
      container.appendChild(buildDisclosure('Spec registry (' + specRegistry.rows.length + ')', buildTable([
        { key: 'spec_id', label: 'Spec', format: fmtSnake },
        { key: 'public_claim_role', label: 'Role', format: fmtLabel },
        { key: 'outcome', label: 'Outcome', format: fmtSnake },
        { key: 'frequency', label: 'Freq' },
        { key: 'sample_start', label: 'Start' },
        { key: 'sample_end', label: 'End' }
      ], specRegistry.rows)));
    }
    if (scenarios && scenarios.rows) {
      container.appendChild(buildDisclosure('Scenario table', buildTable([
        { key: 'scenario_label', label: 'Scenario' },
        { key: 'dependent_label', label: 'Outcome' },
        { key: 'implied_bp_change', label: 'Implied bp', numeric: true, format: function (v) { return fmtNum(v, 2); } },
        { key: 'p_value', label: 'p', numeric: true, format: fmtPval }
      ], scenarios.rows)));
    }
    if (subsample && subsample.rows && subsample.rows.length) {
      container.appendChild(buildDisclosure('Subsample grid', buildTable([
        { key: 'spec_id', label: 'Spec', format: fmtSnake },
        { key: 'dependent_label', label: 'Outcome' },
        { key: 'coef', label: 'Coef', numeric: true, format: function (v) { return fmtNum(v, 3); } },
        { key: 'p_value', label: 'p', numeric: true, format: fmtPval }
      ], subsample.rows)));
    }
    if (robustness && robustness.rows && robustness.rows.length) {
      container.appendChild(buildDisclosure('Robustness pack', buildTable([
        { key: 'variant_family', label: 'Family', format: fmtSnake },
        { key: 'dependent_label', label: 'Outcome' },
        { key: 'coef', label: 'Coef', numeric: true, format: function (v) { return fmtNum(v, 3); } },
        { key: 'p_value', label: 'p', numeric: true, format: fmtPval }
      ], robustness.rows.slice(0, 20))));
    }
    if (leaveOneOut && leaveOneOut.rows && leaveOneOut.rows.length) {
      container.appendChild(buildDisclosure('Leave-one-out', buildTable([
        { key: 'dependent_label', label: 'Outcome' },
        { key: 'omitted_release_id', label: 'Omitted', format: fmtSnake },
        { key: 'coef', label: 'Coef', numeric: true, format: function (v) { return fmtNum(v, 3); } },
        { key: 'p_value', label: 'p', numeric: true, format: fmtPval }
      ], leaveOneOut.rows.slice(0, 20))));
    }

    container.appendChild(el('p', { class: 'card-meta' }, [
      document.createTextNode('Raw: '),
      el('a', { href: 'data/pricing_regression_summary.json', target: '_blank', text: 'Regressions' }),
      document.createTextNode(' / '),
      el('a', { href: 'data/pricing_spec_registry.json', target: '_blank', text: 'Specs' }),
      document.createTextNode(' / '),
      el('a', { href: 'data/pricing_scenario_translation.json', target: '_blank', text: 'Scenarios' })
    ]));
  }

  // ── Bounded QRA Causal Lane ──

  async function renderCausal() {
    var container = $('#causal-content');
    var results = await Promise.all([
      fetchJSON('causal_claims_status.json'),
      fetchJSON('event_design_status.json'),
      fetchJSON('qra_benchmark_evidence_registry.json'),
      fetchJSON('qra_event_summary.json'),
      fetchJSON('qra_event_table.json'),
      fetchJSON('dataset_status.json')
    ]);
    var causalClaims = results[0], eventDesign = results[1], benchmarkEvidence = results[2], eventSummary = results[3], eventTable = results[4], dsStatus = results[5];

    container.appendChild(el('div', { class: 'interpretation-note' }, [
      el('strong', { text: 'Scope boundary: ' }),
      document.createTextNode('This section describes a narrower, bounded supporting lane. The event-causal surface is not the main pricing coefficient source in this round. Results here should be read as a small-sample audit pilot, not a settled full-sample causal design.')
    ]));

    // Summary cards from causal claims and event design
    if (causalClaims && causalClaims.rows && causalClaims.rows.length) {
      var cc = causalClaims.rows[0];
      var cards = el('div', { class: 'card-grid' });
      cards.appendChild(el('div', { class: 'card' }, [
        el('div', { class: 'card-title', text: 'Financing Components' }),
        el('div', { class: 'card-value', text: String(cc.current_sample_financing_component_count || 0) }),
        el('div', { class: 'card-meta', text: 'Current-sample post-2022Q3' })
      ]));
      cards.appendChild(el('div', { class: 'card' }, [
        el('div', { class: 'card-title', text: 'Benchmark-Ready' }),
        el('div', { class: 'card-value', text: String(cc.benchmark_ready_count || 0) }),
        el('div', { class: 'card-meta', text: 'Verified pre-release external' })
      ]));
      cards.appendChild(el('div', { class: 'card' }, [
        el('div', { class: 'card-title', text: 'Tier A' }),
        el('div', { class: 'card-value', text: String(cc.tier_a_count || 0) }),
        el('div', { class: 'card-meta', text: 'Causal-pilot eligible' })
      ]));
      cards.appendChild(el('div', { class: 'card' }, [
        el('div', { class: 'card-title', text: 'Source-Exhausted' }),
        el('div', { class: 'card-value', text: String(cc.source_family_exhausted_count || 0) }),
        el('div', { class: 'card-meta', text: 'Blocked, no open candidates' })
      ]));
      cards.appendChild(el('div', { class: 'card' }, [
        el('div', { class: 'card-title', text: 'Open Candidates' }),
        el('div', { class: 'card-value', text: String(cc.open_candidate_count || 0) }),
        el('div', { class: 'card-meta', text: 'Remaining benchmark families' })
      ]));
      container.appendChild(cards);

      // Can / Cannot claim (compact)
      var panels = el('div', { class: 'claim-panels' });
      var canPanel = el('div', { class: 'claim-panel claim-panel-can' });
      canPanel.appendChild(el('div', { class: 'claim-panel-label', text: 'Can claim' }));
      canPanel.appendChild(el('p', { text: cc.can_claim || '' }));
      panels.appendChild(canPanel);
      var cannotPanel = el('div', { class: 'claim-panel claim-panel-cannot' });
      cannotPanel.appendChild(el('div', { class: 'claim-panel-label', text: 'Cannot claim' }));
      cannotPanel.appendChild(el('p', { text: cc.cannot_claim || '' }));
      panels.appendChild(cannotPanel);
      container.appendChild(panels);
    }

    // Event summary behind disclosure
    if (eventSummary && eventSummary.rows) {
      var metrics = [
        { key: 'DGS10', label: '10Y Yield' },
        { key: 'THREEFYTP10', label: 'Term Premium' },
        { key: 'SP500', label: 'S&P 500' },
        { key: 'VIXCLS', label: 'VIX' }
      ];
      var sumRows = [];
      for (var mi = 0; mi < metrics.length; mi++) {
        for (var wi = 0; wi < 2; wi++) {
          var w = wi === 0 ? 'd1' : 'd3';
          var srow = { metric: metrics[mi].label, window: wi === 0 ? '1-day' : '3-day' };
          for (var si = 0; si < eventSummary.rows.length; si++) {
            var sr = eventSummary.rows[si];
            var bucket = sr.headline_bucket || sr.expected_direction || '';
            srow[bucket] = sr[metrics[mi].key + '_' + w];
          }
          sumRows.push(srow);
        }
      }
      var buckets = eventSummary.rows.map(function (r) { return r.headline_bucket || r.expected_direction || ''; });
      var sumColumns = [
        { key: 'metric', label: 'Metric' },
        { key: 'window', label: 'Window' }
      ];
      for (var bi = 0; bi < buckets.length; bi++) {
        (function (b) {
          sumColumns.push({
            key: b, label: fmtDirection(b), numeric: true, colorSign: true,
            format: function (v) { return v == null ? dash() : fmtNum(v, 3); }
          });
        })(buckets[bi]);
      }
      var evSumTable = buildTable(sumColumns, sumRows);
      container.appendChild(buildDisclosure('View QRA event summary by direction', evSumTable));
    }

    // Event table behind disclosure
    if (eventTable && eventTable.rows) {
      var officialEvents = eventTable.rows.filter(function (r) {
        return r.event_date_type === 'official_release_date';
      });
      var displayEvents = officialEvents.length > 0 ? officialEvents : eventTable.rows.slice(0, 10);
      var evTable = buildTable([
        { key: 'event_label', label: 'Event' },
        { key: 'event_date_aligned', label: 'Date' },
        { key: 'expected_direction', label: 'Direction', format: fmtDirection },
        { key: 'DGS10_d1', label: '10Y d1', numeric: true, colorSign: true, format: function (v) { return fmtNum(v, 3); } },
        { key: 'THREEFYTP10_d1', label: 'TP d1', numeric: true, colorSign: true, format: function (v) { return fmtNum(v, 4); } }
      ], displayEvents);
      container.appendChild(buildDisclosure('View individual QRA events (' + displayEvents.length + ' rows)', evTable));
    }

    // Benchmark evidence behind disclosure
    if (benchmarkEvidence && benchmarkEvidence.rows && benchmarkEvidence.rows.length) {
      var currentSample = benchmarkEvidence.rows
        .filter(function (row) {
          return quarterRank(row.quarter) != null && quarterRank(row.quarter) >= quarterRank('2022Q3');
        })
        .sort(function (a, b) { return String(a.quarter).localeCompare(String(b.quarter)); });

      if (currentSample.length) {
        var benchTable = buildTable([
          { key: 'release_component_id', label: 'Component' },
          { key: 'quarter', label: 'Quarter' },
          { key: 'quality_tier', label: 'Tier', format: fmtLabel },
          { key: 'benchmark_timing_status', label: 'Timing', format: fmtLabel },
          { key: 'external_benchmark_ready', label: 'Ready', format: checkMark },
          { key: 'terminal_disposition', label: 'Outcome', format: benchmarkDispositionLabel },
          { key: 'claim_scope', label: 'Scope', format: fmtLabel }
        ], currentSample);
        container.appendChild(buildDisclosure('View benchmark evidence registry (' + currentSample.length + ' components)', benchTable));
      }
    }

    // Event design status counts behind disclosure
    if (eventDesign && eventDesign.rows) {
      var edTable = buildTable([
        { key: 'metric', label: 'Metric', format: fmtSnake },
        { key: 'value', label: 'Count', numeric: true },
        { key: 'notes', label: 'Notes' }
      ], eventDesign.rows);
      container.appendChild(buildDisclosure('View event design status counts (' + eventDesign.rows.length + ' metrics)', edTable));
    }
  }

  // ── Provenance ──

  async function renderProvenance() {
    var container = $('#provenance-content');
    var results = await Promise.all([
      fetchJSON('dataset_status.json'),
      fetchJSON('extension_status.json'),
      fetchJSON('series_metadata_catalog.json'),
      fetchJSON('data_sources_summary.json'),
      fetchJSON('index.json'),
      fetchJSON('official_capture_readiness.json'),
      fetchJSON('official_capture_completion.json')
    ]);
    var dsStatus = results[0], extStatus = results[1], catalog = results[2], sources = results[3], index = results[4], captureReadiness = results[5], captureCompletion = results[6];

    // Snapshot card
    if (index) {
      var snapGrid = el('div', { class: 'card-grid' });
      snapGrid.appendChild(el('div', { class: 'card' }, [
        el('div', { class: 'card-title', text: 'Published Artifacts' }),
        el('div', { class: 'card-value', text: String(index.artifact_count) }),
        el('div', { class: 'card-meta', text: 'Generated: ' + fmtTimestamp(index.generated_at_utc) })
      ]));
      if (dsStatus) {
        var headlineCount = dsStatus.rows.filter(function (r) { return r.public_role === 'headline'; }).length;
        var totalCount = dsStatus.rows.length;
        snapGrid.appendChild(el('div', { class: 'card' }, [
          el('div', { class: 'card-title', text: 'Dataset Readiness' }),
          el('div', { class: 'card-value', text: headlineCount + ' / ' + totalCount }),
          el('div', { class: 'card-meta', text: 'Headline public role / total tracked' })
        ]));
      }
      container.appendChild(snapGrid);
    }

    container.appendChild(el('div', { class: 'supporting-note', text: 'This site reads only from published backend artifacts mirrored to site/data/ and site/figures/. No raw or interim data is accessed by the frontend. All outputs can be regenerated from source via make regenerate.' }));

    // Dataset readiness behind disclosure
    if (dsStatus) {
      var dsTable = buildTable([
        { key: 'dataset', label: 'Dataset', format: fmtSnake },
        { key: 'readiness_tier', label: 'Readiness', format: function (v) { return statusBadge(v); } },
        { key: 'public_role', label: 'Role', format: function (v, row) { return rolePill(publicRoleForDataset(row.dataset)); } },
        { key: 'source_quality', label: 'Source', format: fmtLabel },
        { key: 'headline_ready', label: 'Headline', format: function (v) { return checkMark(v); } },
        { key: 'last_regenerated_utc', label: 'Updated', format: fmtTimestamp }
      ], dsStatus.rows);
      container.appendChild(buildDisclosure('View full dataset readiness table (' + dsStatus.rows.length + ' datasets)', dsTable));
    }

    // Capture readiness behind disclosure
    if (captureReadiness && captureReadiness.rows) {
      var crTable = buildTable([
        { key: 'quarter', label: 'Quarter' },
        { key: 'readiness_tier', label: 'Readiness', format: function (v) { return statusBadge(v); } },
        { key: 'source_quality', label: 'Source', format: fmtLabel },
        { key: 'headline_ready', label: 'Headline', format: checkMark }
      ], captureReadiness.rows.slice(0, 20));
      container.appendChild(buildDisclosure('View official capture readiness', crTable));
    }

    // Capture completion behind disclosure
    if (captureCompletion && captureCompletion.rows) {
      var ccTable = buildTable([
        { key: 'quarter', label: 'Quarter' },
        { key: 'completion_tier', label: 'Completion', format: fmtLabel },
        { key: 'qa_status', label: 'QA', format: fmtLabel },
        { key: 'is_headline_ready', label: 'Headline', format: checkMark }
      ], captureCompletion.rows.slice(0, 20));
      container.appendChild(buildDisclosure('View capture completion history', ccTable));
    }

    // Series catalog behind disclosure
    if (catalog && catalog.rows) {
      var catTable = buildTable([
        { key: 'dataset', label: 'Dataset' },
        { key: 'series_id', label: 'Series', format: fmtSnake },
        { key: 'frequency', label: 'Frequency' },
        { key: 'value_units', label: 'Units' },
        { key: 'source_quality', label: 'Quality', format: fmtLabel }
      ], catalog.rows);
      container.appendChild(buildDisclosure('View series metadata catalog (' + catalog.rows.length + ' series)', catTable));
    }

    // Data sources behind disclosure
    if (sources && sources.rows) {
      var srcTable = buildTable([
        { key: 'source_family', label: 'Source', format: fmtSnake },
        { key: 'raw_dir_exists', label: 'Raw Dir', format: checkMark },
        { key: 'file_count', label: 'Files', numeric: true },
        { key: 'manifest_exists', label: 'Manifest', format: checkMark }
      ], sources.rows);
      container.appendChild(buildDisclosure('View data sources summary', srcTable));
    }

    // Extension backend status behind disclosure
    if (extStatus && extStatus.rows) {
      var extTable = buildTable([
        { key: 'extension', label: 'Extension', format: fmtSnake },
        { key: 'readiness_tier', label: 'Readiness', format: function (v) { return statusBadge(v); } },
        { key: 'panel_exists', label: 'Panel', format: checkMark },
        { key: 'panel_rows', label: 'Rows', numeric: true, format: function (v) { return v != null ? v.toLocaleString() : dash(); } },
        { key: 'publish_exists', label: 'Published', format: checkMark }
      ], extStatus.rows);
      container.appendChild(buildDisclosure('View extension backend status', extTable));
    }

    // Artifact manifest behind disclosure
    if (index && index.artifacts) {
      var artifactList = el('div', { style: 'padding: 16px; max-height: 300px; overflow-y: auto; font-size: 0.82rem; font-family: var(--font-mono);' });
      for (var ai = 0; ai < index.artifacts.length; ai++) {
        artifactList.appendChild(el('div', { text: index.artifacts[ai] }));
      }
      container.appendChild(buildDisclosure('View full artifact manifest (' + index.artifact_count + ' files)', artifactList));
    }
  }

  // ── Extensions ──

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
    if (extStatus && extStatus.rows) {
      var grid = el('div', { class: 'card-grid' });
      for (var i = 0; i < extStatus.rows.length; i++) {
        var row = extStatus.rows[i];
        var name = row.extension.replace(/_/g, ' ');
        grid.appendChild(el('div', { class: 'card' }, [
          el('div', { class: 'card-title', text: name }),
          el('div', { class: 'card-value' }, [statusBadge(row.readiness_tier)]),
          el('div', { class: 'card-meta', text: row.panel_exists ? row.panel_rows.toLocaleString() + ' panel rows' : 'No panel data' }),
          el('div', { class: 'card-meta', text: 'Backend: ' + fmtSnake(row.backend_status) })
        ]));
      }
      container.appendChild(grid);
    }

    // Investor allotments behind disclosure
    if (investor && investor.rows) {
      var coverage = investor.rows.filter(function (r) { return r.summary_type === 'coverage'; });
      if (coverage.length) {
        var invTable = buildTable([
          { key: 'security_family', label: 'Security', format: fmtSnake },
          { key: 'measure', label: 'Measure', format: fmtSnake },
          { key: 'value', label: 'Value', numeric: true, format: function (v) { return v != null ? v.toLocaleString() : dash(); } },
          { key: 'as_of_date', label: 'As Of' }
        ], coverage);
        container.appendChild(buildDisclosure('View investor allotments coverage', invTable));
      }
    }

    // Primary dealer behind disclosure
    if (dealer && dealer.rows) {
      var available = dealer.rows.filter(function (r) { return r.summary_type === 'available_series'; });
      if (available.length) {
        var dlrTable = buildTable([
          { key: 'dataset_type', label: 'Dataset', format: fmtSnake },
          { key: 'value', label: 'Series', numeric: true },
          { key: 'frequency', label: 'Frequency' },
          { key: 'as_of_date', label: 'As Of' }
        ], available);
        container.appendChild(buildDisclosure('View primary dealer data', dlrTable));
      }
    }

    // SEC N-MFP behind disclosure
    if (nmfp && nmfp.rows) {
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

      container.appendChild(el('p', { class: 'card-meta', text: periods.length + ' quarterly archives tracked, ' + fullyOk + ' fully available.' }));

      var periodRows = periods.sort().slice(-8).map(function (p) {
        return {
          period: p,
          checks: periodMap[p].total,
          passed: periodMap[p].ok,
          status: periodMap[p].ok === periodMap[p].total ? 'complete' : 'partial'
        };
      });
      var nmfpTable = buildTable([
        { key: 'period', label: 'Period' },
        { key: 'checks', label: 'Checks', numeric: true },
        { key: 'passed', label: 'Passed', numeric: true },
        { key: 'status', label: 'Status', format: function (v) { return v === 'complete' ? '\u2713 Complete' : '\u26a0 Partial'; } }
      ], periodRows);
      container.appendChild(buildDisclosure('View N-MFP archive status', nmfpTable));
    }

    // TIC placeholder
    container.appendChild(el('div', { class: 'card', style: 'margin-top: 16px;' }, [
      el('div', { class: 'card-title', text: 'Treasury International Capital (TIC)' }),
      el('div', { class: 'card-value' }, [statusBadge('not_started')]),
      el('div', { class: 'card-meta', text: 'Out of scope for the current release.' })
    ]));
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
      renderHeroStrip(),
      renderEvidence(),
      renderFindings(),
      renderComposition(),
      renderMechanism(),
      renderPricing(),
      renderCausal(),
      renderProvenance(),
      renderExtensions()
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
