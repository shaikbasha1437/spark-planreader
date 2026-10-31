/* Spark PlanReader text helpers: bracket-aware splitting, expression-id stripping and size parsing.
   Pure functions, shared by the browser and the Node tests. */
(function (root, factory) {
  const m = factory();
  if (typeof module === 'object' && module.exports) module.exports = m;
  else (root.SP = root.SP || {}).text = m;
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';
  const OPEN = { '(': ')', '[': ']', '{': '}', '<': '>' };
  const CLOSE = { ')': 1, ']': 1, '}': 1, '>': 1 };
  // Split `s` on `sep` (a string) only at bracket depth 0. Angle brackets count only when asked
  // (struct<...> in ReadSchema), since `<` also appears as a comparison operator.
  function splitTop(s, sep, opts) {
    const angles = !!(opts && opts.angles);
    const out = []; let depth = 0, start = 0, quote = null;
    for (let i = 0; i < s.length; i++) {
      const c = s[i];
      if (quote) { if (c === quote && s[i - 1] !== '\\') quote = null; continue; }
      if (c === '"' || c === '`') { quote = c; continue; }
      if (c === '(' || c === '[' || c === '{' || (angles && c === '<')) depth++;
      else if (c === ')' || c === ']' || c === '}' || (angles && c === '>' && s[i - 1] !== '-')) depth = Math.max(0, depth - 1);
      else if (depth === 0 && s.startsWith(sep, i)) { out.push(s.slice(start, i)); start = i + sep.length; i += sep.length - 1; }
    }
    out.push(s.slice(start));
    return out.map((x) => x.trim()).filter((x) => x.length);
  }
  // Index of the bracket that closes the one at `i`, or -1.
  function matchClose(s, i) {
    const open = s[i], close = OPEN[open]; if (!close) return -1;
    let depth = 0, quote = null;
    for (let j = i; j < s.length; j++) {
      const c = s[j];
      if (quote) { if (c === quote) quote = null; continue; }
      if (c === '"' || c === '`') { quote = c; continue; }
      if (c === open) depth++; else if (c === close) { depth--; if (!depth) return j; }
    }
    return -1;
  }
  // "[a#1, b#2L]" -> ["a#1", "b#2L"]; tolerates a missing closing bracket (truncated text).
  function parseList(s) {
    if (s == null) return null;
    s = String(s).trim();
    if (!s.startsWith('[')) return s ? [s] : [];
    const end = matchClose(s, 0);
    const inner = end > 0 ? s.slice(1, end) : s.slice(1);
    return splitTop(inner, ',');
  }
  // Leading bracketed lists of an argument string: "[a], [b], Inner" -> { lists: [[a],[b]], rest: "Inner" }
  function leadingLists(s) {
    const lists = []; let rest = s.trim();
    while (rest.startsWith('[')) {
      const end = matchClose(rest, 0); if (end < 0) { lists.push(parseList(rest)); rest = ''; break; }
      lists.push(parseList(rest.slice(0, end + 1)));
      rest = rest.slice(end + 1).replace(/^\s*,\s*/, '').trim();
    }
    return { lists, rest };
  }
  // "Key: value, Key2: [x, y], ..." at depth 0 -> ordered pairs. Used by FileScan / BatchScan lines.
  function keyValues(s) {
    const out = []; const parts = splitTop(s, ', ');
    let cur = null;
    parts.forEach((p) => {
      const m = /^([A-Z][A-Za-z ]{1,40}?):\s?(.*)$/s.exec(p);
      if (m) { cur = [m[1].trim(), m[2].trim()]; out.push(cur); } else if (cur) cur[1] += ', ' + p;
    });
    return out;
  }
  // Remove expression ids, plan ids and other run-specific numbers so two plans can be compared.
  function stripIds(s) {
    return String(s || '')
      .replace(/#\d+L?/g, '')
      .replace(/\[plan_id=\d+\]/g, '[plan_id]')
      .replace(/\[id=#?\d+\]/g, '[id]')
      .replace(/pythonUDF\d+/g, 'pythonUDF')
      .replace(/\s+/g, ' ').trim();
  }
  const UNITS = { B: 1, KIB: 1024, MIB: 1024 ** 2, GIB: 1024 ** 3, TIB: 1024 ** 4, PIB: 1024 ** 5, KB: 1e3, MB: 1e6, GB: 1e9, TB: 1e12, EIB: 1024 ** 6 };
  // "82.3 MiB" / "1063.1 KiB" / "8.0 EiB" -> bytes
  function parseSize(s) {
    const m = /([\d.]+(?:E[+-]?\d+)?)\s*([KMGTPE]i?B|B)\b/i.exec(String(s || ''));
    if (!m) return null; const u = UNITS[m[2].toUpperCase()]; return u ? Number(m[1]) * u : null;
  }
  function fmtBytes(n) {
    if (n == null || !isFinite(n)) return '–';
    const u = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']; let i = 0; let v = Number(n);
    while (Math.abs(v) >= 1024 && i < u.length - 1) { v /= 1024; i++; }
    return (i ? v.toFixed(v < 10 ? 2 : v < 100 ? 1 : 0) : String(Math.round(v))) + ' ' + u[i];
  }
  function fmtNum(n) {
    if (n == null || !isFinite(n)) return '–';
    const a = Math.abs(n);
    if (a >= 1e9) return (n / 1e9).toFixed(a >= 1e10 ? 0 : 1) + 'B';
    if (a >= 1e6) return (n / 1e6).toFixed(a >= 1e7 ? 0 : 1) + 'M';
    if (a >= 1e4) return (n / 1e3).toFixed(a >= 1e5 ? 0 : 1) + 'K';
    return String(Math.round(n * 100) / 100);
  }
  function fmtMs(ms) {
    if (ms == null || !isFinite(ms)) return '–';
    if (ms < 1000) return Math.round(ms) + ' ms';
    if (ms < 60000) return (ms / 1000).toFixed(ms < 10000 ? 2 : 1) + ' s';
    const m = Math.floor(ms / 60000), s = Math.round((ms % 60000) / 1000); return m + ' min ' + s + ' s';
  }
  // Column names from "struct<a:int,b:string,c:struct<x:int>>" (top level only). Detects "..." truncation.
  function schemaCols(s) {
    if (!s) return { cols: [], truncated: false };
    const m = /struct<(.*)$/s.exec(s.trim()); if (!m) return { cols: [], truncated: /\.\.\./.test(s) };
    let inner = m[1]; const truncated = /\.\.\.\s*$/.test(inner) || /\.\.\.$/.test(s.trim());
    if (inner.endsWith('>')) inner = inner.slice(0, -1);
    const cols = splitTop(inner.replace(/\.\.\.$/, ''), ',', { angles: true }).map((c) => c.split(':')[0].replace(/`/g, '').trim()).filter(Boolean);
    if (truncated && cols.length) cols.pop(); // last one is cut mid-name
    return { cols, truncated };
  }
  // "customer_id#31L" -> "customer_id"
  const attrName = (a) => String(a).replace(/#\d+L?$/, '').replace(/^.*\./, '').replace(/`/g, '').trim();
  const esc = (s) => String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  return { splitTop, matchClose, parseList, leadingLists, keyValues, stripIds, parseSize, fmtBytes, fmtNum, fmtMs, schemaCols, attrName, esc };
});
