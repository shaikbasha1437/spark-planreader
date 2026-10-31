/* Spark PlanReader graph layout (pure, no DOM): a tidy top-down tree. The root (query result) is at the top,
   scans at the bottom; data flows upward. Plumbing operators can be hidden; their children move up. */
(function (root, factory) {
  const m = factory();
  if (typeof module === 'object' && module.exports) module.exports = m;
  else (root.SP = root.SP || {}).layout = m;
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';
  const PLUMBING = /^(ColumnarToRow|RowToColumnar|InputAdapter|WholeStageCodegen|PhotonRowToColumnar|PhotonColumnarToRow|PhotonAdapter|PhotonResultStage|PhotonShuffleMapStage)$/;
  const isPlumbing = (n) => PLUMBING.test(n.name);
  function visibleTree(rootNode, hide) {
    const build = (n) => {
      const kids = [];
      n.children.forEach((c) => { const v = build(c); if (hide && hide(c)) kids.push(...v.children); else kids.push(v); });
      return { node: n, children: kids };
    };
    return build(rootNode);
  }
  function layout(rootNode, opts) {
    opts = opts || {};
    const W = opts.nodeWidth || 196, H = opts.nodeHeight || 58, GX = opts.gapX || 22, GY = opts.gapY || 46;
    if (!rootNode) return { nodes: [], edges: [], width: 0, height: 0, stages: [] };
    const tree = visibleTree(rootNode, opts.hidePlumbing ? isPlumbing : null);
    const out = []; const edges = []; let nextX = 0; let maxDepth = 0;
    const place = (t, depth) => {
      maxDepth = Math.max(maxDepth, depth);
      t.children.forEach((c) => place(c, depth + 1));
      let x;
      if (!t.children.length) { x = nextX; nextX += W + GX; }
      else { const xs = t.children.map((c) => c.x); x = (xs[0] + xs[xs.length - 1]) / 2; }
      // Keep a parent from overlapping something already placed at its depth.
      t.x = x; t.depth = depth;
    };
    place(tree, 0);
    // Resolve overlaps per depth by shifting subtrees right (simple contour pass).
    const byDepth = {};
    const collect = (t) => { (byDepth[t.depth] = byDepth[t.depth] || []).push(t); t.children.forEach(collect); };
    collect(tree);
    const shift = (t, dx) => { t.x += dx; t.children.forEach((c) => shift(c, dx)); };
    for (let pass = 0; pass < 3; pass++) {
      Object.keys(byDepth).forEach((d) => { const row = byDepth[d].slice().sort((a, b) => a.x - b.x); for (let i = 1; i < row.length; i++) { const need = row[i - 1].x + W + GX - row[i].x; if (need > 0.5) shift(row[i], need); } });
      const recenter = (t) => { t.children.forEach(recenter); if (t.children.length) { const xs = t.children.map((c) => c.x).sort((a, b) => a - b); const want = (xs[0] + xs[xs.length - 1]) / 2; if (want > t.x) t.x = want; } };
      recenter(tree);
    }
    const emit = (t, parent) => {
      const n = t.node;
      out.push({ id: n.id, x: t.x, y: t.depth * (H + GY), w: W, h: H, node: n });
      if (parent) edges.push({ from: n.id, to: parent.node.id, kind: 'tree', boundary: n.stage !== parent.node.stage });
      t.children.forEach((c) => emit(c, t));
    };
    emit(tree, null);
    const ids = new Set(out.map((o) => o.id));
    out.forEach((o) => { if (o.node.reuseOf && ids.has(o.node.reuseOf)) edges.push({ from: o.node.reuseOf, to: o.id, kind: 'reuse' }); });
    const minX = Math.min(...out.map((o) => o.x)); out.forEach((o) => { o.x -= minX; });
    const width = Math.max(...out.map((o) => o.x + W)), height = (maxDepth + 1) * (H + GY) - GY;
    // Stage boxes: bounding box per stage id.
    const st = {};
    out.forEach((o) => { const s = o.node.stage; if (s == null) return; const b = st[s] || (st[s] = { stage: s, x0: Infinity, y0: Infinity, x1: -Infinity, y1: -Infinity, count: 0 }); b.x0 = Math.min(b.x0, o.x); b.y0 = Math.min(b.y0, o.y); b.x1 = Math.max(b.x1, o.x + W); b.y1 = Math.max(b.y1, o.y + H); b.count++; });
    return { nodes: out, edges, width, height, stages: Object.values(st), nodeWidth: W, nodeHeight: H };
  }
  return { layout, isPlumbing, visibleTree };
});
