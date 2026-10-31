/* Spark PlanReader service worker: network-first for app files, per-file cache, offline fallback.
   Bump CACHE on each release; old caches are removed on activate. */
const CACHE = 'planreader-v1';
const ASSETS = [
  './', './index.html', './styles.css', './manifest.webmanifest', './icons/icon.svg', './icons/icon-192.png',
  './icons/icon-512.png', './js/textutil.js', './js/parser.js', './js/eventlog.js', './js/glossary.js', './js/diagnose.js',
  './js/diff.js', './js/layout.js', './js/report.js', './js/samples.js', './js/share.js', './js/store.js', './js/modal.js',
  './js/ui.js', './js/views.js', './js/app.js'
];
// Samples are cached too, so the sample gallery works offline.
const SAMPLES = [
  './samples/aqe-final-broadcast.txt', './samples/aqe-skew-final.txt', './samples/broadcast-fix.txt', './samples/cached.txt',
  './samples/cartesian.txt', './samples/cost-stats.txt', './samples/count-distinct.txt', './samples/csv-cast.txt',
  './samples/ctas-command.txt', './samples/eventlog.json', './samples/explode-collect.txt', './samples/formatted-aqe-off.txt',
  './samples/formatted-final.txt', './samples/nested-loop-join.txt', './samples/pandas-udf.txt', './samples/partition-udf.txt',
  './samples/photon-illustrative.txt', './samples/python-udf.txt', './samples/repartition-one.txt',
  './samples/reused-exchange.txt', './samples/smj-aqe-off.txt', './samples/sort-coalesce.txt',
  './samples/spark35-aqe-skew-final.txt', './samples/spark35-formatted.txt', './samples/sql-explain.txt',
  './samples/star-join-extended.txt', './samples/star-join-formatted.txt', './samples/subquery-dpp.txt',
  './samples/window-no-partition.txt', './samples/fixes/approx.txt', './samples/fixes/native.txt',
  './samples/fixes/no_coalesce.txt', './samples/fixes/partition_filter.txt', './samples/fixes/partitioned.txt',
  './samples/fixes/salted.txt', './samples/fixes/typed.txt'
];
self.addEventListener('install', (e) => {
  self.skipWaiting();
  // Cache each file on its own so one failed request can't break install.
  e.waitUntil(caches.open(CACHE).then((c) => Promise.all(ASSETS.concat(SAMPLES).map((a) => c.add(new Request(a, { cache: 'reload' })).catch(() => {})))));
});
self.addEventListener('activate', (e) => e.waitUntil(
  caches.keys().then((keys) => Promise.all(keys.filter((k) => k.startsWith('planreader-') && k !== CACHE).map((k) => caches.delete(k)))).then(() => self.clients.claim())
));
self.addEventListener('fetch', (e) => {
  const req = e.request;
  if (req.method !== 'GET' || new URL(req.url).origin !== location.origin) return;
  // no-cache: always revalidate with the server, so a new release shows up on the next visit.
  e.respondWith(fetch(req, { cache: 'no-cache' }).then((res) => {
    if (res && res.ok && res.type === 'basic') { const copy = res.clone(); caches.open(CACHE).then((c) => c.put(req, copy)); }
    return res;
  }).catch(() => caches.match(req, { ignoreSearch: true })
    .then((r) => r || (req.mode === 'navigate' ? caches.match('./index.html').then((i) => i || caches.match('./')) : undefined))
    .then((r) => r || Response.error())));
});
