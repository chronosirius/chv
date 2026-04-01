const CACHE_VERSION = 'v1';
const STATIC_CACHE = `chv-static-${CACHE_VERSION}`;
const CDN_CACHE = `chv-cdn-${CACHE_VERSION}`;

// CDN assets to pre-cache on install
const CDN_ASSETS = [
    'https://cdn.tailwindcss.com',
    'https://cdn.jsdelivr.net/npm/chart.js',
];

// App pages to pre-cache on install
const APP_PAGES = [
    '/',
    '/help',
];

self.addEventListener('install', (event) => {
    event.waitUntil(
        Promise.all([
            caches.open(CDN_CACHE).then((cache) => cache.addAll(CDN_ASSETS)),
            caches.open(STATIC_CACHE).then((cache) => cache.addAll(APP_PAGES)),
        ]).then(() => self.skipWaiting())
    );
});

self.addEventListener('activate', (event) => {
    event.waitUntil(
        caches.keys().then((keys) =>
            Promise.all(
                keys
                    .filter((key) => key !== STATIC_CACHE && key !== CDN_CACHE)
                    .map((key) => caches.delete(key))
            )
        ).then(() => self.clients.claim())
    );
});

self.addEventListener('fetch', (event) => {
    const { request } = event;
    const url = new URL(request.url);

    // Never cache API calls, uploads, or non-GET requests
    if (
        request.method !== 'GET' ||
        url.pathname.startsWith('/api/') ||
        url.pathname.startsWith('/upload/') ||
        url.pathname === '/login' ||
        url.pathname === '/logout'
    ) {
        event.respondWith(fetch(request));
        return;
    }

    // Cache-first for CDN assets (they are versioned/immutable by CDN)
    if (url.hostname !== self.location.hostname) {
        event.respondWith(
            caches.open(CDN_CACHE).then((cache) =>
                cache.match(request).then((cached) => {
                    if (cached) return cached;
                    return fetch(request).then((response) => {
                        if (response.ok) cache.put(request, response.clone());
                        return response;
                    });
                })
            )
        );
        return;
    }

    // Network-first for app pages, fall back to cache for offline support
    event.respondWith(
        caches.open(STATIC_CACHE).then((cache) =>
            fetch(request)
                .then((response) => {
                    if (response.ok) cache.put(request, response.clone());
                    return response;
                })
                .catch(() =>
                    cache.match(request).then(
                        (cached) =>
                            cached ||
                            new Response('Offline – page not available', {
                                status: 503,
                                headers: { 'Content-Type': 'text/plain' },
                            })
                    )
                )
        )
    );
});
