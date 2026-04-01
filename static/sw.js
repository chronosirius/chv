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

    // On logout: clear the static cache so stale user-specific pages are not
    // served to the next user, then pass the request through to the server.
    if (url.pathname === '/logout') {
        event.respondWith(
            caches.delete(STATIC_CACHE)
                .catch(() => {})  // ignore cache deletion errors
                .then(() => fetch(request))
        );
        return;
    }

    // Never cache API calls, uploads, login, or non-GET requests
    if (
        request.method !== 'GET' ||
        url.pathname.startsWith('/api/') ||
        url.pathname.startsWith('/upload/') ||
        url.pathname === '/login'
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

    // Cache-first for app pages – the server already caches its responses, so
    // serving from the client cache is safe and avoids redundant round-trips.
    // When the page is not in cache yet, fetch it from the network and store it.
    event.respondWith(
        caches.open(STATIC_CACHE).then((cache) =>
            cache.match(request).then((cached) => {
                if (cached) return cached;
                return fetch(request).then((response) => {
                    if (response.ok) cache.put(request, response.clone());
                    return response;
                }).catch(() =>
                    new Response('Offline – page not available', {
                        status: 503,
                        headers: { 'Content-Type': 'text/plain' },
                    })
                );
            })
        )
    );
});
