'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  KeralaCaptain — Secure Stream Service Worker  (sw.js)
//
//  HOW IT WORKS:
//  ─────────────
//  1. player.html registers this SW and sends the initial token via postMessage.
//  2. player.html sets <video src="/secure-stream/<messageId>"> — this NEVER
//     changes again. There is no src-swap, no .load(), no black screen.
//  3. When the browser's <video> element requests /secure-stream/<messageId>
//     (including every byte-range / seek request), this SW intercepts it,
//     silently rewrites the URL to /stream/<messageId>?token=<currentToken>,
//     and forwards the request to the real backend.
//  4. player.html fetches a fresh token every 40 s and posts it here via
//     postMessage({type:'SET_TOKEN', token:'...'}). The update is instant
//     and invisible — the <video> element never sees it.
//  5. When the backend force-drops the connection at 50 s (V4.5 lifespan
//     enforcement), the browser automatically retries with a new Range header.
//     This SW intercepts the retry, attaches the already-refreshed token, and
//     forwards it. The player resumes seamlessly — zero blink, zero buffering.
// ═══════════════════════════════════════════════════════════════════════════

// ── State ─────────────────────────────────────────────────────────────────
// currentToken is kept up-to-date by postMessage from the page.
// It is also self-fetched as a fallback on the very first intercept.
let currentToken = null;

// ── Install ────────────────────────────────────────────────────────────────
// skipWaiting() makes this SW activate immediately on first registration
// instead of waiting for all open tabs to be closed.
self.addEventListener('install', function (event) {
    self.skipWaiting();
});

// ── Activate ───────────────────────────────────────────────────────────────
// clients.claim() makes this SW take control of all open pages immediately
// (including the page that just registered it), without requiring a reload.
// This is critical: without it, the first page load after registration would
// NOT be controlled by this SW, and /secure-stream/* requests would 404.
self.addEventListener('activate', function (event) {
    event.waitUntil(self.clients.claim());
});

// ── Message: receive live token updates from the page ──────────────────────
// Called by player.html every 40 s (before the 60 s backend expiry).
// This is how the "seamless" part works — the token rotates here,
// not on the <video> element.
self.addEventListener('message', function (event) {
    if (!event.data || !event.data.type) return;

    switch (event.data.type) {
        case 'SET_TOKEN':
            // New token from the page — store it for the next intercept
            if (event.data.token) {
                currentToken = event.data.token;
            }
            break;
    }
});

// ── Fetch: intercept /secure-stream/* and proxy to /stream/*?token=... ────
// Every other fetch (API calls, fonts, ad scripts) passes through untouched.
self.addEventListener('fetch', function (event) {
    var url = new URL(event.request.url);

    // Only handle our virtual secure-stream paths
    if (url.pathname.startsWith('/secure-stream/')) {
        event.respondWith(handleSecureStreamRequest(event.request, url));
        // All other requests: do NOT call event.respondWith → default browser behaviour
    }
});

// ── Core Proxy Logic ───────────────────────────────────────────────────────
async function handleSecureStreamRequest(request, parsedUrl) {
    // Extract numeric message_id from /secure-stream/<messageId>
    var rawSegment = parsedUrl.pathname.replace('/secure-stream/', '');
    var messageId  = rawSegment.split('?')[0].split('/')[0];

    if (!messageId || !/^\d+$/.test(messageId)) {
        return new Response('Bad Request: Invalid stream ID.', { status: 400 });
    }

    // ── Fallback token fetch ───────────────────────────────────────────────
    // On the very first request (before the page has had a chance to postMessage
    // the token), fetch it ourselves so playback is never blocked.
    if (!currentToken) {
        try {
            var tokenRes = await fetch(
                '/api/get_token?video_id=' + messageId + '&_=' + Date.now(),
                { credentials: 'include' }
            );
            if (tokenRes.ok) {
                var tokenData = await tokenRes.json();
                if (tokenData && tokenData.token) {
                    currentToken = tokenData.token;
                }
            }
        } catch (tokenErr) {
            // Will proceed without token; backend will reject if required
        }
    }

    // ── Build real backend URL ────────────────────────────────────────────
    var targetUrl = '/stream/' + messageId;
    if (currentToken) {
        targetUrl += '?token=' + encodeURIComponent(currentToken);
    }

    // ── Forward all original headers (Range header is critical for seeking) ─
    // Cloning via new Headers(request.headers) preserves Range, Accept, etc.
    var forwardHeaders = new Headers(request.headers);

    // ── Proxy the request ─────────────────────────────────────────────────
    try {
        var response = await fetch(targetUrl, {
            method:      request.method,
            headers:     forwardHeaders,
            credentials: 'include',
            mode:        'same-origin',
        });

        // Return the backend response (including 206 Partial Content) as-is.
        // The browser's <video> element will process it normally.
        return response;

    } catch (networkErr) {
        return new Response('Stream proxy network error.', { status: 503 });
    }
}
