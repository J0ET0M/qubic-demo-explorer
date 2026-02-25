// Service Worker for QLI Explorer Push Notifications
// This runs in the background even when the browser tab is closed.

self.addEventListener('install', (event) => {
  self.skipWaiting()
})

self.addEventListener('activate', (event) => {
  event.waitUntil(self.clients.claim())
})

// Handle incoming push events
self.addEventListener('push', (event) => {
  if (!event.data) return

  let data
  try {
    data = event.data.json()
  } catch {
    data = { title: 'QLI Explorer', body: event.data.text() }
  }

  const options = {
    body: data.body || '',
    icon: '/favicon.svg',
    badge: '/favicon.svg',
    tag: 'qli-' + (data.timestamp || Date.now()),
    data: { url: data.url || '/' },
    requireInteraction: false,
    silent: false,
  }

  event.waitUntil(
    self.registration.showNotification(data.title || 'QLI Explorer', options)
  )
})

// Handle notification click — open the relevant page
self.addEventListener('notificationclick', (event) => {
  event.notification.close()

  const url = event.notification.data?.url || '/'

  event.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clients) => {
      // If a tab is already open, focus it and navigate
      for (const client of clients) {
        if (client.url.includes(self.location.origin)) {
          client.focus()
          client.postMessage({ type: 'NOTIFICATION_CLICK', url })
          return
        }
      }
      // Otherwise open a new tab
      return self.clients.openWindow(url)
    })
  )
})
