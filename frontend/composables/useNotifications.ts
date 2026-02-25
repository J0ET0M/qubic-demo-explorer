const PREFS_KEY = 'qli-notification-prefs'

export type NotificationEventType = 'incoming' | 'outgoing' | 'large_transfer' | 'balance_threshold'

export interface NotificationPrefs {
  enabled: boolean
  events: NotificationEventType[]
  largeTransferThreshold: number // in QU
  balanceMinThreshold: number // in QU, 0 = disabled
  balanceMaxThreshold: number // in QU, 0 = disabled
}

const defaultPrefs: NotificationPrefs = {
  enabled: false,
  events: ['incoming', 'outgoing', 'large_transfer'],
  largeTransferThreshold: 1_000_000_000, // 1B QU
  balanceMinThreshold: 0,
  balanceMaxThreshold: 0,
}

const prefs = ref<NotificationPrefs>({ ...defaultPrefs })
const permissionState = ref<NotificationPermission>('default')
const pushSupported = ref(false)
const subscriptionActive = ref(false)
let initialized = false
let swRegistration: ServiceWorkerRegistration | null = null

function loadPrefs() {
  if (typeof window === 'undefined') return
  try {
    const stored = localStorage.getItem(PREFS_KEY)
    if (stored) {
      prefs.value = { ...defaultPrefs, ...JSON.parse(stored) }
    }
  } catch {
    prefs.value = { ...defaultPrefs }
  }
  if ('Notification' in window) {
    permissionState.value = Notification.permission
  }
  pushSupported.value = 'serviceWorker' in navigator && 'PushManager' in window
}

function savePrefs() {
  if (typeof window === 'undefined') return
  localStorage.setItem(PREFS_KEY, JSON.stringify(prefs.value))
}

async function registerServiceWorker(): Promise<ServiceWorkerRegistration | null> {
  if (!('serviceWorker' in navigator)) return null
  try {
    const reg = await navigator.serviceWorker.register('/sw.js')
    swRegistration = reg
    return reg
  } catch (err) {
    console.error('SW registration failed:', err)
    return null
  }
}

async function getVapidKey(): Promise<string | null> {
  try {
    const config = useRuntimeConfig()
    const baseUrl = config.public.apiBase || '/api'
    const res = await fetch(`${baseUrl}/notifications/vapid-key`)
    if (!res.ok) return null
    const data = await res.json()
    return data.publicKey || null
  } catch {
    return null
  }
}

function urlBase64ToUint8Array(base64String: string): Uint8Array {
  const padding = '='.repeat((4 - (base64String.length % 4)) % 4)
  const base64 = (base64String + padding).replace(/-/g, '+').replace(/_/g, '/')
  const raw = atob(base64)
  const arr = new Uint8Array(raw.length)
  for (let i = 0; i < raw.length; i++) {
    arr[i] = raw.charCodeAt(i)
  }
  return arr
}

async function subscribeToPush(
  reg: ServiceWorkerRegistration,
  vapidKey: string
): Promise<PushSubscription | null> {
  try {
    const sub = await reg.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: urlBase64ToUint8Array(vapidKey),
    })
    return sub
  } catch (err) {
    console.error('Push subscription failed:', err)
    return null
  }
}

async function sendSubscriptionToServer(
  sub: PushSubscription,
  addresses: string[],
  notifPrefs: NotificationPrefs
) {
  try {
    const config = useRuntimeConfig()
    const baseUrl = config.public.apiBase || '/api'
    const subJson = sub.toJSON()
    await fetch(`${baseUrl}/notifications/subscribe`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        subscription: {
          endpoint: sub.endpoint,
          keys: {
            p256dh: subJson.keys?.p256dh || '',
            auth: subJson.keys?.auth || '',
          },
        },
        addresses,
        events: notifPrefs.events,
        largeTransferThreshold: notifPrefs.largeTransferThreshold,
        balanceMinThreshold: notifPrefs.balanceMinThreshold,
        balanceMaxThreshold: notifPrefs.balanceMaxThreshold,
      }),
    })
  } catch (err) {
    console.error('Failed to send subscription to server:', err)
  }
}

async function removeSubscriptionFromServer(endpoint: string) {
  try {
    const config = useRuntimeConfig()
    const baseUrl = config.public.apiBase || '/api'
    await fetch(`${baseUrl}/notifications/unsubscribe`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ endpoint }),
    })
  } catch {
    // Ignore errors during unsubscribe
  }
}

export const useNotifications = () => {
  if (!initialized) {
    initialized = true
    loadPrefs()
  }

  const { addresses } = usePortfolio()
  const { show: showToast } = useToast()
  const router = useRouter()

  const updatePrefs = async (update: Partial<NotificationPrefs>) => {
    prefs.value = { ...prefs.value, ...update }
    savePrefs()

    // Re-sync subscription with server when prefs change
    if (prefs.value.enabled && swRegistration) {
      const existingSub = await swRegistration.pushManager.getSubscription()
      if (existingSub && addresses.value.length > 0) {
        await sendSubscriptionToServer(
          existingSub,
          [...addresses.value],
          prefs.value
        )
      }
    }
  }

  const enable = async () => {
    if (!pushSupported.value) {
      showToast('Push notifications not supported in this browser', { type: 'error' })
      return
    }

    // Request notification permission
    const permission = await Notification.requestPermission()
    permissionState.value = permission
    if (permission !== 'granted') {
      showToast('Notification permission denied', { type: 'error' })
      return
    }

    // Register service worker
    const reg = await registerServiceWorker()
    if (!reg) {
      showToast('Failed to register service worker', { type: 'error' })
      return
    }

    // Get VAPID key from server
    const vapidKey = await getVapidKey()
    if (!vapidKey) {
      showToast('Failed to get push configuration', { type: 'error' })
      return
    }

    // Subscribe to push
    const sub = await subscribeToPush(reg, vapidKey)
    if (!sub) {
      showToast('Failed to subscribe to push notifications', { type: 'error' })
      return
    }

    // Send subscription to server with current addresses
    if (addresses.value.length > 0) {
      await sendSubscriptionToServer(
        sub,
        [...addresses.value],
        prefs.value
      )
    }

    subscriptionActive.value = true
    prefs.value.enabled = true
    savePrefs()
    showToast('Push notifications enabled', { type: 'success' })
  }

  const disable = async () => {
    if (swRegistration) {
      const sub = await swRegistration.pushManager.getSubscription()
      if (sub) {
        await removeSubscriptionFromServer(sub.endpoint)
        await sub.unsubscribe()
      }
    }

    subscriptionActive.value = false
    prefs.value.enabled = false
    savePrefs()
    showToast('Push notifications disabled', { type: 'info' })
  }

  // Sync subscription when addresses change
  const syncSubscription = async () => {
    if (!prefs.value.enabled || !swRegistration) return
    const sub = await swRegistration.pushManager.getSubscription()
    if (!sub || addresses.value.length === 0) return

    await sendSubscriptionToServer(
      sub,
      [...addresses.value],
      prefs.value
    )
  }

  // Listen for notification clicks from SW
  if (typeof window !== 'undefined') {
    navigator.serviceWorker?.addEventListener('message', (event) => {
      if (event.data?.type === 'NOTIFICATION_CLICK' && event.data.url) {
        router.push(event.data.url)
      }
    })
  }

  // Initialize: check existing subscription state
  if (typeof window !== 'undefined' && prefs.value.enabled && pushSupported.value) {
    registerServiceWorker().then(async (reg) => {
      if (!reg) return
      const sub = await reg.pushManager.getSubscription()
      subscriptionActive.value = !!sub
    })
  }

  // Watch for address changes and re-sync
  watch(addresses, () => syncSubscription(), { deep: true })

  return {
    prefs: readonly(prefs),
    permissionState: readonly(permissionState),
    pushSupported: readonly(pushSupported),
    subscriptionActive: readonly(subscriptionActive),
    enable,
    disable,
    updatePrefs,
    syncSubscription,
  }
}
