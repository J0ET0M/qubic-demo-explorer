const PREFS_KEY = 'qli-notification-prefs'

export type NotificationEventType = 'incoming' | 'outgoing' | 'large_transfer'

export interface NotificationPrefs {
  enabled: boolean
  events: NotificationEventType[]
  largeTransferThreshold: number // in QU
  pollIntervalSec: number
}

const defaultPrefs: NotificationPrefs = {
  enabled: false,
  events: ['incoming', 'outgoing', 'large_transfer'],
  largeTransferThreshold: 1_000_000_000, // 1B QU
  pollIntervalSec: 60,
}

const prefs = ref<NotificationPrefs>({ ...defaultPrefs })
const permissionState = ref<NotificationPermission>('default')
const lastCheckedTick = ref<Record<string, number>>({})
let pollTimer: ReturnType<typeof setInterval> | null = null
let initialized = false

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
}

function savePrefs() {
  if (typeof window === 'undefined') return
  localStorage.setItem(PREFS_KEY, JSON.stringify(prefs.value))
}

async function requestPermission(): Promise<boolean> {
  if (typeof window === 'undefined' || !('Notification' in window)) return false
  const result = await Notification.requestPermission()
  permissionState.value = result
  return result === 'granted'
}

function showBrowserNotification(title: string, body: string, url?: string) {
  if (typeof window === 'undefined' || !('Notification' in window)) return
  if (Notification.permission !== 'granted') return

  const notification = new Notification(title, {
    body,
    icon: '/favicon.ico',
    tag: 'qli-explorer',
  })

  if (url) {
    notification.onclick = () => {
      window.focus()
      window.location.href = url
      notification.close()
    }
  }
}

const formatAmount = (amount: number) => {
  if (amount >= 1e12) return (amount / 1e12).toFixed(1) + 'T'
  if (amount >= 1e9) return (amount / 1e9).toFixed(1) + 'B'
  if (amount >= 1e6) return (amount / 1e6).toFixed(1) + 'M'
  if (amount >= 1e3) return (amount / 1e3).toFixed(1) + 'K'
  return amount.toLocaleString()
}

const truncateAddr = (addr: string) =>
  addr.length > 16 ? addr.slice(0, 6) + '...' + addr.slice(-6) : addr

export const useNotifications = () => {
  if (!initialized) {
    initialized = true
    loadPrefs()
  }

  const { addresses } = usePortfolio()
  const { show: showToast } = useToast()
  const api = useApi()

  const updatePrefs = (update: Partial<NotificationPrefs>) => {
    prefs.value = { ...prefs.value, ...update }
    savePrefs()
  }

  const enable = async () => {
    const granted = await requestPermission()
    if (granted) {
      updatePrefs({ enabled: true })
      startPolling()
      showToast('Notifications enabled', { type: 'success' })
    } else {
      showToast('Notification permission denied', { type: 'error' })
    }
  }

  const disable = () => {
    updatePrefs({ enabled: false })
    stopPolling()
  }

  const checkAddressesForUpdates = async () => {
    if (!prefs.value.enabled || addresses.value.length === 0) return

    for (const address of addresses.value) {
      try {
        // Fetch latest transfers for this address
        const data = await api.getAddressTransfers(address, 1, 5)
        if (!data?.transfers?.length) continue

        const lastTick = lastCheckedTick.value[address] || 0
        const newTransfers = data.transfers.filter(
          (t: any) => t.tickNumber > lastTick
        )

        if (newTransfers.length === 0) continue

        // Update last checked tick
        lastCheckedTick.value[address] = Math.max(
          ...data.transfers.map((t: any) => t.tickNumber)
        )

        // Don't notify on first check (just set baseline)
        if (lastTick === 0) continue

        for (const transfer of newTransfers) {
          const isIncoming = transfer.destAddress === address
          const isOutgoing = transfer.sourceAddress === address
          const isLarge = transfer.amount >= prefs.value.largeTransferThreshold

          if (isLarge && prefs.value.events.includes('large_transfer')) {
            showBrowserNotification(
              'Large Transfer Detected',
              `${formatAmount(transfer.amount)} QU ${isIncoming ? 'received by' : 'sent from'} ${truncateAddr(address)}`,
              `/address/${address}`
            )
            showToast(
              `Large transfer: ${formatAmount(transfer.amount)} QU`,
              {
                type: 'info',
                action: { label: 'View Address', to: `/address/${address}` },
              }
            )
          } else if (isIncoming && prefs.value.events.includes('incoming')) {
            showBrowserNotification(
              'Incoming Transfer',
              `${formatAmount(transfer.amount)} QU received by ${truncateAddr(address)}`,
              `/address/${address}`
            )
          } else if (isOutgoing && prefs.value.events.includes('outgoing')) {
            showBrowserNotification(
              'Outgoing Transfer',
              `${formatAmount(transfer.amount)} QU sent from ${truncateAddr(address)}`,
              `/address/${address}`
            )
          }
        }
      } catch {
        // Silently ignore errors during polling
      }
    }
  }

  const startPolling = () => {
    if (pollTimer) return
    if (!prefs.value.enabled) return

    // Initial check after a short delay
    setTimeout(() => checkAddressesForUpdates(), 5000)

    pollTimer = setInterval(
      () => checkAddressesForUpdates(),
      prefs.value.pollIntervalSec * 1000
    )
  }

  const stopPolling = () => {
    if (pollTimer) {
      clearInterval(pollTimer)
      pollTimer = null
    }
  }

  // Auto-start polling if enabled
  if (typeof window !== 'undefined' && prefs.value.enabled) {
    onMounted(() => startPolling())
    onUnmounted(() => stopPolling())
  }

  return {
    prefs: readonly(prefs),
    permissionState: readonly(permissionState),
    enable,
    disable,
    updatePrefs,
    startPolling,
    stopPolling,
  }
}
