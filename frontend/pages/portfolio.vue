<script setup lang="ts">
import { Star, Plus, Trash2, RefreshCw, Bell, BellOff } from 'lucide-vue-next'

useHead({ title: 'Portfolio - QLI Explorer' })

const api = useApi()
const { addresses, addAddress, removeAddress } = usePortfolio()
const { getLabel, fetchLabels } = useAddressLabels()
const { show: showToast } = useToast()
const {
  prefs: notifPrefs,
  permissionState,
  pushSupported,
  subscriptionActive,
  enable: enableNotifications,
  disable: disableNotifications,
  updatePrefs,
} = useNotifications()

const newAddress = ref('')
const loading = ref(false)
const portfolioData = ref<any[]>([])
const showNotifSettings = ref(false)

const fetchPortfolio = async () => {
  if (addresses.value.length === 0) {
    portfolioData.value = []
    return
  }
  loading.value = true
  try {
    const data = await api.getAddressesBatch([...addresses.value])
    portfolioData.value = data
    await fetchLabels([...addresses.value])
  } catch (e) {
    console.error('Failed to fetch portfolio:', e)
  } finally {
    loading.value = false
  }
}

const handleAdd = () => {
  const addr = newAddress.value.trim()
  if (addr && addr.length >= 50) {
    addAddress(addr)
    newAddress.value = ''
    showToast('Address added to portfolio', {
      type: 'success',
    })
    fetchPortfolio()
  }
}

const handleRemove = (addr: string) => {
  removeAddress(addr)
  portfolioData.value = portfolioData.value.filter(d => d.address !== addr)
}

const formatAmount = (amount: number) => {
  if (amount >= 1_000_000_000_000) return (amount / 1_000_000_000_000).toFixed(2) + 'T'
  if (amount >= 1_000_000_000) return (amount / 1_000_000_000).toFixed(2) + 'B'
  if (amount >= 1_000_000) return (amount / 1_000_000).toFixed(2) + 'M'
  if (amount >= 1_000) return (amount / 1_000).toFixed(2) + 'K'
  return amount.toLocaleString()
}

const totalBalance = computed(() =>
  portfolioData.value.reduce((sum, d) => sum + (d.balance || 0), 0)
)

const totalTxCount = computed(() =>
  portfolioData.value.reduce((sum, d) => sum + (d.txCount || 0), 0)
)

const truncateAddress = (addr: string) =>
  addr.length > 16 ? addr.slice(0, 8) + '...' + addr.slice(-8) : addr

const thresholdOptions = [
  { label: '1M QU', value: 1_000_000 },
  { label: '100M QU', value: 100_000_000 },
  { label: '1B QU', value: 1_000_000_000 },
  { label: '10B QU', value: 10_000_000_000 },
  { label: '100B QU', value: 100_000_000_000 },
]

onMounted(() => fetchPortfolio())
</script>

<template>
  <div class="space-y-6">
    <div class="flex items-center justify-between">
      <h1 class="page-title flex items-center gap-2">
        <Star class="h-5 w-5 text-accent" />
        Portfolio
      </h1>
      <button
        @click="showNotifSettings = !showNotifSettings"
        class="btn btn-ghost flex items-center gap-1.5 text-sm"
        :class="{ 'text-accent': notifPrefs.enabled }"
      >
        <Bell v-if="notifPrefs.enabled" class="h-4 w-4" />
        <BellOff v-else class="h-4 w-4" />
        Notifications
      </button>
    </div>

    <!-- Notification Settings -->
    <div v-if="showNotifSettings" class="card space-y-4">
      <h2 class="text-sm font-medium">Push Notifications</h2>
      <p class="text-xs text-foreground-muted">
        Receive browser notifications for portfolio address activity, even when this tab is closed.
      </p>

      <div v-if="!pushSupported" class="text-sm text-foreground-muted">
        Push notifications are not supported in this browser.
      </div>

      <div v-else-if="permissionState === 'denied'" class="text-sm text-destructive">
        Browser notifications are blocked. Please enable them in your browser settings.
      </div>

      <template v-else>
        <div class="flex items-center justify-between">
          <div>
            <span class="text-sm">Enable push notifications</span>
            <span v-if="subscriptionActive" class="ml-2 text-xs text-success">Active</span>
          </div>
          <button
            v-if="!notifPrefs.enabled"
            @click="enableNotifications"
            class="btn btn-primary btn-sm"
          >
            Enable
          </button>
          <button
            v-else
            @click="disableNotifications"
            class="btn btn-ghost btn-sm text-destructive"
          >
            Disable
          </button>
        </div>

        <template v-if="notifPrefs.enabled">
          <!-- Event types -->
          <div class="space-y-2">
            <span class="text-xs text-foreground-muted uppercase">Notify me about</span>
            <label class="flex items-center gap-2 text-sm cursor-pointer">
              <input
                type="checkbox"
                :checked="notifPrefs.events.includes('incoming')"
                @change="updatePrefs({
                  events: ($event.target as HTMLInputElement).checked
                    ? [...notifPrefs.events, 'incoming']
                    : notifPrefs.events.filter(e => e !== 'incoming')
                })"
                class="accent-accent"
              />
              Incoming transfers
            </label>
            <label class="flex items-center gap-2 text-sm cursor-pointer">
              <input
                type="checkbox"
                :checked="notifPrefs.events.includes('outgoing')"
                @change="updatePrefs({
                  events: ($event.target as HTMLInputElement).checked
                    ? [...notifPrefs.events, 'outgoing']
                    : notifPrefs.events.filter(e => e !== 'outgoing')
                })"
                class="accent-accent"
              />
              Outgoing transfers
            </label>
            <label class="flex items-center gap-2 text-sm cursor-pointer">
              <input
                type="checkbox"
                :checked="notifPrefs.events.includes('large_transfer')"
                @change="updatePrefs({
                  events: ($event.target as HTMLInputElement).checked
                    ? [...notifPrefs.events, 'large_transfer']
                    : notifPrefs.events.filter(e => e !== 'large_transfer')
                })"
                class="accent-accent"
              />
              Large transfers
            </label>
          </div>

          <!-- Threshold -->
          <div class="space-y-1">
            <span class="text-xs text-foreground-muted uppercase">Large transfer threshold</span>
            <div class="flex gap-1.5 flex-wrap">
              <button
                v-for="opt in thresholdOptions"
                :key="opt.value"
                @click="updatePrefs({ largeTransferThreshold: opt.value })"
                class="btn btn-sm"
                :class="notifPrefs.largeTransferThreshold === opt.value ? 'btn-primary' : 'btn-ghost'"
              >
                {{ opt.label }}
              </button>
            </div>
          </div>
        </template>
      </template>
    </div>

    <!-- Add Address -->
    <div class="card">
      <div class="flex gap-2">
        <input
          v-model="newAddress"
          type="text"
          placeholder="Enter Qubic address to track..."
          class="input flex-1"
          @keyup.enter="handleAdd"
        />
        <button
          @click="handleAdd"
          :disabled="!newAddress.trim() || newAddress.trim().length < 50"
          class="btn btn-primary flex items-center gap-1.5"
        >
          <Plus class="h-4 w-4" />
          Add
        </button>
        <button
          v-if="addresses.length > 0"
          @click="fetchPortfolio"
          class="btn btn-ghost flex items-center gap-1.5"
          :disabled="loading"
        >
          <RefreshCw class="h-4 w-4" :class="loading ? 'animate-spin' : ''" />
        </button>
      </div>
    </div>

    <!-- Summary -->
    <div v-if="portfolioData.length > 0" class="grid grid-cols-1 md:grid-cols-3 gap-4">
      <div class="card-elevated text-center">
        <div class="text-2xl font-bold text-accent">{{ addresses.length }}</div>
        <div class="text-xs text-foreground-muted uppercase mt-1">Tracked Addresses</div>
      </div>
      <div class="card-elevated text-center">
        <div class="text-2xl font-bold text-success">{{ formatAmount(totalBalance) }} QU</div>
        <div class="text-xs text-foreground-muted uppercase mt-1">Combined Balance</div>
      </div>
      <div class="card-elevated text-center">
        <div class="text-2xl font-bold text-accent">{{ totalTxCount.toLocaleString() }}</div>
        <div class="text-xs text-foreground-muted uppercase mt-1">Total Transactions</div>
      </div>
    </div>

    <!-- Portfolio Table -->
    <div v-if="loading" class="card">
      <div class="loading py-12">Loading portfolio...</div>
    </div>

    <div v-else-if="portfolioData.length > 0" class="card">
      <div class="table-wrapper">
        <table>
          <thead>
            <tr>
              <th>Address</th>
              <th class="text-right">Balance</th>
              <th class="text-right">Incoming</th>
              <th class="text-right">Outgoing</th>
              <th class="text-right">Txs</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="data in portfolioData" :key="data.address">
              <td>
                <NuxtLink :to="`/address/${data.address}`" class="font-mono text-xs text-accent">
                  {{ getLabel(data.address)?.label || truncateAddress(data.address) }}
                </NuxtLink>
              </td>
              <td class="text-right font-semibold text-accent whitespace-nowrap">
                {{ formatAmount(data.balance) }} QU
              </td>
              <td class="text-right text-success whitespace-nowrap">
                {{ formatAmount(data.incomingAmount) }}
              </td>
              <td class="text-right text-destructive whitespace-nowrap">
                {{ formatAmount(data.outgoingAmount) }}
              </td>
              <td class="text-right">{{ data.txCount }}</td>
              <td class="text-right">
                <button
                  @click="handleRemove(data.address)"
                  class="btn btn-ghost btn-sm text-destructive"
                  title="Remove from portfolio"
                >
                  <Trash2 class="h-3.5 w-3.5" />
                </button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <div v-else-if="addresses.length === 0" class="card">
      <div class="text-center py-12 text-foreground-muted text-sm">
        No addresses tracked yet. Add a Qubic address above to start tracking.
      </div>
    </div>
  </div>
</template>
