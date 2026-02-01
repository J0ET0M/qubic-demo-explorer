<script setup lang="ts">
import { Wallet, Copy, Check, ArrowDownLeft, ArrowUpRight, Gift, GitBranch, Filter, X } from 'lucide-vue-next'

const api = useApi()
const route = useRoute()
const router = useRouter()
const { getLabel, fetchLabels } = useAddressLabels()

const address = route.params.id as string

// Log type definitions for transfers
const logTypes = [
  { value: 0, name: 'QU Transfer' },
  { value: 1, name: 'Asset Issuance' },
  { value: 2, name: 'Asset Ownership' },
  { value: 3, name: 'Asset Possession' },
]

// Initialize state from URL query params
const activeTab = ref<'transactions' | 'transfers' | 'rewards' | 'flow'>(
  (route.query.tab as 'transactions' | 'transfers' | 'rewards' | 'flow') || 'transactions'
)
const direction = ref<string>((route.query.direction as string) || '')
const page = ref(Number(route.query.page) || 1)
const minAmount = ref(route.query.minAmount ? Number(route.query.minAmount) : undefined)
const transferType = ref<number | undefined>(
  route.query.type !== undefined ? Number(route.query.type) : undefined
)
const txExecuted = ref<boolean | undefined>(
  route.query.executed === 'true' ? true :
  route.query.executed === 'false' ? false : undefined
)
const limit = 20
const copied = ref(false)

// UI state
const showTxFilters = ref(false)
const showTransferFilters = ref(false)
const minAmountInput = ref(minAmount.value?.toString() || '')

// Sync state to URL
const updateUrl = () => {
  const query: Record<string, string | number> = {}
  if (activeTab.value !== 'transactions') query.tab = activeTab.value
  if (direction.value) query.direction = direction.value
  if (page.value > 1) query.page = page.value
  if (minAmount.value !== undefined) query.minAmount = minAmount.value
  if (transferType.value !== undefined) query.type = transferType.value
  if (txExecuted.value !== undefined) query.executed = String(txExecuted.value)
  router.push({ query })
}

watch([activeTab, direction, page, minAmount, transferType, txExecuted], updateUrl)

// Fetch label for this address
onMounted(() => fetchLabels([address]))

const addressLabel = computed(() => getLabel(address))
const isSmartContract = computed(() => addressLabel.value?.type === 'smartcontract')

// Check if filters are active
const hasTxFilters = computed(() => minAmount.value !== undefined || txExecuted.value !== undefined)
const hasTransferFilters = computed(() => minAmount.value !== undefined || transferType.value !== undefined || direction.value !== '')

const { data: addressData, pending: addressLoading } = await useAsyncData(
  `address-${address}`,
  () => api.getAddress(address)
)

// Transaction filters
const txFilterOptions = computed(() => {
  const opts: { minAmount?: number; executed?: boolean } = {}
  if (minAmount.value !== undefined) opts.minAmount = minAmount.value
  if (txExecuted.value !== undefined) opts.executed = txExecuted.value
  return opts
})

const { data: transactions, pending: txLoading } = await useAsyncData(
  () => `address-tx-${address}-${page.value}-${minAmount.value}-${txExecuted.value}`,
  () => api.getAddressTransactions(address, page.value, limit,
    Object.keys(txFilterOptions.value).length > 0 ? txFilterOptions.value : undefined),
  { watch: [page, minAmount, txExecuted] }
)

// Transfer filters
const transferFilterOptions = computed(() => {
  const opts: { direction?: 'in' | 'out'; type?: number; minAmount?: number } = {}
  if (direction.value === 'in' || direction.value === 'out') opts.direction = direction.value
  if (transferType.value !== undefined) opts.type = transferType.value
  if (minAmount.value !== undefined) opts.minAmount = minAmount.value
  return opts
})

const { data: transfers, pending: transfersLoading } = await useAsyncData(
  () => `address-transfers-${address}-${page.value}-${direction.value}-${transferType.value}-${minAmount.value}`,
  () => api.getAddressTransfers(address, page.value, limit,
    Object.keys(transferFilterOptions.value).length > 0 ? transferFilterOptions.value : undefined),
  { watch: [page, direction, transferType, minAmount] }
)

// Rewards pagination (separate from tx/transfers)
const rewardsPage = ref(1)

// Fetch rewards only for smart contract addresses
const { data: rewards, pending: rewardsLoading } = await useAsyncData(
  () => `address-rewards-${address}-${rewardsPage.value}`,
  () => api.getAddressRewards(address, rewardsPage.value, limit),
  { lazy: true, watch: [rewardsPage] }
)

// Fetch flow data (top counterparties)
const { data: flowData, pending: flowLoading } = await useAsyncData(
  `address-flow-${address}`,
  () => api.getAddressFlow(address, 10),
  { lazy: true }
)

// Format volume for display
const formatVolume = (volume: number) => {
  if (volume >= 1_000_000_000_000) return (volume / 1_000_000_000_000).toFixed(2) + 'T'
  if (volume >= 1_000_000_000) return (volume / 1_000_000_000).toFixed(2) + 'B'
  if (volume >= 1_000_000) return (volume / 1_000_000).toFixed(2) + 'M'
  if (volume >= 1_000) return (volume / 1_000).toFixed(2) + 'K'
  return volume.toLocaleString()
}

const formatDate = (dateStr: string) => {
  return new Date(dateStr).toLocaleString()
}

const formatAmount = (amount: number) => {
  // Qubic has no decimals, amount is already in QU
  const qu = Math.floor(amount)
  if (qu >= 1_000_000_000) return Math.floor(qu / 1_000_000_000).toLocaleString() + 'B'
  if (qu >= 1_000_000) return Math.floor(qu / 1_000_000).toLocaleString() + 'M'
  if (qu >= 1_000) return Math.floor(qu / 1_000).toLocaleString() + 'K'
  return qu.toLocaleString()
}

const formatAmountFull = (amount: number) => {
  // Qubic has no decimals, balance is already in QU
  return Math.floor(amount).toLocaleString()
}

const copyToClipboard = async (text: string) => {
  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(text)
    } else {
      // Fallback for non-secure contexts
      const textArea = document.createElement('textarea')
      textArea.value = text
      textArea.style.position = 'fixed'
      textArea.style.left = '-999999px'
      textArea.style.top = '-999999px'
      document.body.appendChild(textArea)
      textArea.focus()
      textArea.select()
      document.execCommand('copy')
      textArea.remove()
    }
    copied.value = true
    setTimeout(() => copied.value = false, 2000)
  } catch (err) {
    console.error('Failed to copy:', err)
  }
}

const switchTab = (tab: 'transactions' | 'transfers' | 'rewards' | 'flow') => {
  activeTab.value = tab
  page.value = 1
  rewardsPage.value = 1
  // Reset filters when switching tabs
  minAmount.value = undefined
  minAmountInput.value = ''
  transferType.value = undefined
  txExecuted.value = undefined
  direction.value = ''
  showTxFilters.value = false
  showTransferFilters.value = false
}

const applyTxFilters = () => {
  page.value = 1
  minAmount.value = minAmountInput.value ? Number(minAmountInput.value) : undefined
  showTxFilters.value = false
}

const clearTxFilters = () => {
  minAmountInput.value = ''
  minAmount.value = undefined
  txExecuted.value = undefined
  page.value = 1
}

const applyTransferFilters = () => {
  page.value = 1
  minAmount.value = minAmountInput.value ? Number(minAmountInput.value) : undefined
  showTransferFilters.value = false
}

const clearTransferFilters = () => {
  minAmountInput.value = ''
  minAmount.value = undefined
  transferType.value = undefined
  direction.value = ''
  page.value = 1
}

const getTypeName = (type: number) => {
  return logTypes.find(t => t.value === type)?.name || `Type ${type}`
}
</script>

<template>
  <div class="space-y-6">
    <!-- Address Overview Card -->
    <div class="card">
      <h2 class="section-title mb-4">
        <Wallet class="h-5 w-5 text-accent" />
        Address Overview
      </h2>

      <div v-if="addressLoading" class="loading">Loading...</div>

      <template v-else-if="addressData">
        <div class="space-y-0">
          <div v-if="addressLabel?.label" class="detail-row">
            <span class="detail-label">Label</span>
            <span class="detail-value">
              <span :class="['badge', {
                'badge-warning': addressLabel.type === 'exchange',
                'badge-info': addressLabel.type === 'smartcontract',
                'badge-accent': addressLabel.type === 'tokenissuer',
                'badge-error': addressLabel.type === 'burn',
                'badge-success': addressLabel.type === 'known'
              }]">
                {{ addressLabel.label }}
              </span>
              <span class="text-foreground-muted text-sm ml-2">({{ addressLabel.type }})</span>
            </span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Address</span>
            <span class="detail-value flex items-center gap-2 flex-wrap">
              <span class="address break-all">{{ address }}</span>
              <button
                @click="copyToClipboard(address)"
                class="btn btn-ghost p-1"
                :title="copied ? 'Copied!' : 'Copy'"
              >
                <Check v-if="copied" class="h-4 w-4 text-success" />
                <Copy v-else class="h-4 w-4" />
              </button>
            </span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Balance</span>
            <span class="detail-value font-semibold text-accent text-lg">
              {{ formatAmountFull(addressData.balance) }} QU
            </span>
          </div>
        </div>

        <!-- Stats Grid -->
        <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mt-6">
          <div class="card-elevated text-center">
            <div class="text-lg font-semibold text-success flex items-center justify-center gap-1">
              <ArrowDownLeft class="h-4 w-4" />
              {{ formatAmount(addressData.incomingAmount) }}
            </div>
            <div class="text-xs text-foreground-muted uppercase mt-1">Incoming</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-lg font-semibold text-destructive flex items-center justify-center gap-1">
              <ArrowUpRight class="h-4 w-4" />
              {{ formatAmount(addressData.outgoingAmount) }}
            </div>
            <div class="text-xs text-foreground-muted uppercase mt-1">Outgoing</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-lg font-semibold text-accent">{{ addressData.txCount }}</div>
            <div class="text-xs text-foreground-muted uppercase mt-1">Transactions</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-lg font-semibold text-accent">{{ addressData.transferCount }}</div>
            <div class="text-xs text-foreground-muted uppercase mt-1">Transfers</div>
          </div>
        </div>
      </template>
    </div>

    <!-- Activity Card -->
    <div class="card">
      <div class="tabs">
        <button
          :class="{ active: activeTab === 'transactions' }"
          @click="switchTab('transactions')"
        >
          Transactions
        </button>
        <button
          :class="{ active: activeTab === 'transfers' }"
          @click="switchTab('transfers')"
        >
          Transfers
        </button>
        <button
          v-if="isSmartContract"
          :class="{ active: activeTab === 'rewards' }"
          @click="switchTab('rewards')"
        >
          <Gift class="h-4 w-4 inline mr-1" />
          Rewards
        </button>
        <button
          :class="{ active: activeTab === 'flow' }"
          @click="switchTab('flow')"
        >
          <GitBranch class="h-4 w-4 inline mr-1" />
          Flow
        </button>
      </div>

      <!-- Transactions tab -->
      <template v-if="activeTab === 'transactions'">
        <!-- Transaction Filters -->
        <div class="mb-4">
          <div class="flex items-center justify-between flex-wrap gap-2">
            <div class="flex items-center gap-2 flex-wrap">
              <button
                @click="showTxFilters = !showTxFilters"
                :class="['btn btn-sm btn-ghost', { 'text-accent': hasTxFilters }]"
              >
                <Filter class="h-4 w-4 mr-1" />
                Filters
                <span v-if="hasTxFilters" class="badge badge-accent ml-1 text-xs">Active</span>
              </button>

              <!-- Quick executed filter -->
              <div class="flex items-center gap-1">
                <button
                  @click="txExecuted = txExecuted === true ? undefined : true; page = 1"
                  :class="['btn btn-sm', txExecuted === true ? 'btn-success' : 'btn-ghost']"
                >
                  Executed
                </button>
                <button
                  @click="txExecuted = txExecuted === false ? undefined : false; page = 1"
                  :class="['btn btn-sm', txExecuted === false ? 'btn-error' : 'btn-ghost']"
                >
                  Failed
                </button>
              </div>

              <!-- Active filter pills -->
              <span
                v-if="minAmount !== undefined"
                class="badge badge-info flex items-center gap-1"
              >
                Min: {{ minAmount.toLocaleString() }}
                <button @click="minAmount = undefined; page = 1" class="hover:text-white">
                  <X class="h-3 w-3" />
                </button>
              </span>
            </div>

            <button
              v-if="hasTxFilters"
              @click="clearTxFilters"
              class="btn btn-sm btn-ghost"
            >
              Clear filters
            </button>
          </div>

          <!-- Expanded filter panel -->
          <div v-if="showTxFilters" class="mt-3 p-3 bg-background-elevated rounded-lg">
            <div class="grid grid-cols-1 sm:grid-cols-3 gap-3">
              <div>
                <label class="block text-xs font-medium mb-1">Min Amount (QU)</label>
                <input
                  v-model="minAmountInput"
                  type="number"
                  min="0"
                  class="input input-sm w-full"
                  placeholder="e.g., 1000000"
                />
              </div>
              <div>
                <label class="block text-xs font-medium mb-1">Status</label>
                <select v-model="txExecuted" class="input input-sm w-full">
                  <option :value="undefined">All</option>
                  <option :value="true">Executed only</option>
                  <option :value="false">Failed only</option>
                </select>
              </div>
              <div class="flex items-end">
                <button @click="applyTxFilters" class="btn btn-sm btn-primary">Apply</button>
              </div>
            </div>
          </div>
        </div>

        <div v-if="txLoading" class="loading">Loading...</div>
        <template v-else-if="transactions">
          <div v-if="transactions.items.length === 0" class="text-center py-8 text-foreground-muted">
            No transactions found for this address.
          </div>
          <template v-else>
            <TransactionTable
              :transactions="transactions.items"
              :highlight-address="address"
            />
            <Pagination
              :current-page="page"
              :total-pages="transactions.totalPages"
              :has-next="transactions.hasNextPage"
              :has-previous="transactions.hasPreviousPage"
              @update:current-page="(p) => page = p"
            />
          </template>
        </template>
      </template>

      <!-- Transfers tab -->
      <template v-if="activeTab === 'transfers'">
        <!-- Transfer Filters -->
        <div class="mb-4">
          <div class="flex items-center justify-between flex-wrap gap-2">
            <div class="flex items-center gap-2 flex-wrap">
              <button
                @click="showTransferFilters = !showTransferFilters"
                :class="['btn btn-sm btn-ghost', { 'text-accent': hasTransferFilters }]"
              >
                <Filter class="h-4 w-4 mr-1" />
                Filters
                <span v-if="hasTransferFilters" class="badge badge-accent ml-1 text-xs">Active</span>
              </button>

              <!-- Quick direction filter -->
              <select v-model="direction" class="input input-sm w-auto" @change="page = 1">
                <option value="">All Directions</option>
                <option value="in">Incoming</option>
                <option value="out">Outgoing</option>
              </select>

              <!-- Quick type filter -->
              <select v-model="transferType" class="input input-sm w-auto" @change="page = 1">
                <option :value="undefined">All Types</option>
                <option v-for="type in logTypes" :key="type.value" :value="type.value">
                  {{ type.name }}
                </option>
              </select>

              <!-- Active filter pills -->
              <span
                v-if="minAmount !== undefined"
                class="badge badge-info flex items-center gap-1"
              >
                Min: {{ minAmount.toLocaleString() }}
                <button @click="minAmount = undefined; page = 1" class="hover:text-white">
                  <X class="h-3 w-3" />
                </button>
              </span>
            </div>

            <button
              v-if="hasTransferFilters"
              @click="clearTransferFilters"
              class="btn btn-sm btn-ghost"
            >
              Clear filters
            </button>
          </div>

          <!-- Expanded filter panel -->
          <div v-if="showTransferFilters" class="mt-3 p-3 bg-background-elevated rounded-lg">
            <div class="grid grid-cols-1 sm:grid-cols-4 gap-3">
              <div>
                <label class="block text-xs font-medium mb-1">Min Amount (QU)</label>
                <input
                  v-model="minAmountInput"
                  type="number"
                  min="0"
                  class="input input-sm w-full"
                  placeholder="e.g., 1000000"
                />
              </div>
              <div>
                <label class="block text-xs font-medium mb-1">Direction</label>
                <select v-model="direction" class="input input-sm w-full">
                  <option value="">All</option>
                  <option value="in">Incoming</option>
                  <option value="out">Outgoing</option>
                </select>
              </div>
              <div>
                <label class="block text-xs font-medium mb-1">Type</label>
                <select v-model="transferType" class="input input-sm w-full">
                  <option :value="undefined">All Types</option>
                  <option v-for="type in logTypes" :key="type.value" :value="type.value">
                    {{ type.name }}
                  </option>
                </select>
              </div>
              <div class="flex items-end">
                <button @click="applyTransferFilters" class="btn btn-sm btn-primary">Apply</button>
              </div>
            </div>
          </div>
        </div>

        <div v-if="transfersLoading" class="loading">Loading...</div>
        <template v-else-if="transfers">
          <div v-if="transfers.items.length === 0" class="text-center py-8 text-foreground-muted">
            No transfers found for this address.
          </div>
          <template v-else>
            <TransferTable
              :transfers="transfers.items"
              :highlight-address="address"
            />
            <Pagination
              :current-page="page"
              :total-pages="transfers.totalPages"
              :has-next="transfers.hasNextPage"
              :has-previous="transfers.hasPreviousPage"
              @update:current-page="(p) => page = p"
            />
          </template>
        </template>
      </template>

      <!-- Rewards tab (smart contracts only) -->
      <template v-if="activeTab === 'rewards' && isSmartContract">
        <div v-if="rewardsLoading" class="loading">Loading...</div>
        <template v-else-if="rewards">
          <!-- Summary -->
          <div class="card-elevated mb-4 text-center">
            <div class="text-2xl font-bold text-success">
              {{ formatVolume(rewards.totalAllTimeDistributed) }} QU
            </div>
            <div class="text-xs text-foreground-muted uppercase mt-1">
              Total Distributed All Time
            </div>
          </div>

          <div v-if="!rewards.distributions?.length" class="text-center py-8 text-foreground-muted">
            No reward distributions found for this smart contract.
          </div>

          <!-- Epoch-by-epoch history -->
          <template v-else>
            <div class="table-wrapper">
              <table>
                <thead>
                  <tr>
                    <th>Epoch</th>
                    <th>Tick</th>
                    <th>Total Amount</th>
                    <th>Per Share</th>
                    <th>Transfers</th>
                    <th>Date</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="dist in rewards.distributions" :key="`${dist.epoch}-${dist.tickNumber}`">
                    <td>
                      <NuxtLink :to="`/epochs/${dist.epoch}`" class="text-accent">
                        {{ dist.epoch }}
                      </NuxtLink>
                    </td>
                    <td>
                      <NuxtLink :to="`/ticks/${dist.tickNumber}`" class="text-accent">
                        {{ dist.tickNumber.toLocaleString() }}
                      </NuxtLink>
                    </td>
                    <td>{{ formatVolume(dist.totalAmount) }} QU</td>
                    <td>{{ formatVolume(dist.amountPerShare) }} QU</td>
                    <td>{{ dist.transferCount.toLocaleString() }}</td>
                    <td>{{ formatDate(dist.timestamp) }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
            <Pagination
              :current-page="rewardsPage"
              :total-pages="rewards.totalPages"
              :has-next="rewards.hasNextPage"
              :has-previous="rewards.hasPreviousPage"
              @update:current-page="(p) => rewardsPage = p"
            />
          </template>
        </template>
      </template>

      <!-- Flow tab -->
      <template v-if="activeTab === 'flow'">
        <div v-if="flowLoading" class="loading">Loading flow data...</div>
        <template v-else-if="flowData">
          <AddressFlowChart :flow="flowData" />
        </template>
        <div v-else class="text-center py-8 text-foreground-muted">
          No flow data available for this address.
        </div>
      </template>
    </div>
  </div>
</template>
