<script setup lang="ts">
import { Users, Activity, History, ArrowLeftRight, Building2, Cpu, UserPlus } from 'lucide-vue-next'

const api = useApi()

// Active addresses trend
const { data: activeAddresses, pending: activeAddressesLoading } = await useAsyncData(
  'network-active-addresses',
  () => api.getActiveAddressTrends('epoch', 30)
)

// New vs Returning
const { data: newVsReturning, pending: newVsReturningLoading } = await useAsyncData(
  'network-new-vs-returning',
  () => api.getNewVsReturningAddresses(30)
)

// Avg Tx Size
const { data: avgTxSize, pending: avgTxSizeLoading } = await useAsyncData(
  'network-avg-tx-size',
  () => api.getAvgTxSizeTrends('epoch', 30)
)

// Network stats history (periodic snapshots) - reactive to time range
const { timeRange } = useTimeRange()
const networkStatsHistory = ref<Awaited<ReturnType<typeof api.getNetworkStatsHistory>>>([])
const networkStatsHistoryLoading = ref(true)

const fetchNetworkStatsHistory = async () => {
  networkStatsHistoryLoading.value = true
  try {
    const { from, to } = timeRange.value
    networkStatsHistory.value = await api.getNetworkStatsHistory(500, from ?? undefined, to ?? undefined)
  } catch (e) {
    console.error('Failed to fetch network stats history', e)
  } finally {
    networkStatsHistoryLoading.value = false
  }
}

watch(() => timeRange.value, fetchNetworkStatsHistory, { deep: true })
await fetchNetworkStatsHistory()

// Active addresses chart data
const activeAddressChartLabels = computed(() => {
  if (!activeAddresses.value) return []
  return activeAddresses.value.map(d => d.epoch?.toString() || '')
})

const activeAddressChartData = computed(() => {
  if (!activeAddresses.value) return { senders: [], receivers: [] }
  return {
    senders: activeAddresses.value.map(d => d.uniqueSenders),
    receivers: activeAddresses.value.map(d => d.uniqueReceivers)
  }
})

// New vs Returning chart data
const newVsReturningChartLabels = computed(() => {
  if (!newVsReturning.value) return []
  return newVsReturning.value.map(d => d.epoch.toString())
})

const newVsReturningChartData = computed(() => {
  if (!newVsReturning.value) return { new: [], returning: [] }
  return {
    new: newVsReturning.value.map(d => d.newAddresses),
    returning: newVsReturning.value.map(d => d.returningAddresses)
  }
})

// Avg tx size chart data
const avgTxSizeChartLabels = computed(() => {
  if (!avgTxSize.value) return []
  return avgTxSize.value.map(d => d.epoch?.toString() || '')
})

const avgTxSizeChartData = computed(() => {
  if (!avgTxSize.value) return { avg: [], median: [] }
  return {
    avg: avgTxSize.value.map(d => d.avgTxSize),
    median: avgTxSize.value.map(d => d.medianTxSize)
  }
})

// Network stats history chart data
const networkStatsHistoryChartLabels = computed(() => {
  if (!networkStatsHistory.value) return []
  return networkStatsHistory.value.map(d => {
    if (d.tickStart > 0 && d.tickEnd > 0) {
      return new Date(d.snapshotAt).toLocaleDateString('en-US', { month: 'short', day: 'numeric', hour: '2-digit' })
    }
    return `E${d.epoch}`
  })
})

const networkStatsHistoryChartData = computed(() => {
  if (!networkStatsHistory.value) return { transactions: [], activeAddresses: [], scCalls: [] }
  return {
    transactions: networkStatsHistory.value.map(d => d.totalTransactions),
    activeAddresses: networkStatsHistory.value.map(d => d.totalActiveAddresses),
    scCalls: networkStatsHistory.value.map(d => d.scCallCount)
  }
})

// Volume history chart data
const volumeHistoryChartData = computed(() => {
  if (!networkStatsHistory.value) return { volume: [], transfers: [] }
  return {
    volume: networkStatsHistory.value.map(d => d.totalVolume),
    transfers: networkStatsHistory.value.map(d => d.totalTransfers)
  }
})

// Exchange flow history chart data
const exchangeFlowHistoryChartData = computed(() => {
  if (!networkStatsHistory.value) return { inflow: [], outflow: [], netFlow: [] }
  return {
    inflow: networkStatsHistory.value.map(d => d.exchangeInflowVolume),
    outflow: networkStatsHistory.value.map(d => d.exchangeOutflowVolume),
    netFlow: networkStatsHistory.value.map(d => d.exchangeNetFlow)
  }
})

// New vs returning history chart data
const newVsReturningHistoryChartData = computed(() => {
  if (!networkStatsHistory.value) return { new: [], returning: [] }
  return {
    new: networkStatsHistory.value.map(d => d.newAddresses),
    returning: networkStatsHistory.value.map(d => d.returningAddresses)
  }
})

// Tx size history chart data
const txSizeHistoryChartData = computed(() => {
  if (!networkStatsHistory.value) return { avg: [], median: [] }
  return {
    avg: networkStatsHistory.value.map(d => d.avgTxSize),
    median: networkStatsHistory.value.map(d => d.medianTxSize)
  }
})

// New users with high balance history chart data
const newUsersHighBalanceChartData = computed(() => {
  if (!networkStatsHistory.value) return { users100M: [], users1B: [], users10B: [] }
  return {
    users100M: networkStatsHistory.value.map(d => d.newUsers100MPlus || 0),
    users1B: networkStatsHistory.value.map(d => d.newUsers1BPlus || 0),
    users10B: networkStatsHistory.value.map(d => d.newUsers10BPlus || 0)
  }
})

const formatVolume = (volume: number) => {
  if (volume >= 1_000_000_000_000) return (volume / 1_000_000_000_000).toFixed(1) + 'T'
  if (volume >= 1_000_000_000) return (volume / 1_000_000_000).toFixed(1) + 'B'
  if (volume >= 1_000_000) return (volume / 1_000_000).toFixed(1) + 'M'
  if (volume >= 1_000) return (volume / 1_000).toFixed(1) + 'K'
  return volume.toLocaleString()
}
</script>

<template>
  <div class="space-y-6">
    <!-- Active Addresses & New vs Returning Row -->
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <!-- Active Addresses Trend -->
      <div class="card">
        <h2 class="section-title mb-4">
          <Users class="h-5 w-5 text-accent" />
          Active Addresses per Epoch
        </h2>

        <div v-if="activeAddressesLoading" class="loading">Loading...</div>
        <template v-else-if="activeAddresses?.length">
          <ClientOnly>
            <ChartsEpochLineChart
              :labels="activeAddressChartLabels"
              :datasets="[
                {
                  label: 'Senders',
                  data: activeAddressChartData.senders,
                  borderColor: 'rgb(16, 185, 129)',
                  backgroundColor: 'rgba(16, 185, 129, 0.1)'
                },
                {
                  label: 'Receivers',
                  data: activeAddressChartData.receivers,
                  borderColor: 'rgb(59, 130, 246)',
                  backgroundColor: 'rgba(59, 130, 246, 0.1)'
                }
              ]"
              :height="250"
            />
            <template #fallback>
              <div class="h-[250px] flex items-center justify-center text-foreground-muted">
                Loading chart...
              </div>
            </template>
          </ClientOnly>
        </template>
        <div v-else class="text-center py-8 text-foreground-muted">
          No active address data available
        </div>
      </div>

      <!-- New vs Returning Addresses -->
      <div class="card">
        <h2 class="section-title mb-4">
          <Users class="h-5 w-5 text-accent" />
          New vs Returning Addresses
        </h2>

        <div v-if="newVsReturningLoading" class="loading">Loading...</div>
        <template v-else-if="newVsReturning?.length">
          <ClientOnly>
            <ChartsEpochBarChart
              :labels="newVsReturningChartLabels"
              :datasets="[
                {
                  label: 'New Addresses',
                  data: newVsReturningChartData.new,
                  backgroundColor: 'rgba(16, 185, 129, 0.8)'
                },
                {
                  label: 'Returning Addresses',
                  data: newVsReturningChartData.returning,
                  backgroundColor: 'rgba(59, 130, 246, 0.8)'
                }
              ]"
              :height="250"
              :stacked="true"
            />
            <template #fallback>
              <div class="h-[250px] flex items-center justify-center text-foreground-muted">
                Loading chart...
              </div>
            </template>
          </ClientOnly>
        </template>
        <div v-else class="text-center py-8 text-foreground-muted">
          No new/returning address data available
        </div>
      </div>
    </div>

    <!-- Average Tx Size Trend -->
    <div class="card">
      <h2 class="section-title mb-4">
        <Activity class="h-5 w-5 text-accent" />
        Average Transaction Size Trend
      </h2>

      <div v-if="avgTxSizeLoading" class="loading">Loading...</div>
      <template v-else-if="avgTxSize?.length">
        <ClientOnly>
          <ChartsEpochLineChart
            :labels="avgTxSizeChartLabels"
            :datasets="[
              {
                label: 'Average',
                data: avgTxSizeChartData.avg,
                borderColor: 'rgb(139, 92, 246)',
                backgroundColor: 'rgba(139, 92, 246, 0.1)'
              },
              {
                label: 'Median',
                data: avgTxSizeChartData.median,
                borderColor: 'rgb(245, 158, 11)',
                backgroundColor: 'rgba(245, 158, 11, 0.1)'
              }
            ]"
            :height="250"
          />
          <template #fallback>
            <div class="h-[250px] flex items-center justify-center text-foreground-muted">
              Loading chart...
            </div>
          </template>
        </ClientOnly>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        No average transaction size data available
      </div>
    </div>

    <!-- Network Stats History (Periodic Snapshots) -->
    <div class="card">
      <h2 class="section-title mb-4">
        <History class="h-5 w-5 text-accent" />
        Network Activity History (4-Hour Snapshots)
      </h2>

      <div v-if="networkStatsHistoryLoading" class="loading">Loading...</div>
      <template v-else-if="networkStatsHistory?.length">
        <!-- Latest snapshot summary -->
        <div class="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-4">
          <div class="card-elevated text-center">
            <div class="text-lg font-bold text-accent">
              {{ networkStatsHistory[networkStatsHistory.length - 1]?.totalTransactions?.toLocaleString() || 0 }}
            </div>
            <div class="text-xs text-foreground-muted">Transactions</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-lg font-bold text-accent">
              {{ networkStatsHistory[networkStatsHistory.length - 1]?.totalActiveAddresses?.toLocaleString() || 0 }}
            </div>
            <div class="text-xs text-foreground-muted">Active Addresses</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-lg font-bold text-accent">
              {{ formatVolume(networkStatsHistory[networkStatsHistory.length - 1]?.totalVolume || 0) }}
            </div>
            <div class="text-xs text-foreground-muted">Volume</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-lg font-bold text-accent">
              {{ networkStatsHistory[networkStatsHistory.length - 1]?.scCallCount?.toLocaleString() || 0 }}
            </div>
            <div class="text-xs text-foreground-muted">SC Calls</div>
          </div>
        </div>

        <ClientOnly>
          <ChartsEpochLineChart
            :labels="networkStatsHistoryChartLabels"
            :datasets="[
              {
                label: 'Active Addresses',
                data: networkStatsHistoryChartData.activeAddresses,
                borderColor: 'rgb(16, 185, 129)',
                backgroundColor: 'rgba(16, 185, 129, 0.1)'
              },
              {
                label: 'SC Calls',
                data: networkStatsHistoryChartData.scCalls,
                borderColor: 'rgb(139, 92, 246)',
                backgroundColor: 'rgba(139, 92, 246, 0.1)'
              }
            ]"
            :height="250"
          />
          <template #fallback>
            <div class="h-[250px] flex items-center justify-center text-foreground-muted">
              Loading chart...
            </div>
          </template>
        </ClientOnly>
        <p class="text-xs text-foreground-muted mt-2">
          Historical snapshots taken every 4 hours showing network activity trends.
        </p>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        No historical network stats available yet. Snapshots are taken every 4 hours.
      </div>
    </div>

    <!-- Volume & Transfers History -->
    <div class="card">
      <h2 class="section-title mb-4">
        <ArrowLeftRight class="h-5 w-5 text-accent" />
        Volume & Transfers History (4-Hour Snapshots)
      </h2>

      <div v-if="networkStatsHistoryLoading" class="loading">Loading...</div>
      <template v-else-if="networkStatsHistory?.length && volumeHistoryChartData.volume.some(v => v > 0)">
        <ClientOnly>
          <ChartsEpochLineChart
            :labels="networkStatsHistoryChartLabels"
            :datasets="[
              {
                label: 'Transfers',
                data: volumeHistoryChartData.transfers,
                borderColor: 'rgb(59, 130, 246)',
                backgroundColor: 'rgba(59, 130, 246, 0.1)'
              }
            ]"
            :height="250"
          />
          <template #fallback>
            <div class="h-[250px] flex items-center justify-center text-foreground-muted">
              Loading chart...
            </div>
          </template>
        </ClientOnly>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        No volume history available yet.
      </div>
    </div>

    <!-- Exchange Flow History -->
    <div class="card">
      <h2 class="section-title mb-4">
        <Building2 class="h-5 w-5 text-accent" />
        Exchange Flow History (4-Hour Snapshots)
      </h2>

      <div v-if="networkStatsHistoryLoading" class="loading">Loading...</div>
      <template v-else-if="networkStatsHistory?.length && exchangeFlowHistoryChartData.inflow.some(v => v > 0)">
        <!-- Latest exchange flow summary -->
        <div class="grid grid-cols-3 gap-4 mb-4">
          <div class="card-elevated text-center">
            <div class="text-lg font-bold text-success">
              {{ formatVolume(networkStatsHistory[networkStatsHistory.length - 1]?.exchangeInflowVolume || 0) }}
            </div>
            <div class="text-xs text-foreground-muted">Inflow</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-lg font-bold text-destructive">
              {{ formatVolume(networkStatsHistory[networkStatsHistory.length - 1]?.exchangeOutflowVolume || 0) }}
            </div>
            <div class="text-xs text-foreground-muted">Outflow</div>
          </div>
          <div class="card-elevated text-center">
            <div :class="['text-lg font-bold', (networkStatsHistory[networkStatsHistory.length - 1]?.exchangeNetFlow || 0) >= 0 ? 'text-success' : 'text-destructive']">
              {{ formatVolume(Math.abs(networkStatsHistory[networkStatsHistory.length - 1]?.exchangeNetFlow || 0)) }}
            </div>
            <div class="text-xs text-foreground-muted">Net Flow</div>
          </div>
        </div>

        <ClientOnly>
          <ChartsEpochLineChart
            :labels="networkStatsHistoryChartLabels"
            :datasets="[
              {
                label: 'Inflow',
                data: exchangeFlowHistoryChartData.inflow,
                borderColor: 'rgb(16, 185, 129)',
                backgroundColor: 'rgba(16, 185, 129, 0.1)'
              },
              {
                label: 'Outflow',
                data: exchangeFlowHistoryChartData.outflow,
                borderColor: 'rgb(239, 68, 68)',
                backgroundColor: 'rgba(239, 68, 68, 0.1)'
              }
            ]"
            :height="250"
          />
          <template #fallback>
            <div class="h-[250px] flex items-center justify-center text-foreground-muted">
              Loading chart...
            </div>
          </template>
        </ClientOnly>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        No exchange flow history available yet. Add exchange labels to enable this feature.
      </div>
    </div>

    <!-- New vs Returning Addresses History -->
    <div class="card">
      <h2 class="section-title mb-4">
        <Users class="h-5 w-5 text-accent" />
        New vs Returning History (4-Hour Snapshots)
      </h2>

      <div v-if="networkStatsHistoryLoading" class="loading">Loading...</div>
      <template v-else-if="networkStatsHistory?.length && newVsReturningHistoryChartData.new.some(v => v > 0)">
        <ClientOnly>
          <ChartsEpochBarChart
            :labels="networkStatsHistoryChartLabels"
            :datasets="[
              {
                label: 'New Addresses',
                data: newVsReturningHistoryChartData.new,
                backgroundColor: 'rgba(16, 185, 129, 0.8)'
              },
              {
                label: 'Returning Addresses',
                data: newVsReturningHistoryChartData.returning,
                backgroundColor: 'rgba(59, 130, 246, 0.8)'
              }
            ]"
            :height="250"
            :stacked="true"
          />
          <template #fallback>
            <div class="h-[250px] flex items-center justify-center text-foreground-muted">
              Loading chart...
            </div>
          </template>
        </ClientOnly>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        No new/returning address history available yet.
      </div>
    </div>

    <!-- Transaction Size History -->
    <div class="card">
      <h2 class="section-title mb-4">
        <Activity class="h-5 w-5 text-accent" />
        Transaction Size History (4-Hour Snapshots)
      </h2>

      <div v-if="networkStatsHistoryLoading" class="loading">Loading...</div>
      <template v-else-if="networkStatsHistory?.length && txSizeHistoryChartData.avg.some(v => v > 0)">
        <!-- Latest tx size summary -->
        <div class="grid grid-cols-2 gap-4 mb-4">
          <div class="card-elevated text-center">
            <div class="text-lg font-bold text-accent">
              {{ formatVolume(networkStatsHistory[networkStatsHistory.length - 1]?.avgTxSize || 0) }}
            </div>
            <div class="text-xs text-foreground-muted">Average Tx Size</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-lg font-bold text-accent">
              {{ formatVolume(networkStatsHistory[networkStatsHistory.length - 1]?.medianTxSize || 0) }}
            </div>
            <div class="text-xs text-foreground-muted">Median Tx Size</div>
          </div>
        </div>

        <ClientOnly>
          <ChartsEpochLineChart
            :labels="networkStatsHistoryChartLabels"
            :datasets="[
              {
                label: 'Average',
                data: txSizeHistoryChartData.avg,
                borderColor: 'rgb(139, 92, 246)',
                backgroundColor: 'rgba(139, 92, 246, 0.1)'
              },
              {
                label: 'Median',
                data: txSizeHistoryChartData.median,
                borderColor: 'rgb(245, 158, 11)',
                backgroundColor: 'rgba(245, 158, 11, 0.1)'
              }
            ]"
            :height="250"
          />
          <template #fallback>
            <div class="h-[250px] flex items-center justify-center text-foreground-muted">
              Loading chart...
            </div>
          </template>
        </ClientOnly>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        No transaction size history available yet.
      </div>
    </div>

    <!-- Smart Contract Activity History -->
    <div class="card">
      <h2 class="section-title mb-4">
        <Cpu class="h-5 w-5 text-accent" />
        Smart Contract Activity History (4-Hour Snapshots)
      </h2>

      <div v-if="networkStatsHistoryLoading" class="loading">Loading...</div>
      <template v-else-if="networkStatsHistory?.length && networkStatsHistoryChartData.scCalls.some(v => v > 0)">
        <!-- Latest SC activity summary -->
        <div class="grid grid-cols-2 gap-4 mb-4">
          <div class="card-elevated text-center">
            <div class="text-lg font-bold text-accent">
              {{ networkStatsHistory[networkStatsHistory.length - 1]?.scCallCount?.toLocaleString() || 0 }}
            </div>
            <div class="text-xs text-foreground-muted">SC Calls</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-lg font-bold text-accent">
              {{ networkStatsHistory[networkStatsHistory.length - 1]?.scUniqueCallers?.toLocaleString() || 0 }}
            </div>
            <div class="text-xs text-foreground-muted">Unique Callers</div>
          </div>
        </div>

        <ClientOnly>
          <ChartsEpochLineChart
            :labels="networkStatsHistoryChartLabels"
            :datasets="[
              {
                label: 'SC Calls',
                data: networkStatsHistoryChartData.scCalls,
                borderColor: 'rgb(139, 92, 246)',
                backgroundColor: 'rgba(139, 92, 246, 0.1)'
              }
            ]"
            :height="250"
          />
          <template #fallback>
            <div class="h-[250px] flex items-center justify-center text-foreground-muted">
              Loading chart...
            </div>
          </template>
        </ClientOnly>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        No smart contract activity history available yet.
      </div>
    </div>

    <!-- New Users with High Balance History -->
    <div class="card">
      <h2 class="section-title mb-4">
        <UserPlus class="h-5 w-5 text-accent" />
        New Users with High Balance (4-Hour Snapshots)
      </h2>

      <div v-if="networkStatsHistoryLoading" class="loading">Loading...</div>
      <template v-else-if="networkStatsHistory?.length && newUsersHighBalanceChartData.users100M.some(v => v > 0)">
        <!-- Latest new users summary -->
        <div class="grid grid-cols-3 gap-4 mb-4">
          <div class="card-elevated text-center">
            <div class="text-lg font-bold text-accent">
              {{ networkStatsHistory[networkStatsHistory.length - 1]?.newUsers100MPlus?.toLocaleString() || 0 }}
            </div>
            <div class="text-xs text-foreground-muted">New Users ≥100M</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-lg font-bold text-accent">
              {{ networkStatsHistory[networkStatsHistory.length - 1]?.newUsers1BPlus?.toLocaleString() || 0 }}
            </div>
            <div class="text-xs text-foreground-muted">New Users ≥1B</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-lg font-bold text-accent">
              {{ networkStatsHistory[networkStatsHistory.length - 1]?.newUsers10BPlus?.toLocaleString() || 0 }}
            </div>
            <div class="text-xs text-foreground-muted">New Users ≥10B</div>
          </div>
        </div>

        <ClientOnly>
          <ChartsEpochBarChart
            :labels="networkStatsHistoryChartLabels"
            :datasets="[
              {
                label: '≥100M',
                data: newUsersHighBalanceChartData.users100M,
                backgroundColor: 'rgba(16, 185, 129, 0.8)'
              },
              {
                label: '≥1B',
                data: newUsersHighBalanceChartData.users1B,
                backgroundColor: 'rgba(59, 130, 246, 0.8)'
              },
              {
                label: '≥10B',
                data: newUsersHighBalanceChartData.users10B,
                backgroundColor: 'rgba(139, 92, 246, 0.8)'
              }
            ]"
            :height="250"
            :stacked="true"
          />
          <template #fallback>
            <div class="h-[250px] flex items-center justify-center text-foreground-muted">
              Loading chart...
            </div>
          </template>
        </ClientOnly>
        <p class="text-xs text-foreground-muted mt-2">
          New addresses (first seen in each window) that received significant balances. Shows adoption of new high-value users.
        </p>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        No new user data available yet. Snapshots are taken every 4 hours.
      </div>
    </div>
  </div>
</template>
