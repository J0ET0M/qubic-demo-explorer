<script setup lang="ts">
import { Users, Activity, Building2, PieChart } from 'lucide-vue-next'

const api = useApi()

// Only fetch summary stats for overview
const { data: holderDistribution, pending: holderLoading } = await useAsyncData(
  'holders-distribution',
  () => api.getHolderDistribution()
)

const { data: exchangeFlows, pending: exchangeLoading } = await useAsyncData(
  'overview-exchange-flows',
  () => api.getExchangeFlows(7)
)

const { data: activeAddresses, pending: activeLoading } = await useAsyncData(
  'overview-active-addresses',
  () => api.getActiveAddressTrends('epoch', 7)
)

const { data: avgTxSize, pending: txSizeLoading } = await useAsyncData(
  'overview-avg-tx-size',
  () => api.getAvgTxSizeTrends('epoch', 7)
)

const { formatVolume } = useFormatting()

// Calculate recent stats
const recentSenders = computed(() => {
  if (!activeAddresses.value?.length) return 0
  return activeAddresses.value[activeAddresses.value.length - 1]?.uniqueSenders || 0
})

const recentReceivers = computed(() => {
  if (!activeAddresses.value?.length) return 0
  return activeAddresses.value[activeAddresses.value.length - 1]?.uniqueReceivers || 0
})

const recentAvgTxSize = computed(() => {
  if (!avgTxSize.value?.length) return 0
  return avgTxSize.value[avgTxSize.value.length - 1]?.avgTxSize || 0
})
</script>

<template>
  <div class="space-y-6">
    <!-- Summary Stats Cards -->
    <div class="grid grid-cols-2 lg:grid-cols-4 gap-4">
      <!-- Total Holders -->
      <div class="card text-center">
        <PieChart class="h-8 w-8 text-accent mx-auto mb-2" />
        <div v-if="holderLoading" class="loading-sm">...</div>
        <template v-else>
          <div class="text-2xl font-bold text-accent">
            {{ holderDistribution?.totalHolders?.toLocaleString() || '-' }}
          </div>
          <div class="text-sm text-foreground-muted">Total Holders</div>
        </template>
      </div>

      <!-- Active Senders -->
      <div class="card text-center">
        <Users class="h-8 w-8 text-success mx-auto mb-2" />
        <div v-if="activeLoading" class="loading-sm">...</div>
        <template v-else>
          <div class="text-2xl font-bold text-success">
            {{ recentSenders.toLocaleString() }}
          </div>
          <div class="text-sm text-foreground-muted">Active Senders (Last Epoch)</div>
        </template>
      </div>

      <!-- Active Receivers -->
      <div class="card text-center">
        <Users class="h-8 w-8 text-info mx-auto mb-2" />
        <div v-if="activeLoading" class="loading-sm">...</div>
        <template v-else>
          <div class="text-2xl font-bold text-info">
            {{ recentReceivers.toLocaleString() }}
          </div>
          <div class="text-sm text-foreground-muted">Active Receivers (Last Epoch)</div>
        </template>
      </div>

      <!-- Avg Tx Size -->
      <div class="card text-center">
        <Activity class="h-8 w-8 text-warning mx-auto mb-2" />
        <div v-if="txSizeLoading" class="loading-sm">...</div>
        <template v-else>
          <div class="text-2xl font-bold text-warning">
            {{ formatVolume(recentAvgTxSize) }}
          </div>
          <div class="text-sm text-foreground-muted">Avg Tx Size (Last Epoch)</div>
        </template>
      </div>
    </div>

    <!-- Exchange Flow Summary -->
    <div class="card">
      <h2 class="section-title mb-4">
        <Building2 class="h-5 w-5 text-accent" />
        Exchange Flow Summary (Last 7 Epochs)
      </h2>

      <div v-if="exchangeLoading" class="loading">Loading...</div>
      <template v-else-if="exchangeFlows?.dataPoints?.length">
        <div class="grid grid-cols-3 gap-4">
          <div class="card-elevated text-center">
            <div class="text-xl font-bold text-success">{{ formatVolume(exchangeFlows.totalInflow) }}</div>
            <div class="text-xs text-foreground-muted">Total Inflow</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-xl font-bold text-destructive">{{ formatVolume(exchangeFlows.totalOutflow) }}</div>
            <div class="text-xs text-foreground-muted">Total Outflow</div>
          </div>
          <div class="card-elevated text-center">
            <div :class="['text-xl font-bold', exchangeFlows.totalInflow > exchangeFlows.totalOutflow ? 'text-success' : 'text-destructive']">
              {{ formatVolume(Math.abs(exchangeFlows.totalInflow - exchangeFlows.totalOutflow)) }}
            </div>
            <div class="text-xs text-foreground-muted">Net Flow</div>
          </div>
        </div>
      </template>
      <div v-else class="text-center py-4 text-foreground-muted">
        No exchange flow data available
      </div>
    </div>

    <!-- Holder Distribution Summary -->
    <div class="card">
      <h2 class="section-title mb-4">
        <PieChart class="h-5 w-5 text-accent" />
        Holder Distribution
      </h2>

      <div v-if="holderLoading" class="loading">Loading...</div>
      <template v-else-if="holderDistribution?.brackets?.length">
        <div class="grid grid-cols-2 gap-4 mb-4">
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold text-accent">{{ holderDistribution.totalHolders.toLocaleString() }}</div>
            <div class="text-xs text-foreground-muted">Total Holders</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold text-accent">{{ formatVolume(holderDistribution.totalBalance) }}</div>
            <div class="text-xs text-foreground-muted">Total Balance</div>
          </div>
        </div>

        <ClientOnly>
          <ChartsDoughnutChart
            :labels="holderDistribution.brackets.map(b => b.name)"
            :data="holderDistribution.brackets.map(b => b.count)"
            :height="200"
          />
          <template #fallback>
            <div class="h-[200px] flex items-center justify-center text-foreground-muted">
              Loading chart...
            </div>
          </template>
        </ClientOnly>
      </template>
      <div v-else class="text-center py-4 text-foreground-muted">
        No holder distribution data available
      </div>
    </div>

  </div>
</template>
