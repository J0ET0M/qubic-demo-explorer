<script setup lang="ts">
import { Building2, History } from 'lucide-vue-next'

const api = useApi()

// Exchange flows (per epoch)
const { data: exchangeFlows, pending: exchangeFlowsLoading } = await useAsyncData(
  'exchange-flows-full',
  () => api.getExchangeFlows(30)
)

// Network stats history (4-hour snapshots) for exchange flow history
const { data: networkStatsHistory, pending: networkStatsHistoryLoading } = await useAsyncData(
  'exchange-network-stats-history',
  () => api.getNetworkStatsHistory(30)
)

// Exchange flow chart data (per epoch)
const exchangeFlowChartLabels = computed(() => {
  if (!exchangeFlows.value?.dataPoints) return []
  return exchangeFlows.value.dataPoints.map(d => d.epoch.toString())
})

const exchangeFlowChartData = computed(() => {
  if (!exchangeFlows.value?.dataPoints) return { inflow: [], outflow: [], netflow: [] }
  return {
    inflow: exchangeFlows.value.dataPoints.map(d => d.inflowVolume),
    outflow: exchangeFlows.value.dataPoints.map(d => d.outflowVolume),
    netflow: exchangeFlows.value.dataPoints.map(d => d.netFlow)
  }
})

// Exchange flow history chart data (4-hour snapshots)
const exchangeFlowHistoryChartLabels = computed(() => {
  if (!networkStatsHistory.value) return []
  return networkStatsHistory.value.map(d => {
    if (d.tickStart > 0 && d.tickEnd > 0) {
      return new Date(d.snapshotAt).toLocaleDateString('en-US', { month: 'short', day: 'numeric', hour: '2-digit' })
    }
    return `E${d.epoch}`
  })
})

const exchangeFlowHistoryChartData = computed(() => {
  if (!networkStatsHistory.value) return { inflow: [], outflow: [], netFlow: [] }
  return {
    inflow: networkStatsHistory.value.map(d => d.exchangeInflowVolume),
    outflow: networkStatsHistory.value.map(d => d.exchangeOutflowVolume),
    netFlow: networkStatsHistory.value.map(d => d.exchangeNetFlow)
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
    <!-- Exchange Flows -->
    <div class="card">
      <h2 class="section-title mb-4">
        <Building2 class="h-5 w-5 text-accent" />
        Exchange Inflows/Outflows
      </h2>

      <div v-if="exchangeFlowsLoading" class="loading">Loading...</div>
      <template v-else-if="exchangeFlows?.dataPoints?.length">
        <!-- Summary stats -->
        <div class="grid grid-cols-3 gap-4 mb-6">
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold text-success">{{ formatVolume(exchangeFlows.totalInflow) }}</div>
            <div class="text-sm text-foreground-muted">Total Inflow</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold text-destructive">{{ formatVolume(exchangeFlows.totalOutflow) }}</div>
            <div class="text-sm text-foreground-muted">Total Outflow</div>
          </div>
          <div class="card-elevated text-center">
            <div :class="['text-2xl font-bold', exchangeFlows.totalInflow > exchangeFlows.totalOutflow ? 'text-success' : 'text-destructive']">
              {{ formatVolume(Math.abs(exchangeFlows.totalInflow - exchangeFlows.totalOutflow)) }}
            </div>
            <div class="text-sm text-foreground-muted">Net Flow</div>
          </div>
        </div>

        <ClientOnly>
          <ChartsEpochLineChart
            :labels="exchangeFlowChartLabels"
            :datasets="[
              {
                label: 'Inflow',
                data: exchangeFlowChartData.inflow,
                borderColor: 'rgb(16, 185, 129)',
                backgroundColor: 'rgba(16, 185, 129, 0.1)'
              },
              {
                label: 'Outflow',
                data: exchangeFlowChartData.outflow,
                borderColor: 'rgb(239, 68, 68)',
                backgroundColor: 'rgba(239, 68, 68, 0.1)'
              }
            ]"
            :height="300"
          />
          <template #fallback>
            <div class="h-[300px] flex items-center justify-center text-foreground-muted">
              Loading chart...
            </div>
          </template>
        </ClientOnly>

        <!-- Per-epoch breakdown table -->
        <div class="table-wrapper mt-6">
          <table>
            <thead>
              <tr>
                <th>Epoch</th>
                <th>Inflow</th>
                <th>Outflow</th>
                <th>Net Flow</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="point in exchangeFlows.dataPoints.slice().reverse().slice(0, 10)" :key="point.epoch">
                <td class="font-medium">{{ point.epoch }}</td>
                <td class="text-success">{{ formatVolume(point.inflowVolume) }}</td>
                <td class="text-destructive">{{ formatVolume(point.outflowVolume) }}</td>
                <td :class="point.netFlow >= 0 ? 'text-success' : 'text-destructive'">
                  {{ point.netFlow >= 0 ? '+' : '' }}{{ formatVolume(point.netFlow) }}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        <Building2 class="h-12 w-12 mx-auto mb-4 opacity-50" />
        <p class="mb-2">No exchange flow data available</p>
        <p class="text-sm">Add exchange labels to addresses to enable this feature.</p>
        <NuxtLink to="/addresses" class="btn btn-primary mt-4">
          Manage Address Labels
        </NuxtLink>
      </div>
    </div>

    <!-- Exchange Flow History (4-Hour Snapshots) -->
    <div class="card">
      <h2 class="section-title mb-4">
        <History class="h-5 w-5 text-accent" />
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
            <div class="text-xs text-foreground-muted">Latest Inflow</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-lg font-bold text-destructive">
              {{ formatVolume(networkStatsHistory[networkStatsHistory.length - 1]?.exchangeOutflowVolume || 0) }}
            </div>
            <div class="text-xs text-foreground-muted">Latest Outflow</div>
          </div>
          <div class="card-elevated text-center">
            <div :class="['text-lg font-bold', (networkStatsHistory[networkStatsHistory.length - 1]?.exchangeNetFlow || 0) >= 0 ? 'text-success' : 'text-destructive']">
              {{ formatVolume(Math.abs(networkStatsHistory[networkStatsHistory.length - 1]?.exchangeNetFlow || 0)) }}
            </div>
            <div class="text-xs text-foreground-muted">Latest Net Flow</div>
          </div>
        </div>

        <ClientOnly>
          <ChartsEpochLineChart
            :labels="exchangeFlowHistoryChartLabels"
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
            :height="300"
          />
          <template #fallback>
            <div class="h-[300px] flex items-center justify-center text-foreground-muted">
              Loading chart...
            </div>
          </template>
        </ClientOnly>
        <p class="text-xs text-foreground-muted mt-2">
          Historical snapshots taken every 4 hours showing exchange flow trends.
        </p>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        No exchange flow history available yet. Snapshots are taken every 4 hours.
      </div>
    </div>
  </div>
</template>
