<script setup lang="ts">
import { PieChart, History, TrendingUp } from 'lucide-vue-next'

const api = useApi()

// Current holder distribution
const { data: holderDistribution, pending: holderDistributionLoading } = await useAsyncData(
  'holders-distribution',
  () => api.getHolderDistribution()
)

// Holder distribution history (periodic snapshots) - reactive to time range
const { timeRange } = useTimeRange()
const holderDistributionHistory = ref<Awaited<ReturnType<typeof api.getHolderDistributionHistory>>>([])
const holderDistributionHistoryLoading = ref(true)

const fetchHolderHistory = async () => {
  holderDistributionHistoryLoading.value = true
  try {
    const { from, to } = timeRange.value
    holderDistributionHistory.value = await api.getHolderDistributionHistory(500, from ?? undefined, to ?? undefined)
  } catch (e) {
    console.error('Failed to fetch holder distribution history', e)
  } finally {
    holderDistributionHistoryLoading.value = false
  }
}

watch(() => timeRange.value, fetchHolderHistory, { deep: true })
await fetchHolderHistory()

// Holder distribution chart data
const holderDistributionChartLabels = computed(() => {
  if (!holderDistribution.value?.brackets) return []
  return holderDistribution.value.brackets.map(b => b.name)
})

const holderDistributionChartData = computed(() => {
  if (!holderDistribution.value?.brackets) return []
  return holderDistribution.value.brackets.map(b => b.count)
})

// Holder distribution history chart data
const holderHistoryChartLabels = computed(() => {
  if (!holderDistributionHistory.value) return []
  return holderDistributionHistory.value.map(d => {
    // Show snapshot time for windowed data, or epoch for epoch-based data
    if (d.tickStart > 0 && d.tickEnd > 0) {
      return new Date(d.snapshotAt).toLocaleDateString('en-US', { month: 'short', day: 'numeric', hour: '2-digit' })
    }
    return `E${d.epoch}`
  })
})

const holderHistoryChartData = computed(() => {
  if (!holderDistributionHistory.value) return { whales: [], large: [], medium: [], small: [], micro: [], totalHolders: [] }
  return {
    whales: holderDistributionHistory.value.map(d => d.brackets.find(b => b.name.includes('100B'))?.count || 0),
    large: holderDistributionHistory.value.map(d => d.brackets.find(b => b.name.includes('20B'))?.count || 0),
    medium: holderDistributionHistory.value.map(d => d.brackets.find(b => b.name.includes('5B'))?.count || 0),
    small: holderDistributionHistory.value.map(d => d.brackets.find(b => b.name.includes('500M'))?.count || 0),
    micro: holderDistributionHistory.value.map(d => d.brackets.find(b => b.name.includes('<'))?.count || 0),
    totalHolders: holderDistributionHistory.value.map(d => d.totalHolders)
  }
})

// Concentration metrics history (top 10/50/100 balances)
const concentrationChartData = computed(() => {
  if (!holderDistributionHistory.value) return { top10: [], top50: [], top100: [] }
  return {
    top10: holderDistributionHistory.value.map(d => d.concentration?.top10Balance || 0),
    top50: holderDistributionHistory.value.map(d => d.concentration?.top50Balance || 0),
    top100: holderDistributionHistory.value.map(d => d.concentration?.top100Balance || 0)
  }
})

// Concentration percentages
const concentrationPercentData = computed(() => {
  if (!holderDistributionHistory.value) return { top10: [], top50: [], top100: [] }
  return {
    top10: holderDistributionHistory.value.map(d => d.concentration?.top10Percent || 0),
    top50: holderDistributionHistory.value.map(d => d.concentration?.top50Percent || 0),
    top100: holderDistributionHistory.value.map(d => d.concentration?.top100Percent || 0)
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
    <!-- Current Holder Distribution -->
    <div class="card">
      <h2 class="section-title mb-4">
        <PieChart class="h-5 w-5 text-accent" />
        Current Holder Distribution
      </h2>

      <div v-if="holderDistributionLoading" class="loading">Loading...</div>
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

        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div>
            <ClientOnly>
              <ChartsDoughnutChart
                :labels="holderDistributionChartLabels"
                :data="holderDistributionChartData"
                :height="250"
              />
              <template #fallback>
                <div class="h-[250px] flex items-center justify-center text-foreground-muted">
                  Loading chart...
                </div>
              </template>
            </ClientOnly>
          </div>

          <!-- Breakdown table -->
          <div class="table-wrapper">
            <table class="text-sm">
              <thead>
                <tr>
                  <th>Bracket</th>
                  <th>Count</th>
                  <th>Balance</th>
                  <th>%</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="bracket in holderDistribution.brackets" :key="bracket.name">
                  <td>{{ bracket.name }}</td>
                  <td>{{ bracket.count.toLocaleString() }}</td>
                  <td>{{ formatVolume(bracket.balance) }}</td>
                  <td>{{ bracket.percentageOfSupply.toFixed(1) }}%</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        No holder distribution data available
      </div>
    </div>

    <!-- Holder Distribution History (Periodic Snapshots) -->
    <div class="card">
      <h2 class="section-title mb-4">
        <History class="h-5 w-5 text-accent" />
        Holder Count History (4-Hour Snapshots)
      </h2>

      <div v-if="holderDistributionHistoryLoading" class="loading">Loading...</div>
      <template v-else-if="holderDistributionHistory?.length">
        <ClientOnly>
          <ChartsEpochLineChart
            :labels="holderHistoryChartLabels"
            :datasets="[
              {
                label: 'Total Holders',
                data: holderHistoryChartData.totalHolders,
                borderColor: 'rgb(59, 130, 246)',
                backgroundColor: 'rgba(59, 130, 246, 0.1)'
              },
              {
                label: 'Whales (≥100B)',
                data: holderHistoryChartData.whales,
                borderColor: 'rgb(239, 68, 68)',
                backgroundColor: 'rgba(239, 68, 68, 0.1)'
              },
              {
                label: 'Large (20B-100B)',
                data: holderHistoryChartData.large,
                borderColor: 'rgb(245, 158, 11)',
                backgroundColor: 'rgba(245, 158, 11, 0.1)'
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
          Historical snapshots taken every 4 hours showing holder distribution trends over time.
        </p>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        No historical holder distribution available yet. Snapshots are taken every 4 hours.
      </div>
    </div>

    <!-- Concentration Metrics History (Top 10/50/100 Balance) -->
    <div class="card">
      <h2 class="section-title mb-4">
        <TrendingUp class="h-5 w-5 text-accent" />
        Wealth Concentration History
      </h2>

      <div v-if="holderDistributionHistoryLoading" class="loading">Loading...</div>
      <template v-else-if="holderDistributionHistory?.length && concentrationChartData.top10.some(v => v > 0)">
        <!-- Current concentration stats -->
        <div class="grid grid-cols-3 gap-4 mb-4">
          <div class="card-elevated text-center">
            <div class="text-xl font-bold text-accent">
              {{ holderDistributionHistory[holderDistributionHistory.length - 1]?.concentration?.top10Percent?.toFixed(1) || 0 }}%
            </div>
            <div class="text-xs text-foreground-muted">Top 10 Holders</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-xl font-bold text-accent">
              {{ holderDistributionHistory[holderDistributionHistory.length - 1]?.concentration?.top50Percent?.toFixed(1) || 0 }}%
            </div>
            <div class="text-xs text-foreground-muted">Top 50 Holders</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-xl font-bold text-accent">
              {{ holderDistributionHistory[holderDistributionHistory.length - 1]?.concentration?.top100Percent?.toFixed(1) || 0 }}%
            </div>
            <div class="text-xs text-foreground-muted">Top 100 Holders</div>
          </div>
        </div>

        <ClientOnly>
          <ChartsEpochLineChart
            :labels="holderHistoryChartLabels"
            :datasets="[
              {
                label: 'Top 10 %',
                data: concentrationPercentData.top10,
                borderColor: 'rgb(239, 68, 68)',
                backgroundColor: 'rgba(239, 68, 68, 0.1)'
              },
              {
                label: 'Top 50 %',
                data: concentrationPercentData.top50,
                borderColor: 'rgb(245, 158, 11)',
                backgroundColor: 'rgba(245, 158, 11, 0.1)'
              },
              {
                label: 'Top 100 %',
                data: concentrationPercentData.top100,
                borderColor: 'rgb(59, 130, 246)',
                backgroundColor: 'rgba(59, 130, 246, 0.1)'
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
          Shows the percentage of total supply held by top holders over time. Lower values indicate more decentralization.
        </p>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        No concentration metrics available yet. Snapshots are taken every 4 hours.
      </div>
    </div>
  </div>
</template>
