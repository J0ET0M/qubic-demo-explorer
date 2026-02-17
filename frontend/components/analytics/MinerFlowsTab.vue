<script setup lang="ts">
import { Pickaxe, TrendingDown, Building2, ArrowRight, Eye, Coins, ChevronDown, ChevronUp, Network } from 'lucide-vue-next'
import type { MinerFlowStatsDto, EmissionDetailsDto } from '~/composables/useApi'

const api = useApi()

// Miner flow stats - reactive to time range
const { timeRange } = useTimeRange()
const minerFlowSummary = ref<Awaited<ReturnType<typeof api.getMinerFlowStats>> | null>(null)
const minerFlowLoading = ref(true)

const fetchMinerFlowStats = async () => {
  minerFlowLoading.value = true
  try {
    const { from, to } = timeRange.value
    minerFlowSummary.value = await api.getMinerFlowStats(500, from ?? undefined, to ?? undefined)
  } catch (e) {
    console.error('Failed to fetch miner flow stats', e)
  } finally {
    minerFlowLoading.value = false
  }
}

watch(() => timeRange.value, fetchMinerFlowStats, { deep: true })
await fetchMinerFlowStats()

// Emissions - fetch for the emission epoch (previous epoch from latest flow stats)
const selectedEmissionEpoch = ref<number | null>(null)
const emissionDetails = ref<EmissionDetailsDto | null>(null)
const emissionLoading = ref(false)
const showEmissionDetails = ref(false)

// Initialize selected epoch when flow data loads
watch(() => minerFlowSummary.value?.latest?.emissionEpoch, (epoch) => {
  if (epoch && !selectedEmissionEpoch.value) {
    selectedEmissionEpoch.value = epoch
  }
}, { immediate: true })

// Fetch emission details when epoch changes
const fetchEmissions = async () => {
  if (!selectedEmissionEpoch.value) return
  emissionLoading.value = true
  try {
    emissionDetails.value = await api.getEmissionDetails(selectedEmissionEpoch.value)
  } catch (e) {
    emissionDetails.value = null
  } finally {
    emissionLoading.value = false
  }
}

watch(selectedEmissionEpoch, fetchEmissions, { immediate: true })

// Chart data computed from history
const flowChartLabels = computed(() => {
  if (!minerFlowSummary.value?.history) return []
  return minerFlowSummary.value.history.slice().reverse().map(d => {
    const date = new Date(d.snapshotAt)
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', hour: '2-digit' })
  })
})

const flowChartData = computed(() => {
  if (!minerFlowSummary.value?.history) return { outflow: [], toExchange: [] }
  const reversed = minerFlowSummary.value.history.slice().reverse()
  return {
    outflow: reversed.map(d => d.totalOutflow),
    toExchange: reversed.map(d => d.flowToExchangeTotal)
  }
})

// Hop distribution chart
const hopChartLabels = computed(() => ['Direct', '1 Hop', '2 Hops', '3+ Hops'])

const hopChartData = computed(() => {
  const latest = minerFlowSummary.value?.latest
  if (!latest) return []
  return [
    latest.flowToExchangeDirect,
    latest.flowToExchange1Hop,
    latest.flowToExchange2Hop,
    latest.flowToExchange3Plus
  ]
})

// Volume by hop level (latest snapshot)
const volumeByHop = computed(() => {
  const latest = minerFlowSummary.value?.latest
  if (!latest) return []
  return [
    { hop: 1, volume: latest.hop1Volume },
    { hop: 2, volume: latest.hop2Volume },
    { hop: 3, volume: latest.hop3Volume },
    { hop: '4+', volume: latest.hop4PlusVolume }
  ]
})

// Total exchange flow by hop level (sum across all history)
const totalExchangeByHop = computed(() => {
  const history = minerFlowSummary.value?.history
  if (!history?.length) return { direct: 0, hop1: 0, hop2: 0, hop3Plus: 0, total: 0 }

  const direct = history.reduce((sum, s) => sum + s.flowToExchangeDirect, 0)
  const hop1 = history.reduce((sum, s) => sum + s.flowToExchange1Hop, 0)
  const hop2 = history.reduce((sum, s) => sum + s.flowToExchange2Hop, 0)
  const hop3Plus = history.reduce((sum, s) => sum + s.flowToExchange3Plus, 0)

  return {
    direct,
    hop1,
    hop2,
    hop3Plus,
    total: direct + hop1 + hop2 + hop3Plus
  }
})

const formatVolume = (volume: number) => {
  if (volume >= 1_000_000_000_000) return (volume / 1_000_000_000_000).toFixed(2) + 'T'
  if (volume >= 1_000_000_000) return (volume / 1_000_000_000).toFixed(2) + 'B'
  if (volume >= 1_000_000) return (volume / 1_000_000).toFixed(2) + 'M'
  if (volume >= 1_000) return (volume / 1_000).toFixed(2) + 'K'
  return volume.toLocaleString()
}

const formatPercent = (value: number) => {
  return value.toFixed(2) + '%'
}

const calculateExchangePercent = (stats: MinerFlowStatsDto) => {
  if (stats.totalOutflow === 0) return 0
  return (stats.flowToExchangeTotal / stats.totalOutflow) * 100
}

// For visualization link - use emission epoch
const latestEmissionEpoch = computed(() => minerFlowSummary.value?.latest?.emissionEpoch)
</script>

<template>
  <div class="space-y-6">
    <!-- Summary Cards -->
    <div class="grid grid-cols-1 md:grid-cols-4 gap-4">
      <div class="card">
        <div class="flex items-center gap-2 mb-2">
          <Pickaxe class="h-5 w-5 text-accent" />
          <span class="text-sm text-foreground-muted">Epochs Tracked</span>
        </div>
        <div class="text-2xl font-bold">
          {{ minerFlowLoading ? '...' : minerFlowSummary?.history?.length || 0 }}
        </div>
        <div class="text-xs text-foreground-muted mt-1">
          4-hour snapshots
        </div>
      </div>

      <div class="card">
        <div class="flex items-center gap-2 mb-2">
          <Building2 class="h-5 w-5 text-destructive" />
          <span class="text-sm text-foreground-muted">Total to Exchanges</span>
        </div>
        <div class="text-2xl font-bold text-destructive">
          {{ minerFlowLoading ? '...' : formatVolume(minerFlowSummary?.totalFlowToExchange || 0) }}
        </div>
        <div class="text-xs text-foreground-muted mt-1">
          Avg: {{ formatPercent(minerFlowSummary?.averageExchangeFlowPercent || 0) }} of outflow
        </div>
      </div>

      <div class="card">
        <div class="flex items-center gap-2 mb-2">
          <TrendingDown class="h-5 w-5 text-warning" />
          <span class="text-sm text-foreground-muted">Latest Outflow</span>
        </div>
        <div class="text-2xl font-bold">
          {{ minerFlowLoading ? '...' : formatVolume(minerFlowSummary?.latest?.totalOutflow || 0) }}
        </div>
        <div class="text-xs text-foreground-muted mt-1">
          {{ minerFlowSummary?.latest?.outflowTxCount || 0 }} transactions
        </div>
      </div>

      <div class="card">
        <div class="flex items-center gap-2 mb-2">
          <ArrowRight class="h-5 w-5 text-destructive" />
          <span class="text-sm text-foreground-muted">Latest to Exchange</span>
        </div>
        <div class="text-2xl font-bold text-destructive">
          {{ minerFlowLoading ? '...' : formatVolume(minerFlowSummary?.latest?.flowToExchangeTotal || 0) }}
        </div>
        <div class="text-xs text-foreground-muted mt-1">
          {{ minerFlowSummary?.latest?.flowToExchangeCount || 0 }} transactions
        </div>
      </div>
    </div>

    <!-- Flow Charts -->
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <!-- Outflow vs Exchange Flow Over Time -->
      <div class="card">
        <h2 class="section-title mb-4">
          <Pickaxe class="h-5 w-5 text-accent" />
          Computor Outflow & Exchange Flow History
        </h2>

        <div v-if="minerFlowLoading" class="loading">Loading...</div>
        <template v-else-if="flowChartLabels.length > 0">
          <ClientOnly>
            <ChartsEpochLineChart
              :labels="flowChartLabels"
              :datasets="[
                {
                  label: 'Computor Outflow',
                  data: flowChartData.outflow,
                  borderColor: 'rgb(99, 102, 241)',
                  backgroundColor: 'rgba(99, 102, 241, 0.1)'
                },
                {
                  label: 'Flow to Exchanges',
                  data: flowChartData.toExchange,
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
        </template>
        <div v-else class="text-center py-8 text-foreground-muted">
          No miner flow data available yet. Snapshots are taken every 4 hours.
        </div>
      </div>

      <!-- Flow to Exchange by Hop Level -->
      <div class="card">
        <h2 class="section-title mb-4">
          <Building2 class="h-5 w-5 text-accent" />
          Exchange Flow by Hop Level
        </h2>

        <div v-if="minerFlowLoading" class="loading">Loading...</div>
        <template v-else-if="minerFlowSummary?.history?.length">
          <!-- Total Exchange Flow Summary (all epochs) -->
          <div class="mb-6 p-4 bg-surface-elevated rounded-lg border border-border">
            <h3 class="text-sm font-medium text-foreground-muted mb-3">Total Exchange Flow (All Tracked Epochs)</h3>
            <div class="grid grid-cols-2 sm:grid-cols-5 gap-3">
              <div class="text-center">
                <div class="text-xl font-bold text-success">{{ formatVolume(totalExchangeByHop.direct) }}</div>
                <div class="text-xs text-foreground-muted">Direct</div>
              </div>
              <div class="text-center">
                <div class="text-xl font-bold text-warning">{{ formatVolume(totalExchangeByHop.hop1) }}</div>
                <div class="text-xs text-foreground-muted">1 Hop</div>
              </div>
              <div class="text-center">
                <div class="text-xl font-bold text-accent">{{ formatVolume(totalExchangeByHop.hop2) }}</div>
                <div class="text-xs text-foreground-muted">2 Hops</div>
              </div>
              <div class="text-center">
                <div class="text-xl font-bold text-destructive">{{ formatVolume(totalExchangeByHop.hop3Plus) }}</div>
                <div class="text-xs text-foreground-muted">3+ Hops</div>
              </div>
              <div class="text-center border-l border-border pl-3">
                <div class="text-xl font-bold text-foreground">{{ formatVolume(totalExchangeByHop.total) }}</div>
                <div class="text-xs text-foreground-muted">Total</div>
              </div>
            </div>
          </div>

          <!-- Latest Snapshot Values -->
          <h3 class="text-sm font-medium text-foreground-muted mb-3">Latest Snapshot</h3>
          <div class="grid grid-cols-2 gap-4 mb-4">
            <div v-for="(item, idx) in [
              { label: 'Direct', value: minerFlowSummary.latest?.flowToExchangeDirect || 0, color: 'text-success' },
              { label: '1 Hop', value: minerFlowSummary.latest?.flowToExchange1Hop || 0, color: 'text-warning' },
              { label: '2 Hops', value: minerFlowSummary.latest?.flowToExchange2Hop || 0, color: 'text-accent' },
              { label: '3+ Hops', value: minerFlowSummary.latest?.flowToExchange3Plus || 0, color: 'text-destructive' }
            ]" :key="idx" class="card-elevated text-center">
              <div :class="['text-lg font-bold', item.color]">{{ formatVolume(item.value) }}</div>
              <div class="text-xs text-foreground-muted">{{ item.label }}</div>
            </div>
          </div>

          <!-- Volume by hop level -->
          <h3 class="text-sm font-medium text-foreground-muted mb-2">Total Volume by Hop Level</h3>
          <div class="space-y-2">
            <div v-for="item in volumeByHop" :key="item.hop" class="flex items-center gap-2">
              <span class="text-sm text-foreground-muted w-16">Hop {{ item.hop }}</span>
              <div class="flex-1 h-6 bg-surface rounded overflow-hidden">
                <div
                  class="h-full bg-accent transition-all"
                  :style="{
                    width: `${Math.min(100, (item.volume / (minerFlowSummary?.latest?.hop1Volume || 1)) * 100)}%`
                  }"
                />
              </div>
              <span class="text-sm font-medium w-24 text-right">{{ formatVolume(item.volume) }}</span>
            </div>
          </div>
        </template>
        <div v-else class="text-center py-8 text-foreground-muted">
          No hop-level data available yet.
        </div>
      </div>
    </div>

    <!-- Flow Visualization Link -->
    <div class="card bg-accent/5 border-accent/20">
      <div class="flex items-center justify-between flex-wrap gap-4">
        <div>
          <h2 class="section-title mb-2">
            <Network class="h-5 w-5 text-accent" />
            Flow Visualization
          </h2>
          <p class="text-sm text-foreground-muted">
            View detailed flow diagrams showing how emission flows from computors through intermediaries to exchanges.
          </p>
        </div>
        <NuxtLink
          v-if="latestEmissionEpoch"
          :to="`/analytics/miner-flow/${latestEmissionEpoch}`"
          class="btn btn-primary flex items-center gap-2"
        >
          <Eye class="h-4 w-4" />
          View Flow Diagram
        </NuxtLink>
      </div>
    </div>

    <!-- Emissions Section -->
    <div class="card">
      <div class="flex items-center justify-between mb-4">
        <h2 class="section-title">
          <Coins class="h-5 w-5 text-accent" />
          Epoch Emissions
        </h2>
        <div class="flex items-center gap-2">
          <label class="text-sm text-foreground-muted">Epoch:</label>
          <input
            v-model.number="selectedEmissionEpoch"
            type="number"
            class="input w-24 text-center"
            :min="1"
          />
        </div>
      </div>

      <div v-if="emissionLoading" class="loading">Loading emissions...</div>
      <template v-else-if="emissionDetails">
        <!-- Emission Summary -->
        <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold text-success">{{ formatVolume(emissionDetails.totalEmission) }}</div>
            <div class="text-xs text-foreground-muted">Total Emission</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold">{{ emissionDetails.computorCount }}</div>
            <div class="text-xs text-foreground-muted">Computors</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold">{{ emissionDetails.emissionTick?.toLocaleString() }}</div>
            <div class="text-xs text-foreground-muted">Emission Tick</div>
          </div>
        </div>

        <!-- Collapsible Computor List -->
        <div class="border border-border rounded-lg overflow-hidden">
          <button
            @click="showEmissionDetails = !showEmissionDetails"
            class="w-full flex items-center justify-between px-4 py-3 bg-surface hover:bg-surface-hover transition-colors"
          >
            <span class="font-medium">Computor Emissions ({{ emissionDetails.emissions?.length || 0 }})</span>
            <component :is="showEmissionDetails ? ChevronUp : ChevronDown" class="h-5 w-5" />
          </button>

          <div v-if="showEmissionDetails" class="max-h-96 overflow-y-auto">
            <table class="w-full">
              <thead class="sticky top-0 bg-surface">
                <tr>
                  <th class="text-left px-4 py-2 text-sm">#</th>
                  <th class="text-left px-4 py-2 text-sm">Address</th>
                  <th class="text-left px-4 py-2 text-sm">Label</th>
                  <th class="text-right px-4 py-2 text-sm">Emission</th>
                </tr>
              </thead>
              <tbody>
                <tr
                  v-for="emission in emissionDetails.emissions"
                  :key="emission.computorIndex"
                  class="border-t border-border hover:bg-surface-hover"
                >
                  <td class="px-4 py-2 text-sm text-foreground-muted">{{ emission.computorIndex }}</td>
                  <td class="px-4 py-2">
                    <NuxtLink
                      :to="`/address/${emission.address}`"
                      class="font-mono text-sm text-accent hover:underline"
                    >
                      {{ emission.address.slice(0, 10) }}...{{ emission.address.slice(-6) }}
                    </NuxtLink>
                  </td>
                  <td class="px-4 py-2 text-sm">{{ emission.label || '-' }}</td>
                  <td class="px-4 py-2 text-right text-sm text-success">{{ formatVolume(emission.emissionAmount) }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

        <p class="text-xs text-foreground-muted mt-2">
          Imported at: {{ new Date(emissionDetails.importedAt).toLocaleString() }}
        </p>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        <Coins class="h-12 w-12 mx-auto mb-4 opacity-50" />
        <p class="mb-2">No emission data available for epoch {{ selectedEmissionEpoch }}</p>
        <p class="text-sm">Emissions are captured when an epoch completes.</p>
      </div>
    </div>

    <!-- History Table -->
    <div class="card">
      <div class="flex items-center justify-between mb-4">
        <h2 class="section-title">
          <Pickaxe class="h-5 w-5 text-accent" />
          Miner Flow History (4-Hour Snapshots)
        </h2>
        <NuxtLink
          v-if="latestEmissionEpoch"
          :to="`/analytics/miner-flow/${latestEmissionEpoch}`"
          class="btn btn-sm btn-primary flex items-center gap-1"
        >
          <Eye class="h-4 w-4" />
          View Flow Visualization
        </NuxtLink>
      </div>

      <div v-if="minerFlowLoading" class="loading">Loading...</div>
      <template v-else-if="minerFlowSummary?.history?.length">
        <div class="table-wrapper">
          <table>
            <thead>
              <tr>
                <th>Snapshot</th>
                <th>Epoch</th>
                <th>Emission Epoch</th>
                <th>Outflow</th>
                <th>To Exchange</th>
                <th>Exchange %</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="stats in minerFlowSummary.history.slice(0, 10)" :key="`${stats.epoch}-${stats.tickStart}`">
                <td class="text-sm">
                  {{ new Date(stats.snapshotAt).toLocaleDateString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) }}
                </td>
                <td class="font-medium">{{ stats.epoch }}</td>
                <td class="text-foreground-muted">{{ stats.emissionEpoch }}</td>
                <td>{{ formatVolume(stats.totalOutflow) }}</td>
                <td class="text-destructive">{{ formatVolume(stats.flowToExchangeTotal) }}</td>
                <td :class="calculateExchangePercent(stats) > 50 ? 'text-destructive' : 'text-foreground-muted'">
                  {{ formatPercent(calculateExchangePercent(stats)) }}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
        <p class="text-xs text-foreground-muted mt-2">
          Showing latest 10 snapshots. Each snapshot tracks computor outflow within a 4-hour window and flow to exchanges (up to 10 hops).
        </p>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        <Pickaxe class="h-12 w-12 mx-auto mb-4 opacity-50" />
        <p class="mb-2">No miner flow data available</p>
        <p class="text-sm">Flow analysis runs every 4 hours to track computor emission distribution.</p>
      </div>
    </div>

    <!-- Info Card -->
    <div class="card bg-accent/5 border-accent/20">
      <h3 class="font-medium mb-2">About Miner Flow Tracking</h3>
      <div class="text-sm text-foreground-muted space-y-2">
        <p>
          This feature tracks how Qubic emission flows from computors (miners) through the network.
          Revenue distribution happens at the end of each epoch (in the last tick via logs/events).
        </p>
        <p>
          The flow analysis tracks transfers up to 10 hops from the original computor addresses,
          identifying how much eventually reaches exchanges (potential sell pressure).
        </p>
        <ul class="list-disc list-inside space-y-1 mt-2">
          <li><strong>Direct:</strong> Computor sends directly to an exchange</li>
          <li><strong>1 Hop:</strong> Computor → Intermediary → Exchange</li>
          <li><strong>2 Hops:</strong> Computor → A → B → Exchange</li>
          <li><strong>3+ Hops:</strong> Longer chains before reaching exchange</li>
        </ul>
      </div>
    </div>
  </div>
</template>
