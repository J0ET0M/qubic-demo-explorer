<script setup lang="ts">
import { Monitor, Search, ArrowUpDown, BarChart3 } from 'lucide-vue-next'

const api = useApi()
const { formatAmount, formatVolume, formatEpochDate } = useFormatting()

const { data: revenueData, status } = await useAsyncData('computor-revenue', () => api.getComputorRevenue())

const loading = computed(() => status.value === 'pending')

// Tick vote progression chart
const tickVoteEpoch = computed(() => revenueData.value?.epoch)

const { data: tickVotes, pending: tickVotesLoading } = await useAsyncData(
  () => `tick-votes-${tickVoteEpoch.value}`,
  () => tickVoteEpoch.value ? api.getTickVotes(tickVoteEpoch.value) : Promise.resolve(null),
  { watch: [tickVoteEpoch], immediate: !!tickVoteEpoch.value }
)

const tickVoteChartLabels = computed(() => {
  if (!tickVotes.value?.summary) return []
  return tickVotes.value.summary.map(w => w.tick.toLocaleString())
})

const tickVoteChartData = computed(() => {
  if (!tickVotes.value?.summary) return { quorum: [], avg: [], min: [], max: [] }
  return {
    quorum: tickVotes.value.summary.map(w => w.quorumThreshold),
    avg: tickVotes.value.summary.map(w => w.avgVotes),
    min: tickVotes.value.summary.map(w => w.minVotes),
    max: tickVotes.value.summary.map(w => w.maxVotes),
  }
})

// Search / sort state
const searchQuery = ref('')
const sortKey = ref<string>('computorIndex')
const sortAsc = ref(true)

// Active computors (revenue > 0)
const activeCount = computed(() =>
  revenueData.value?.computors.filter(c => c.revenue > 0).length ?? 0
)

// Max possible revenue per computor
const maxRevenue = computed(() =>
  revenueData.value ? Math.floor(revenueData.value.issuanceRate / revenueData.value.computorCount) : 0
)

// Revenue stats: min, max, average (only among active computors)
const revenueStats = computed(() => {
  const active = revenueData.value?.computors.filter(c => c.revenue > 0) ?? []
  if (active.length === 0) return { min: 0, max: 0, avg: 0, avgPct: 0 }
  const revenues = active.map(c => c.revenue)
  const min = Math.min(...revenues)
  const max = Math.max(...revenues)
  const avg = revenues.reduce((a, b) => a + b, 0) / revenues.length
  const maxPossible = maxRevenue.value || 1
  return { min, max, avg, avgPct: (avg / maxPossible) * 100 }
})

// Revenue percentage of max possible
const revenuePct = (revenue: number) => {
  const m = maxRevenue.value
  return m > 0 ? ((revenue / m) * 100).toFixed(2) : '0.00'
}

// Factor as percentage
const factorPct = (factor: number) => ((factor / 1024) * 100).toFixed(1)

// Color class for factor percentage
const factorClass = (factor: number) => {
  if (factor >= 1024) return 'text-success'
  if (factor > 0) return 'text-warning'
  return 'text-destructive'
}

// Filtered + sorted computors
const filteredComputors = computed(() => {
  let list = revenueData.value?.computors ?? []

  // Search
  if (searchQuery.value) {
    const q = searchQuery.value.toLowerCase()
    list = list.filter(c =>
      c.address.toLowerCase().includes(q) ||
      c.label?.toLowerCase().includes(q) ||
      String(c.computorIndex).includes(q)
    )
  }

  // Sort
  const key = sortKey.value
  const asc = sortAsc.value
  return [...list].sort((a, b) => {
    const va = (a as any)[key] ?? 0
    const vb = (b as any)[key] ?? 0
    if (va < vb) return asc ? -1 : 1
    if (va > vb) return asc ? 1 : -1
    return 0
  })
})

// Pagination
const page = ref(1)
const pageSize = 50
const totalPages = computed(() => Math.ceil(filteredComputors.value.length / pageSize))
const paginatedComputors = computed(() => {
  const start = (page.value - 1) * pageSize
  return filteredComputors.value.slice(start, start + pageSize)
})

// Sort handler
const toggleSort = (key: string) => {
  if (sortKey.value === key) {
    sortAsc.value = !sortAsc.value
  } else {
    sortKey.value = key
    sortAsc.value = key === 'computorIndex'
  }
  page.value = 1
}

// Reset page on search change
watch(searchQuery, () => { page.value = 1 })

const { truncateAddress } = useFormatting()
</script>

<template>
  <div class="space-y-6">
    <!-- Overview -->
    <div class="card">
      <h2 class="section-title mb-4">
        <Monitor class="h-5 w-5 text-accent" />
        Revenue Overview
      </h2>

      <div v-if="loading" class="loading">Loading...</div>
      <template v-else-if="revenueData">
        <!-- Summary cards -->
        <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold text-accent">E{{ revenueData.epoch }}</div>
            <div class="text-sm text-foreground-muted">{{ formatEpochDate(revenueData.epoch) }}</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold text-success">{{ formatVolume(revenueData.totalComputorRevenue) }}</div>
            <div class="text-sm text-foreground-muted">Total Computor Revenue</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold text-warning">{{ formatVolume(revenueData.arbRevenue) }}</div>
            <div class="text-sm text-foreground-muted">Arbitrator Revenue</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold text-info">{{ activeCount }} / {{ revenueData.computorCount }}</div>
            <div class="text-sm text-foreground-muted">Active Computors</div>
          </div>
        </div>

        <!-- Revenue distribution -->
        <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
          <div class="card-elevated text-center">
            <div class="text-lg font-bold text-destructive">{{ formatAmount(revenueStats.min) }}</div>
            <div class="text-xs text-foreground-muted">Min Revenue</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-lg font-bold text-success">{{ formatAmount(revenueStats.max) }}</div>
            <div class="text-xs text-foreground-muted">Max Revenue</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-lg font-bold">{{ formatAmount(Math.round(revenueStats.avg)) }}</div>
            <div class="text-xs text-foreground-muted">Avg Revenue ({{ revenueStats.avgPct.toFixed(2) }}%)</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-lg font-bold text-foreground-muted">{{ formatAmount(maxRevenue) }}</div>
            <div class="text-xs text-foreground-muted">Max Possible (100%)</div>
          </div>
        </div>

        <!-- Quorum scores -->
        <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
          <div class="card-elevated text-center">
            <div class="text-lg font-bold">{{ formatAmount(revenueData.txQuorumScore) }}</div>
            <div class="text-xs text-foreground-muted">TX Quorum Score (451st)</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-lg font-bold">{{ formatAmount(revenueData.voteQuorumScore) }}</div>
            <div class="text-xs text-foreground-muted">Vote Quorum Score (451st)</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-lg font-bold">{{ formatAmount(revenueData.miningQuorumScore) }}</div>
            <div class="text-xs text-foreground-muted">Mining Quorum Score (451st)</div>
          </div>
        </div>
      </template>
      <div v-else class="text-foreground-muted text-center py-8">No revenue data available</div>
    </div>

    <!-- Vote Progression Chart -->
    <div v-if="revenueData" class="card">
      <h2 class="section-title mb-4">
        <BarChart3 class="h-5 w-5 text-accent" />
        Vote Score Progression (Epoch {{ revenueData.epoch }})
      </h2>

      <div v-if="tickVotesLoading" class="loading">Loading vote progression...</div>
      <template v-else-if="tickVotes?.summary?.length">
        <ClientOnly>
          <ChartsEpochLineChart
            :labels="tickVoteChartLabels"
            :datasets="[
              {
                label: 'Quorum Threshold (451st)',
                data: tickVoteChartData.quorum,
                borderColor: 'rgb(239, 68, 68)',
                backgroundColor: 'rgba(239, 68, 68, 0.05)'
              },
              {
                label: 'Average',
                data: tickVoteChartData.avg,
                borderColor: 'rgb(59, 130, 246)',
                backgroundColor: 'rgba(59, 130, 246, 0.1)'
              },
              {
                label: 'Max',
                data: tickVoteChartData.max,
                borderColor: 'rgb(16, 185, 129)',
                backgroundColor: 'rgba(16, 185, 129, 0.05)'
              },
              {
                label: 'Min',
                data: tickVoteChartData.min,
                borderColor: 'rgb(245, 158, 11)',
                backgroundColor: 'rgba(245, 158, 11, 0.05)'
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
          Accumulated vote scores per 676-tick window. The quorum threshold (red) is the 451st highest score — computors at or above this line receive 100% vote factor.
        </p>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        No vote progression data available yet. Data is collected every 676 ticks.
      </div>
    </div>

    <!-- Computor Table -->
    <div v-if="revenueData" class="card">
      <div class="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-3 mb-4">
        <h2 class="section-title">
          <ArrowUpDown class="h-5 w-5 text-accent" />
          Per-Computor Breakdown
        </h2>

        <!-- Search -->
        <div class="relative w-full sm:w-64">
          <Search class="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-foreground-muted" />
          <input
            v-model="searchQuery"
            type="text"
            placeholder="Search address, label, or index..."
            class="w-full pl-9 pr-3 py-2 bg-background-secondary border border-border rounded-lg text-sm focus:outline-none focus:ring-1 focus:ring-accent"
          />
        </div>
      </div>

      <div class="table-wrapper">
        <table>
          <thead>
            <tr>
              <th class="cursor-pointer select-none" @click="toggleSort('computorIndex')">#</th>
              <th>Address</th>
              <th>Label</th>
              <th class="cursor-pointer select-none text-right" @click="toggleSort('txScore')">TX Score</th>
              <th class="cursor-pointer select-none text-right" @click="toggleSort('voteScore')">Vote Score</th>
              <th class="cursor-pointer select-none text-right" @click="toggleSort('miningScore')">Mining Score</th>
              <th class="cursor-pointer select-none text-right" @click="toggleSort('txFactor')">TX %</th>
              <th class="cursor-pointer select-none text-right" @click="toggleSort('voteFactor')">Vote %</th>
              <th class="cursor-pointer select-none text-right" @click="toggleSort('miningFactor')">Mining %</th>
              <th class="cursor-pointer select-none text-right" @click="toggleSort('revenue')">Revenue</th>
              <th class="text-right">%</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="c in paginatedComputors" :key="c.computorIndex">
              <td class="text-foreground-muted">{{ c.computorIndex }}</td>
              <td>
                <NuxtLink :to="`/address/${c.address}`" class="text-accent hover:underline font-mono text-xs">
                  {{ truncateAddress(c.address, 6) }}
                </NuxtLink>
              </td>
              <td class="text-foreground-muted text-xs">{{ c.label || '-' }}</td>
              <td class="text-right font-mono text-xs">{{ formatAmount(c.txScore) }}</td>
              <td class="text-right font-mono text-xs">{{ formatAmount(c.voteScore) }}</td>
              <td class="text-right font-mono text-xs">{{ formatAmount(c.miningScore) }}</td>
              <td class="text-right font-mono text-xs" :class="factorClass(c.txFactor)">{{ factorPct(c.txFactor) }}%</td>
              <td class="text-right font-mono text-xs" :class="factorClass(c.voteFactor)">{{ factorPct(c.voteFactor) }}%</td>
              <td class="text-right font-mono text-xs" :class="factorClass(c.miningFactor)">{{ factorPct(c.miningFactor) }}%</td>
              <td class="text-right font-bold">{{ formatAmount(c.revenue) }}</td>
              <td class="text-right font-mono text-xs" :class="c.revenue > 0 ? 'text-success' : 'text-destructive'">{{ revenuePct(c.revenue) }}%</td>
            </tr>
            <tr v-if="paginatedComputors.length === 0">
              <td colspan="11" class="text-center text-foreground-muted py-4">No matching computors</td>
            </tr>
          </tbody>
        </table>
      </div>

      <!-- Pagination -->
      <div v-if="totalPages > 1" class="flex items-center justify-between mt-4 text-sm">
        <div class="text-foreground-muted">
          Showing {{ (page - 1) * pageSize + 1 }}-{{ Math.min(page * pageSize, filteredComputors.length) }} of {{ filteredComputors.length }}
        </div>
        <div class="flex gap-2">
          <button
            class="px-3 py-1 rounded border border-border hover:bg-background-secondary disabled:opacity-40"
            :disabled="page <= 1"
            @click="page--"
          >Prev</button>
          <span class="px-3 py-1 text-foreground-muted">{{ page }} / {{ totalPages }}</span>
          <button
            class="px-3 py-1 rounded border border-border hover:bg-background-secondary disabled:opacity-40"
            :disabled="page >= totalPages"
            @click="page++"
          >Next</button>
        </div>
      </div>
    </div>
  </div>
</template>
