<script setup lang="ts">
import { Flame, History } from 'lucide-vue-next'

const api = useApi()

// Burn stats history (4-hour snapshots) - reactive to time range
const { timeRange } = useTimeRange()
const burnStatsHistory = ref<Awaited<ReturnType<typeof api.getBurnStatsHistory>>>([])
const burnStatsLoading = ref(true)

const fetchBurnStats = async () => {
  burnStatsLoading.value = true
  try {
    const { from, to } = timeRange.value
    burnStatsHistory.value = await api.getBurnStatsHistory(500, from ?? undefined, to ?? undefined)
  } catch (e) {
    console.error('Failed to fetch burn stats history', e)
  } finally {
    burnStatsLoading.value = false
  }
}

watch(() => timeRange.value, fetchBurnStats, { deep: true })
await fetchBurnStats()

// Summary stats
const allTimeBurned = computed(() => {
  if (!burnStatsHistory.value?.length) return 0
  return burnStatsHistory.value[burnStatsHistory.value.length - 1]?.cumulativeBurned || 0
})

const latestSnapshot = computed(() => {
  if (!burnStatsHistory.value?.length) return null
  return burnStatsHistory.value[burnStatsHistory.value.length - 1]
})

// Burn volume chart (per 4h window)
const burnVolumeChartLabels = computed(() => {
  if (!burnStatsHistory.value) return []
  return burnStatsHistory.value.map(d => {
    if (d.tickStart > 0 && d.tickEnd > 0) {
      return new Date(d.snapshotAt).toLocaleDateString('en-US', { month: 'short', day: 'numeric', hour: '2-digit' })
    }
    return `E${d.epoch}`
  })
})

const burnVolumeChartData = computed(() => {
  if (!burnStatsHistory.value) return { total: [], explicit: [], dust: [], transfer: [] }
  return {
    total: burnStatsHistory.value.map(d => d.totalBurned),
    explicit: burnStatsHistory.value.map(d => d.burnAmount),
    dust: burnStatsHistory.value.map(d => d.dustBurned),
    transfer: burnStatsHistory.value.map(d => d.transferBurned)
  }
})

// Cumulative burn chart
const cumulativeChartData = computed(() => {
  if (!burnStatsHistory.value) return { cumulative: [] }
  return {
    cumulative: burnStatsHistory.value.map(d => d.cumulativeBurned)
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
    <!-- Burn Volume Over Time -->
    <div class="card">
      <h2 class="section-title mb-4">
        <Flame class="h-5 w-5 text-accent" />
        Burn Volume (4-Hour Snapshots)
      </h2>

      <div v-if="burnStatsLoading" class="loading">Loading...</div>
      <template v-else-if="burnStatsHistory?.length">
        <!-- Summary cards -->
        <div class="grid grid-cols-4 gap-4 mb-6">
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold text-destructive">{{ formatVolume(allTimeBurned) }}</div>
            <div class="text-sm text-foreground-muted">All-Time Burned</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold text-destructive">{{ formatVolume(latestSnapshot?.totalBurned || 0) }}</div>
            <div class="text-sm text-foreground-muted">Latest Window</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold">{{ (latestSnapshot?.burnCount || 0) + (latestSnapshot?.dustBurnCount || 0) + (latestSnapshot?.transferBurnCount || 0) }}</div>
            <div class="text-sm text-foreground-muted">Burn Events (Latest)</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold">{{ latestSnapshot?.uniqueBurners || 0 }}</div>
            <div class="text-sm text-foreground-muted">Unique Burners (Latest)</div>
          </div>
        </div>

        <ClientOnly>
          <ChartsEpochLineChart
            :labels="burnVolumeChartLabels"
            :datasets="[
              {
                label: 'Total Burned',
                data: burnVolumeChartData.total,
                borderColor: 'rgb(239, 68, 68)',
                backgroundColor: 'rgba(239, 68, 68, 0.1)'
              },
              {
                label: 'Explicit Burns',
                data: burnVolumeChartData.explicit,
                borderColor: 'rgb(249, 115, 22)',
                backgroundColor: 'rgba(249, 115, 22, 0.1)'
              },
              {
                label: 'Dust Burns',
                data: burnVolumeChartData.dust,
                borderColor: 'rgb(234, 179, 8)',
                backgroundColor: 'rgba(234, 179, 8, 0.1)'
              },
              {
                label: 'Transfer Burns',
                data: burnVolumeChartData.transfer,
                borderColor: 'rgb(168, 85, 247)',
                backgroundColor: 'rgba(168, 85, 247, 0.1)'
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
          Historical snapshots taken every 4 hours. Explicit = BurnQubic SC calls, Dust = protocol dust collection, Transfer = direct sends to null address.
        </p>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        <Flame class="h-12 w-12 mx-auto mb-4 opacity-50" />
        <p>No burn data available yet. Snapshots are taken every 4 hours.</p>
      </div>
    </div>

    <!-- Cumulative Burn -->
    <div class="card">
      <h2 class="section-title mb-4">
        <History class="h-5 w-5 text-accent" />
        Cumulative Burn
      </h2>

      <div v-if="burnStatsLoading" class="loading">Loading...</div>
      <template v-else-if="burnStatsHistory?.length && cumulativeChartData.cumulative.some(v => v > 0)">
        <ClientOnly>
          <ChartsEpochLineChart
            :labels="burnVolumeChartLabels"
            :datasets="[
              {
                label: 'Cumulative Burned',
                data: cumulativeChartData.cumulative,
                borderColor: 'rgb(239, 68, 68)',
                backgroundColor: 'rgba(239, 68, 68, 0.15)',
                fill: true
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
          Running total of all burned QU across all snapshot windows.
        </p>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        No cumulative burn data available yet.
      </div>
    </div>
  </div>
</template>
