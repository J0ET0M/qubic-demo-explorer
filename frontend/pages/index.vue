<script setup lang="ts">
import { Blocks, ArrowLeftRight, TrendingUp, Activity, BarChart3 } from 'lucide-vue-next'

const api = useApi()
const liveUpdates = useLiveUpdates()

const { data: stats, pending: statsLoading } = await useAsyncData(
  'stats',
  () => api.getStats()
)

const { data: recentTicks, pending: ticksLoading } = await useAsyncData(
  'recent-ticks',
  () => api.getTicks(1, 10)
)

// Fetch epoch data for charts
const { data: epochs, pending: epochsLoading } = await useAsyncData(
  'recent-epochs',
  () => api.getEpochs(20)
)

// Prepare chart data from epochs (reversed to show oldest first)
const chartLabels = computed(() => {
  if (!epochs.value) return []
  return [...epochs.value].reverse().map(e => `E${e.epoch}`)
})

const txCountData = computed(() => {
  if (!epochs.value) return []
  return [...epochs.value].reverse().map(e => e.txCount)
})

const volumeData = computed(() => {
  if (!epochs.value) return []
  return [...epochs.value].reverse().map(e => e.totalVolume)
})

// Track subscription state
const isSubscribed = ref(false)

const subscribeToUpdates = async () => {
  if (isSubscribed.value) return
  try {
    await liveUpdates.connect()
    await liveUpdates.subscribeToTicks()
    isSubscribed.value = true
  } catch (err) {
    console.warn('SignalR subscription failed:', err)
  }
}

const unsubscribeFromUpdates = async () => {
  if (!isSubscribed.value) return
  try {
    await liveUpdates.unsubscribeFromTicks()
    isSubscribed.value = false
  } catch {
    // Ignore errors during cleanup
  }
}

// Handle visibility change - pause updates when tab is hidden
const handleVisibilityChange = () => {
  if (document.hidden) {
    unsubscribeFromUpdates()
  } else {
    subscribeToUpdates()
  }
}

// Real-time updates via SignalR
onMounted(async () => {
  // Listen for visibility changes
  document.addEventListener('visibilitychange', handleVisibilityChange)

  // Only subscribe if tab is visible
  if (!document.hidden) {
    await subscribeToUpdates()
  }

  liveUpdates.onNewTick((tickData: { tickNumber: number; epoch: number; txCount: number }) => {
    // Only process if subscribed and tab is visible
    if (!isSubscribed.value || document.hidden) return

    // Update stats in-place without refetching
    if (stats.value) {
      stats.value.latestTick = tickData.tickNumber
      stats.value.currentEpoch = tickData.epoch
    }

    // Prepend new tick to the list without refetching
    if (recentTicks.value?.items) {
      const newTick = {
        tickNumber: tickData.tickNumber,
        epoch: tickData.epoch,
        timestamp: new Date().toISOString(),
        txCount: tickData.txCount,
        logCount: 0 // Will be updated on next full refresh
      }
      // Avoid duplicates
      if (!recentTicks.value.items.some(t => t.tickNumber === tickData.tickNumber)) {
        recentTicks.value.items = [newTick, ...recentTicks.value.items.slice(0, 9)]
      }
    }
  })
})

onUnmounted(async () => {
  document.removeEventListener('visibilitychange', handleVisibilityChange)
  await unsubscribeFromUpdates()
})

const formatNumber = (num: number | undefined) => {
  if (!num) return '0'
  return num.toLocaleString()
}

const formatVolume = (amount: number | undefined) => {
  if (!amount) return '0'
  // Qubic has no decimals, amount is already in QU
  const qu = Math.floor(amount)
  if (qu >= 1_000_000_000) return Math.floor(qu / 1_000_000_000).toLocaleString() + 'B'
  if (qu >= 1_000_000) return Math.floor(qu / 1_000_000).toLocaleString() + 'M'
  if (qu >= 1_000) return Math.floor(qu / 1_000).toLocaleString() + 'K'
  return qu.toLocaleString()
}
</script>

<template>
  <div class="space-y-6">
    <!-- Stats -->
    <div class="stats-grid" v-if="!statsLoading && stats">
      <div class="stat-card">
        <div class="flex items-center gap-2 mb-3">
          <div class="p-1.5 rounded-md bg-accent/10">
            <Blocks class="h-4 w-4 text-accent" />
          </div>
        </div>
        <div class="value">{{ formatNumber(stats.latestTick) }}</div>
        <div class="label">Latest Tick</div>
      </div>
      <div class="stat-card">
        <div class="flex items-center gap-2 mb-3">
          <div class="p-1.5 rounded-md bg-secondary/10">
            <Activity class="h-4 w-4 text-secondary" />
          </div>
        </div>
        <div class="value">{{ stats.currentEpoch }}</div>
        <div class="label">Current Epoch</div>
      </div>
      <div class="stat-card">
        <div class="flex items-center gap-2 mb-3">
          <div class="p-1.5 rounded-md bg-success/10">
            <ArrowLeftRight class="h-4 w-4 text-success" />
          </div>
        </div>
        <div class="value">{{ formatNumber(stats.totalTransactions) }}</div>
        <div class="label">Total Transactions</div>
      </div>
      <div class="stat-card">
        <div class="flex items-center gap-2 mb-3">
          <div class="p-1.5 rounded-md bg-warning/10">
            <TrendingUp class="h-4 w-4 text-warning" />
          </div>
        </div>
        <div class="value">{{ formatVolume(stats.totalVolume) }}</div>
        <div class="label">Total Volume (QU)</div>
      </div>
    </div>

    <!-- Loading state for stats -->
    <div v-else-if="statsLoading" class="stats-grid">
      <div class="stat-card animate-pulse" v-for="i in 4" :key="i">
        <div class="h-6 bg-surface-elevated rounded-md w-8 mb-3"></div>
        <div class="h-7 bg-surface-elevated rounded w-24 mb-2"></div>
        <div class="h-3 bg-surface-elevated rounded w-20"></div>
      </div>
    </div>

    <!-- Epoch Countdown -->
    <EpochCountdown />

    <!-- Charts Section -->
    <div v-if="!epochsLoading && epochs?.length" class="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <!-- Transactions Chart -->
      <div class="card">
        <div class="flex items-center justify-between mb-4">
          <h3 class="section-title mb-0">
            <BarChart3 class="h-4 w-4 text-accent" />
            Transactions per Epoch
          </h3>
          <NuxtLink to="/epochs" class="btn btn-outline text-xs">
            View All
          </NuxtLink>
        </div>
        <ClientOnly>
          <ChartsEpochBarChart
            :labels="chartLabels"
            :datasets="[{ label: 'Transactions', data: txCountData }]"
            :height="200"
          />
          <template #fallback>
            <div class="h-[200px] flex items-center justify-center text-foreground-muted text-sm">
              Loading chart...
            </div>
          </template>
        </ClientOnly>
      </div>

      <!-- Volume Chart -->
      <div class="card">
        <div class="flex items-center justify-between mb-4">
          <h3 class="section-title mb-0">
            <TrendingUp class="h-4 w-4 text-success" />
            Volume per Epoch (QU)
          </h3>
          <NuxtLink to="/epochs" class="btn btn-outline text-xs">
            View All
          </NuxtLink>
        </div>
        <ClientOnly>
          <ChartsEpochLineChart
            :labels="chartLabels"
            :datasets="[{
              label: 'Volume',
              data: volumeData,
              borderColor: 'rgb(102, 187, 154)',
              backgroundColor: 'rgba(102, 187, 154, 0.08)'
            }]"
            :height="200"
          />
          <template #fallback>
            <div class="h-[200px] flex items-center justify-center text-foreground-muted text-sm">
              Loading chart...
            </div>
          </template>
        </ClientOnly>
      </div>
    </div>

    <!-- Recent Ticks -->
    <div class="card">
      <div class="flex items-center justify-between mb-4">
        <h2 class="section-title mb-0">
          <Blocks class="h-4 w-4 text-accent" />
          Recent Ticks
        </h2>
        <NuxtLink to="/ticks" class="btn btn-outline text-xs">
          View All
        </NuxtLink>
      </div>

      <div v-if="ticksLoading" class="loading">Loading...</div>
      <TickTable v-else-if="recentTicks?.items" :ticks="recentTicks.items" />
    </div>
  </div>
</template>
