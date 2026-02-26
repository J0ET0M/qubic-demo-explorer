<script setup lang="ts">
import { ArrowLeft, Radar, RefreshCw, Copy, Check } from 'lucide-vue-next'

const route = useRoute()
const api = useApi()
const jobId = computed(() => route.params.id as string)

const copied = ref(false)

// Fetch job data
const { data: job, refresh: refreshJob, pending: loadingJob } = await useAsyncData(
  `custom-flow-job-${jobId.value}`,
  () => api.getCustomFlowJob(jobId.value)
)

// Fetch visualization
const { data: vizData, refresh: refreshViz, pending: loadingViz } = await useAsyncData(
  `custom-flow-viz-${jobId.value}`,
  () => api.getCustomFlowVisualization(jobId.value)
)

// Fetch tracking state
const { data: stateData, refresh: refreshState } = await useAsyncData(
  `custom-flow-state-${jobId.value}`,
  () => api.getCustomFlowState(jobId.value)
)

// Auto-refresh while pending/processing
const refreshInterval = ref<ReturnType<typeof setInterval> | null>(null)

const isActive = computed(() => {
  const status = job.value?.status
  return status === 'pending' || status === 'processing'
})

watch(isActive, (active) => {
  if (active && !refreshInterval.value) {
    refreshInterval.value = setInterval(() => {
      refreshJob()
      refreshViz()
      refreshState()
    }, 30000)
  } else if (!active && refreshInterval.value) {
    clearInterval(refreshInterval.value)
    refreshInterval.value = null
  }
}, { immediate: true })

onUnmounted(() => {
  if (refreshInterval.value) clearInterval(refreshInterval.value)
})

// Visualization helpers
const nodes = computed(() => vizData.value?.nodes ?? [])
const links = computed(() => vizData.value?.links ?? [])
const states = computed(() => stateData.value?.states ?? [])

const nodesByDepth = computed(() => {
  const map = new Map<number, any[]>()
  for (const node of nodes.value) {
    const list = map.get(node.depth) || []
    list.push(node)
    map.set(node.depth, list)
  }
  for (const [, list] of map) {
    list.sort((a: any, b: any) => b.totalOutflow - a.totalOutflow)
  }
  return map
})

const trackedStates = computed(() => states.value.filter((s: any) => s.addressType === 'tracked'))
const intermediaryCount = computed(() => {
  const addrs = new Set(states.value.filter((s: any) => s.addressType === 'intermediary').map((s: any) => s.address))
  return addrs.size
})
const terminalCount = computed(() => {
  const addrs = new Set(states.value.filter((s: any) => s.isTerminal).map((s: any) => s.address))
  return addrs.size
})

const formatVolume = (volume: number) => {
  if (volume >= 1_000_000_000_000) return (volume / 1_000_000_000_000).toFixed(2) + 'T'
  if (volume >= 1_000_000_000) return (volume / 1_000_000_000).toFixed(2) + 'B'
  if (volume >= 1_000_000) return (volume / 1_000_000).toFixed(2) + 'M'
  if (volume >= 1_000) return (volume / 1_000).toFixed(2) + 'K'
  return volume.toLocaleString()
}

const getStatusColor = (status: string) => {
  switch (status) {
    case 'pending': return 'bg-yellow-500/20 text-yellow-500 border-yellow-500'
    case 'processing': return 'bg-blue-500/20 text-blue-500 border-blue-500'
    case 'complete': return 'bg-green-500/20 text-green-500 border-green-500'
    case 'stale': return 'bg-gray-500/20 text-gray-500 border-gray-500'
    default: return 'bg-muted text-muted-foreground'
  }
}

const getNodeColor = (type: string) => {
  switch (type) {
    case 'tracked': return 'bg-primary/20 border-primary text-primary'
    case 'exchange': return 'bg-destructive/20 border-destructive text-destructive'
    case 'smartcontract': return 'bg-amber-500/20 border-amber-500 text-amber-500'
    default: return 'bg-accent/20 border-accent text-accent'
  }
}

const shortenAddress = (addr: string) => addr.slice(0, 8) + '...' + addr.slice(-8)

const copyLink = () => {
  navigator.clipboard.writeText(window.location.href)
  copied.value = true
  setTimeout(() => copied.value = false, 2000)
}

const manualRefresh = () => {
  refreshJob()
  refreshViz()
  refreshState()
}

useHead({ title: computed(() => job.value?.alias || 'Flow Tracking') })
</script>

<template>
  <div class="space-y-6">
    <!-- Header -->
    <div class="flex items-center justify-between">
      <div class="flex items-center gap-3">
        <NuxtLink to="/analytics/flow-tracking" class="text-muted-foreground hover:text-foreground">
          <ArrowLeft class="h-5 w-5" />
        </NuxtLink>
        <div>
          <h1 class="text-2xl font-bold flex items-center gap-2">
            <Radar class="h-6 w-6" />
            {{ job?.alias || 'Flow Tracking' }}
          </h1>
          <p class="text-sm text-muted-foreground font-mono">{{ jobId }}</p>
        </div>
      </div>
      <div class="flex items-center gap-2">
        <button @click="copyLink" class="text-muted-foreground hover:text-foreground p-2 rounded-md hover:bg-accent" title="Copy link">
          <Check v-if="copied" class="h-4 w-4 text-success" />
          <Copy v-else class="h-4 w-4" />
        </button>
        <button @click="manualRefresh" class="text-muted-foreground hover:text-foreground p-2 rounded-md hover:bg-accent" title="Refresh">
          <RefreshCw class="h-4 w-4" :class="{ 'animate-spin': loadingJob }" />
        </button>
      </div>
    </div>

    <!-- Not found -->
    <div v-if="!job && !loadingJob" class="bg-card rounded-lg border p-8 text-center">
      <p class="text-muted-foreground">Flow tracking job not found.</p>
      <NuxtLink to="/analytics/flow-tracking" class="text-primary hover:underline text-sm mt-2 block">
        Create a new tracking
      </NuxtLink>
    </div>

    <template v-if="job">
      <!-- Status & Meta -->
      <div class="bg-card rounded-lg border p-4">
        <div class="flex flex-wrap items-center gap-4">
          <span :class="['px-2 py-1 rounded-md border text-xs font-medium', getStatusColor(job.status)]">
            {{ job.status.toUpperCase() }}
          </span>
          <span class="text-sm text-muted-foreground">
            Start tick: <span class="font-mono">{{ job.startTick.toLocaleString() }}</span>
          </span>
          <span class="text-sm text-muted-foreground">
            Last processed: <span class="font-mono">{{ job.lastProcessedTick > 0 ? job.lastProcessedTick.toLocaleString() : 'N/A' }}</span>
          </span>
          <span class="text-sm text-muted-foreground">
            Max hops: {{ job.maxHops }}
          </span>
          <span v-if="isActive" class="text-xs text-muted-foreground">
            Auto-refreshing every 30s
          </span>
        </div>
      </div>

      <!-- Tracked addresses -->
      <div class="bg-card rounded-lg border p-4">
        <h3 class="text-sm font-semibold mb-3">Tracked Addresses</h3>
        <div class="space-y-2">
          <div v-for="(addr, i) in job.addresses" :key="addr" class="flex items-center justify-between gap-2 text-sm">
            <NuxtLink :to="`/address/${addr}`" class="font-mono text-primary hover:underline truncate">
              {{ addr }}
            </NuxtLink>
            <div class="text-right shrink-0">
              <span class="text-muted-foreground">Balance: </span>
              <span class="font-mono">{{ formatVolume(job.balances[i]) }}</span>
              <template v-if="trackedStates.find((s: any) => s.address === addr)">
                <span class="text-muted-foreground ml-2">Pending: </span>
                <span class="font-mono">{{ formatVolume(trackedStates.find((s: any) => s.address === addr)?.pendingAmount ?? 0) }}</span>
              </template>
            </div>
          </div>
        </div>
      </div>

      <!-- Stats cards -->
      <div class="grid grid-cols-2 md:grid-cols-5 gap-3">
        <div class="bg-card rounded-lg border p-3">
          <div class="text-xs text-muted-foreground">Hops Recorded</div>
          <div class="text-lg font-bold">{{ job.totalHopsRecorded.toLocaleString() }}</div>
        </div>
        <div class="bg-card rounded-lg border p-3">
          <div class="text-xs text-muted-foreground">Intermediaries</div>
          <div class="text-lg font-bold">{{ intermediaryCount }}</div>
        </div>
        <div class="bg-card rounded-lg border p-3">
          <div class="text-xs text-muted-foreground">Terminals</div>
          <div class="text-lg font-bold">{{ terminalCount }}</div>
        </div>
        <div class="bg-card rounded-lg border p-3">
          <div class="text-xs text-muted-foreground">Terminal Amount</div>
          <div class="text-lg font-bold">{{ formatVolume(job.totalTerminalAmount) }}</div>
        </div>
        <div class="bg-card rounded-lg border p-3">
          <div class="text-xs text-muted-foreground">Pending Amount</div>
          <div class="text-lg font-bold">{{ formatVolume(job.totalPendingAmount) }}</div>
        </div>
      </div>

      <!-- Sankey visualization -->
      <div v-if="vizData && nodes.length > 0" class="bg-card rounded-lg border p-4">
        <h3 class="text-sm font-semibold mb-3">Flow Visualization</h3>
        <LazyChartsSankeyChart
          v-if="links.length > 0"
          :nodes="nodes"
          :links="links"
        />
        <p v-else class="text-muted-foreground text-sm text-center py-4">
          No flow data yet. Waiting for processing...
        </p>
      </div>

      <!-- Flow by depth -->
      <div v-if="nodesByDepth.size > 0" class="space-y-4">
        <h3 class="text-sm font-semibold">Flow by Hop Level</h3>
        <div v-for="[depth, depthNodes] in [...nodesByDepth.entries()].sort((a, b) => a[0] - b[0])" :key="depth">
          <div class="text-xs text-muted-foreground mb-2">
            {{ depth === 0 ? 'Tracked Addresses' : `Hop Level ${depth}` }}
            ({{ depthNodes.length }} {{ depthNodes.length === 1 ? 'address' : 'addresses' }})
          </div>
          <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2">
            <div
              v-for="node in depthNodes.slice(0, 10)"
              :key="node.id"
              :class="['rounded-md border p-2 text-xs', getNodeColor(node.type)]"
            >
              <div class="font-mono truncate" :title="node.address">
                {{ node.label || shortenAddress(node.address) }}
              </div>
              <div class="flex justify-between mt-1 opacity-75">
                <span>In: {{ formatVolume(node.totalInflow) }}</span>
                <span>Out: {{ formatVolume(node.totalOutflow) }}</span>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- Top links -->
      <div v-if="links.length > 0" class="bg-card rounded-lg border p-4">
        <h3 class="text-sm font-semibold mb-3">Top Flows ({{ links.length }} total)</h3>
        <div class="overflow-x-auto">
          <table class="w-full text-sm">
            <thead>
              <tr class="border-b text-xs text-muted-foreground">
                <th class="text-left py-2">Source</th>
                <th class="text-left py-2">Destination</th>
                <th class="text-right py-2">Amount</th>
                <th class="text-right py-2">Txs</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="link in [...links].sort((a: any, b: any) => b.amount - a.amount).slice(0, 20)" :key="`${link.sourceId}-${link.targetId}`" class="border-b border-border/50">
                <td class="py-1.5 font-mono text-xs truncate max-w-[200px]">
                  {{ nodes.find((n: any) => n.id === link.sourceId)?.label || shortenAddress(link.sourceId) }}
                </td>
                <td class="py-1.5 font-mono text-xs truncate max-w-[200px]">
                  {{ nodes.find((n: any) => n.id === link.targetId)?.label || shortenAddress(link.targetId) }}
                </td>
                <td class="py-1.5 text-right font-mono">{{ formatVolume(link.amount) }}</td>
                <td class="py-1.5 text-right">{{ link.transactionCount }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </template>
  </div>
</template>
