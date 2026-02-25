<script setup lang="ts">
import { Pickaxe, ArrowLeft, Network, Building2, Users, Eye, EyeOff, X } from 'lucide-vue-next'
import type { FlowVisualizationNodeDto } from '~/composables/useApi'

const route = useRoute()
const api = useApi()

// The epoch in the URL is the emission epoch (computors from this epoch received emission)
const emissionEpoch = computed(() => Number(route.params.epoch))
const maxDepth = ref(10)
const showAllNodes = ref(false)

// Sankey diagram filter state
const sankeyFilteredNodeId = ref<string | null>(null)
const sankeyFilteredNode = computed(() => {
  if (!sankeyFilteredNodeId.value || !flowData.value?.nodes) return null
  return flowData.value.nodes.find(n => n.id === sankeyFilteredNodeId.value) || null
})

const handleNodeFilter = (nodeId: string | null) => {
  sankeyFilteredNodeId.value = nodeId
}

const clearSankeyFilter = () => {
  sankeyFilteredNodeId.value = null
}

// Fetch flow visualization data for this emission epoch
const { data: flowData, pending: loading } = await useAsyncData(
  `flow-viz-${route.params.epoch}-${maxDepth.value}`,
  () => api.getFlowVisualization(emissionEpoch.value, maxDepth.value),
  { watch: [maxDepth] }
)

// Group nodes by depth for visualization
const nodesByDepth = computed(() => {
  if (!flowData.value?.nodes) return new Map<number, FlowVisualizationNodeDto[]>()

  const map = new Map<number, FlowVisualizationNodeDto[]>()
  for (const node of flowData.value.nodes) {
    const depth = node.depth
    if (!map.has(depth)) {
      map.set(depth, [])
    }
    map.get(depth)!.push(node)
  }

  // Sort nodes by outflow within each depth level
  for (const [depth, nodes] of map.entries()) {
    map.set(depth, nodes.sort((a, b) => b.totalOutflow - a.totalOutflow))
  }

  return map
})

// Filter nodes to show - limit per depth unless showing all
const filteredNodesByDepth = computed(() => {
  const maxPerDepth = showAllNodes.value ? 100 : 10
  const filtered = new Map<number, FlowVisualizationNodeDto[]>()

  for (const [depth, nodes] of nodesByDepth.value.entries()) {
    filtered.set(depth, nodes.slice(0, maxPerDepth))
  }

  return filtered
})

// Get visible node IDs for filtering links
const visibleNodeIds = computed(() => {
  const ids = new Set<string>()
  for (const nodes of filteredNodesByDepth.value.values()) {
    for (const node of nodes) {
      ids.add(node.id)
    }
  }
  return ids
})

// Filter links to only show visible nodes
const filteredLinks = computed(() => {
  if (!flowData.value?.links) return []
  return flowData.value.links.filter(
    link => visibleNodeIds.value.has(link.sourceId) && visibleNodeIds.value.has(link.targetId)
  )
})

// Stats
const stats = computed(() => {
  if (!flowData.value) return null

  const exchangeNodes = flowData.value.nodes.filter(n => n.type === 'exchange')
  const computorNodes = flowData.value.nodes.filter(n => n.type === 'computor')
  const intermediaryNodes = flowData.value.nodes.filter(n => n.type !== 'exchange' && n.type !== 'computor')

  return {
    totalNodes: flowData.value.nodes.length,
    totalLinks: flowData.value.links.length,
    computorCount: computorNodes.length,
    exchangeCount: exchangeNodes.length,
    intermediaryCount: intermediaryNodes.length,
    totalVolume: flowData.value.totalTrackedVolume,
    exchangeVolume: exchangeNodes.reduce((sum, n) => sum + n.totalInflow, 0)
  }
})

const { formatVolume, truncateAddress } = useFormatting()

const getNodeColor = (type: string) => {
  switch (type) {
    case 'computor': return 'bg-success/20 border-success text-success'
    case 'exchange': return 'bg-destructive/20 border-destructive text-destructive'
    case 'smartcontract': return 'bg-amber-500/20 border-amber-500 text-amber-500'
    default: return 'bg-accent/20 border-accent text-accent'
  }
}

const getNodeIcon = (type: string) => {
  switch (type) {
    case 'computor': return Pickaxe
    case 'exchange': return Building2
    case 'smartcontract': return Network // Use Network icon for smart contracts
    default: return Users
  }
}
</script>

<template>
  <div class="space-y-6">
    <!-- Header -->
    <div class="flex items-center justify-between flex-wrap gap-4">
      <div class="flex items-center gap-4">
        <NuxtLink to="/analytics/miners" class="btn btn-ghost">
          <ArrowLeft class="h-4 w-4" />
          Back
        </NuxtLink>
        <h1 class="text-2xl font-bold flex items-center gap-2">
          <Network class="h-6 w-6 text-accent" />
          Miner Flow Visualization - Emission Epoch {{ emissionEpoch }}
        </h1>
      </div>

      <div class="flex items-center gap-2">
        <label class="text-sm text-foreground-muted">Max Depth:</label>
        <select v-model="maxDepth" class="input w-20">
          <option :value="3">3</option>
          <option :value="5">5</option>
          <option :value="7">7</option>
          <option :value="10">10</option>
        </select>

        <button
          class="btn btn-sm"
          :class="showAllNodes ? 'btn-primary' : 'btn-ghost'"
          @click="showAllNodes = !showAllNodes"
        >
          <component :is="showAllNodes ? Eye : EyeOff" class="h-4 w-4" />
          {{ showAllNodes ? 'Showing All' : 'Top 10/Level' }}
        </button>
      </div>
    </div>

    <!-- Stats Cards -->
    <div v-if="stats" class="grid grid-cols-2 md:grid-cols-5 gap-4">
      <div class="card text-center">
        <div class="text-2xl font-bold">{{ stats.computorCount }}</div>
        <div class="text-xs text-foreground-muted">Computors</div>
      </div>
      <div class="card text-center">
        <div class="text-2xl font-bold">{{ stats.intermediaryCount }}</div>
        <div class="text-xs text-foreground-muted">Intermediaries</div>
      </div>
      <div class="card text-center">
        <div class="text-2xl font-bold text-destructive">{{ stats.exchangeCount }}</div>
        <div class="text-xs text-foreground-muted">Exchanges</div>
      </div>
      <div class="card text-center">
        <div class="text-2xl font-bold">{{ formatVolume(stats.totalVolume) }}</div>
        <div class="text-xs text-foreground-muted">Total Volume</div>
      </div>
      <div class="card text-center">
        <div class="text-2xl font-bold text-destructive">{{ formatVolume(stats.exchangeVolume) }}</div>
        <div class="text-xs text-foreground-muted">To Exchanges</div>
      </div>
    </div>

    <!-- Sankey Flow Diagram -->
    <div class="card">
      <h2 class="section-title mb-4">
        <Network class="h-5 w-5 text-accent" />
        Sankey Flow Diagram
      </h2>

      <div v-if="loading" class="loading py-12">Loading flow data...</div>

      <template v-else-if="flowData && flowData.nodes.length > 0 && flowData.links.length > 0">
        <!-- Legend and Filter Status -->
        <div class="flex items-center justify-between flex-wrap gap-4 mb-4">
          <div class="flex items-center gap-4 text-sm flex-wrap">
            <div class="flex items-center gap-1">
              <div class="w-3 h-3 rounded bg-success"></div>
              <span>Computor</span>
            </div>
            <div class="flex items-center gap-1">
              <div class="w-3 h-3 rounded bg-accent"></div>
              <span>Intermediary</span>
            </div>
            <div class="flex items-center gap-1">
              <div class="w-3 h-3 rounded bg-amber-500"></div>
              <span>Smart Contract</span>
            </div>
            <div class="flex items-center gap-1">
              <div class="w-3 h-3 rounded bg-destructive"></div>
              <span>Exchange</span>
            </div>
          </div>

          <!-- Filter Status -->
          <div v-if="sankeyFilteredNode" class="flex items-center gap-2 px-3 py-1.5 bg-accent/10 border border-accent/30 rounded-lg text-sm">
            <span class="text-foreground-muted">Filtering:</span>
            <span class="font-medium">{{ sankeyFilteredNode.label || truncateAddress(sankeyFilteredNode.address) }}</span>
            <span class="text-xs text-foreground-muted">({{ sankeyFilteredNode.type }})</span>
            <button
              class="p-0.5 hover:bg-accent/20 rounded"
              title="Clear filter"
              @click="clearSankeyFilter"
            >
              <X class="h-4 w-4" />
            </button>
          </div>
        </div>

        <!-- Sankey Chart -->
        <div class="overflow-x-auto border border-border rounded-lg p-4 bg-surface">
          <ClientOnly>
            <ChartsSankeyChart
              :nodes="flowData.nodes"
              :links="flowData.links"
              :width="1200"
              :height="Math.max(500, flowData.nodes.length * 6)"
              :filtered-node-id="sankeyFilteredNodeId"
              @node-filter="handleNodeFilter"
            />
            <template #fallback>
              <div class="h-[500px] flex items-center justify-center text-foreground-muted">
                Loading Sankey diagram...
              </div>
            </template>
          </ClientOnly>
        </div>

        <p class="text-xs text-foreground-muted mt-2">
          Click on any node to highlight its flow paths. Hover over nodes and links for details.
          Tick range: {{ flowData.tickStart.toLocaleString() }} - {{ flowData.tickEnd.toLocaleString() }}
        </p>
      </template>

      <div v-else class="text-center py-12 text-foreground-muted">
        <Network class="h-12 w-12 mx-auto mb-4 opacity-50" />
        <p>No flow data available for Sankey diagram</p>
      </div>
    </div>

    <!-- Depth-based Flow Visualization -->
    <div class="card">
      <h2 class="section-title mb-4">
        <Network class="h-5 w-5 text-accent" />
        Flow by Depth Level
      </h2>

      <div v-if="loading" class="loading py-12">Loading flow data...</div>

      <template v-else-if="flowData && flowData.nodes.length > 0">
        <!-- Legend -->
        <div class="flex items-center gap-4 mb-4 text-sm flex-wrap">
          <div class="flex items-center gap-1">
            <div class="w-3 h-3 rounded bg-success"></div>
            <span>Computor</span>
          </div>
          <div class="flex items-center gap-1">
            <div class="w-3 h-3 rounded bg-accent"></div>
            <span>Intermediary</span>
          </div>
          <div class="flex items-center gap-1">
            <div class="w-3 h-3 rounded bg-amber-500"></div>
            <span>Smart Contract</span>
          </div>
          <div class="flex items-center gap-1">
            <div class="w-3 h-3 rounded bg-destructive"></div>
            <span>Exchange</span>
          </div>
        </div>

        <!-- Flow layers -->
        <div class="overflow-x-auto">
          <div class="flex gap-8 min-w-max pb-4">
            <div
              v-for="depth in Array.from(filteredNodesByDepth.keys()).sort((a, b) => a - b)"
              :key="depth"
              class="flex flex-col gap-2 min-w-[200px]"
            >
              <div class="text-sm font-medium text-foreground-muted text-center mb-2 sticky top-0 bg-surface py-1">
                {{ depth === 0 ? 'Computors' : `Hop ${depth}` }}
                <span class="text-xs">({{ nodesByDepth.get(depth)?.length || 0 }} addresses)</span>
              </div>

              <div
                v-for="node in filteredNodesByDepth.get(depth) || []"
                :key="node.id"
                :class="[
                  'p-2 rounded border text-xs',
                  getNodeColor(node.type)
                ]"
              >
                <div class="flex items-center gap-1 mb-1">
                  <component :is="getNodeIcon(node.type)" class="h-3 w-3" />
                  <span class="font-medium truncate">
                    {{ node.label || truncateAddress(node.address) }}
                  </span>
                </div>
                <div class="flex justify-between text-[10px] opacity-75">
                  <span>In: {{ formatVolume(node.totalInflow) }}</span>
                  <span>Out: {{ formatVolume(node.totalOutflow) }}</span>
                </div>
                <NuxtLink
                  :to="`/address/${node.address}`"
                  class="text-[10px] text-accent hover:underline block truncate mt-1"
                >
                  {{ truncateAddress(node.address) }}
                </NuxtLink>
              </div>

              <div
                v-if="(nodesByDepth.get(depth)?.length || 0) > (filteredNodesByDepth.get(depth)?.length || 0)"
                class="text-xs text-foreground-muted text-center py-2"
              >
                +{{ (nodesByDepth.get(depth)?.length || 0) - (filteredNodesByDepth.get(depth)?.length || 0) }} more
              </div>
            </div>
          </div>
        </div>

        <!-- Links summary -->
        <div class="mt-4 pt-4 border-t border-border">
          <h3 class="text-sm font-medium mb-2">Top Flow Links</h3>
          <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-2 max-h-[300px] overflow-y-auto">
            <div
              v-for="(link, idx) in filteredLinks.slice(0, 30)"
              :key="idx"
              class="flex items-center gap-2 text-xs p-2 rounded bg-surface-elevated"
            >
              <span class="truncate flex-1">{{ truncateAddress(link.sourceId) }}</span>
              <span class="text-foreground-muted">→</span>
              <span class="truncate flex-1">{{ truncateAddress(link.targetId) }}</span>
              <span class="font-medium text-accent whitespace-nowrap">{{ formatVolume(link.amount) }}</span>
            </div>
          </div>
          <p class="text-xs text-foreground-muted mt-2">
            Showing {{ Math.min(30, filteredLinks.length) }} of {{ flowData.links.length }} total links
          </p>
        </div>
      </template>

      <div v-else class="text-center py-12 text-foreground-muted">
        <Network class="h-12 w-12 mx-auto mb-4 opacity-50" />
        <p>No flow visualization data available for emission epoch {{ emissionEpoch }}</p>
        <p class="text-sm mt-2">Flow data is generated during periodic snapshots.</p>
      </div>
    </div>

    <!-- Info -->
    <div class="card bg-accent/5 border-accent/20">
      <h3 class="font-medium mb-2">How to Read This Visualization</h3>
      <div class="text-sm text-foreground-muted space-y-2">
        <p>
          This diagram shows how Qubic flows from computors (miners) through the network.
          Each column represents a "hop" distance from the original computor.
        </p>
        <ul class="list-disc list-inside space-y-1">
          <li><strong>Hop 0 (Computors):</strong> The 676 computors who received emission</li>
          <li><strong>Hop 1:</strong> Direct recipients from computors</li>
          <li><strong>Hop 2+:</strong> Further transfers down the chain</li>
          <li><strong class="text-amber-500">Orange nodes:</strong> Smart contracts (sinks - flow tracking stops here)</li>
          <li><strong class="text-destructive">Red nodes:</strong> Exchanges (potential sell destinations)</li>
        </ul>
        <p class="text-xs mt-2">
          <strong>Note:</strong> Qutil is treated as a pass-through contract - funds sent through Qutil continue to be tracked to their final destinations.
        </p>
      </div>
    </div>
  </div>
</template>

<style scoped>
.input {
  background: var(--color-surface);
  border: 1px solid var(--color-border);
  border-radius: 0.375rem;
  padding: 0.375rem 0.5rem;
  font-size: 0.875rem;
}
</style>
