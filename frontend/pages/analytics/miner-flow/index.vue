<script setup lang="ts">
import { Network, ArrowLeft, ArrowRight } from 'lucide-vue-next'

const api = useApi()

// Fetch miner flow stats to get available epochs
const { data: minerFlowSummary, pending: loading } = await useAsyncData(
  'miner-flow-epochs',
  () => api.getMinerFlowStats(50)
)

// Get unique emission epochs from history
const availableEpochs = computed(() => {
  if (!minerFlowSummary.value?.history) return []
  const epochs = new Set<number>()
  for (const stat of minerFlowSummary.value.history) {
    if (stat.emissionEpoch) {
      epochs.add(stat.emissionEpoch)
    }
  }
  return Array.from(epochs).sort((a, b) => b - a)
})

const latestEpoch = computed(() => availableEpochs.value[0])

// Selected epoch for navigation
const selectedEpoch = ref<number | null>(null)

watch(latestEpoch, (epoch) => {
  if (epoch && !selectedEpoch.value) {
    selectedEpoch.value = epoch
  }
}, { immediate: true })

const navigateToEpoch = () => {
  if (selectedEpoch.value) {
    navigateTo(`/analytics/miner-flow/${selectedEpoch.value}`)
  }
}
</script>

<template>
  <div class="space-y-6">
    <!-- Header -->
    <div class="flex items-center gap-4">
      <NuxtLink to="/analytics/miners" class="btn btn-ghost">
        <ArrowLeft class="h-4 w-4" />
        Back to Analytics
      </NuxtLink>
      <h1 class="text-2xl font-bold flex items-center gap-2">
        <Network class="h-6 w-6 text-accent" />
        Miner Flow Visualization
      </h1>
    </div>

    <!-- Epoch Selection -->
    <div class="card">
      <h2 class="section-title mb-4">
        <Network class="h-5 w-5 text-accent" />
        Select Emission Epoch
      </h2>

      <div v-if="loading" class="loading py-8">Loading available epochs...</div>

      <template v-else-if="availableEpochs.length > 0">
        <p class="text-sm text-foreground-muted mb-4">
          Select an emission epoch to visualize how funds flowed from computors through the network.
          The visualization shows transfers from computors (who received emission at end of the selected epoch)
          through intermediaries to exchanges.
        </p>

        <div class="flex items-center gap-4 mb-6">
          <select v-model="selectedEpoch" class="input flex-1 max-w-xs">
            <option v-for="epoch in availableEpochs" :key="epoch" :value="epoch">
              Epoch {{ epoch }}
            </option>
          </select>
          <button
            class="btn btn-primary flex items-center gap-2"
            :disabled="!selectedEpoch"
            @click="navigateToEpoch"
          >
            View Flow Diagram
            <ArrowRight class="h-4 w-4" />
          </button>
        </div>

        <!-- Quick Links -->
        <div class="border-t border-border pt-4">
          <h3 class="text-sm font-medium text-foreground-muted mb-3">Recent Epochs</h3>
          <div class="flex flex-wrap gap-2">
            <NuxtLink
              v-for="epoch in availableEpochs.slice(0, 10)"
              :key="epoch"
              :to="`/analytics/miner-flow/${epoch}`"
              class="btn btn-sm btn-ghost"
            >
              EP{{ epoch }}
            </NuxtLink>
          </div>
        </div>
      </template>

      <div v-else class="text-center py-12 text-foreground-muted">
        <Network class="h-12 w-12 mx-auto mb-4 opacity-50" />
        <p class="mb-2">No flow visualization data available yet</p>
        <p class="text-sm">Flow data is generated during periodic 4-hour snapshots.</p>
      </div>
    </div>

    <!-- Info -->
    <div class="card bg-accent/5 border-accent/20">
      <h3 class="font-medium mb-2">About Flow Visualization</h3>
      <div class="text-sm text-foreground-muted space-y-2">
        <p>
          The flow visualization shows how Qubic emission moves from computors through the network.
          Each emission epoch corresponds to the computors who received rewards at the end of that epoch.
        </p>
        <ul class="list-disc list-inside space-y-1">
          <li><strong>Hop 0 (Computors):</strong> The 676 computors who received emission</li>
          <li><strong>Hop 1+:</strong> Recipients at each hop level from the computors</li>
          <li><strong class="text-destructive">Red nodes:</strong> Exchanges (potential sell destinations)</li>
        </ul>
      </div>
    </div>
  </div>
</template>

<style scoped>
.input {
  background: var(--color-surface);
  border: 1px solid var(--color-border);
  border-radius: 0.375rem;
  padding: 0.5rem 0.75rem;
  font-size: 0.875rem;
}
</style>
