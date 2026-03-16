<script setup lang="ts">
import { AlertTriangle } from 'lucide-vue-next'

const api = useApi()
const { fetchLabels, getLabel } = useAddressLabels()

const { data: stats } = await useAsyncData('stats', () => api.getStats())

const emptyTickStats = ref<Awaited<ReturnType<typeof api.getEpochEmptyTicks>> | null>(null)
const loading = ref(true)

// Fetch empty tick data for the current epoch
watch(stats, async (s) => {
  if (s?.currentEpoch) {
    try {
      emptyTickStats.value = await api.getEpochEmptyTicks(s.currentEpoch)
      if (emptyTickStats.value?.computors?.length) {
        const addresses = emptyTickStats.value.computors
          .map(c => c.address)
          .filter(a => a)
        await fetchLabels(addresses)
      }
    } finally {
      loading.value = false
    }
  }
}, { immediate: true })

const emptyRate = computed(() => {
  if (!emptyTickStats.value || emptyTickStats.value.totalTicks === 0) return 0
  return (emptyTickStats.value.totalEmptyTicks / emptyTickStats.value.totalTicks) * 100
})

// Top 5 worst offenders (most empty ticks)
const topOffenders = computed(() => {
  if (!emptyTickStats.value?.computors) return []
  return [...emptyTickStats.value.computors]
    .filter(c => c.emptyTickCount > 0)
    .sort((a, b) => b.emptyTickCount - a.emptyTickCount)
    .slice(0, 5)
})
</script>

<template>
  <div v-if="!loading && emptyTickStats && stats" class="card">
    <div class="flex items-center justify-between mb-4">
      <h3 class="section-title mb-0">
        <AlertTriangle class="h-4 w-4 text-warning" />
        Empty Ticks — Epoch {{ stats.currentEpoch }}
      </h3>
      <NuxtLink
        :to="`/epochs/${stats.currentEpoch}?tab=empty-ticks`"
        class="btn btn-outline text-xs"
      >
        View All
      </NuxtLink>
    </div>

    <!-- Summary stats -->
    <div class="grid grid-cols-3 gap-4 mb-4">
      <div class="card-elevated text-center">
        <div class="text-xl font-bold text-destructive">{{ emptyTickStats.totalEmptyTicks.toLocaleString() }}</div>
        <div class="text-[10px] text-foreground-muted uppercase mt-1">Empty</div>
      </div>
      <div class="card-elevated text-center">
        <div class="text-xl font-bold text-accent">{{ emptyTickStats.totalTicks.toLocaleString() }}</div>
        <div class="text-[10px] text-foreground-muted uppercase mt-1">Total</div>
      </div>
      <div class="card-elevated text-center">
        <div class="text-xl font-bold" :class="emptyRate > 10 ? 'text-destructive' : 'text-warning'">
          {{ emptyRate.toFixed(1) }}%
        </div>
        <div class="text-[10px] text-foreground-muted uppercase mt-1">Empty Rate</div>
      </div>
    </div>

    <!-- Top offenders -->
    <div v-if="topOffenders.length" class="table-wrapper">
      <table>
        <thead>
          <tr>
            <th>#</th>
            <th>Address</th>
            <th>Empty</th>
            <th>Rate</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="comp in topOffenders" :key="comp.computorIndex">
            <td class="text-foreground-muted">{{ comp.computorIndex }}</td>
            <td>
              <AddressDisplay
                v-if="comp.address"
                :address="comp.address"
                :label="getLabel(comp.address)"
                :short="true"
              />
            </td>
            <td class="text-destructive font-semibold">{{ comp.emptyTickCount.toLocaleString() }}</td>
            <td>
              <span :class="comp.totalTickCount > 0 && (comp.emptyTickCount / comp.totalTickCount) > 0.5 ? 'text-destructive font-semibold' : ''">
                {{ comp.totalTickCount > 0 ? ((comp.emptyTickCount / comp.totalTickCount) * 100).toFixed(1) : '0.0' }}%
              </span>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
