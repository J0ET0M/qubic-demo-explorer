<script setup lang="ts">
import { Fish, RefreshCw } from 'lucide-vue-next'
import type { WhaleAlertDto } from '~/composables/useApi'

useHead({ title: 'Whale Alerts - QLI Explorer' })

const api = useApi()
const { formatDate, getTypeClass, truncateAddress } = useFormatting()

const thresholds = [
  { label: '1B', value: 1_000_000_000 },
  { label: '5B', value: 5_000_000_000 },
  { label: '10B', value: 10_000_000_000 },
  { label: '50B', value: 50_000_000_000 },
  { label: '100B', value: 100_000_000_000 },
]

const selectedThreshold = ref(10_000_000_000)
const autoRefresh = ref(false)
const refreshInterval = ref<ReturnType<typeof setInterval> | null>(null)

const { data: alerts, pending, refresh } = await useAsyncData(
  () => `whale-alerts-${selectedThreshold.value}`,
  () => api.getWhaleAlerts(selectedThreshold.value, 100),
  { watch: [selectedThreshold] }
)

function toggleAutoRefresh() {
  autoRefresh.value = !autoRefresh.value
  if (autoRefresh.value) {
    refreshInterval.value = setInterval(() => refresh(), 30000)
  } else if (refreshInterval.value) {
    clearInterval(refreshInterval.value)
    refreshInterval.value = null
  }
}

onUnmounted(() => {
  if (refreshInterval.value) clearInterval(refreshInterval.value)
})

</script>

<template>
  <div class="space-y-6">
    <h1 class="page-title flex items-center gap-2">
      <Fish class="h-5 w-5 text-accent" />
      Whale Alerts
    </h1>

    <!-- Controls -->
    <div class="card">
      <div class="flex flex-wrap items-center gap-4">
        <!-- Threshold selector -->
        <div class="flex items-center gap-2">
          <span class="text-xs text-foreground-muted">Min amount:</span>
          <div class="flex gap-1">
            <button
              v-for="t in thresholds"
              :key="t.value"
              class="px-2.5 py-1 text-xs rounded-md font-medium transition-colors"
              :class="selectedThreshold === t.value
                ? 'bg-accent text-white'
                : 'bg-surface-elevated text-foreground-muted hover:text-foreground'"
              @click="selectedThreshold = t.value"
            >
              {{ t.label }}
            </button>
          </div>
        </div>

        <!-- Auto-refresh toggle -->
        <button
          class="flex items-center gap-1.5 px-3 py-1 text-xs rounded-md font-medium transition-colors"
          :class="autoRefresh
            ? 'bg-success/20 text-success'
            : 'bg-surface-elevated text-foreground-muted hover:text-foreground'"
          @click="toggleAutoRefresh"
        >
          <RefreshCw class="h-3 w-3" :class="autoRefresh ? 'animate-spin' : ''" />
          {{ autoRefresh ? 'Auto (30s)' : 'Auto-refresh' }}
        </button>

        <!-- Manual refresh -->
        <button
          class="flex items-center gap-1.5 px-3 py-1 text-xs rounded-md font-medium bg-surface-elevated text-foreground-muted hover:text-foreground transition-colors"
          @click="refresh()"
        >
          <RefreshCw class="h-3 w-3" />
          Refresh
        </button>
      </div>
    </div>

    <!-- Results -->
    <div v-if="pending" class="card">
      <div class="loading py-12">Loading whale alerts...</div>
    </div>

    <div v-else-if="alerts && alerts.length > 0" class="card">
      <div class="text-xs text-foreground-muted mb-3">
        {{ alerts.length }} large transfers found
      </div>
      <div class="table-wrapper">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Tick</th>
              <th>From</th>
              <th>To</th>
              <th class="text-right">Amount</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="alert in alerts" :key="`${alert.tickNumber}-${alert.txHash}`">
              <td class="text-xs text-foreground-muted whitespace-nowrap">
                {{ formatDate(alert.timestamp) }}
              </td>
              <td>
                <NuxtLink :to="`/ticks/${alert.tickNumber}`" class="text-accent font-mono text-xs">
                  {{ alert.tickNumber.toLocaleString() }}
                </NuxtLink>
              </td>
              <td>
                <NuxtLink
                  :to="`/address/${alert.sourceAddress}`"
                  class="font-mono text-xs"
                  :class="getTypeClass(alert.sourceType)"
                >
                  {{ alert.sourceLabel || truncateAddress(alert.sourceAddress) }}
                </NuxtLink>
                <span v-if="alert.sourceType === 'exchange'" class="ml-1 badge badge-warning text-[0.625rem]">CEX</span>
              </td>
              <td>
                <NuxtLink
                  :to="`/address/${alert.destAddress}`"
                  class="font-mono text-xs"
                  :class="getTypeClass(alert.destType)"
                >
                  {{ alert.destLabel || truncateAddress(alert.destAddress) }}
                </NuxtLink>
                <span v-if="alert.destType === 'exchange'" class="ml-1 badge badge-warning text-[0.625rem]">CEX</span>
              </td>
              <td class="text-right font-semibold text-accent whitespace-nowrap">
                {{ alert.amountFormatted }}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <div v-else class="card">
      <div class="text-center py-12 text-foreground-muted text-sm">
        No transfers found above the selected threshold.
      </div>
    </div>
  </div>
</template>
