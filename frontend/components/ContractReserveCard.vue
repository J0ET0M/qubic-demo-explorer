<script setup lang="ts">
import { Flame, TrendingDown, Coins, Clock } from 'lucide-vue-next'

const props = defineProps<{
  address: string
}>()

const api = useApi()
const { formatVolume, formatDateTime } = useFormatting()

const days = ref<1 | 7 | 30>(7)

const { data, pending } = await useAsyncData(
  () => `contract-reserve-${props.address}-${days.value}`,
  () => api.getContractReserveHistory(props.address, days.value),
  { watch: [days] }
)

const labels = computed(() =>
  data.value?.samples.map(s => formatDateTime(s.timestamp)) ?? [])
const balances = computed(() =>
  data.value?.samples.map(s => s.balance) ?? [])

const runwayLabel = computed(() => {
  const d = data.value?.estimatedRunwayDays
  if (d == null || !Number.isFinite(d)) return '—'
  if (d < 1) return `${(d * 24).toFixed(1)} h`
  if (d < 30) return `${d.toFixed(1)} d`
  if (d < 365) return `${(d / 30).toFixed(1)} mo`
  return `${(d / 365).toFixed(1)} y`
})

const burnLabel = computed(() => {
  const r = data.value?.burnRatePerDay ?? 0
  if (r === 0) return '—'
  return r > 0
    ? `${formatVolume(r)} / day`
    : `+${formatVolume(-r)} / day (growing)`
})

const burnClass = computed(() => {
  const r = data.value?.burnRatePerDay ?? 0
  if (r > 0) return 'text-warning'
  if (r < 0) return 'text-success'
  return 'text-foreground-muted'
})
</script>

<template>
  <div class="card">
    <div class="flex items-center justify-between flex-wrap gap-2 mb-4">
      <h2 class="section-title">
        <Flame class="h-5 w-5 text-accent" />
        Execution Fee Reserve
      </h2>
      <div class="flex items-center gap-1 text-xs">
        <button
          v-for="opt in [1, 7, 30] as const"
          :key="opt"
          :class="[
            'px-2 py-1 rounded',
            days === opt
              ? 'bg-accent/15 text-accent font-semibold'
              : 'bg-surface-elevated hover:bg-surface-hover text-foreground-muted'
          ]"
          @click="days = opt"
        >
          {{ opt }}d
        </button>
      </div>
    </div>

    <div v-if="pending && !data" class="loading py-8">Loading reserve history…</div>
    <template v-else-if="data">
      <div class="grid grid-cols-2 md:grid-cols-3 gap-3 mb-4">
        <div class="rounded p-2 bg-surface-elevated">
          <div class="text-xs text-foreground-muted flex items-center gap-1">
            <Coins class="h-3 w-3" /> Current reserve
          </div>
          <div class="font-mono font-bold text-lg text-accent">
            {{ formatVolume(data.currentBalance) }}
          </div>
        </div>
        <div class="rounded p-2 bg-surface-elevated">
          <div class="text-xs text-foreground-muted flex items-center gap-1">
            <TrendingDown class="h-3 w-3" /> Burn rate ({{ days }}d window)
          </div>
          <div class="font-mono" :class="burnClass">{{ burnLabel }}</div>
        </div>
        <div class="rounded p-2 bg-surface-elevated">
          <div class="text-xs text-foreground-muted flex items-center gap-1">
            <Clock class="h-3 w-3" /> Est. runway
          </div>
          <div class="font-mono">{{ runwayLabel }}</div>
        </div>
      </div>

      <div v-if="data.sampleCount === 0" class="text-center py-6 text-foreground-muted text-sm">
        No snapshots yet. The analytics service samples every ~10 minutes —
        check back shortly.
      </div>
      <div v-else-if="data.sampleCount < 2" class="text-center py-6 text-foreground-muted text-sm">
        Only one snapshot so far ({{ formatDateTime(data.samples[0].timestamp) }}).
        At least two are needed to draw a trend.
      </div>
      <ClientOnly v-else>
        <LazyChartsEpochLineChart
          :labels="labels"
          :datasets="[{
            label: 'Reserve (QU)',
            data: balances,
            borderColor: 'rgb(108, 140, 204)',
            backgroundColor: 'rgba(108, 140, 204, 0.08)',
            fill: true,
            tension: 0.3,
          }]"
          y-axis-label="QU"
          :height="240"
        />
      </ClientOnly>
    </template>
  </div>
</template>
