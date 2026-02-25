<script setup lang="ts">
import { Flame, Gift } from 'lucide-vue-next'

const api = useApi()

const { data: qearnStats, status } = await useAsyncData('qearn-stats', () => api.getQearnStats())

const loading = computed(() => status.value === 'pending')

const epochs = computed(() => qearnStats.value?.epochs ?? [])

// Chart labels (epoch numbers)
const chartLabels = computed(() => epochs.value.map(e => `E${e.epoch}`))

// Burn per epoch chart
const burnChartData = computed(() => ({
  total: epochs.value.map(e => e.totalBurned),
}))

// Reward per epoch chart
const rewardChartData = computed(() => ({
  total: epochs.value.map(e => e.totalRewarded),
}))

// Burn vs Reward combined chart
const combinedChartData = computed(() => ({
  burn: epochs.value.map(e => e.totalBurned),
  reward: epochs.value.map(e => e.totalRewarded),
}))

const formatVolume = (volume: number) => {
  if (volume >= 1_000_000_000_000) return (volume / 1_000_000_000_000).toFixed(1) + 'T'
  if (volume >= 1_000_000_000) return (volume / 1_000_000_000).toFixed(1) + 'B'
  if (volume >= 1_000_000) return (volume / 1_000_000).toFixed(1) + 'M'
  if (volume >= 1_000) return (volume / 1_000).toFixed(1) + 'K'
  return volume.toLocaleString()
}

// Net burn (burned - rewarded)
const netBurn = computed(() => {
  const burned = qearnStats.value?.allTimeTotalBurned ?? 0
  const rewarded = qearnStats.value?.allTimeTotalRewarded ?? 0
  return burned - rewarded
})

// Latest epoch with data
const latestEpoch = computed(() => {
  if (!epochs.value.length) return null
  return epochs.value[epochs.value.length - 1]
})
</script>

<template>
  <div class="space-y-6">
    <!-- Burn vs Reward Overview -->
    <div class="card">
      <h2 class="section-title mb-4">
        <Flame class="h-5 w-5 text-accent" />
        Qearn Burns &amp; Rewards
      </h2>

      <div v-if="loading" class="loading">Loading...</div>
      <template v-else-if="epochs.length">
        <!-- Summary cards -->
        <div class="grid grid-cols-4 gap-4 mb-6">
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold text-destructive">{{ formatVolume(qearnStats?.allTimeTotalBurned || 0) }}</div>
            <div class="text-sm text-foreground-muted">Total Burned</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold text-success">{{ formatVolume(qearnStats?.allTimeTotalRewarded || 0) }}</div>
            <div class="text-sm text-foreground-muted">Total Rewarded</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold" :class="netBurn >= 0 ? 'text-destructive' : 'text-success'">{{ formatVolume(Math.abs(netBurn)) }}</div>
            <div class="text-sm text-foreground-muted">Net Burn</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold">{{ epochs.length }}</div>
            <div class="text-sm text-foreground-muted">Epochs Active</div>
          </div>
        </div>

        <!-- Combined burn vs reward chart -->
        <ClientOnly>
          <ChartsEpochBarChart
            :labels="chartLabels"
            :datasets="[
              {
                label: 'Burned',
                data: combinedChartData.burn,
                backgroundColor: 'rgba(239, 68, 68, 0.7)',
                borderColor: 'rgb(239, 68, 68)'
              },
              {
                label: 'Rewarded',
                data: combinedChartData.reward,
                backgroundColor: 'rgba(102, 187, 154, 0.7)',
                borderColor: 'rgb(102, 187, 154)'
              }
            ]"
            :height="300"
            y-axis-label="QU"
          />
          <template #fallback>
            <div class="h-[300px] flex items-center justify-center text-foreground-muted">
              Loading chart...
            </div>
          </template>
        </ClientOnly>
        <p class="text-xs text-foreground-muted mt-2">
          Burns and rewards per epoch from the Qearn smart contract. Burns = QU permanently removed. Rewards = QU distributed to stakers at epoch end.
        </p>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        <Flame class="h-12 w-12 mx-auto mb-4 opacity-50" />
        <p>No Qearn data available yet.</p>
      </div>
    </div>

    <!-- Per-Epoch Table -->
    <div class="card">
      <h2 class="section-title mb-4">
        <Gift class="h-5 w-5 text-accent" />
        Per-Epoch Breakdown
      </h2>

      <div v-if="loading" class="loading">Loading...</div>
      <template v-else-if="epochs.length">
        <div class="overflow-x-auto">
          <table class="data-table w-full">
            <thead>
              <tr>
                <th>Epoch</th>
                <th class="text-right">Burned</th>
                <th class="text-right">Burn Count</th>
                <th class="text-right">Rewarded</th>
                <th class="text-right">Reward Count</th>
                <th class="text-right">Recipients</th>
                <th class="text-right">Net Burn</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="ep in [...epochs].reverse()" :key="ep.epoch">
                <td class="font-mono">{{ ep.epoch }}</td>
                <td class="text-right text-destructive">{{ formatVolume(ep.totalBurned) }}</td>
                <td class="text-right">{{ ep.burnCount.toLocaleString() }}</td>
                <td class="text-right text-success">{{ formatVolume(ep.totalRewarded) }}</td>
                <td class="text-right">{{ ep.rewardCount.toLocaleString() }}</td>
                <td class="text-right">{{ ep.uniqueRewardRecipients.toLocaleString() }}</td>
                <td class="text-right font-mono" :class="ep.totalBurned >= ep.totalRewarded ? 'text-destructive' : 'text-success'">
                  {{ ep.totalBurned >= ep.totalRewarded ? '-' : '+' }}{{ formatVolume(Math.abs(ep.totalBurned - ep.totalRewarded)) }}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        No epoch data available.
      </div>
    </div>
  </div>
</template>
