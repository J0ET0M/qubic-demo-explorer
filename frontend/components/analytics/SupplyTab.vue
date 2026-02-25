<script setup lang="ts">
import { Coins, Flame, Pickaxe } from 'lucide-vue-next'

const api = useApi()

const { data: supply, pending } = await useAsyncData(
  'supply-dashboard',
  () => api.getSupplyDashboard()
)

const formatVolume = (volume: number) => {
  if (volume >= 1_000_000_000_000) return (volume / 1_000_000_000_000).toFixed(2) + 'T'
  if (volume >= 1_000_000_000) return (volume / 1_000_000_000).toFixed(2) + 'B'
  if (volume >= 1_000_000) return (volume / 1_000_000).toFixed(2) + 'M'
  if (volume >= 1_000) return (volume / 1_000).toFixed(2) + 'K'
  return volume.toLocaleString()
}

// Chart data for emissions (reversed for chronological order)
const emissionLabels = computed(() => {
  if (!supply.value?.emissionHistory) return []
  return [...supply.value.emissionHistory].reverse().map(e => `E${e.epoch}`)
})

const emissionData = computed(() => {
  if (!supply.value?.emissionHistory) return []
  return [...supply.value.emissionHistory].reverse().map(e => e.totalEmission)
})

// Chart data for burns
const burnLabels = computed(() => {
  if (!supply.value?.burnHistory) return []
  return [...supply.value.burnHistory].reverse().map(e => `E${e.epoch}`)
})

const burnData = computed(() => {
  if (!supply.value?.burnHistory) return []
  return [...supply.value.burnHistory].reverse().map(e => e.burnAmount)
})
</script>

<template>
  <div class="space-y-6">
    <div v-if="pending" class="loading py-12">Loading supply data...</div>

    <template v-else-if="supply">
      <!-- Summary Cards -->
      <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div class="card-elevated text-center">
          <div class="flex items-center justify-center gap-2 mb-2">
            <Coins class="h-4 w-4 text-accent" />
          </div>
          <div class="text-2xl font-bold text-accent">{{ formatVolume(supply.circulatingSupply) }} QU</div>
          <div class="text-xs text-foreground-muted uppercase mt-1">Circulating Supply</div>
          <div class="text-xs text-foreground-muted mt-0.5">Epoch {{ supply.snapshotEpoch }}</div>
        </div>
        <div class="card-elevated text-center">
          <div class="flex items-center justify-center gap-2 mb-2">
            <Flame class="h-4 w-4 text-destructive" />
          </div>
          <div class="text-2xl font-bold text-destructive">{{ formatVolume(supply.totalBurned) }} QU</div>
          <div class="text-xs text-foreground-muted uppercase mt-1">Total Burned</div>
        </div>
        <div class="card-elevated text-center">
          <div class="flex items-center justify-center gap-2 mb-2">
            <Pickaxe class="h-4 w-4 text-success" />
          </div>
          <div class="text-2xl font-bold text-success">{{ formatVolume(supply.latestEpochEmission) }} QU</div>
          <div class="text-xs text-foreground-muted uppercase mt-1">Latest Epoch Emission</div>
        </div>
      </div>

      <!-- Charts -->
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <!-- Emission History -->
        <div class="card" v-if="emissionLabels.length">
          <h2 class="section-title mb-4">
            <Pickaxe class="h-5 w-5 text-success" />
            Emission per Epoch
          </h2>
          <ClientOnly>
            <ChartsEpochBarChart
              :labels="emissionLabels"
              :datasets="[{ label: 'Emission (QU)', data: emissionData }]"
              :height="250"
            />
            <template #fallback>
              <div class="h-[250px] flex items-center justify-center text-foreground-muted text-sm">
                Loading chart...
              </div>
            </template>
          </ClientOnly>
        </div>

        <!-- Burn History -->
        <div class="card" v-if="burnLabels.length">
          <h2 class="section-title mb-4">
            <Flame class="h-5 w-5 text-destructive" />
            Burns per Epoch
          </h2>
          <ClientOnly>
            <ChartsEpochBarChart
              :labels="burnLabels"
              :datasets="[{ label: 'Burned (QU)', data: burnData }]"
              :height="250"
            />
            <template #fallback>
              <div class="h-[250px] flex items-center justify-center text-foreground-muted text-sm">
                Loading chart...
              </div>
            </template>
          </ClientOnly>
        </div>
      </div>

      <!-- Emission History Table -->
      <div class="card" v-if="supply.emissionHistory.length">
        <h2 class="section-title mb-4">
          <Pickaxe class="h-5 w-5 text-success" />
          Emission History
        </h2>
        <div class="table-wrapper">
          <table>
            <thead>
              <tr>
                <th>Epoch</th>
                <th>Total Emission</th>
                <th>Computors</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="e in supply.emissionHistory" :key="e.epoch">
                <td>
                  <NuxtLink :to="`/epochs/${e.epoch}`" class="text-accent">{{ e.epoch }}</NuxtLink>
                </td>
                <td class="font-semibold text-success">{{ formatVolume(e.totalEmission) }} QU</td>
                <td>{{ e.computorCount }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </template>
  </div>
</template>
