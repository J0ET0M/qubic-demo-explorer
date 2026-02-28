<script setup lang="ts">
import { Coins, Flame, Pickaxe, Target, Hash } from 'lucide-vue-next'

const api = useApi()
const { formatVolume } = useFormatting()

const { data: supply, pending } = await useAsyncData(
  'supply-dashboard',
  () => api.getSupplyDashboard()
)

// Chart data for emissions (reversed for chronological order)
const emissionChartSorted = computed(() => {
  if (!supply.value?.emissionHistory) return []
  return [...supply.value.emissionHistory].reverse()
})

const emissionLabels = computed(() => emissionChartSorted.value.map(e => `E${e.epoch}`))

const emissionComputorData = computed(() =>
  emissionChartSorted.value.map(e => e.computorEmission)
)

const emissionArbData = computed(() =>
  emissionChartSorted.value.map(e => e.arbRevenue)
)

const emissionDonationData = computed(() =>
  emissionChartSorted.value.map(e => e.donationTotal)
)

// Chart data for burns
const burnChartSorted = computed(() => {
  if (!supply.value?.burnHistory) return []
  return [...supply.value.burnHistory].reverse()
})

const burnLabels = computed(() =>
  burnChartSorted.value.map(e =>
    new Date(e.snapshotAt).toLocaleDateString('en-US', { month: 'short', day: 'numeric', hour: '2-digit' })
  )
)

const burnData = computed(() => burnChartSorted.value.map(e => e.burnAmount))

// Derived metrics
const inflationRate = computed(() => {
  if (!supply.value || !supply.value.circulatingSupply) return 0
  return (supply.value.latestEpochEmission / supply.value.circulatingSupply) * 100
})

const burnRate = computed(() => {
  if (!supply.value || !supply.value.totalEmitted) return 0
  return (supply.value.totalBurned / supply.value.totalEmitted) * 100
})

// Collect unique donation recipient names across all epochs
const donationRecipients = computed(() => {
  if (!supply.value?.emissionHistory) return []
  const names = new Set<string>()
  for (const e of supply.value.emissionHistory) {
    for (const d of e.donations) {
      names.add(d.label || d.address.slice(0, 8))
    }
  }
  return [...names]
})
</script>

<template>
  <div class="space-y-6">
    <div v-if="pending" class="loading py-12">Loading supply data...</div>

    <template v-else-if="supply && (supply.snapshotEpoch > 0 || supply.emissionHistory.length > 0 || supply.burnHistory.length > 0)">
      <!-- Summary Cards - Row 1 -->
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
          <div class="text-xs text-foreground-muted mt-0.5">{{ burnRate.toFixed(1) }}% of emitted</div>
        </div>
        <div class="card-elevated text-center">
          <div class="flex items-center justify-center gap-2 mb-2">
            <Pickaxe class="h-4 w-4 text-success" />
          </div>
          <div class="text-2xl font-bold text-success">{{ formatVolume(supply.latestEpochEmission) }} QU</div>
          <div class="text-xs text-foreground-muted uppercase mt-1">Latest Epoch Emission</div>
          <div class="text-xs text-foreground-muted mt-0.5">~{{ inflationRate.toFixed(3) }}% per epoch</div>
        </div>
      </div>

      <!-- Summary Cards - Row 2 -->
      <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div class="card-elevated text-center">
          <div class="flex items-center justify-center gap-2 mb-2">
            <Target class="h-4 w-4 text-warning" />
          </div>
          <div class="text-2xl font-bold text-warning">{{ formatVolume(supply.supplyCap) }} QU</div>
          <div class="text-xs text-foreground-muted uppercase mt-1">Supply Cap</div>
          <div class="mt-2">
            <div class="w-full bg-background-secondary rounded-full h-2">
              <div
                class="bg-warning rounded-full h-2 transition-all"
                :style="{ width: `${Math.min(supply.supplyCapProgress, 100)}%` }"
              />
            </div>
            <div class="text-xs text-foreground-muted mt-1">{{ supply.supplyCapProgress.toFixed(2) }}% reached</div>
          </div>
        </div>
        <div class="card-elevated text-center">
          <div class="flex items-center justify-center gap-2 mb-2">
            <Hash class="h-4 w-4 text-foreground-muted" />
          </div>
          <div class="text-2xl font-bold">{{ supply.epochCount }}</div>
          <div class="text-xs text-foreground-muted uppercase mt-1">Epochs</div>
          <div class="text-xs text-foreground-muted mt-0.5">1T QU emitted per epoch</div>
        </div>
      </div>

      <!-- Charts -->
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <!-- Emission History (stacked: computor + ARB + donations) -->
        <div class="card" v-if="emissionLabels.length">
          <h2 class="section-title mb-4">
            <Pickaxe class="h-5 w-5 text-success" />
            Emission Breakdown per Epoch
          </h2>
          <ClientOnly>
            <ChartsEpochBarChart
              :labels="emissionLabels"
              :datasets="[
                { label: 'Computors', data: emissionComputorData },
                { label: 'Arbitrator', data: emissionArbData },
                { label: 'Donations', data: emissionDonationData }
              ]"
              :height="250"
              :stacked="true"
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
            Burns (4-Hour Snapshots)
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
                <th>Computors</th>
                <th>Arbitrator</th>
                <th v-for="name in donationRecipients" :key="name">{{ name }}</th>
                <th>Donations Total</th>
                <th># Computors</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="e in supply.emissionHistory" :key="e.epoch">
                <td>
                  <NuxtLink :to="`/epochs/${e.epoch}`" class="text-accent">{{ e.epoch }}</NuxtLink>
                </td>
                <td class="font-semibold text-success">{{ formatVolume(e.computorEmission) }}</td>
                <td class="font-semibold text-warning">{{ formatVolume(e.arbRevenue) }}</td>
                <td v-for="name in donationRecipients" :key="name" class="text-info">
                  {{ formatVolume(e.donations.find(d => (d.label || d.address.slice(0, 8)) === name)?.amount || 0) }}
                </td>
                <td class="text-foreground-muted">{{ formatVolume(e.donationTotal) }}</td>
                <td>{{ e.computorCount }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </template>

    <div v-else class="card">
      <div class="text-center py-12 text-foreground-muted text-sm">
        No supply data available yet. Spectrum files need to be imported first.
      </div>
    </div>
  </div>
</template>
