<script setup lang="ts">
import { Calendar, BarChart3, TrendingUp, Users } from 'lucide-vue-next'

const api = useApi()

const { data: epochs, pending } = await useAsyncData(
  'epochs',
  () => api.getEpochs(100)
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

const activeAddressesData = computed(() => {
  if (!epochs.value) return []
  return [...epochs.value].reverse().map(e => e.activeAddresses)
})

const tickCountData = computed(() => {
  if (!epochs.value) return []
  return [...epochs.value].reverse().map(e => e.tickCount)
})

const { formatVolume, formatDateShort: formatDate } = useFormatting()

const formatDuration = (startStr: string, endStr: string) => {
  const days = Math.floor((new Date(endStr).getTime() - new Date(startStr).getTime()) / (1000 * 60 * 60 * 24))
  return `${days} days`
}

// Calculate summary stats
const totalTx = computed(() => {
  if (!epochs.value) return 0
  return epochs.value.reduce((sum, e) => sum + e.txCount, 0)
})

const totalVolume = computed(() => {
  if (!epochs.value) return 0
  return epochs.value.reduce((sum, e) => sum + e.totalVolume, 0)
})

const avgActiveAddresses = computed(() => {
  if (!epochs.value || epochs.value.length === 0) return 0
  return Math.round(epochs.value.reduce((sum, e) => sum + e.activeAddresses, 0) / epochs.value.length)
})
</script>

<template>
  <div class="space-y-6">
    <!-- Summary Stats -->
    <div class="stats-grid" v-if="!pending && epochs?.length">
      <div class="stat-card">
        <div class="flex items-center gap-2 mb-2">
          <Calendar class="h-5 w-5 text-accent" />
        </div>
        <div class="value">{{ epochs.length }}</div>
        <div class="label">Total Epochs</div>
      </div>
      <div class="stat-card">
        <div class="flex items-center gap-2 mb-2">
          <BarChart3 class="h-5 w-5 text-accent" />
        </div>
        <div class="value">{{ totalTx.toLocaleString() }}</div>
        <div class="label">Total Transactions</div>
      </div>
      <div class="stat-card">
        <div class="flex items-center gap-2 mb-2">
          <TrendingUp class="h-5 w-5 text-accent" />
        </div>
        <div class="value">{{ formatVolume(totalVolume) }}</div>
        <div class="label">Total Volume (QU)</div>
      </div>
      <div class="stat-card">
        <div class="flex items-center gap-2 mb-2">
          <Users class="h-5 w-5 text-accent" />
        </div>
        <div class="value">{{ avgActiveAddresses.toLocaleString() }}</div>
        <div class="label">Avg Active Addresses</div>
      </div>
    </div>

    <!-- Charts Section -->
    <div v-if="!pending && epochs?.length" class="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <!-- Transactions Chart -->
      <div class="card">
        <h3 class="section-title mb-4">
          <BarChart3 class="h-5 w-5 text-accent" />
          Transactions per Epoch
        </h3>
        <ClientOnly>
          <ChartsEpochBarChart
            :labels="chartLabels"
            :datasets="[{ label: 'Transactions', data: txCountData }]"
            :height="250"
          />
          <template #fallback>
            <div class="h-[250px] flex items-center justify-center text-foreground-muted">
              Loading chart...
            </div>
          </template>
        </ClientOnly>
      </div>

      <!-- Volume Chart -->
      <div class="card">
        <h3 class="section-title mb-4">
          <TrendingUp class="h-5 w-5 text-accent" />
          Volume per Epoch (QU)
        </h3>
        <ClientOnly>
          <ChartsEpochLineChart
            :labels="chartLabels"
            :datasets="[{
              label: 'Volume',
              data: volumeData,
              borderColor: 'rgb(16, 185, 129)',
              backgroundColor: 'rgba(16, 185, 129, 0.1)'
            }]"
            :height="250"
          />
          <template #fallback>
            <div class="h-[250px] flex items-center justify-center text-foreground-muted">
              Loading chart...
            </div>
          </template>
        </ClientOnly>
      </div>

      <!-- Active Addresses Chart -->
      <div class="card">
        <h3 class="section-title mb-4">
          <Users class="h-5 w-5 text-accent" />
          Active Addresses per Epoch
        </h3>
        <ClientOnly>
          <ChartsEpochLineChart
            :labels="chartLabels"
            :datasets="[{
              label: 'Active Addresses',
              data: activeAddressesData,
              borderColor: 'rgb(139, 92, 246)',
              backgroundColor: 'rgba(139, 92, 246, 0.1)'
            }]"
            :height="250"
          />
          <template #fallback>
            <div class="h-[250px] flex items-center justify-center text-foreground-muted">
              Loading chart...
            </div>
          </template>
        </ClientOnly>
      </div>

      <!-- Ticks Chart -->
      <div class="card">
        <h3 class="section-title mb-4">
          <Calendar class="h-5 w-5 text-accent" />
          Ticks per Epoch
        </h3>
        <ClientOnly>
          <ChartsEpochBarChart
            :labels="chartLabels"
            :datasets="[{
              label: 'Ticks',
              data: tickCountData,
              backgroundColor: 'rgba(245, 158, 11, 0.8)'
            }]"
            :height="250"
          />
          <template #fallback>
            <div class="h-[250px] flex items-center justify-center text-foreground-muted">
              Loading chart...
            </div>
          </template>
        </ClientOnly>
      </div>
    </div>

    <!-- Epochs Table -->
    <div class="card">
      <h2 class="section-title mb-4">
        <Calendar class="h-5 w-5 text-accent" />
        All Epochs
      </h2>

      <div v-if="pending" class="loading">Loading...</div>

      <template v-else-if="epochs?.length">
        <div class="table-wrapper">
          <table>
            <thead>
              <tr>
                <th>Epoch</th>
                <th>Ticks</th>
                <th>Transactions</th>
                <th>Volume (QU)</th>
                <th class="hide-mobile">Active Addresses</th>
                <th class="hide-mobile">Duration</th>
                <th class="hide-mobile">Start</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="epoch in epochs" :key="epoch.epoch">
                <td>
                  <NuxtLink :to="`/epochs/${epoch.epoch}`" class="text-accent font-semibold">
                    {{ epoch.epoch }}
                  </NuxtLink>
                </td>
                <td>{{ epoch.tickCount.toLocaleString() }}</td>
                <td>{{ epoch.txCount.toLocaleString() }}</td>
                <td>{{ formatVolume(epoch.totalVolume) }}</td>
                <td class="hide-mobile">{{ epoch.activeAddresses.toLocaleString() }}</td>
                <td class="hide-mobile">{{ formatDuration(epoch.startTime, epoch.endTime) }}</td>
                <td class="hide-mobile">{{ formatDate(epoch.startTime) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </template>

      <div v-else class="text-center py-8 text-foreground-muted">
        No epoch data available yet.
      </div>
    </div>
  </div>
</template>
