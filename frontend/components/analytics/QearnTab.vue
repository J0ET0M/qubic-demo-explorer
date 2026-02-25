<script setup lang="ts">
import { Flame, ArrowDownToLine, ArrowUpFromLine, Table } from 'lucide-vue-next'

const api = useApi()

const { data: qearnStats, status } = await useAsyncData('qearn-stats', () => api.getQearnStats())

const loading = computed(() => status.value === 'pending')

const epochs = computed(() => qearnStats.value?.epochs ?? [])

// Chart labels (epoch numbers)
const chartLabels = computed(() => epochs.value.map(e => `E${e.epoch}`))

// All-time yield = outputs - inputs + burns
const allTimeYield = computed(() => {
  const out = qearnStats.value?.allTimeTotalOutput ?? 0
  const inp = qearnStats.value?.allTimeTotalInput ?? 0
  const burned = qearnStats.value?.allTimeTotalBurned ?? 0
  return out - inp + burned
})

// Per-epoch yield
const epochYield = (ep: { totalOutput: number, totalInput: number, totalBurned: number }) =>
  ep.totalOutput - ep.totalInput + ep.totalBurned

// Balance = input - output (what's still in the contract)
const allTimeBalance = computed(() => {
  const inp = qearnStats.value?.allTimeTotalInput ?? 0
  const out = qearnStats.value?.allTimeTotalOutput ?? 0
  return inp - out
})

const formatVolume = (volume: number) => {
  const abs = Math.abs(volume)
  const sign = volume < 0 ? '-' : ''
  if (abs >= 1_000_000_000_000) return sign + (abs / 1_000_000_000_000).toFixed(1) + 'T'
  if (abs >= 1_000_000_000) return sign + (abs / 1_000_000_000).toFixed(1) + 'B'
  if (abs >= 1_000_000) return sign + (abs / 1_000_000).toFixed(1) + 'M'
  if (abs >= 1_000) return sign + (abs / 1_000).toFixed(1) + 'K'
  return sign + abs.toLocaleString()
}
</script>

<template>
  <div class="space-y-6">
    <!-- Overview -->
    <div class="card">
      <h2 class="section-title mb-4">
        <Flame class="h-5 w-5 text-accent" />
        Qearn Overview
      </h2>

      <div v-if="loading" class="loading">Loading...</div>
      <template v-else-if="epochs.length">
        <!-- Summary cards -->
        <div class="grid grid-cols-2 md:grid-cols-5 gap-4 mb-6">
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold text-destructive">{{ formatVolume(qearnStats?.allTimeTotalBurned || 0) }}</div>
            <div class="text-sm text-foreground-muted">Total Burned</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold text-info">{{ formatVolume(qearnStats?.allTimeTotalInput || 0) }}</div>
            <div class="text-sm text-foreground-muted">Total Locked (In)</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold text-warning">{{ formatVolume(qearnStats?.allTimeTotalOutput || 0) }}</div>
            <div class="text-sm text-foreground-muted">Total Unlocked (Out)</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold text-success">{{ formatVolume(allTimeYield) }}</div>
            <div class="text-sm text-foreground-muted">Total Yield</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold">{{ formatVolume(allTimeBalance) }}</div>
            <div class="text-sm text-foreground-muted">Balance (In - Out)</div>
          </div>
        </div>

        <!-- Burns per epoch chart -->
        <ClientOnly>
          <ChartsEpochBarChart
            :labels="chartLabels"
            :datasets="[
              {
                label: 'Burned',
                data: epochs.map(e => e.totalBurned),
                backgroundColor: 'rgba(239, 68, 68, 0.7)',
                borderColor: 'rgb(239, 68, 68)'
              }
            ]"
            :height="250"
            y-axis-label="QU"
          />
          <template #fallback>
            <div class="h-[250px] flex items-center justify-center text-foreground-muted">
              Loading chart...
            </div>
          </template>
        </ClientOnly>
        <p class="text-xs text-foreground-muted mt-2">
          QU burned by Qearn per epoch. Burns come from the unrewarded portion of the bonus pool at epoch maturity and early-unlock penalties.
        </p>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        <Flame class="h-12 w-12 mx-auto mb-4 opacity-50" />
        <p>No Qearn data available yet.</p>
      </div>
    </div>

    <!-- Inputs vs Outputs chart -->
    <div class="card" v-if="epochs.length">
      <h2 class="section-title mb-4">
        <ArrowDownToLine class="h-5 w-5 text-accent" />
        Locked vs Unlocked per Epoch
      </h2>

      <ClientOnly>
        <ChartsEpochBarChart
          :labels="chartLabels"
          :datasets="[
            {
              label: 'Locked (In)',
              data: epochs.map(e => e.totalInput),
              backgroundColor: 'rgba(108, 140, 204, 0.7)',
              borderColor: 'rgb(108, 140, 204)'
            },
            {
              label: 'Unlocked (Out)',
              data: epochs.map(e => e.totalOutput),
              backgroundColor: 'rgba(240, 184, 90, 0.7)',
              borderColor: 'rgb(240, 184, 90)'
            }
          ]"
          :height="250"
          y-axis-label="QU"
        />
        <template #fallback>
          <div class="h-[250px] flex items-center justify-center text-foreground-muted">
            Loading chart...
          </div>
        </template>
      </ClientOnly>
      <p class="text-xs text-foreground-muted mt-2">
        Locked = QU deposited into Qearn by users. Unlocked = QU paid out by Qearn (principal + yield). Yield per epoch = Out - In + Burns.
      </p>
    </div>

    <!-- Per-Epoch Table -->
    <div class="card" v-if="epochs.length">
      <h2 class="section-title mb-4">
        <Table class="h-5 w-5 text-accent" />
        Per-Epoch Breakdown
      </h2>

      <div class="overflow-x-auto">
        <table class="data-table w-full">
          <thead>
            <tr>
              <th>Epoch</th>
              <th class="text-right">Burned</th>
              <th class="text-right">Locked (In)</th>
              <th class="text-right">Unlocked (Out)</th>
              <th class="text-right">Yield</th>
              <th class="text-right">Lockers</th>
              <th class="text-right">Unlockers</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="ep in [...epochs].reverse()" :key="ep.epoch">
              <td class="font-mono">{{ ep.epoch }}</td>
              <td class="text-right text-destructive">{{ formatVolume(ep.totalBurned) }}</td>
              <td class="text-right text-info">{{ formatVolume(ep.totalInput) }}</td>
              <td class="text-right text-warning">{{ formatVolume(ep.totalOutput) }}</td>
              <td class="text-right font-mono" :class="epochYield(ep) >= 0 ? 'text-success' : 'text-destructive'">
                {{ formatVolume(epochYield(ep)) }}
              </td>
              <td class="text-right">{{ ep.uniqueLockers.toLocaleString() }}</td>
              <td class="text-right">{{ ep.uniqueUnlockers.toLocaleString() }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>
</template>
