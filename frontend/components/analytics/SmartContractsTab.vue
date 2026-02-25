<script setup lang="ts">
import { Cpu } from 'lucide-vue-next'
import type { SmartContractUsageDto } from '~/composables/useApi'

const api = useApi()

// Smart contract usage
const { data: scUsage, pending: scUsageLoading } = await useAsyncData(
  'smart-contract-usage',
  () => api.getSmartContractUsage()
)

// Prepare chart data for SC usage
const scChartLabels = computed(() => {
  if (!scUsage.value) return []
  return scUsage.value.slice(0, 10).map(sc => sc.name)
})

const scChartData = computed(() => {
  if (!scUsage.value) return []
  return scUsage.value.slice(0, 10).map(sc => sc.callCount)
})

const { formatVolume, truncateAddress } = useFormatting()
const getAddressDisplay = (item: SmartContractUsageDto) => item.name || truncateAddress(item.address)
</script>

<template>
  <div class="space-y-6">
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <!-- Smart Contract Usage Chart -->
      <div class="card">
        <h2 class="section-title mb-4">
          <Cpu class="h-5 w-5 text-accent" />
          Smart Contract Usage
        </h2>

        <div v-if="scUsageLoading" class="loading">Loading...</div>
        <template v-else-if="scUsage?.length">
          <ClientOnly>
            <ChartsEpochBarChart
              :labels="scChartLabels"
              :datasets="[{
                label: 'Calls',
                data: scChartData,
                backgroundColor: 'rgba(139, 92, 246, 0.8)'
              }]"
              :height="300"
            />
            <template #fallback>
              <div class="h-[300px] flex items-center justify-center text-foreground-muted">
                Loading chart...
              </div>
            </template>
          </ClientOnly>
        </template>
        <div v-else class="text-center py-8 text-foreground-muted">
          No smart contract data available
        </div>
      </div>

      <!-- Smart Contract Summary Stats -->
      <div class="card">
        <h2 class="section-title mb-4">
          <Cpu class="h-5 w-5 text-accent" />
          Summary
        </h2>

        <div v-if="scUsageLoading" class="loading">Loading...</div>
        <template v-else-if="scUsage?.length">
          <div class="grid grid-cols-2 gap-4 mb-4">
            <div class="card-elevated text-center">
              <div class="text-2xl font-bold text-accent">{{ scUsage.length }}</div>
              <div class="text-sm text-foreground-muted">Active Contracts</div>
            </div>
            <div class="card-elevated text-center">
              <div class="text-2xl font-bold text-accent">
                {{ scUsage.reduce((sum, sc) => sum + sc.callCount, 0).toLocaleString() }}
              </div>
              <div class="text-sm text-foreground-muted">Total Calls</div>
            </div>
          </div>
          <div class="grid grid-cols-2 gap-4">
            <div class="card-elevated text-center">
              <div class="text-2xl font-bold text-accent">
                {{ formatVolume(scUsage.reduce((sum, sc) => sum + sc.totalAmount, 0)) }}
              </div>
              <div class="text-sm text-foreground-muted">Total Volume</div>
            </div>
            <div class="card-elevated text-center">
              <div class="text-2xl font-bold text-accent">
                {{ scUsage.reduce((sum, sc) => sum + sc.uniqueCallers, 0).toLocaleString() }}
              </div>
              <div class="text-sm text-foreground-muted">Unique Callers</div>
            </div>
          </div>
        </template>
        <div v-else class="text-center py-8 text-foreground-muted">
          No smart contract data available
        </div>
      </div>
    </div>

    <!-- Smart Contract Details Table -->
    <div class="card">
      <h2 class="section-title mb-4">
        <Cpu class="h-5 w-5 text-accent" />
        Smart Contract Details
      </h2>

      <div v-if="scUsageLoading" class="loading">Loading...</div>
      <template v-else-if="scUsage?.length">
        <div class="table-wrapper">
          <table>
            <thead>
              <tr>
                <th>#</th>
                <th>Contract</th>
                <th>Calls</th>
                <th>Volume</th>
                <th>Unique Callers</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(sc, index) in scUsage" :key="sc.address">
                <td class="text-foreground-muted">{{ index + 1 }}</td>
                <td>
                  <NuxtLink :to="`/address/${sc.address}`" class="text-accent font-medium">
                    {{ getAddressDisplay(sc) }}
                  </NuxtLink>
                  <div v-if="sc.name" class="text-xs text-foreground-muted">
                    {{ truncateAddress(sc.address) }}
                  </div>
                </td>
                <td class="font-semibold">{{ sc.callCount.toLocaleString() }}</td>
                <td>{{ formatVolume(sc.totalAmount) }}</td>
                <td>{{ sc.uniqueCallers.toLocaleString() }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        No smart contract data available
      </div>
    </div>
  </div>
</template>
