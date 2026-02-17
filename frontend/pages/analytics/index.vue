<script setup lang="ts">
import { BarChart3 } from 'lucide-vue-next'
import type { TimeRangePreset } from '~/composables/useTimeRange'

// Current active tab - persisted in URL
const route = useRoute()
const router = useRouter()

const activeTab = ref((route.query.tab as string) || 'overview')

// Time range - shared across snapshot tabs
const { timeRange, setPreset } = useTimeRange()

// Initialize time range from URL if present
const initialRange = route.query.range as string
if (initialRange && ['24h', '7d', '30d', '90d', 'all'].includes(initialRange)) {
  setPreset(initialRange as TimeRangePreset)
}

// Tabs that use 4-hour snapshot data
const snapshotTabs = ['network', 'holders', 'exchanges', 'miner-flows']

// Update URL when tab or range changes
watch(activeTab, (newTab) => {
  router.replace({ query: { ...route.query, tab: newTab } })
})

watch(() => timeRange.value.preset, (preset) => {
  router.replace({ query: { ...route.query, range: preset } })
})

// Tab components loaded lazily
const OverviewTab = defineAsyncComponent(() => import('~/components/analytics/OverviewTab.vue'))
const NetworkActivityTab = defineAsyncComponent(() => import('~/components/analytics/NetworkActivityTab.vue'))
const HoldersTab = defineAsyncComponent(() => import('~/components/analytics/HoldersTab.vue'))
const ExchangeFlowsTab = defineAsyncComponent(() => import('~/components/analytics/ExchangeFlowsTab.vue'))
const MinerFlowsTab = defineAsyncComponent(() => import('~/components/analytics/MinerFlowsTab.vue'))
const SmartContractsTab = defineAsyncComponent(() => import('~/components/analytics/SmartContractsTab.vue'))
const TopAddressesTab = defineAsyncComponent(() => import('~/components/analytics/TopAddressesTab.vue'))
</script>

<template>
  <div class="space-y-6">
    <h1 class="text-2xl font-bold flex items-center gap-2">
      <BarChart3 class="h-6 w-6 text-accent" />
      Network Analytics
    </h1>

    <!-- Tab Navigation -->
    <AnalyticsTabs v-model="activeTab" />

    <!-- Time Range Selector - only for tabs with 4-hour snapshot data -->
    <AnalyticsTimeRangeSelector v-if="snapshotTabs.includes(activeTab)" />

    <!-- Tab Content with Suspense for async loading -->
    <Suspense>
      <template #default>
        <KeepAlive>
          <component
            :is="
              activeTab === 'overview' ? OverviewTab :
              activeTab === 'network' ? NetworkActivityTab :
              activeTab === 'holders' ? HoldersTab :
              activeTab === 'exchanges' ? ExchangeFlowsTab :
              activeTab === 'miner-flows' ? MinerFlowsTab :
              activeTab === 'contracts' ? SmartContractsTab :
              activeTab === 'top-addresses' ? TopAddressesTab :
              OverviewTab
            "
            :key="activeTab"
          />
        </KeepAlive>
      </template>
      <template #fallback>
        <div class="card">
          <div class="loading py-12">Loading analytics data...</div>
        </div>
      </template>
    </Suspense>
  </div>
</template>
