<script setup lang="ts">
import { BarChart3 } from 'lucide-vue-next'

// Current active tab - persisted in URL
const route = useRoute()
const router = useRouter()

const activeTab = ref((route.query.tab as string) || 'overview')

// Update URL when tab changes
watch(activeTab, (newTab) => {
  router.replace({ query: { ...route.query, tab: newTab } })
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
