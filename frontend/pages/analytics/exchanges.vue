<script setup lang="ts">
import { Building2 } from 'lucide-vue-next'
import ExchangeFlowsTab from '~/components/analytics/ExchangeFlowsTab.vue'

useHead({ title: 'Exchange Flows - Analytics' })
</script>

<template>
  <div class="space-y-6">
    <h1 class="page-title flex items-center gap-2">
      <Building2 class="h-5 w-5 text-accent" />
      Exchange Flows
    </h1>

    <AnalyticsTimeRangeSelector />

    <Suspense>
      <template #default>
        <ExchangeFlowsTab />
      </template>
      <template #fallback>
        <div class="card">
          <div class="loading py-12">Loading analytics data...</div>
        </div>
      </template>
    </Suspense>
  </div>
</template>
