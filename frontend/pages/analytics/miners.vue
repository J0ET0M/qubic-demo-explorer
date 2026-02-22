<script setup lang="ts">
import { Pickaxe } from 'lucide-vue-next'
import MinerFlowsTab from '~/components/analytics/MinerFlowsTab.vue'

useHead({ title: 'Miner Flows - Analytics' })
</script>

<template>
  <div class="space-y-6">
    <h1 class="page-title flex items-center gap-2">
      <Pickaxe class="h-5 w-5 text-accent" />
      Miner Flows
    </h1>

    <AnalyticsTimeRangeSelector />

    <Suspense>
      <template #default>
        <MinerFlowsTab />
      </template>
      <template #fallback>
        <div class="card">
          <div class="loading py-12">Loading analytics data...</div>
        </div>
      </template>
    </Suspense>
  </div>
</template>
