<script setup lang="ts">
import { PieChart } from 'lucide-vue-next'
import HoldersTab from '~/components/analytics/HoldersTab.vue'

useHead({ title: 'Holders & Distribution - Analytics' })
</script>

<template>
  <div class="space-y-6">
    <h1 class="page-title flex items-center gap-2">
      <PieChart class="h-5 w-5 text-accent" />
      Holders & Distribution
    </h1>

    <AnalyticsTimeRangeSelector />

    <Suspense>
      <template #default>
        <HoldersTab />
      </template>
      <template #fallback>
        <div class="card">
          <div class="loading py-12">Loading analytics data...</div>
        </div>
      </template>
    </Suspense>
  </div>
</template>
