<script setup lang="ts">
import { Flame } from 'lucide-vue-next'
import BurnsTab from '~/components/analytics/BurnsTab.vue'

useHead({ title: 'Burns - Analytics' })
</script>

<template>
  <div class="space-y-6">
    <h1 class="page-title flex items-center gap-2">
      <Flame class="h-5 w-5 text-accent" />
      Burns
    </h1>

    <AnalyticsTimeRangeSelector />

    <Suspense>
      <template #default>
        <BurnsTab />
      </template>
      <template #fallback>
        <div class="card">
          <div class="loading py-12">Loading analytics data...</div>
        </div>
      </template>
    </Suspense>
  </div>
</template>
