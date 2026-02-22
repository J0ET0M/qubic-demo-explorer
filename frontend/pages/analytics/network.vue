<script setup lang="ts">
import { Users } from 'lucide-vue-next'
import NetworkActivityTab from '~/components/analytics/NetworkActivityTab.vue'

useHead({ title: 'Network Activity - Analytics' })
</script>

<template>
  <div class="space-y-6">
    <h1 class="page-title flex items-center gap-2">
      <Users class="h-5 w-5 text-accent" />
      Network Activity
    </h1>

    <AnalyticsTimeRangeSelector />

    <Suspense>
      <template #default>
        <NetworkActivityTab />
      </template>
      <template #fallback>
        <div class="card">
          <div class="loading py-12">Loading analytics data...</div>
        </div>
      </template>
    </Suspense>
  </div>
</template>
