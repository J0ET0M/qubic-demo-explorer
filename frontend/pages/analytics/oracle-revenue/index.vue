<script setup lang="ts">
import { Database } from 'lucide-vue-next'
import OracleRevenueTab from '~/components/analytics/OracleRevenueTab.vue'

useHead({ title: 'Oracle Revenue - Analytics' })
</script>

<template>
  <div class="space-y-6">
    <h1 class="page-title flex items-center gap-2">
      <Database class="h-5 w-5 text-accent" />
      Oracle Revenue
    </h1>

    <Suspense>
      <template #default>
        <OracleRevenueTab />
      </template>
      <template #fallback>
        <div class="card">
          <div class="loading py-12">Loading oracle revenue data...</div>
        </div>
      </template>
    </Suspense>
  </div>
</template>
