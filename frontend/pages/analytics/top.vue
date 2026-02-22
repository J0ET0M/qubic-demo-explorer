<script setup lang="ts">
import { TrendingUp } from 'lucide-vue-next'
import TopAddressesTab from '~/components/analytics/TopAddressesTab.vue'

useHead({ title: 'Top Addresses - Analytics' })
</script>

<template>
  <div class="space-y-6">
    <h1 class="page-title flex items-center gap-2">
      <TrendingUp class="h-5 w-5 text-accent" />
      Top Addresses
    </h1>

    <Suspense>
      <template #default>
        <TopAddressesTab />
      </template>
      <template #fallback>
        <div class="card">
          <div class="loading py-12">Loading analytics data...</div>
        </div>
      </template>
    </Suspense>
  </div>
</template>
