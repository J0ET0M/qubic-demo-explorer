<script setup lang="ts">
import { Coins } from 'lucide-vue-next'
import SupplyTab from '~/components/analytics/SupplyTab.vue'

useHead({ title: 'Supply Dashboard - Analytics' })
</script>

<template>
  <div class="space-y-6">
    <h1 class="page-title flex items-center gap-2">
      <Coins class="h-5 w-5 text-accent" />
      Supply Dashboard
    </h1>

    <Suspense>
      <template #default>
        <SupplyTab />
      </template>
      <template #fallback>
        <div class="card">
          <div class="loading py-12">Loading supply data...</div>
        </div>
      </template>
    </Suspense>
  </div>
</template>
