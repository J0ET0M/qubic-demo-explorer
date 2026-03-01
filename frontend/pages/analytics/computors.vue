<script setup lang="ts">
import { Monitor } from 'lucide-vue-next'
import ComputorRevenueTab from '~/components/analytics/ComputorRevenueTab.vue'

useHead({ title: 'Computor Revenue - Analytics' })
</script>

<template>
  <div class="space-y-6">
    <h1 class="page-title flex items-center gap-2">
      <Monitor class="h-5 w-5 text-accent" />
      Computor Revenue
    </h1>

    <Suspense>
      <template #default>
        <ComputorRevenueTab />
      </template>
      <template #fallback>
        <div class="card">
          <div class="loading py-12">Calculating computor revenue...</div>
        </div>
      </template>
    </Suspense>
  </div>
</template>
