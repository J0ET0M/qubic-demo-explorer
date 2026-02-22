<script setup lang="ts">
import { Cpu } from 'lucide-vue-next'
import SmartContractsTab from '~/components/analytics/SmartContractsTab.vue'

useHead({ title: 'Smart Contracts - Analytics' })
</script>

<template>
  <div class="space-y-6">
    <h1 class="page-title flex items-center gap-2">
      <Cpu class="h-5 w-5 text-accent" />
      Smart Contracts
    </h1>

    <Suspense>
      <template #default>
        <SmartContractsTab />
      </template>
      <template #fallback>
        <div class="card">
          <div class="loading py-12">Loading analytics data...</div>
        </div>
      </template>
    </Suspense>
  </div>
</template>
