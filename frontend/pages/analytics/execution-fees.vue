<script setup lang="ts">
import { Receipt } from 'lucide-vue-next'
import ExecutionFeesTab from '~/components/analytics/ExecutionFeesTab.vue'

useHead({ title: 'Execution Fees - Analytics' })
</script>

<template>
  <div class="space-y-6">
    <h1 class="page-title flex items-center gap-2">
      <Receipt class="h-5 w-5 text-accent" />
      Execution Fees
    </h1>

    <Suspense>
      <template #default>
        <ExecutionFeesTab />
      </template>
      <template #fallback>
        <div class="card">
          <div class="loading py-12">Loading execution fee reports...</div>
        </div>
      </template>
    </Suspense>
  </div>
</template>
