<script setup lang="ts">
import { Landmark } from 'lucide-vue-next'
import CcfTab from '~/components/analytics/CcfTab.vue'

useHead({ title: 'CCF - Analytics' })
</script>

<template>
  <div class="space-y-6">
    <h1 class="page-title flex items-center gap-2">
      <Landmark class="h-5 w-5 text-accent" />
      Computor Controlled Fund
    </h1>

    <Suspense>
      <template #default>
        <CcfTab />
      </template>
      <template #fallback>
        <div class="card">
          <div class="loading py-12">Loading CCF analytics...</div>
        </div>
      </template>
    </Suspense>
  </div>
</template>
