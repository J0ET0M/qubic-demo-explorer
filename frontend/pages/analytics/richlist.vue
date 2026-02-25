<script setup lang="ts">
import { Crown } from 'lucide-vue-next'
import RichListTab from '~/components/analytics/RichListTab.vue'

useHead({ title: 'Rich List - Analytics' })
</script>

<template>
  <div class="space-y-6">
    <h1 class="page-title flex items-center gap-2">
      <Crown class="h-5 w-5 text-accent" />
      Rich List
    </h1>

    <Suspense>
      <template #default>
        <RichListTab />
      </template>
      <template #fallback>
        <div class="card">
          <div class="loading py-12">Loading rich list...</div>
        </div>
      </template>
    </Suspense>
  </div>
</template>
