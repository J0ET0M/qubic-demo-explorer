<script setup lang="ts">
import { Lock } from 'lucide-vue-next'
import QearnTab from '~/components/analytics/QearnTab.vue'

useHead({ title: 'Qearn - Analytics' })
</script>

<template>
  <div class="space-y-6">
    <h1 class="page-title flex items-center gap-2">
      <Lock class="h-5 w-5 text-accent" />
      Qearn
    </h1>

    <Suspense>
      <template #default>
        <QearnTab />
      </template>
      <template #fallback>
        <div class="card">
          <div class="loading py-12">Loading Qearn analytics...</div>
        </div>
      </template>
    </Suspense>
  </div>
</template>
