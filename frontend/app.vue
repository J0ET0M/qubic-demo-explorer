<script setup lang="ts">
// Show a one-time relocation notice when the explorer is reached under the
// old host. Detected SSR-side from the request headers + client-side from
// window.location so it works on first paint and after hydration.
const LEGACY_HOSTS = new Set(['explorer-demo.qubic.tools'])
const NEW_URL = 'https://analytics.qubic.li'

const reqHost = useRequestHeaders(['host'])['host']?.split(':')[0] ?? ''
const clientHost = import.meta.client ? window.location.hostname : ''
const isLegacyHost = computed(() =>
  LEGACY_HOSTS.has(reqHost.toLowerCase()) || LEGACY_HOSTS.has(clientHost.toLowerCase())
)
</script>

<template>
  <LegacyExplorerNotice v-if="isLegacyHost" :new-url="NEW_URL" />
  <NuxtLayout v-else>
    <NuxtPage />
  </NuxtLayout>
</template>
