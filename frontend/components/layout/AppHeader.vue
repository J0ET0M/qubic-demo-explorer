<script setup lang="ts">
import { Menu, ChevronRight } from 'lucide-vue-next'

interface Emits {
  (e: 'toggle-sidebar'): void
}

const emit = defineEmits<Emits>()

const route = useRoute()

interface Breadcrumb {
  label: string
  path?: string
}

const breadcrumbs = computed<Breadcrumb[]>(() => {
  const p = route.path

  // Static routes
  if (p === '/') return [{ label: 'Dashboard' }]
  if (p === '/ticks') return [{ label: 'Ticks' }]
  if (p === '/transactions') return [{ label: 'Transactions' }]
  if (p === '/transfers') return [{ label: 'Transfers' }]
  if (p === '/addresses') return [{ label: 'Known Addresses' }]
  if (p === '/search') return [{ label: 'Search Results' }]
  if (p === '/epochs') return [{ label: 'Epochs' }]

  // Dynamic routes
  if (p.startsWith('/ticks/')) {
    const tick = p.split('/')[2]
    return [{ label: 'Ticks', path: '/ticks' }, { label: `Tick ${tick}` }]
  }
  if (p.startsWith('/epochs/')) {
    const epoch = p.split('/')[2]
    return [{ label: 'Epochs', path: '/epochs' }, { label: `Epoch ${epoch}` }]
  }
  if (p.startsWith('/tx/')) {
    const hash = p.split('/')[2]
    return [{ label: 'Transactions', path: '/transactions' }, { label: hash.slice(0, 12) + '...' }]
  }
  if (p.startsWith('/address/')) {
    const id = p.split('/')[2]
    return [{ label: 'Addresses' }, { label: id.slice(0, 12) + '...' }]
  }

  // Analytics routes
  if (p === '/analytics') return [{ label: 'Analytics' }]

  const analyticsLabels: Record<string, string> = {
    '/analytics/network': 'Network Activity',
    '/analytics/holders': 'Holders',
    '/analytics/exchanges': 'Exchange Flows',
    '/analytics/miners': 'Miner Flows',
    '/analytics/burns': 'Burns',
    '/analytics/contracts': 'Smart Contracts',
    '/analytics/top': 'Top Addresses',
    '/analytics/miner-flow': 'Flow Visualization',
  }

  if (analyticsLabels[p]) {
    return [{ label: 'Analytics', path: '/analytics' }, { label: analyticsLabels[p] }]
  }

  // Miner flow detail: /analytics/miner-flow/[epoch]
  if (p.startsWith('/analytics/miner-flow/')) {
    const epoch = p.split('/')[3]
    return [
      { label: 'Analytics', path: '/analytics' },
      { label: 'Flow Visualization', path: '/analytics/miner-flow' },
      { label: `Epoch ${epoch}` },
    ]
  }

  return [{ label: 'QLI Explorer' }]
})

const pageTitle = computed(() => {
  const last = breadcrumbs.value[breadcrumbs.value.length - 1]
  return last?.label || 'QLI Explorer'
})
</script>

<template>
  <header class="sticky top-0 z-40 flex h-14 items-center gap-4 border-b border-border bg-surface/95 backdrop-blur-sm px-4 lg:px-6">
    <!-- Mobile menu button -->
    <button
      class="lg:hidden p-2 rounded-md hover:bg-surface-elevated transition-colors"
      @click="emit('toggle-sidebar')"
    >
      <Menu class="h-5 w-5" />
    </button>

    <!-- Breadcrumbs -->
    <nav class="flex items-center gap-1.5 min-w-0">
      <template v-for="(crumb, i) in breadcrumbs" :key="i">
        <ChevronRight v-if="i > 0" class="h-3.5 w-3.5 text-foreground-muted/50 shrink-0" />
        <NuxtLink
          v-if="crumb.path"
          :to="crumb.path"
          class="text-[0.8125rem] font-medium text-foreground-muted hover:text-foreground transition-colors truncate"
        >
          {{ crumb.label }}
        </NuxtLink>
        <h1
          v-else
          class="text-[0.9375rem] font-semibold text-foreground tracking-tight truncate"
        >
          {{ crumb.label }}
        </h1>
      </template>
    </nav>

    <!-- Spacer -->
    <div class="flex-1" />

    <!-- Search bar (desktop) -->
    <div class="hidden md:block w-full max-w-sm">
      <SearchBar />
    </div>

    <!-- Live indicator -->
    <div class="live-indicator hidden sm:flex">
      <span class="dot"></span>
      <span>Live</span>
    </div>
  </header>

  <!-- Mobile search bar -->
  <div class="lg:hidden px-4 py-2 border-b border-border bg-surface/95">
    <SearchBar />
  </div>
</template>
