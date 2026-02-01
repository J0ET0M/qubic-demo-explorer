<script setup lang="ts">
import { Menu, Search } from 'lucide-vue-next'

interface Emits {
  (e: 'toggle-sidebar'): void
}

const emit = defineEmits<Emits>()

const route = useRoute()

const pageTitle = computed(() => {
  const titles: Record<string, string> = {
    '/': 'Dashboard',
    '/ticks': 'Ticks',
    '/transactions': 'Transactions',
    '/transfers': 'Transfers',
    '/search': 'Search Results',
  }

  // Check for exact match
  if (titles[route.path]) {
    return titles[route.path]
  }

  // Check for dynamic routes
  if (route.path.startsWith('/ticks/')) return 'Tick Details'
  if (route.path.startsWith('/tx/')) return 'Transaction Details'
  if (route.path.startsWith('/address/')) return 'Address Details'

  return 'Qubic Explorer'
})
</script>

<template>
  <header class="sticky top-0 z-40 flex h-16 items-center gap-4 border-b border-border bg-surface px-4 lg:px-6">
    <!-- Mobile menu button -->
    <button
      class="lg:hidden p-2 rounded-md hover:bg-surface-elevated"
      @click="emit('toggle-sidebar')"
    >
      <Menu class="h-5 w-5" />
    </button>

    <!-- Page title -->
    <h1 class="text-lg font-semibold text-foreground">
      {{ pageTitle }}
    </h1>

    <!-- Spacer -->
    <div class="flex-1" />

    <!-- Search bar (desktop) -->
    <div class="hidden md:block w-full max-w-md">
      <SearchBar />
    </div>

    <!-- Live indicator -->
    <div class="live-indicator hidden sm:flex">
      <span class="dot"></span>
      <span>Live</span>
    </div>
  </header>

  <!-- Mobile search bar -->
  <div class="lg:hidden px-4 py-2 border-b border-border bg-surface">
    <SearchBar />
  </div>
</template>
