<script setup lang="ts">
import { Blocks, ArrowLeftRight, Send, Calendar, Users, X, BarChart3, ChevronDown, ChevronRight, Pickaxe, Network } from 'lucide-vue-next'

interface Props {
  isOpen?: boolean
}

interface Emits {
  (e: 'close'): void
}

defineProps<Props>()
const emit = defineEmits<Emits>()

const route = useRoute()

interface NavItem {
  label: string
  path: string
  icon: typeof Blocks
  children?: NavItem[]
}

const navItems: NavItem[] = [
  { label: 'Dashboard', path: '/', icon: Blocks },
  { label: 'Epochs', path: '/epochs', icon: Calendar },
  { label: 'Ticks', path: '/ticks', icon: Blocks },
  { label: 'Transactions', path: '/transactions', icon: ArrowLeftRight },
  { label: 'Transfers', path: '/transfers', icon: Send },
  {
    label: 'Analytics',
    path: '/analytics',
    icon: BarChart3,
    children: [
      { label: 'Overview', path: '/analytics', icon: BarChart3 },
      { label: 'Miner Flows', path: '/analytics?tab=miner-flows', icon: Pickaxe },
      { label: 'Flow Visualization', path: '/analytics/miner-flow', icon: Network },
    ]
  },
  { label: 'Known Addresses', path: '/addresses', icon: Users },
]

// Track expanded submenus
const expandedMenus = ref<Set<string>>(new Set())

function isActive(path: string): boolean {
  if (path === '/') {
    return route.path === '/'
  }
  // Handle query params for tab-based navigation
  if (path.includes('?tab=')) {
    const [basePath, query] = path.split('?')
    const tabMatch = query.match(/tab=([^&]+)/)
    if (tabMatch) {
      return route.path === basePath && route.query.tab === tabMatch[1]
    }
  }
  return route.path === path || route.path.startsWith(path + '/')
}

function isParentActive(item: NavItem): boolean {
  if (isActive(item.path)) return true
  if (item.children) {
    return item.children.some(child => isActive(child.path))
  }
  return false
}

function toggleSubmenu(path: string) {
  if (expandedMenus.value.has(path)) {
    expandedMenus.value.delete(path)
  } else {
    expandedMenus.value.add(path)
  }
}

function navigate(path: string) {
  // Handle paths with query params
  if (path.includes('?')) {
    const [basePath, queryString] = path.split('?')
    const params = new URLSearchParams(queryString)
    const query: Record<string, string> = {}
    params.forEach((value, key) => {
      query[key] = value
    })
    navigateTo({ path: basePath, query })
  } else {
    navigateTo(path)
  }
  emit('close')
}

// Auto-expand menu if child is active
watchEffect(() => {
  navItems.forEach(item => {
    if (item.children && isParentActive(item)) {
      expandedMenus.value.add(item.path)
    }
  })
})
</script>

<template>
  <aside
    class="fixed inset-y-0 left-0 z-50 flex w-64 flex-col bg-surface border-r border-border transform transition-transform duration-300 lg:translate-x-0"
    :class="isOpen ? 'translate-x-0' : '-translate-x-full'"
  >
    <!-- Header -->
    <div class="flex h-16 items-center justify-between px-4 border-b border-border">
      <NuxtLink to="/" class="flex items-center gap-2" @click="emit('close')">
        <img src="/logo.svg" alt="Logo" class="h-8 w-auto" />
        <span class="text-lg font-semibold text-foreground">Qubic Explorer</span>
      </NuxtLink>
      <button
        class="lg:hidden p-2 rounded-md hover:bg-surface-elevated"
        @click="emit('close')"
      >
        <X class="h-5 w-5" />
      </button>
    </div>

    <!-- Navigation -->
    <nav class="flex-1 overflow-y-auto p-4">
      <ul class="space-y-1">
        <li v-for="item in navItems" :key="item.path">
          <!-- Item with children (submenu) -->
          <template v-if="item.children">
            <button
              class="flex w-full items-center gap-3 rounded-lg px-3 py-2 text-sm transition-colors"
              :class="
                isParentActive(item)
                  ? 'bg-accent/10 text-accent'
                  : 'text-foreground-muted hover:bg-surface-elevated hover:text-foreground'
              "
              @click="toggleSubmenu(item.path)"
            >
              <component :is="item.icon" class="h-5 w-5" />
              <span class="flex-1 text-left">{{ item.label }}</span>
              <component
                :is="expandedMenus.has(item.path) ? ChevronDown : ChevronRight"
                class="h-4 w-4"
              />
            </button>
            <!-- Submenu items -->
            <ul
              v-if="expandedMenus.has(item.path)"
              class="mt-1 ml-4 space-y-1 border-l border-border pl-2"
            >
              <li v-for="child in item.children" :key="child.path">
                <button
                  class="flex w-full items-center gap-3 rounded-lg px-3 py-2 text-sm transition-colors"
                  :class="
                    isActive(child.path)
                      ? 'bg-accent/10 text-accent'
                      : 'text-foreground-muted hover:bg-surface-elevated hover:text-foreground'
                  "
                  @click="navigate(child.path)"
                >
                  <component :is="child.icon" class="h-4 w-4" />
                  <span>{{ child.label }}</span>
                </button>
              </li>
            </ul>
          </template>

          <!-- Regular item (no children) -->
          <button
            v-else
            class="flex w-full items-center gap-3 rounded-lg px-3 py-2 text-sm transition-colors"
            :class="
              isActive(item.path)
                ? 'bg-accent/10 text-accent'
                : 'text-foreground-muted hover:bg-surface-elevated hover:text-foreground'
            "
            @click="navigate(item.path)"
          >
            <component :is="item.icon" class="h-5 w-5" />
            <span>{{ item.label }}</span>
          </button>
        </li>
      </ul>
    </nav>

    <!-- Footer -->
    <div class="border-t border-border p-4">
      <div class="text-xs text-foreground-muted text-center">
        Qubic Blockchain Explorer
      </div>
    </div>
  </aside>
</template>
