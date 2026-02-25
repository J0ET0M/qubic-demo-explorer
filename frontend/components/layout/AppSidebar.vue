<script setup lang="ts">
import { Blocks, ArrowLeftRight, Send, Calendar, Users, X, BarChart3, ChevronDown, ChevronRight, Pickaxe, Network, PanelLeftClose, PanelLeftOpen, PieChart, Building2, Cpu, TrendingUp, Flame, Radar, Crown, Coins, Fish, Gem, Star } from 'lucide-vue-next'

const APP_VERSION = '0.1.0'

interface Props {
  isOpen?: boolean
  isCollapsed?: boolean
}

interface Emits {
  (e: 'close'): void
  (e: 'toggle-collapse'): void
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
  { label: 'Whale Alerts', path: '/whale-alerts', icon: Fish },
  {
    label: 'Analytics',
    path: '/analytics',
    icon: BarChart3,
    children: [
      { label: 'Overview', path: '/analytics', icon: BarChart3 },
      { label: 'Network', path: '/analytics/network', icon: Users },
      { label: 'Holders', path: '/analytics/holders', icon: PieChart },
      { label: 'Exchanges', path: '/analytics/exchanges', icon: Building2 },
      { label: 'Miner Flows', path: '/analytics/miners', icon: Pickaxe },
      { label: 'Burns', path: '/analytics/burns', icon: Flame },
      { label: 'Contracts', path: '/analytics/contracts', icon: Cpu },
      { label: 'Rich List', path: '/analytics/richlist', icon: Crown },
      { label: 'Supply', path: '/analytics/supply', icon: Coins },
      { label: 'Top Addresses', path: '/analytics/top', icon: TrendingUp },
      { label: 'Flow Viz', path: '/analytics/miner-flow', icon: Network },
      { label: 'Flow Tracking', path: '/analytics/flow-tracking', icon: Radar },
    ]
  },
  { label: 'Assets', path: '/assets', icon: Gem },
  { label: 'Portfolio', path: '/portfolio', icon: Star },
  { label: 'Known Addresses', path: '/addresses', icon: Users },
]

// Track expanded submenus
const expandedMenus = ref<Set<string>>(new Set())

function isActive(path: string): boolean {
  if (path === '/' || path === '/analytics') {
    return route.path === path
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
    class="fixed inset-y-0 left-0 z-50 flex flex-col bg-surface/95 backdrop-blur-sm border-r border-border transform transition-all duration-300 lg:translate-x-0"
    :class="[
      isOpen ? 'translate-x-0' : '-translate-x-full',
      isCollapsed ? 'w-16 overflow-visible' : 'w-64'
    ]"
  >
    <!-- Header -->
    <div class="flex h-16 items-center border-0" :class="isCollapsed ? 'justify-center px-2' : 'justify-between px-4'">
      <NuxtLink to="/" class="flex items-center gap-2.5 min-w-0" @click="emit('close')">
        <img src="/logo.svg" alt="QLI" class="h-7 w-auto shrink-0" />
        <span v-if="!isCollapsed" class="text-[0.9375rem] font-semibold text-foreground tracking-tight truncate">
          QLI Explorer
        </span>
      </NuxtLink>
      <button
        v-if="!isCollapsed"
        class="lg:hidden p-2 rounded-md hover:bg-surface-elevated transition-colors"
        @click="emit('close')"
      >
        <X class="h-5 w-5" />
      </button>
    </div>

    <!-- Navigation -->
    <nav class="flex-1" :class="isCollapsed ? 'p-1.5 overflow-visible' : 'p-3 overflow-y-auto'">
      <ul :class="isCollapsed ? 'space-y-1' : 'space-y-0.5'">
        <li v-for="item in navItems" :key="item.path">
          <!-- Item with children (submenu) -->
          <template v-if="item.children">
            <!-- Collapsed: icon with hover flyout -->
            <div v-if="isCollapsed" class="relative group/flyout">
              <button
                class="flex w-full items-center justify-center rounded-lg p-2.5 transition-all duration-200"
                :class="
                  isParentActive(item)
                    ? 'bg-accent/10 text-accent-light'
                    : 'text-foreground-muted hover:bg-surface-elevated hover:text-foreground'
                "
                @click="navigate(item.path)"
              >
                <component :is="item.icon" class="h-[18px] w-[18px]" />
              </button>
              <!-- Flyout menu with invisible bridge to prevent gap hover-loss -->
              <div class="absolute left-full top-0 hidden group-hover/flyout:block z-[60]">
                <!-- Invisible bridge spanning the gap between icon and menu -->
                <div class="absolute -left-3 top-0 w-3 h-full" />
                <div class="bg-surface border border-border rounded-lg shadow-xl py-1.5 min-w-[11rem] ml-2">
                  <div class="px-3 py-1.5 text-[0.6875rem] font-semibold text-foreground-muted/70 uppercase tracking-wider">
                    {{ item.label }}
                  </div>
                  <button
                    v-for="child in item.children"
                    :key="child.path"
                    class="flex w-full items-center gap-2.5 px-3 py-1.5 text-[0.8125rem] font-medium transition-all duration-150"
                    :class="
                      isActive(child.path)
                        ? 'bg-accent/10 text-accent-light'
                        : 'text-foreground-muted hover:bg-surface-elevated hover:text-foreground'
                    "
                    @click="navigate(child.path)"
                  >
                    <component :is="child.icon" class="h-3.5 w-3.5" />
                    <span>{{ child.label }}</span>
                  </button>
                </div>
              </div>
            </div>
            <!-- Expanded: normal submenu -->
            <template v-else>
              <button
                class="flex w-full items-center gap-3 rounded-lg px-3 py-2 text-[0.8125rem] font-medium transition-all duration-200"
                :class="
                  isParentActive(item)
                    ? 'bg-accent/10 text-accent-light'
                    : 'text-foreground-muted hover:bg-surface-elevated hover:text-foreground'
                "
                @click="toggleSubmenu(item.path)"
              >
                <component :is="item.icon" class="h-[18px] w-[18px]" />
                <span class="flex-1 text-left">{{ item.label }}</span>
                <component
                  :is="expandedMenus.has(item.path) ? ChevronDown : ChevronRight"
                  class="h-4 w-4 opacity-50"
                />
              </button>
              <!-- Submenu items -->
              <ul
                v-if="expandedMenus.has(item.path)"
                class="mt-0.5 ml-4 space-y-0.5 border-l border-border/50 pl-2"
              >
                <li v-for="child in item.children" :key="child.path">
                  <button
                    class="flex w-full items-center gap-3 rounded-lg px-3 py-1.5 text-[0.8125rem] font-medium transition-all duration-200"
                    :class="
                      isActive(child.path)
                        ? 'bg-accent/10 text-accent-light'
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
          </template>

          <!-- Regular item (no children) -->
          <button
            v-else
            class="flex w-full items-center rounded-lg transition-all duration-200"
            :class="[
              isActive(item.path)
                ? 'bg-accent/10 text-accent-light'
                : 'text-foreground-muted hover:bg-surface-elevated hover:text-foreground',
              isCollapsed ? 'justify-center p-2.5' : 'gap-3 px-3 py-2 text-[0.8125rem] font-medium'
            ]"
            :title="isCollapsed ? item.label : undefined"
            @click="navigate(item.path)"
          >
            <component :is="item.icon" class="h-[18px] w-[18px]" />
            <span v-if="!isCollapsed">{{ item.label }}</span>
          </button>
        </li>
      </ul>
    </nav>

    <!-- Footer -->
    <div class="border-t border-border" :class="isCollapsed ? 'p-2' : 'p-3'">
      <!-- Collapse toggle (desktop only) -->
      <button
        class="hidden lg:flex w-full items-center rounded-lg text-foreground-muted hover:bg-surface-elevated hover:text-foreground transition-all duration-200"
        :class="isCollapsed ? 'justify-center p-2.5' : 'gap-3 px-3 py-2 text-[0.8125rem] font-medium'"
        :title="isCollapsed ? 'Expand sidebar' : 'Collapse sidebar'"
        @click="emit('toggle-collapse')"
      >
        <component :is="isCollapsed ? PanelLeftOpen : PanelLeftClose" class="h-[18px] w-[18px]" />
        <span v-if="!isCollapsed">Collapse</span>
      </button>

      <!-- Version -->
      <a
        href="https://qubic.li"
        target="_blank"
        rel="noopener noreferrer"
        class="block text-center mt-2 text-foreground-muted/50 hover:text-foreground-muted transition-colors"
        :class="isCollapsed ? 'text-[0.5rem]' : 'text-[0.625rem]'"
      >
        <template v-if="isCollapsed">v{{ APP_VERSION }}</template>
        <template v-else>QLI Explorer v{{ APP_VERSION }}</template>
      </a>
    </div>
  </aside>
</template>
