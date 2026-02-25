<script setup lang="ts">
import { BarChart3, Users, PieChart, Building2, Pickaxe, Flame, Cpu, TrendingUp, Network, Crown, Coins } from 'lucide-vue-next'
import OverviewTab from '~/components/analytics/OverviewTab.vue'

useHead({ title: 'Analytics - QLI Explorer' })

const subPages = [
  { label: 'Network Activity', description: 'Active addresses, transaction sizes, and network trends', path: '/analytics/network', icon: Users },
  { label: 'Holders', description: 'Holder brackets, wealth concentration, and trends', path: '/analytics/holders', icon: PieChart },
  { label: 'Exchange Flows', description: 'Exchange inflows, outflows, and net flow tracking', path: '/analytics/exchanges', icon: Building2 },
  { label: 'Miner Flows', description: 'Computor emission analysis and exchange sell pressure', path: '/analytics/miners', icon: Pickaxe },
  { label: 'Burns', description: 'Burn volume history, categories, and cumulative totals', path: '/analytics/burns', icon: Flame },
  { label: 'Smart Contracts', description: 'Contract usage, call counts, and volume', path: '/analytics/contracts', icon: Cpu },
  { label: 'Rich List', description: 'Top holders ranked by balance from spectrum snapshots', path: '/analytics/richlist', icon: Crown },
  { label: 'Supply', description: 'Circulating supply, emissions, and burn tracking', path: '/analytics/supply', icon: Coins },
  { label: 'Top Addresses', description: 'Highest volume addresses and activity rankings', path: '/analytics/top', icon: TrendingUp },
  { label: 'Flow Visualization', description: 'Sankey diagrams of miner emission flow paths', path: '/analytics/miner-flow', icon: Network },
]
</script>

<template>
  <div class="space-y-6">
    <h1 class="page-title flex items-center gap-2">
      <BarChart3 class="h-5 w-5 text-accent" />
      Network Analytics
    </h1>

    <!-- Summary Stats -->
    <Suspense>
      <template #default>
        <OverviewTab />
      </template>
      <template #fallback>
        <div class="card">
          <div class="loading py-12">Loading overview...</div>
        </div>
      </template>
    </Suspense>

    <!-- Navigation Cards -->
    <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
      <NuxtLink
        v-for="page in subPages"
        :key="page.path"
        :to="page.path"
        class="card group"
      >
        <div class="flex items-center gap-3 mb-2">
          <div class="p-1.5 rounded-md bg-accent/10 group-hover:bg-accent/20 transition-colors">
            <component :is="page.icon" class="h-4 w-4 text-accent" />
          </div>
          <h3 class="text-[0.8125rem] font-semibold">{{ page.label }}</h3>
        </div>
        <p class="text-xs text-foreground-muted leading-relaxed">{{ page.description }}</p>
      </NuxtLink>
    </div>
  </div>
</template>
