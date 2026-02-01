<script setup lang="ts">
import { BarChart3, Users, PieChart, Building2, Cpu, TrendingUp, Pickaxe } from 'lucide-vue-next'

interface Tab {
  id: string
  label: string
  icon: any
}

const tabs: Tab[] = [
  { id: 'overview', label: 'Overview', icon: BarChart3 },
  { id: 'network', label: 'Network Activity', icon: Users },
  { id: 'holders', label: 'Holders', icon: PieChart },
  { id: 'exchanges', label: 'Exchange Flows', icon: Building2 },
  { id: 'miner-flows', label: 'Miner Flows', icon: Pickaxe },
  { id: 'contracts', label: 'Smart Contracts', icon: Cpu },
  { id: 'top-addresses', label: 'Top Addresses', icon: TrendingUp }
]

const props = defineProps<{
  modelValue: string
}>()

const emit = defineEmits<{
  'update:modelValue': [value: string]
}>()

const selectTab = (tabId: string) => {
  emit('update:modelValue', tabId)
}
</script>

<template>
  <div class="analytics-tabs">
    <nav class="tabs-nav">
      <button
        v-for="tab in tabs"
        :key="tab.id"
        :class="['tab-btn', { active: modelValue === tab.id }]"
        @click="selectTab(tab.id)"
      >
        <component :is="tab.icon" class="h-4 w-4" />
        <span class="tab-label">{{ tab.label }}</span>
      </button>
    </nav>
  </div>
</template>

<style scoped>
.analytics-tabs {
  margin-bottom: 1.5rem;
}

.tabs-nav {
  display: flex;
  gap: 0.25rem;
  padding: 0.25rem;
  background: var(--color-surface);
  border-radius: 0.5rem;
  overflow-x: auto;
  -webkit-overflow-scrolling: touch;
}

.tab-btn {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.625rem 1rem;
  border: none;
  background: transparent;
  color: var(--color-foreground-muted);
  font-size: 0.875rem;
  font-weight: 500;
  border-radius: 0.375rem;
  cursor: pointer;
  white-space: nowrap;
  transition: all 0.15s ease;
}

.tab-btn:hover {
  color: var(--color-foreground);
  background: var(--color-surface-elevated);
}

.tab-btn.active {
  color: var(--color-accent);
  background: var(--color-surface-elevated);
}

@media (max-width: 768px) {
  .tab-label {
    display: none;
  }

  .tab-btn {
    padding: 0.75rem;
  }

  .tabs-nav {
    justify-content: space-around;
  }
}
</style>
