<script setup lang="ts">
import { Calendar } from 'lucide-vue-next'
import type { TimeRangePreset } from '~/composables/useTimeRange'

const { timeRange, setPreset, setCustomRange } = useTimeRange()

const presets: { id: TimeRangePreset; label: string }[] = [
  { id: '24h', label: '24H' },
  { id: '7d', label: '7D' },
  { id: '30d', label: '30D' },
  { id: '90d', label: '90D' },
  { id: 'all', label: 'All' },
]

const showCustom = ref(false)
const customFrom = ref('')
const customTo = ref('')

const toggleCustom = () => {
  showCustom.value = !showCustom.value
}

const applyCustom = () => {
  if (customFrom.value && customTo.value) {
    setCustomRange(
      new Date(customFrom.value).toISOString(),
      new Date(customTo.value + 'T23:59:59').toISOString()
    )
    showCustom.value = false
  }
}

const selectPreset = (preset: TimeRangePreset) => {
  showCustom.value = false
  setPreset(preset)
}
</script>

<template>
  <div class="time-range-selector">
    <div class="range-bar">
      <button
        v-for="preset in presets"
        :key="preset.id"
        :class="['range-btn', { active: timeRange.preset === preset.id }]"
        @click="selectPreset(preset.id)"
      >
        {{ preset.label }}
      </button>
      <button
        :class="['range-btn custom-btn', { active: timeRange.preset === 'custom' }]"
        @click="toggleCustom"
      >
        <Calendar class="h-3.5 w-3.5" />
        <span>Custom</span>
      </button>
    </div>

    <div v-if="showCustom" class="custom-range">
      <input
        v-model="customFrom"
        type="date"
        class="date-input"
      />
      <span class="date-separator">to</span>
      <input
        v-model="customTo"
        type="date"
        class="date-input"
      />
      <button
        class="apply-btn"
        :disabled="!customFrom || !customTo"
        @click="applyCustom"
      >
        Apply
      </button>
    </div>
  </div>
</template>

<style scoped>
.time-range-selector {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.range-bar {
  display: flex;
  gap: 0.25rem;
  padding: 0.25rem;
  background: var(--color-surface);
  border-radius: 0.5rem;
  width: fit-content;
}

.range-btn {
  display: flex;
  align-items: center;
  gap: 0.375rem;
  padding: 0.5rem 0.75rem;
  border: none;
  background: transparent;
  color: var(--color-foreground-muted);
  font-size: 0.8125rem;
  font-weight: 500;
  border-radius: 0.375rem;
  cursor: pointer;
  white-space: nowrap;
  transition: all 0.15s ease;
}

.range-btn:hover {
  color: var(--color-foreground);
  background: var(--color-surface-elevated);
}

.range-btn.active {
  color: var(--color-accent);
  background: var(--color-surface-elevated);
}

.custom-range {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem;
  background: var(--color-surface);
  border-radius: 0.5rem;
  width: fit-content;
}

.date-input {
  padding: 0.375rem 0.5rem;
  background: var(--color-surface-elevated);
  border: 1px solid var(--color-border);
  border-radius: 0.375rem;
  color: var(--color-foreground);
  font-size: 0.8125rem;
  color-scheme: dark;
}

.date-separator {
  color: var(--color-foreground-muted);
  font-size: 0.8125rem;
}

.apply-btn {
  padding: 0.375rem 0.75rem;
  background: var(--color-accent);
  border: none;
  border-radius: 0.375rem;
  color: white;
  font-size: 0.8125rem;
  font-weight: 500;
  cursor: pointer;
  transition: opacity 0.15s ease;
}

.apply-btn:hover {
  opacity: 0.9;
}

.apply-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

@media (max-width: 768px) {
  .range-bar {
    width: 100%;
    justify-content: space-around;
  }

  .custom-range {
    flex-wrap: wrap;
    width: 100%;
  }

  .date-input {
    flex: 1;
    min-width: 0;
  }
}
</style>
