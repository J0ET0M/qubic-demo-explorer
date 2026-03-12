<script setup lang="ts">
import { Filter, X } from 'lucide-vue-next'

const props = defineProps<{
  fromAddress: string
  toAddress: string
  selectedType?: number
  minAmount?: number
  /** Hide from/to address fields (e.g. on address page where context is implicit) */
  hideAddressFields?: boolean
}>()

const emit = defineEmits<{
  'update:fromAddress': [value: string]
  'update:toAddress': [value: string]
  'update:selectedType': [value: number | undefined]
  'update:minAmount': [value: number | undefined]
  'reset-page': []
}>()

const logTypes = [
  { value: 0, name: 'QU Transfer', color: 'badge-success' },
  { value: 1, name: 'Asset Issuance', color: 'badge-info' },
  { value: 2, name: 'Asset Ownership', color: 'badge-warning' },
  { value: 3, name: 'Asset Possession', color: 'badge-accent' },
  { value: 4, name: 'Contract Error', color: 'badge-error' },
  { value: 5, name: 'Contract Warning', color: 'badge-warning' },
  { value: 6, name: 'Contract Info', color: 'badge-info' },
  { value: 7, name: 'Contract Debug', color: 'badge-ghost' },
  { value: 8, name: 'Burning', color: 'badge-error' },
  { value: 9, name: 'Dust Burning', color: 'badge-error' },
]

const showFilters = ref(false)
const minAmountInput = ref(props.minAmount?.toString() || '')

const hasActiveFilters = computed(() =>
  props.fromAddress !== '' || props.toAddress !== '' || props.minAmount !== undefined || props.selectedType !== undefined
)

const toggleTypeFilter = (type: number) => {
  emit('update:selectedType', props.selectedType === type ? undefined : type)
  emit('reset-page')
}

const applyFilters = () => {
  emit('update:minAmount', minAmountInput.value ? Number(minAmountInput.value) : undefined)
  showFilters.value = false
  emit('reset-page')
}

const clearFilters = () => {
  emit('update:fromAddress', '')
  emit('update:toAddress', '')
  emit('update:selectedType', undefined)
  emit('update:minAmount', undefined)
  minAmountInput.value = ''
  emit('reset-page')
}

const getTypeName = (type: number) => {
  return logTypes.find(t => t.value === type)?.name || `Type ${type}`
}
</script>

<template>
  <div>
    <div class="flex items-center justify-between flex-wrap gap-4">
      <div class="flex items-center gap-2 flex-wrap">
        <button
          @click="showFilters = !showFilters"
          :class="['btn btn-sm btn-ghost', { 'text-accent': hasActiveFilters }]"
        >
          <Filter class="h-4 w-4 mr-1" />
          Filters
          <span v-if="hasActiveFilters" class="badge badge-accent ml-1 text-xs">Active</span>
        </button>

        <!-- Quick filter buttons for common log types -->
        <div class="flex items-center gap-1 ml-2 flex-wrap">
          <button
            @click="toggleTypeFilter(0)"
            :class="['btn btn-xs', selectedType === 0 ? 'btn-success' : 'btn-ghost']"
          >
            QU Transfers
          </button>
          <button
            @click="toggleTypeFilter(1)"
            :class="['btn btn-xs', selectedType === 1 ? 'btn-info' : 'btn-ghost']"
          >
            Asset Issuance
          </button>
          <button
            @click="toggleTypeFilter(2)"
            :class="['btn btn-xs', selectedType === 2 ? 'btn-warning' : 'btn-ghost']"
          >
            Asset Ownership
          </button>
          <button
            @click="toggleTypeFilter(3)"
            :class="['btn btn-xs', selectedType === 3 ? 'btn-accent' : 'btn-ghost']"
          >
            Asset Possession
          </button>
        </div>

        <!-- Active filter pills -->
        <div v-if="hasActiveFilters" class="flex items-center gap-2 ml-2 flex-wrap">
          <span
            v-if="fromAddress"
            class="badge badge-info flex items-center gap-1"
          >
            From: {{ fromAddress.slice(0, 8) }}...
            <button @click="emit('update:fromAddress', ''); emit('reset-page')" class="hover:text-white">
              <X class="h-3 w-3" />
            </button>
          </span>
          <span
            v-if="toAddress"
            class="badge badge-info flex items-center gap-1"
          >
            To: {{ toAddress.slice(0, 8) }}...
            <button @click="emit('update:toAddress', ''); emit('reset-page')" class="hover:text-white">
              <X class="h-3 w-3" />
            </button>
          </span>
          <span
            v-if="minAmount !== undefined"
            class="badge badge-info flex items-center gap-1"
          >
            Min: {{ minAmount.toLocaleString() }} QU
            <button @click="emit('update:minAmount', undefined); emit('reset-page')" class="hover:text-white">
              <X class="h-3 w-3" />
            </button>
          </span>
          <span
            v-if="selectedType !== undefined"
            class="badge badge-accent flex items-center gap-1"
          >
            {{ getTypeName(selectedType) }}
            <button @click="emit('update:selectedType', undefined); emit('reset-page')" class="hover:text-white">
              <X class="h-3 w-3" />
            </button>
          </span>
        </div>
      </div>

      <button
        v-if="hasActiveFilters"
        @click="clearFilters"
        class="btn btn-sm btn-ghost"
      >
        Clear all filters
      </button>
    </div>

    <!-- Expanded filter panel -->
    <div v-if="showFilters" class="mt-3 p-3 bg-background-elevated rounded-lg">
      <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-3">
        <div v-if="!hideAddressFields">
          <label class="block text-xs font-medium mb-1">From Address</label>
          <input
            :value="fromAddress"
            @input="emit('update:fromAddress', ($event.target as HTMLInputElement).value)"
            type="text"
            class="input input-sm w-full"
            placeholder="Source address"
          />
        </div>
        <div v-if="!hideAddressFields">
          <label class="block text-xs font-medium mb-1">To Address</label>
          <input
            :value="toAddress"
            @input="emit('update:toAddress', ($event.target as HTMLInputElement).value)"
            type="text"
            class="input input-sm w-full"
            placeholder="Destination address"
          />
        </div>
        <div>
          <label class="block text-xs font-medium mb-1">Minimum Amount (QU)</label>
          <input
            v-model="minAmountInput"
            type="number"
            min="0"
            class="input input-sm w-full"
            placeholder="e.g., 1000000"
          />
        </div>
        <div>
          <label class="block text-xs font-medium mb-1">Log Type</label>
          <select
            :value="selectedType"
            @change="emit('update:selectedType', ($event.target as HTMLSelectElement).value ? Number(($event.target as HTMLSelectElement).value) : undefined)"
            class="input input-sm w-full"
          >
            <option :value="undefined">All Types</option>
            <option v-for="type in logTypes" :key="type.value" :value="type.value">
              {{ type.name }}
            </option>
          </select>
        </div>
        <div class="flex items-end">
          <button @click="applyFilters" class="btn btn-sm btn-primary">Apply</button>
        </div>
      </div>
    </div>
  </div>
</template>
