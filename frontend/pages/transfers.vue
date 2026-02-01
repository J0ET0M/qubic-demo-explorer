<script setup lang="ts">
import { Filter, X } from 'lucide-vue-next'

const api = useApi()
const route = useRoute()
const router = useRouter()

// Log type definitions
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

// Filter state from URL
const page = ref(Number(route.query.page) || 1)
const address = ref((route.query.address as string) || '')
const direction = ref<'in' | 'out' | ''>((route.query.direction as 'in' | 'out' | '') || '')
const minAmount = ref(route.query.minAmount ? Number(route.query.minAmount) : undefined)
const selectedType = ref<number | undefined>(
  route.query.type !== undefined ? Number(route.query.type) : undefined
)
const limit = 20

// UI state for filter panel
const showFilters = ref(false)
const minAmountInput = ref(minAmount.value?.toString() || '')

// Check if any filters are active
const hasActiveFilters = computed(() =>
  address.value !== '' || minAmount.value !== undefined || selectedType.value !== undefined
)

// Build filter options for API
const filterOptions = computed(() => {
  const opts: {
    address?: string
    direction?: 'in' | 'out'
    type?: number
    minAmount?: number
  } = {}
  if (address.value) opts.address = address.value
  if (direction.value === 'in' || direction.value === 'out') opts.direction = direction.value
  if (selectedType.value !== undefined) opts.type = selectedType.value
  if (minAmount.value !== undefined) opts.minAmount = minAmount.value
  return opts
})

const { data, pending } = await useAsyncData(
  () => `transfers-${page.value}-${address.value}-${direction.value}-${selectedType.value}-${minAmount.value}`,
  () => api.getTransfers(page.value, limit, Object.keys(filterOptions.value).length > 0 ? filterOptions.value : undefined),
  { watch: [page, address, direction, selectedType, minAmount] }
)

// Update URL when filters change
const updateUrl = () => {
  const query: Record<string, string | number> = {}
  if (page.value > 1) query.page = page.value
  if (address.value) query.address = address.value
  if (direction.value) query.direction = direction.value
  if (selectedType.value !== undefined) query.type = selectedType.value
  if (minAmount.value !== undefined) query.minAmount = minAmount.value
  router.push({ query })
}

watch([page, address, direction, selectedType, minAmount], updateUrl)

const updatePage = async (newPage: number) => {
  page.value = newPage
}

const applyFilters = () => {
  page.value = 1 // Reset to first page when applying filters
  minAmount.value = minAmountInput.value ? Number(minAmountInput.value) : undefined
  showFilters.value = false
}

const clearFilters = () => {
  address.value = ''
  direction.value = ''
  minAmountInput.value = ''
  minAmount.value = undefined
  selectedType.value = undefined
  page.value = 1
}

const toggleTypeFilter = (type: number) => {
  selectedType.value = selectedType.value === type ? undefined : type
  page.value = 1
}

const getTypeName = (type: number) => {
  return logTypes.find(t => t.value === type)?.name || `Type ${type}`
}
</script>

<template>
  <div class="space-y-6">
    <!-- Filter Bar -->
    <div class="card">
      <div class="flex items-center justify-between flex-wrap gap-4">
        <div class="flex items-center gap-2 flex-wrap">
          <button
            @click="showFilters = !showFilters"
            :class="['btn btn-ghost', { 'text-accent': hasActiveFilters }]"
          >
            <Filter class="h-4 w-4 mr-1" />
            Filters
            <span v-if="hasActiveFilters" class="badge badge-accent ml-2">Active</span>
          </button>

          <!-- Quick filter buttons for common log types -->
          <div class="flex items-center gap-1 ml-4 flex-wrap">
            <button
              @click="toggleTypeFilter(0)"
              :class="['btn btn-sm', selectedType === 0 ? 'btn-success' : 'btn-ghost']"
            >
              QU Transfers
            </button>
            <button
              @click="toggleTypeFilter(1)"
              :class="['btn btn-sm', selectedType === 1 ? 'btn-info' : 'btn-ghost']"
            >
              Asset Issuance
            </button>
            <button
              @click="toggleTypeFilter(2)"
              :class="['btn btn-sm', selectedType === 2 ? 'btn-warning' : 'btn-ghost']"
            >
              Asset Ownership
            </button>
            <button
              @click="toggleTypeFilter(3)"
              :class="['btn btn-sm', selectedType === 3 ? 'btn-accent' : 'btn-ghost']"
            >
              Asset Possession
            </button>
          </div>

          <!-- Active filter pills -->
          <div v-if="hasActiveFilters" class="flex items-center gap-2 ml-4 flex-wrap">
            <span
              v-if="address"
              class="badge badge-info flex items-center gap-1"
            >
              Addr: {{ address.slice(0, 8) }}...
              <button @click="address = ''; page = 1" class="hover:text-white">
                <X class="h-3 w-3" />
              </button>
            </span>
            <span
              v-if="minAmount !== undefined"
              class="badge badge-info flex items-center gap-1"
            >
              Min: {{ minAmount.toLocaleString() }} QU
              <button @click="minAmount = undefined; page = 1" class="hover:text-white">
                <X class="h-3 w-3" />
              </button>
            </span>
            <span
              v-if="selectedType !== undefined"
              class="badge badge-accent flex items-center gap-1"
            >
              {{ getTypeName(selectedType) }}
              <button @click="selectedType = undefined; page = 1" class="hover:text-white">
                <X class="h-3 w-3" />
              </button>
            </span>
          </div>
        </div>

        <button
          v-if="hasActiveFilters"
          @click="clearFilters"
          class="btn btn-ghost text-sm"
        >
          Clear all filters
        </button>
      </div>

      <!-- Expanded filter panel -->
      <div v-if="showFilters" class="mt-4 pt-4 border-t border-border">
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
          <div>
            <label class="block text-sm font-medium mb-1">Address</label>
            <input
              v-model="address"
              type="text"
              class="input w-full"
              placeholder="Filter by address"
            />
          </div>
          <div>
            <label class="block text-sm font-medium mb-1">Direction</label>
            <select v-model="direction" class="input w-full">
              <option value="">Both</option>
              <option value="in">Incoming</option>
              <option value="out">Outgoing</option>
            </select>
          </div>
          <div>
            <label class="block text-sm font-medium mb-1">Minimum Amount (QU)</label>
            <input
              v-model="minAmountInput"
              type="number"
              min="0"
              class="input w-full"
              placeholder="e.g., 1000000"
            />
          </div>
          <div>
            <label class="block text-sm font-medium mb-1">Log Type</label>
            <select v-model="selectedType" class="input w-full">
              <option :value="undefined">All Types</option>
              <option v-for="type in logTypes" :key="type.value" :value="type.value">
                {{ type.name }}
              </option>
            </select>
          </div>
          <div class="flex items-end">
            <button @click="applyFilters" class="btn btn-primary">
              Apply Filters
            </button>
          </div>
        </div>
      </div>
    </div>

    <div v-if="pending" class="loading">Loading...</div>

    <template v-else-if="data">
      <div class="card">
        <div class="flex items-center justify-between mb-4">
          <span class="text-sm text-foreground-muted">
            Showing {{ data.items.length }} of {{ data.totalCount.toLocaleString() }} transfers
          </span>
        </div>
        <TransferTable :transfers="data.items" />
      </div>

      <Pagination
        :current-page="page"
        :total-pages="data.totalPages"
        :has-next="data.hasNextPage"
        :has-previous="data.hasPreviousPage"
        @update:current-page="updatePage"
      />
    </template>
  </div>
</template>
