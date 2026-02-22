<script setup lang="ts">
import { Filter, X } from 'lucide-vue-next'
import { getSupportedContracts, getContractSchema } from '~/utils/contractInputDecoder'

const api = useApi()
const route = useRoute()
const router = useRouter()

// Filter state from URL
const page = ref(Number(route.query.page) || 1)
const address = ref((route.query.address as string) || '')
const direction = ref<'from' | 'to' | ''>((route.query.direction as 'from' | 'to' | '') || '')
const minAmount = ref(route.query.minAmount ? Number(route.query.minAmount) : undefined)
const executed = ref<boolean | undefined>(
  route.query.executed === 'true' ? true :
  route.query.executed === 'false' ? false : undefined
)
const inputType = ref<number | undefined>(
  route.query.inputType !== undefined ? Number(route.query.inputType) : undefined
)
const contractFilter = ref((route.query.contract as string) || '')
const limit = 20

// Contract list with addresses (fetched from labels API)
const contracts = ref<Array<{ index: number; name: string; address: string }>>([])

// Fetch contract addresses on mount
const supportedContracts = getSupportedContracts()
const { data: knownAddresses } = await useAsyncData(
  'contract-addresses',
  () => api.getAllKnownAddresses('smartcontract')
)

watchEffect(() => {
  if (knownAddresses.value) {
    contracts.value = supportedContracts
      .map(sc => {
        const found = knownAddresses.value!.find(ka => ka.contractIndex === sc.index)
        return found ? { index: sc.index, name: sc.name, address: found.address } : null
      })
      .filter((c): c is { index: number; name: string; address: string } => c !== null)
      .sort((a, b) => a.name.localeCompare(b.name))
  }
})

// Core input type options (for burn address transactions)
const coreInputTypeOptions = [
  { value: undefined as number | undefined, label: 'All' },
  { value: 0, label: 'Transfer' },
  { value: 1, label: 'Vote Counter' },
  { value: 2, label: 'Mining Solution' },
  { value: 3, label: 'File Header' },
  { value: 4, label: 'File Fragment' },
  { value: 5, label: 'File Trailer' },
  { value: 6, label: 'Oracle Reply Commit' },
  { value: 7, label: 'Oracle Reply Reveal' },
  { value: 8, label: 'Mining Share Counter' },
  { value: 9, label: 'Execution Fee Report' },
  { value: 10, label: 'Oracle User Query' },
]

// Dynamic input type options: show contract procedures when a contract is selected
const inputTypeOptions = computed(() => {
  if (!contractFilter.value) return coreInputTypeOptions
  const contract = contracts.value.find(c => c.address === contractFilter.value)
  if (!contract) return [{ value: undefined as number | undefined, label: 'All' }]
  const schema = getContractSchema(contract.index)
  if (!schema) return [{ value: undefined as number | undefined, label: 'All' }]
  const procedures = Object.entries(schema.procedures).map(([id, proc]) => ({
    value: parseInt(id) as number | undefined,
    label: proc.name,
  }))
  return [{ value: undefined as number | undefined, label: 'All' }, ...procedures]
})

// Get the selected contract's address for the API
const selectedContractAddress = computed(() => contractFilter.value || undefined)

// Get label for input type pill
const inputTypeLabel = computed(() => {
  if (inputType.value === undefined) return ''
  const opt = inputTypeOptions.value.find(o => o.value === inputType.value)
  return opt?.label || `Type ${inputType.value}`
})

// UI state for filter panel
const showFilters = ref(false)
const minAmountInput = ref(minAmount.value?.toString() || '')

// Check if any filters are active
const hasActiveFilters = computed(() =>
  address.value !== '' || minAmount.value !== undefined || executed.value !== undefined
  || inputType.value !== undefined || contractFilter.value !== ''
)

// Build filter options for API
const filterOptions = computed(() => {
  const opts: {
    address?: string
    direction?: 'from' | 'to'
    minAmount?: number
    executed?: boolean
    inputType?: number
    toAddress?: string
  } = {}
  if (address.value) opts.address = address.value
  if (direction.value === 'from' || direction.value === 'to') opts.direction = direction.value
  if (minAmount.value !== undefined) opts.minAmount = minAmount.value
  if (executed.value !== undefined) opts.executed = executed.value
  if (inputType.value !== undefined) opts.inputType = inputType.value
  if (selectedContractAddress.value) opts.toAddress = selectedContractAddress.value
  return opts
})

const { data, pending, refresh } = await useAsyncData(
  () => `transactions-${page.value}-${address.value}-${direction.value}-${minAmount.value}-${executed.value}-${inputType.value}-${contractFilter.value}`,
  () => api.getTransactions(page.value, limit, Object.keys(filterOptions.value).length > 0 ? filterOptions.value : undefined),
  { watch: [page, address, direction, minAmount, executed, inputType, contractFilter] }
)

// Update URL when filters change
const updateUrl = () => {
  const query: Record<string, string | number> = {}
  if (page.value > 1) query.page = page.value
  if (address.value) query.address = address.value
  if (direction.value) query.direction = direction.value
  if (minAmount.value !== undefined) query.minAmount = minAmount.value
  if (executed.value !== undefined) query.executed = String(executed.value)
  if (inputType.value !== undefined) query.inputType = inputType.value
  if (contractFilter.value) query.contract = contractFilter.value
  router.push({ query })
}

watch([page, address, direction, minAmount, executed, inputType, contractFilter], updateUrl)

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
  executed.value = undefined
  inputType.value = undefined
  contractFilter.value = ''
  page.value = 1
}

const onContractChange = () => {
  inputType.value = undefined // Reset procedure filter when contract changes
  page.value = 1
}

const toggleExecutedFilter = (value: boolean | undefined) => {
  executed.value = executed.value === value ? undefined : value
  page.value = 1
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

          <!-- Quick filter buttons for executed status -->
          <div class="flex items-center gap-1 ml-4">
            <button
              @click="toggleExecutedFilter(true)"
              :class="['btn btn-sm', executed === true ? 'btn-success' : 'btn-ghost']"
            >
              Executed
            </button>
            <button
              @click="toggleExecutedFilter(false)"
              :class="['btn btn-sm', executed === false ? 'btn-error' : 'btn-ghost']"
            >
              Failed
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
              v-if="contractFilter"
              class="badge badge-accent flex items-center gap-1"
            >
              {{ contracts.find(c => c.address === contractFilter)?.name || 'Contract' }}
              <button @click="contractFilter = ''; inputType = undefined; page = 1" class="hover:text-white">
                <X class="h-3 w-3" />
              </button>
            </span>
            <span
              v-if="inputType !== undefined"
              class="badge badge-warning flex items-center gap-1"
            >
              {{ inputTypeLabel }}
              <button @click="inputType = undefined; page = 1" class="hover:text-white">
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
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
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
              <option value="from">From (Sender)</option>
              <option value="to">To (Receiver)</option>
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
            <label class="block text-sm font-medium mb-1">Execution Status</label>
            <select v-model="executed" class="input w-full">
              <option :value="undefined">All</option>
              <option :value="true">Executed only</option>
              <option :value="false">Failed only</option>
            </select>
          </div>
          <div>
            <label class="block text-sm font-medium mb-1">Smart Contract</label>
            <select v-model="contractFilter" class="input w-full" @change="onContractChange">
              <option value="">All (Core + Contracts)</option>
              <option v-for="c in contracts" :key="c.address" :value="c.address">
                {{ c.name }}
              </option>
            </select>
          </div>
          <div>
            <label class="block text-sm font-medium mb-1">{{ contractFilter ? 'Procedure' : 'Input Type' }}</label>
            <select v-model="inputType" class="input w-full" @change="page = 1">
              <option v-for="opt in inputTypeOptions" :key="String(opt.value)" :value="opt.value">
                {{ opt.label }}{{ opt.value !== undefined ? ` (${opt.value})` : '' }}
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
            Showing {{ data.items.length }} of {{ data.totalCount.toLocaleString() }} transactions
          </span>
        </div>
        <TransactionTable :transactions="data.items" />
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
