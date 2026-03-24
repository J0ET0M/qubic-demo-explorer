<script setup lang="ts">
import { Blocks, ArrowLeftRight, FileText, Filter, X } from 'lucide-vue-next'
import { getSupportedContracts, getContractSchema } from '~/utils/contractInputDecoder'

const api = useApi()
const route = useRoute()
const { formatDate } = useFormatting()

const tickNumber = Number(route.params.tickNumber)

// Pagination state
const txPage = ref(1)
const logsPage = ref(1)
const pageLimit = 20

// Transaction filter state
const txAddress = ref('')
const txDirection = ref<'from' | 'to' | ''>('')
const txMinAmount = ref<number | undefined>(undefined)
const txExecuted = ref<boolean | undefined>(undefined)
const txInputType = ref<number | undefined>(undefined)
const txContractFilter = ref('')
const showTxFilters = ref(false)
const txMinAmountInput = ref('')

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
const txInputTypeOptions = computed(() => {
  if (!txContractFilter.value) return coreInputTypeOptions
  const contract = contracts.value.find(c => c.address === txContractFilter.value)
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
const selectedContractAddress = computed(() => txContractFilter.value || undefined)

// Get label for input type pill
const txInputTypeLabel = computed(() => {
  if (txInputType.value === undefined) return ''
  const opt = txInputTypeOptions.value.find(o => o.value === txInputType.value)
  return opt?.label || `Type ${txInputType.value}`
})

// Logs filter state
const logsFromAddress = ref('')
const logsToAddress = ref('')
const logsType = ref<number | undefined>(undefined)
const logsMinAmount = ref<number | undefined>(undefined)

// Check if filters are active
const hasTxFilters = computed(() =>
  txAddress.value !== '' || txMinAmount.value !== undefined || txExecuted.value !== undefined
  || txInputType.value !== undefined || txContractFilter.value !== ''
)

const { data: tick, pending, error } = await useAsyncData(
  `tick-${tickNumber}`,
  () => api.getTick(tickNumber)
)

// Build transaction filter options
const txFilterOptions = computed(() => {
  const opts: { address?: string; direction?: 'from' | 'to'; minAmount?: number; executed?: boolean; inputType?: number; toAddress?: string; coreOnly?: boolean } = {}
  if (txAddress.value) opts.address = txAddress.value
  if (txDirection.value === 'from' || txDirection.value === 'to') opts.direction = txDirection.value
  if (txMinAmount.value !== undefined) opts.minAmount = txMinAmount.value
  if (txExecuted.value !== undefined) opts.executed = txExecuted.value
  if (txInputType.value !== undefined) opts.inputType = txInputType.value
  if (selectedContractAddress.value) opts.toAddress = selectedContractAddress.value
  else opts.coreOnly = true
  return opts
})

// Fetch transactions with pagination and filters
const { data: transactions, pending: txPending } = await useAsyncData(
  () => `tick-${tickNumber}-transactions-${txPage.value}-${txAddress.value}-${txDirection.value}-${txMinAmount.value}-${txExecuted.value}-${txInputType.value}-${txContractFilter.value}`,
  () => api.getTickTransactions(tickNumber, txPage.value, pageLimit,
    Object.keys(txFilterOptions.value).length > 0 ? txFilterOptions.value : undefined),
  { watch: [txPage, txAddress, txDirection, txMinAmount, txExecuted, txInputType, txContractFilter] }
)

// Build logs filter options
const logsFilterOptions = computed(() => {
  const opts: { fromAddress?: string; toAddress?: string; type?: number; minAmount?: number } = {}
  if (logsFromAddress.value) opts.fromAddress = logsFromAddress.value
  if (logsToAddress.value) opts.toAddress = logsToAddress.value
  if (logsType.value !== undefined) opts.type = logsType.value
  if (logsMinAmount.value !== undefined) opts.minAmount = logsMinAmount.value
  return opts
})

// Fetch logs with pagination and filters
const { data: logs, pending: logsPending } = await useAsyncData(
  () => `tick-${tickNumber}-logs-${logsPage.value}-${logsFromAddress.value}-${logsToAddress.value}-${logsType.value}-${logsMinAmount.value}`,
  () => api.getTickLogs(tickNumber, logsPage.value, pageLimit,
    Object.keys(logsFilterOptions.value).length > 0 ? logsFilterOptions.value : undefined),
  { watch: [logsPage, logsFromAddress, logsToAddress, logsType, logsMinAmount] }
)

// Transaction filter functions
const applyTxFilters = () => {
  txPage.value = 1
  txMinAmount.value = txMinAmountInput.value ? Number(txMinAmountInput.value) : undefined
  showTxFilters.value = false
}

const clearTxFilters = () => {
  txAddress.value = ''
  txDirection.value = ''
  txMinAmountInput.value = ''
  txMinAmount.value = undefined
  txExecuted.value = undefined
  txInputType.value = undefined
  txContractFilter.value = ''
  txPage.value = 1
}

const onContractChange = () => {
  txInputType.value = undefined
  txPage.value = 1
}

const toggleTxExecutedFilter = (value: boolean | undefined) => {
  txExecuted.value = txExecuted.value === value ? undefined : value
  txPage.value = 1
}

</script>

<template>
  <div class="space-y-6">
    <div v-if="pending" class="loading">Loading...</div>

    <div v-else-if="error" class="card">
      <div class="text-center py-8">
        <Blocks class="h-12 w-12 text-foreground-muted mx-auto mb-4" />
        <h2 class="text-xl font-semibold mb-2">Tick Not Found</h2>
        <p class="text-foreground-muted">
          Tick {{ tickNumber.toLocaleString() }} does not exist or hasn't been indexed yet.
        </p>
        <NuxtLink to="/ticks" class="btn btn-primary mt-4">
          Back to Ticks
        </NuxtLink>
      </div>
    </div>

    <template v-else-if="tick">
      <!-- Tick Details Card -->
      <div class="card">
        <h2 class="section-title mb-4">
          <Blocks class="h-5 w-5 text-accent" />
          Tick Details
        </h2>

        <div class="space-y-0">
          <div class="detail-row">
            <span class="detail-label">Tick Number</span>
            <span class="detail-value font-semibold text-accent">
              {{ tick.tickNumber.toLocaleString() }}
            </span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Epoch</span>
            <span class="detail-value">{{ tick.epoch }}</span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Timestamp</span>
            <span class="detail-value">{{ formatDate(tick.timestamp) }}</span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Status</span>
            <span class="detail-value" :class="tick.isEmpty ? 'text-destructive' : 'text-success'">
              {{ tick.isEmpty ? 'Empty' : 'OK' }}
            </span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Transactions</span>
            <span class="detail-value">{{ tick.txCount }}</span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Logs</span>
            <span class="detail-value">{{ tick.logCount }}</span>
          </div>
        </div>
      </div>

      <!-- Transactions -->
      <div class="card">
        <h2 class="section-title mb-4">
          <ArrowLeftRight class="h-5 w-5 text-accent" />
          Transactions ({{ transactions?.totalCount || 0 }})
        </h2>

        <!-- Transaction Filters -->
        <div class="mb-4">
          <div class="flex items-center justify-between flex-wrap gap-2">
            <div class="flex items-center gap-2 flex-wrap">
              <button
                @click="showTxFilters = !showTxFilters"
                :class="['btn btn-sm btn-ghost', { 'text-accent': hasTxFilters }]"
              >
                <Filter class="h-4 w-4 mr-1" />
                Filters
                <span v-if="hasTxFilters" class="badge badge-accent ml-1 text-xs">Active</span>
              </button>

              <!-- Quick executed filter -->
              <div class="flex items-center gap-1">
                <button
                  @click="toggleTxExecutedFilter(true)"
                  :class="['btn btn-sm', txExecuted === true ? 'btn-success' : 'btn-ghost']"
                >
                  Executed
                </button>
                <button
                  @click="toggleTxExecutedFilter(false)"
                  :class="['btn btn-sm', txExecuted === false ? 'btn-error' : 'btn-ghost']"
                >
                  Failed
                </button>
              </div>

              <!-- Active filter pills -->
              <span
                v-if="txAddress"
                class="badge badge-info flex items-center gap-1"
              >
                Addr: {{ txAddress.slice(0, 8) }}...
                <button @click="txAddress = ''; txPage = 1" class="hover:text-white">
                  <X class="h-3 w-3" />
                </button>
              </span>
              <span
                v-if="txMinAmount !== undefined"
                class="badge badge-info flex items-center gap-1"
              >
                Min: {{ txMinAmount.toLocaleString() }}
                <button @click="txMinAmount = undefined; txPage = 1" class="hover:text-white">
                  <X class="h-3 w-3" />
                </button>
              </span>
              <span
                v-if="txContractFilter"
                class="badge badge-accent flex items-center gap-1"
              >
                {{ contracts.find(c => c.address === txContractFilter)?.name || 'Contract' }}
                <button @click="txContractFilter = ''; txInputType = undefined; txPage = 1" class="hover:text-white">
                  <X class="h-3 w-3" />
                </button>
              </span>
              <span
                v-if="txInputType !== undefined"
                class="badge badge-warning flex items-center gap-1"
              >
                {{ txInputTypeLabel }}
                <button @click="txInputType = undefined; txPage = 1" class="hover:text-white">
                  <X class="h-3 w-3" />
                </button>
              </span>
            </div>

            <button
              v-if="hasTxFilters"
              @click="clearTxFilters"
              class="btn btn-sm btn-ghost"
            >
              Clear filters
            </button>
          </div>

          <!-- Expanded filter panel -->
          <div v-if="showTxFilters" class="mt-3 p-3 bg-background-elevated rounded-lg">
            <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3">
              <div>
                <label class="block text-xs font-medium mb-1">Address</label>
                <input
                  v-model="txAddress"
                  type="text"
                  class="input input-sm w-full"
                  placeholder="Filter by address"
                />
              </div>
              <div>
                <label class="block text-xs font-medium mb-1">Direction</label>
                <select v-model="txDirection" class="input input-sm w-full">
                  <option value="">Both</option>
                  <option value="from">From (Sender)</option>
                  <option value="to">To (Receiver)</option>
                </select>
              </div>
              <div>
                <label class="block text-xs font-medium mb-1">Min Amount</label>
                <input
                  v-model="txMinAmountInput"
                  type="number"
                  min="0"
                  class="input input-sm w-full"
                  placeholder="e.g., 1000000"
                />
              </div>
              <div>
                <label class="block text-xs font-medium mb-1">Status</label>
                <select v-model="txExecuted" class="input input-sm w-full">
                  <option :value="undefined">All</option>
                  <option :value="true">Executed only</option>
                  <option :value="false">Failed only</option>
                </select>
              </div>
              <div>
                <label class="block text-xs font-medium mb-1">Smart Contract</label>
                <select v-model="txContractFilter" class="input input-sm w-full" @change="onContractChange">
                  <option value="">All Core</option>
                  <option v-for="c in contracts" :key="c.address" :value="c.address">
                    {{ c.name }}
                  </option>
                </select>
              </div>
              <div>
                <label class="block text-xs font-medium mb-1">{{ txContractFilter ? 'Procedure' : 'Input Type' }}</label>
                <select v-model="txInputType" class="input input-sm w-full" @change="txPage = 1">
                  <option v-for="opt in txInputTypeOptions" :key="String(opt.value)" :value="opt.value">
                    {{ opt.label }}{{ opt.value !== undefined ? ` (${opt.value})` : '' }}
                  </option>
                </select>
              </div>
              <div class="flex items-end">
                <button @click="applyTxFilters" class="btn btn-sm btn-primary">Apply</button>
              </div>
            </div>
          </div>
        </div>

        <div v-if="txPending" class="loading">Loading transactions...</div>
        <div v-else-if="transactions?.items?.length">
          <TransactionTable :transactions="transactions.items" />
          <Pagination
            v-if="transactions.totalPages > 1"
            :current-page="txPage"
            :total-pages="transactions.totalPages"
            :has-next="transactions.hasNextPage"
            :has-previous="transactions.hasPreviousPage"
            @update:current-page="txPage = $event"
          />
        </div>
        <div v-else class="text-center py-8 text-foreground-muted">
          No transactions found{{ hasTxFilters ? ' matching filters' : ' in this tick' }}.
        </div>
      </div>

      <!-- Logs/Transfers -->
      <div class="card">
        <h2 class="section-title mb-4">
          <FileText class="h-5 w-5 text-accent" />
          Logs / Transfers ({{ logs?.totalCount || 0 }})
        </h2>

        <div class="mb-4">
          <TransferFilters
            v-model:from-address="logsFromAddress"
            v-model:to-address="logsToAddress"
            v-model:selected-type="logsType"
            v-model:min-amount="logsMinAmount"
            @reset-page="logsPage = 1"
          />
        </div>

        <div v-if="logsPending" class="loading">Loading logs...</div>
        <div v-else-if="logs?.items?.length">
          <TransferTable :transfers="logs.items" hide-tick />
          <Pagination
            v-if="logs.totalPages > 1"
            :current-page="logsPage"
            :total-pages="logs.totalPages"
            :has-next="logs.hasNextPage"
            :has-previous="logs.hasPreviousPage"
            @update:current-page="logsPage = $event"
          />
        </div>
        <div v-else class="text-center py-8 text-foreground-muted">
          No logs found in this tick.
        </div>
      </div>
    </template>
  </div>
</template>
