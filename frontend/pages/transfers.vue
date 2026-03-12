<script setup lang="ts">
const api = useApi()
const route = useRoute()
const router = useRouter()

// Filter state from URL
const page = ref(Number(route.query.page) || 1)
const fromAddress = ref((route.query.from as string) || '')
const toAddress = ref((route.query.to as string) || '')
const minAmount = ref(route.query.minAmount ? Number(route.query.minAmount) : undefined)
const selectedType = ref<number | undefined>(
  route.query.type !== undefined ? Number(route.query.type) : undefined
)
const limit = 20

// Build filter options for API
const filterOptions = computed(() => {
  const opts: {
    fromAddress?: string
    toAddress?: string
    type?: number
    minAmount?: number
  } = {}
  if (fromAddress.value) opts.fromAddress = fromAddress.value
  if (toAddress.value) opts.toAddress = toAddress.value
  if (selectedType.value !== undefined) opts.type = selectedType.value
  if (minAmount.value !== undefined) opts.minAmount = minAmount.value
  return opts
})

const { data, pending } = await useAsyncData(
  () => `transfers-${page.value}-${fromAddress.value}-${toAddress.value}-${selectedType.value}-${minAmount.value}`,
  () => api.getTransfers(page.value, limit, Object.keys(filterOptions.value).length > 0 ? filterOptions.value : undefined),
  { watch: [page, fromAddress, toAddress, selectedType, minAmount] }
)

// Update URL when filters change
const updateUrl = () => {
  const query: Record<string, string | number> = {}
  if (page.value > 1) query.page = page.value
  if (fromAddress.value) query.from = fromAddress.value
  if (toAddress.value) query.to = toAddress.value
  if (selectedType.value !== undefined) query.type = selectedType.value
  if (minAmount.value !== undefined) query.minAmount = minAmount.value
  router.push({ query })
}

watch([page, fromAddress, toAddress, selectedType, minAmount], updateUrl)
</script>

<template>
  <div class="space-y-6">
    <div class="card">
      <TransferFilters
        v-model:from-address="fromAddress"
        v-model:to-address="toAddress"
        v-model:selected-type="selectedType"
        v-model:min-amount="minAmount"
        @reset-page="page = 1"
      />
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
        @update:current-page="(p) => page = p"
      />
    </template>
  </div>
</template>
