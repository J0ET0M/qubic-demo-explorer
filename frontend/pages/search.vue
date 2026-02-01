<script setup lang="ts">
import { Search, Blocks, ArrowLeftRight, Wallet } from 'lucide-vue-next'

const api = useApi()
const route = useRoute()
const router = useRouter()
const { getLabel, fetchLabels } = useAddressLabels()

const query = computed(() => route.query.q as string || '')

// SearchResultType enum values from API (serialized as integers)
const SearchResultType = {
  Tick: 0,
  Transaction: 1,
  Address: 2,
  Asset: 3
} as const

const { data, pending } = await useAsyncData(
  `search-${query.value}`,
  () => api.search(query.value),
  { watch: [query] }
)

// Fetch labels for address results
watch(data, async (searchData) => {
  if (searchData?.results) {
    const addresses = searchData.results
      .filter(r => r.type === SearchResultType.Address)
      .map(r => r.value)
    if (addresses.length) {
      await fetchLabels(addresses)
    }
  }
}, { immediate: true })

const getResultLink = (result: { type: number; value: string }) => {
  switch (result.type) {
    case SearchResultType.Tick: return `/ticks/${result.value}`
    case SearchResultType.Transaction: return `/tx/${result.value}`
    case SearchResultType.Address: return `/address/${result.value}`
    default: return '#'
  }
}

const navigateToResult = (result: { type: number; value: string }) => {
  router.push(getResultLink(result))
}

const getResultIcon = (type: number) => {
  switch (type) {
    case SearchResultType.Tick: return Blocks
    case SearchResultType.Transaction: return ArrowLeftRight
    case SearchResultType.Address: return Wallet
    default: return Search
  }
}

const getDisplayName = (result: { type: number; value: string; displayName?: string }) => {
  if (result.type === SearchResultType.Address) {
    const label = getLabel(result.value)
    if (label?.label) {
      return label.label
    }
  }
  return result.displayName || result.value
}

const getTypeName = (type: number) => {
  switch (type) {
    case SearchResultType.Tick: return 'Tick'
    case SearchResultType.Transaction: return 'Transaction'
    case SearchResultType.Address: return 'Address'
    case SearchResultType.Asset: return 'Asset'
    default: return 'Unknown'
  }
}
</script>

<template>
  <div class="space-y-6">
    <div v-if="query" class="text-foreground-muted">
      Searching for: <span class="text-foreground font-medium">{{ query }}</span>
    </div>

    <div v-if="pending" class="loading">Loading...</div>

    <template v-else-if="data">
      <div v-if="data.results.length === 0" class="card">
        <div class="text-center py-8">
          <Search class="h-12 w-12 text-foreground-muted mx-auto mb-4" />
          <h2 class="text-xl font-semibold mb-2">No Results Found</h2>
          <p class="text-foreground-muted">
            No results found for "{{ query }}"
          </p>
        </div>
      </div>

      <div v-else class="card">
        <div class="text-sm text-foreground-muted mb-4">
          Found {{ data.results.length }} result{{ data.results.length === 1 ? '' : 's' }}
        </div>

        <div class="space-y-2">
          <div
            v-for="(result, index) in data.results"
            :key="`${result.type}-${result.value}-${index}`"
            class="flex items-center gap-4 p-4 rounded-lg border border-border hover:bg-surface-elevated transition-colors cursor-pointer"
            @click="navigateToResult(result)"
          >
            <div class="p-2 rounded-lg bg-accent/10">
              <component :is="getResultIcon(result.type)" class="h-5 w-5 text-accent" />
            </div>
            <div class="flex-1 min-w-0">
              <div class="text-sm text-foreground-muted flex items-center gap-2">
                {{ getTypeName(result.type) }}
                <span
                  v-if="result.type === SearchResultType.Address && getLabel(result.value)?.type && getLabel(result.value)?.type !== 'unknown'"
                  :class="['badge text-xs', {
                    'badge-warning': getLabel(result.value)?.type === 'exchange',
                    'badge-info': getLabel(result.value)?.type === 'smartcontract',
                    'badge-accent': getLabel(result.value)?.type === 'tokenissuer',
                    'badge-error': getLabel(result.value)?.type === 'burn'
                  }]"
                >
                  {{ getLabel(result.value)?.type }}
                </span>
              </div>
              <!-- Use AddressDisplay for address results -->
              <template v-if="result.type === SearchResultType.Address">
                <AddressDisplay :address="result.value" :label="getLabel(result.value)" />
              </template>
              <template v-else>
                <div class="font-medium truncate">
                  {{ getDisplayName(result) }}
                </div>
              </template>
            </div>
          </div>
        </div>
      </div>
    </template>
  </div>
</template>
