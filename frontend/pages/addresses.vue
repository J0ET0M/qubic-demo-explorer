<script setup lang="ts">
import { Users, Building2, FileCode, Coins, Flame, Tag, ExternalLink } from 'lucide-vue-next'

const api = useApi()
const route = useRoute()
const router = useRouter()

const selectedType = ref<string>((route.query.type as string) || '')

// Fetch all addresses once and filter client-side
const { data: allAddresses, pending } = await useAsyncData(
  'known-addresses-all',
  () => api.getAllKnownAddresses()
)

const { data: stats } = await useAsyncData(
  'label-stats',
  () => api.getLabelStats()
)

// Update URL when type changes (use push for browser history)
watch(selectedType, (newType) => {
  router.push({
    query: newType ? { type: newType } : {}
  })
})

const typeOptions = [
  { value: '', label: 'All Types', icon: Users },
  { value: 'exchange', label: 'Exchanges', icon: Building2 },
  { value: 'smartcontract', label: 'Smart Contracts', icon: FileCode },
  { value: 'tokenissuer', label: 'Token Issuers', icon: Coins },
  { value: 'burn', label: 'Burn Addresses', icon: Flame },
  { value: 'known', label: 'Other Known', icon: Tag },
]

const getTypeIcon = (type: string) => {
  switch (type) {
    case 'exchange': return Building2
    case 'smartcontract': return FileCode
    case 'tokenissuer': return Coins
    case 'burn': return Flame
    default: return Tag
  }
}

const { getBadgeClass: getTypeBadgeClass, truncateAddress } = useFormatting()

const formatType = (type: string) => {
  switch (type) {
    case 'exchange': return 'Exchange'
    case 'smartcontract': return 'Smart Contract'
    case 'tokenissuer': return 'Token Issuer'
    case 'burn': return 'Burn'
    default: return 'Known'
  }
}

const filteredAddresses = computed(() => {
  if (!allAddresses.value) return []
  if (!selectedType.value) return allAddresses.value
  return allAddresses.value.filter(addr => addr.type === selectedType.value)
})
</script>

<template>
  <div class="space-y-6">
    <!-- Header with Stats -->
    <div class="card">
      <h1 class="section-title mb-4">
        <Users class="h-5 w-5 text-accent" />
        Known Addresses
      </h1>

      <div v-if="stats" class="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4 mb-6">
        <div
          v-for="option in typeOptions"
          :key="option.value"
          class="p-3 rounded-lg bg-surface-elevated flex flex-col items-center gap-1 text-center cursor-pointer transition-all hover:opacity-80"
          :class="{ 'ring-2 ring-accent': selectedType === option.value }"
          @click="selectedType = option.value"
        >
          <component :is="option.icon" class="h-4 w-4 text-foreground-muted" />
          <div class="text-lg font-semibold">
            {{ option.value === '' ? stats.totalLabels : (stats.byType[option.value] || 0) }}
          </div>
          <div class="text-xs text-foreground-muted">{{ option.label }}</div>
        </div>
      </div>

      <!-- Filter Tabs -->
      <div class="flex flex-wrap gap-2">
        <button
          v-for="option in typeOptions"
          :key="option.value"
          @click="selectedType = option.value"
          :class="[
            'btn text-sm flex items-center gap-2',
            selectedType === option.value ? 'btn-primary' : 'btn-outline'
          ]"
        >
          <component :is="option.icon" class="h-4 w-4" />
          {{ option.label }}
        </button>
      </div>
    </div>

    <!-- Address List -->
    <div class="card">
      <div v-if="pending" class="loading">Loading...</div>

      <template v-else-if="filteredAddresses.length">
        <div class="text-sm text-foreground-muted mb-4">
          Showing {{ filteredAddresses.length }} address{{ filteredAddresses.length === 1 ? '' : 'es' }}
        </div>

        <div class="table-wrapper">
          <table>
            <thead>
              <tr>
                <th>Label</th>
                <th>Address</th>
                <th>Type</th>
                <th class="hide-mobile">Details</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="addr in filteredAddresses" :key="addr.address">
                <td class="font-medium">
                  {{ addr.label }}
                </td>
                <td>
                  <NuxtLink
                    :to="`/address/${addr.address}`"
                    class="hash hover:text-accent transition-colors"
                    :title="addr.address"
                  >
                    {{ truncateAddress(addr.address) }}
                  </NuxtLink>
                </td>
                <td>
                  <span :class="['badge', getTypeBadgeClass(addr.type)]">
                    <component :is="getTypeIcon(addr.type)" class="h-3 w-3 mr-1" />
                    {{ formatType(addr.type) }}
                  </span>
                </td>
                <td class="hide-mobile">
                  <div class="flex items-center gap-2 text-sm text-foreground-muted">
                    <span v-if="addr.contractIndex !== null && addr.contractIndex !== undefined">
                      Index: {{ addr.contractIndex }}
                    </span>
                    <a
                      v-if="addr.website"
                      :href="addr.website"
                      target="_blank"
                      rel="noopener noreferrer"
                      class="flex items-center gap-1 text-accent hover:underline"
                    >
                      <ExternalLink class="h-3 w-3" />
                      Website
                    </a>
                  </div>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </template>

      <div v-else class="text-center py-8 text-foreground-muted">
        No addresses found for this filter.
      </div>
    </div>
  </div>
</template>
