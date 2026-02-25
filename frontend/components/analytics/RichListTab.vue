<script setup lang="ts">
import { Crown } from 'lucide-vue-next'

const api = useApi()
const route = useRoute()
const router = useRouter()

const page = ref(Number(route.query.page) || 1)
const limit = 50

watch(page, (p) => {
  router.push({ query: p > 1 ? { page: p } : {} })
})

const { data: richList, pending } = await useAsyncData(
  () => `rich-list-${page.value}`,
  () => api.getRichList(page.value, limit),
  { watch: [page] }
)

const formatVolume = (volume: number) => {
  if (volume >= 1_000_000_000_000) return (volume / 1_000_000_000_000).toFixed(2) + 'T'
  if (volume >= 1_000_000_000) return (volume / 1_000_000_000).toFixed(2) + 'B'
  if (volume >= 1_000_000) return (volume / 1_000_000).toFixed(2) + 'M'
  if (volume >= 1_000) return (volume / 1_000).toFixed(2) + 'K'
  return volume.toLocaleString()
}

const truncateAddress = (address: string) => {
  if (address.length <= 16) return address
  return address.slice(0, 8) + '...' + address.slice(-8)
}

const getBadgeClass = (type: string | null | undefined) => {
  switch (type) {
    case 'exchange': return 'badge-warning'
    case 'smartcontract': return 'badge-info'
    case 'tokenissuer': return 'badge-accent'
    case 'burn': return 'badge-error'
    default: return 'badge-secondary'
  }
}
</script>

<template>
  <div class="space-y-6">
    <!-- Summary Cards -->
    <div v-if="richList" class="grid grid-cols-2 md:grid-cols-4 gap-4">
      <div class="card-elevated text-center">
        <div class="text-lg font-semibold text-accent">{{ richList.totalCount.toLocaleString() }}</div>
        <div class="text-xs text-foreground-muted uppercase mt-1">Total Holders</div>
      </div>
      <div class="card-elevated text-center">
        <div class="text-lg font-semibold text-success">{{ formatVolume(richList.totalBalance) }} QU</div>
        <div class="text-xs text-foreground-muted uppercase mt-1">Total Balance</div>
      </div>
      <div class="card-elevated text-center">
        <div class="text-lg font-semibold text-accent">
          {{ richList.entries.length > 0 ? richList.entries[0].percentageOfSupply.toFixed(2) + '%' : '-' }}
        </div>
        <div class="text-xs text-foreground-muted uppercase mt-1">#1 Holder Share</div>
      </div>
      <div class="card-elevated text-center">
        <div class="text-lg font-semibold text-foreground-muted">Epoch {{ richList.snapshotEpoch }}</div>
        <div class="text-xs text-foreground-muted uppercase mt-1">Snapshot</div>
      </div>
    </div>

    <!-- Rich List Table -->
    <div class="card">
      <h2 class="section-title mb-4">
        <Crown class="h-5 w-5 text-accent" />
        Top Holders by Balance
      </h2>

      <div v-if="pending" class="loading">Loading...</div>
      <template v-else-if="richList?.entries?.length">
        <div class="table-wrapper">
          <table>
            <thead>
              <tr>
                <th>Rank</th>
                <th>Address</th>
                <th>Type</th>
                <th>Balance</th>
                <th class="hide-mobile">% of Supply</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="entry in richList.entries" :key="entry.address">
                <td class="text-foreground-muted font-medium">{{ entry.rank }}</td>
                <td>
                  <NuxtLink :to="`/address/${entry.address}`" class="text-accent font-medium">
                    {{ entry.label || truncateAddress(entry.address) }}
                  </NuxtLink>
                  <div v-if="entry.label" class="text-xs text-foreground-muted">
                    {{ truncateAddress(entry.address) }}
                  </div>
                </td>
                <td>
                  <span v-if="entry.type && entry.type !== 'unknown'" :class="['badge text-xs', getBadgeClass(entry.type)]">
                    {{ entry.type }}
                  </span>
                  <span v-else class="text-foreground-muted">-</span>
                </td>
                <td class="font-semibold text-accent">{{ entry.balanceFormatted }}</td>
                <td class="hide-mobile">{{ entry.percentageOfSupply.toFixed(2) }}%</td>
              </tr>
            </tbody>
          </table>
        </div>
        <Pagination
          :current-page="page"
          :total-pages="richList.totalPages"
          :has-next="page < richList.totalPages"
          :has-previous="page > 1"
          @update:current-page="(p) => page = p"
        />
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        No holder data available. Spectrum import may be required.
      </div>
    </div>
  </div>
</template>
