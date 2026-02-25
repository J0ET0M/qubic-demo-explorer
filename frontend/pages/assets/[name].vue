<script setup lang="ts">
import { Gem, Users } from 'lucide-vue-next'

const api = useApi()
const route = useRoute()

const name = route.params.name as string
const issuer = (route.query.issuer as string) || undefined

useHead({ title: `${name} - Assets` })

const { data: asset, pending } = await useAsyncData(
  `asset-${name}-${issuer}`,
  () => api.getAsset(name, issuer)
)

// Pagination for holders
const holdersPage = ref(1)
const holdersLimit = 50

const { data: holders, pending: holdersPending } = await useAsyncData(
  () => `asset-holders-${name}-${issuer}-${holdersPage.value}`,
  () => api.getAssetHolders(name, holdersPage.value, holdersLimit, issuer),
  { watch: [holdersPage] }
)

const truncateAddress = (addr: string) =>
  addr.length > 16 ? addr.slice(0, 8) + '...' + addr.slice(-8) : addr

const formatShares = (shares: number) => {
  if (shares >= 1_000_000_000) return (shares / 1_000_000_000).toFixed(2) + 'B'
  if (shares >= 1_000_000) return (shares / 1_000_000).toFixed(2) + 'M'
  if (shares >= 1_000) return (shares / 1_000).toFixed(2) + 'K'
  return shares.toLocaleString()
}
</script>

<template>
  <div class="space-y-6">
    <div v-if="pending" class="card">
      <div class="loading py-12">Loading asset...</div>
    </div>

    <template v-else-if="asset">
      <!-- Asset Overview -->
      <div class="card">
        <h1 class="page-title flex items-center gap-2 mb-4">
          <Gem class="h-5 w-5 text-accent" />
          {{ asset.assetName }}
        </h1>

        <div class="space-y-0">
          <div class="detail-row">
            <span class="detail-label">Asset Name</span>
            <span class="detail-value font-semibold text-accent">{{ asset.assetName }}</span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Issuer</span>
            <span class="detail-value">
              <NuxtLink :to="`/address/${asset.issuerAddress}`" class="font-mono text-xs text-accent">
                {{ asset.issuerLabel || asset.issuerAddress }}
              </NuxtLink>
            </span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Decimal Places</span>
            <span class="detail-value">{{ asset.numberOfDecimalPlaces }}</span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Snapshot Epoch</span>
            <span class="detail-value">
              <NuxtLink :to="`/epochs/${asset.snapshotEpoch}`" class="text-accent">{{ asset.snapshotEpoch }}</NuxtLink>
            </span>
          </div>
        </div>

        <!-- Stats -->
        <div class="grid grid-cols-2 gap-4 mt-6">
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold text-accent">{{ formatShares(asset.totalSupply) }}</div>
            <div class="text-xs text-foreground-muted uppercase mt-1">Total Supply</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold text-accent">{{ asset.holderCount.toLocaleString() }}</div>
            <div class="text-xs text-foreground-muted uppercase mt-1">Holders</div>
          </div>
        </div>
      </div>

      <!-- Holders Table -->
      <div class="card">
        <h2 class="section-title mb-4">
          <Users class="h-5 w-5 text-accent" />
          Holders
        </h2>

        <div v-if="holdersPending" class="loading py-8">Loading holders...</div>

        <template v-else-if="holders && holders.holders.length > 0">
          <div class="table-wrapper">
            <table>
              <thead>
                <tr>
                  <th>#</th>
                  <th>Address</th>
                  <th class="text-right">Possessed</th>
                  <th class="text-right">Owned</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(holder, idx) in holders.holders" :key="holder.address">
                  <td class="text-foreground-muted">{{ (holdersPage - 1) * holdersLimit + idx + 1 }}</td>
                  <td>
                    <NuxtLink :to="`/address/${holder.address}`" class="font-mono text-xs text-accent">
                      {{ holder.label || truncateAddress(holder.address) }}
                    </NuxtLink>
                    <span v-if="holder.type === 'exchange'" class="ml-1 badge badge-warning text-[0.625rem]">CEX</span>
                  </td>
                  <td class="text-right font-semibold">{{ formatShares(holder.possessedShares) }}</td>
                  <td class="text-right text-foreground-muted">{{ formatShares(holder.ownedShares) }}</td>
                </tr>
              </tbody>
            </table>
          </div>

          <!-- Pagination -->
          <div v-if="holders.totalPages > 1" class="flex items-center justify-center gap-2 mt-4">
            <button
              :disabled="holdersPage <= 1"
              class="btn btn-sm btn-ghost"
              @click="holdersPage--"
            >
              Previous
            </button>
            <span class="text-sm text-foreground-muted">
              Page {{ holdersPage }} of {{ holders.totalPages }}
            </span>
            <button
              :disabled="holdersPage >= holders.totalPages"
              class="btn btn-sm btn-ghost"
              @click="holdersPage++"
            >
              Next
            </button>
          </div>
        </template>

        <div v-else class="text-center py-8 text-foreground-muted text-sm">
          No holders found for this asset.
        </div>
      </div>
    </template>

    <div v-else class="card">
      <div class="text-center py-12 text-foreground-muted text-sm">
        Asset "{{ name }}" not found.
      </div>
    </div>
  </div>
</template>
