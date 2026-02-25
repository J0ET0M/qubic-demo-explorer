<script setup lang="ts">
import { Gem } from 'lucide-vue-next'

useHead({ title: 'Assets - QLI Explorer' })

const api = useApi()

const { data: assets, pending } = await useAsyncData(
  'assets-list',
  () => api.getAssets()
)

const truncateAddress = (addr: string) =>
  addr.length > 16 ? addr.slice(0, 8) + '...' + addr.slice(-8) : addr

const formatSupply = (supply: number) => {
  if (supply >= 1_000_000_000) return (supply / 1_000_000_000).toFixed(2) + 'B'
  if (supply >= 1_000_000) return (supply / 1_000_000).toFixed(2) + 'M'
  if (supply >= 1_000) return (supply / 1_000).toFixed(2) + 'K'
  return supply.toLocaleString()
}
</script>

<template>
  <div class="space-y-6">
    <h1 class="page-title flex items-center gap-2">
      <Gem class="h-5 w-5 text-accent" />
      Assets
    </h1>

    <div v-if="pending" class="card">
      <div class="loading py-12">Loading assets...</div>
    </div>

    <div v-else-if="assets && assets.length > 0" class="card">
      <div class="text-xs text-foreground-muted mb-3">
        {{ assets.length }} assets found
      </div>
      <div class="table-wrapper">
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Issuer</th>
              <th class="text-right">Total Supply</th>
              <th class="text-right">Holders</th>
              <th class="text-right">Decimals</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="asset in assets" :key="`${asset.assetName}-${asset.issuerAddress}`">
              <td>
                <NuxtLink
                  :to="`/assets/${asset.assetName}?issuer=${asset.issuerAddress}`"
                  class="text-accent font-semibold"
                >
                  {{ asset.assetName }}
                </NuxtLink>
              </td>
              <td>
                <NuxtLink
                  :to="`/address/${asset.issuerAddress}`"
                  class="font-mono text-xs"
                >
                  {{ asset.issuerLabel || truncateAddress(asset.issuerAddress) }}
                </NuxtLink>
              </td>
              <td class="text-right font-semibold">{{ formatSupply(asset.totalSupply) }}</td>
              <td class="text-right">{{ asset.holderCount.toLocaleString() }}</td>
              <td class="text-right text-foreground-muted">{{ asset.numberOfDecimalPlaces }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <div v-else class="card">
      <div class="text-center py-12 text-foreground-muted text-sm">
        No assets found. Universe file may not be imported yet.
      </div>
    </div>
  </div>
</template>
