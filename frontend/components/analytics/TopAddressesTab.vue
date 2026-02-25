<script setup lang="ts">
import { TrendingUp, ArrowRightLeft } from 'lucide-vue-next'
import type { TopAddressDto } from '~/composables/useApi'

const api = useApi()

// Top addresses by volume
const { data: topAddresses, pending: topAddressesLoading } = await useAsyncData(
  'top-addresses-full',
  () => api.getTopAddresses(50)
)

const { formatVolume, truncateAddress, getBadgeClass } = useFormatting()
const getAddressDisplay = (item: TopAddressDto) => item.label || truncateAddress(item.address)
</script>

<template>
  <div class="space-y-6">
    <!-- Top Addresses by Volume -->
    <div class="card">
      <h2 class="section-title mb-4">
        <TrendingUp class="h-5 w-5 text-accent" />
        Top Addresses by Volume
      </h2>

      <div v-if="topAddressesLoading" class="loading">Loading...</div>
      <template v-else-if="topAddresses?.length">
        <div class="table-wrapper">
          <table>
            <thead>
              <tr>
                <th>#</th>
                <th>Address</th>
                <th>Type</th>
                <th>Total Volume</th>
                <th class="hide-mobile">Sent</th>
                <th class="hide-mobile">Received</th>
                <th class="hide-mobile">Tx Count</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(addr, index) in topAddresses" :key="addr.address">
                <td class="text-foreground-muted">{{ index + 1 }}</td>
                <td>
                  <NuxtLink :to="`/address/${addr.address}`" class="text-accent font-medium">
                    {{ getAddressDisplay(addr) }}
                  </NuxtLink>
                  <div v-if="addr.label" class="text-xs text-foreground-muted">
                    {{ truncateAddress(addr.address) }}
                  </div>
                </td>
                <td>
                  <span v-if="addr.type && addr.type !== 'unknown'" :class="['badge text-xs', getBadgeClass(addr.type)]">
                    {{ addr.type }}
                  </span>
                  <span v-else class="text-foreground-muted">-</span>
                </td>
                <td class="font-semibold text-accent">{{ formatVolume(addr.totalVolume) }}</td>
                <td class="hide-mobile">{{ formatVolume(addr.sentVolume) }}</td>
                <td class="hide-mobile">{{ formatVolume(addr.receivedVolume) }}</td>
                <td class="hide-mobile">{{ addr.totalCount.toLocaleString() }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        No address data available
      </div>
    </div>

    <!-- Info about Address Flow -->
    <div class="card bg-surface-elevated">
      <h2 class="section-title mb-2">
        <ArrowRightLeft class="h-5 w-5 text-accent" />
        Address Transaction Flow
      </h2>
      <p class="text-foreground-muted">
        View transaction flow for any address by visiting its detail page. The flow visualization shows
        top counterparties (senders and receivers) for that address.
      </p>
      <div class="mt-4">
        <NuxtLink to="/addresses" class="btn btn-primary">
          Browse Known Addresses
        </NuxtLink>
      </div>
    </div>
  </div>
</template>
