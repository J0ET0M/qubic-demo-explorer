<script setup lang="ts">
import { Calendar, ArrowLeftRight, Users, Blocks, TrendingUp, Coins, Gift } from 'lucide-vue-next'

const api = useApi()
const route = useRoute()
const router = useRouter()
const { fetchLabels, getLabel } = useAddressLabels()

const epoch = Number(route.params.epoch)

// Initialize tab state from URL query params
const activeTab = ref<'transfers' | 'rewards'>(
  (route.query.tab as 'transfers' | 'rewards') || 'transfers'
)

// Sync tab state to URL
const updateUrl = () => {
  const query: Record<string, string> = {}
  if (activeTab.value !== 'transfers') query.tab = activeTab.value
  router.push({ query })
}

watch(activeTab, updateUrl)

const { data: stats, pending, error } = await useAsyncData(
  `epoch-${epoch}`,
  () => api.getEpoch(epoch)
)

const { data: transfersByType } = await useAsyncData(
  `epoch-${epoch}-transfers`,
  () => api.getEpochTransfersByType(epoch)
)

const { data: rewards } = await useAsyncData(
  `epoch-${epoch}-rewards`,
  () => api.getEpochRewards(epoch)
)

// Fetch labels for reward contract addresses
const labelsLoaded = ref(0)
watch(rewards, async (data) => {
  if (data?.distributions?.length) {
    const addresses = data.distributions.map(d => d.contractAddress)
    await fetchLabels(addresses)
    labelsLoaded.value++ // Trigger reactivity after labels are loaded
  }
}, { immediate: true })

// Group rewards by contract (depends on labelsLoaded for reactivity)
const rewardsByContract = computed(() => {
  // Access labelsLoaded to trigger reactivity when labels are fetched
  void labelsLoaded.value
  if (!rewards.value?.distributions?.length) return []

  const grouped = new Map<string, {
    contractAddress: string
    totalAmount: number
    totalPerShare: number
    totalTransfers: number
    distributions: typeof rewards.value.distributions
  }>()

  for (const dist of rewards.value.distributions) {
    const existing = grouped.get(dist.contractAddress)
    if (existing) {
      existing.totalAmount += dist.totalAmount
      existing.totalPerShare += dist.amountPerShare
      existing.totalTransfers += dist.transferCount
      existing.distributions.push(dist)
    } else {
      grouped.set(dist.contractAddress, {
        contractAddress: dist.contractAddress,
        totalAmount: dist.totalAmount,
        totalPerShare: dist.amountPerShare,
        totalTransfers: dist.transferCount,
        distributions: [dist]
      })
    }
  }

  return Array.from(grouped.values())
})

const { formatVolume, formatDate, getLogTypeBadgeClass } = useFormatting()

const formatDuration = (startStr: string, endStr: string) => {
  const diffMs = new Date(endStr).getTime() - new Date(startStr).getTime()
  const days = Math.floor(diffMs / (1000 * 60 * 60 * 24))
  const hours = Math.floor((diffMs % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60))
  return `${days}d ${hours}h`
}
</script>

<template>
  <div class="space-y-6">
    <div v-if="pending" class="loading">Loading...</div>

    <div v-else-if="error" class="card">
      <div class="text-center py-8 text-foreground-muted">
        Epoch {{ epoch }} not found.
      </div>
    </div>

    <template v-else-if="stats">
      <!-- Epoch Header -->
      <div class="card">
        <h2 class="section-title mb-4">
          <Calendar class="h-5 w-5 text-accent" />
          Epoch {{ stats.epoch }}
        </h2>

        <div class="space-y-0">
          <div class="detail-row">
            <span class="detail-label">Tick Range</span>
            <span class="detail-value">
              <NuxtLink :to="`/ticks/${stats.firstTick}`" class="text-accent">
                {{ stats.firstTick.toLocaleString() }}
              </NuxtLink>
              -
              <NuxtLink :to="`/ticks/${stats.lastTick}`" class="text-accent">
                {{ stats.lastTick.toLocaleString() }}
              </NuxtLink>
            </span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Start Time</span>
            <span class="detail-value">{{ formatDate(stats.startTime) }}</span>
          </div>
          <div class="detail-row">
            <span class="detail-label">End Time</span>
            <span class="detail-value">{{ formatDate(stats.endTime) }}</span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Duration</span>
            <span class="detail-value">{{ formatDuration(stats.startTime, stats.endTime) }}</span>
          </div>
        </div>
      </div>

      <!-- Stats Grid -->
      <div class="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4">
        <div class="card-elevated text-center">
          <Blocks class="h-5 w-5 text-accent mx-auto mb-2" />
          <div class="text-2xl font-bold text-accent">{{ stats.tickCount.toLocaleString() }}</div>
          <div class="text-xs text-foreground-muted uppercase mt-1">Ticks</div>
        </div>

        <div class="card-elevated text-center">
          <ArrowLeftRight class="h-5 w-5 text-accent mx-auto mb-2" />
          <div class="text-2xl font-bold text-accent">{{ stats.txCount.toLocaleString() }}</div>
          <div class="text-xs text-foreground-muted uppercase mt-1">Transactions</div>
        </div>

        <div class="card-elevated text-center">
          <TrendingUp class="h-5 w-5 text-success mx-auto mb-2" />
          <div class="text-2xl font-bold text-success">{{ formatVolume(stats.totalVolume) }}</div>
          <div class="text-xs text-foreground-muted uppercase mt-1">TX Volume (QU)</div>
        </div>

        <div class="card-elevated text-center">
          <Users class="h-5 w-5 text-accent mx-auto mb-2" />
          <div class="text-2xl font-bold text-accent">{{ stats.activeAddresses.toLocaleString() }}</div>
          <div class="text-xs text-foreground-muted uppercase mt-1">Active Addresses</div>
        </div>

        <div class="card-elevated text-center">
          <Users class="h-5 w-5 text-info mx-auto mb-2" />
          <div class="text-2xl font-bold text-info">{{ stats.uniqueSenders.toLocaleString() }}</div>
          <div class="text-xs text-foreground-muted uppercase mt-1">Unique Senders</div>
        </div>

        <div class="card-elevated text-center">
          <Users class="h-5 w-5 text-info mx-auto mb-2" />
          <div class="text-2xl font-bold text-info">{{ stats.uniqueReceivers.toLocaleString() }}</div>
          <div class="text-xs text-foreground-muted uppercase mt-1">Unique Receivers</div>
        </div>

        <div class="card-elevated text-center">
          <Coins class="h-5 w-5 text-success mx-auto mb-2" />
          <div class="text-2xl font-bold text-success">{{ formatVolume(stats.quTransferred) }}</div>
          <div class="text-xs text-foreground-muted uppercase mt-1">QU Transferred</div>
        </div>

        <div class="card-elevated text-center">
          <ArrowLeftRight class="h-5 w-5 text-warning mx-auto mb-2" />
          <div class="text-2xl font-bold text-warning">{{ stats.assetTransferCount.toLocaleString() }}</div>
          <div class="text-xs text-foreground-muted uppercase mt-1">Asset Transfers</div>
        </div>
      </div>

      <!-- Activity Tabs -->
      <div class="card">
        <div class="tabs">
          <button
            :class="{ active: activeTab === 'transfers' }"
            @click="activeTab = 'transfers'"
          >
            <ArrowLeftRight class="h-4 w-4 inline mr-1" />
            Transfers by Type
          </button>
          <button
            :class="{ active: activeTab === 'rewards' }"
            @click="activeTab = 'rewards'"
          >
            <Gift class="h-4 w-4 inline mr-1" />
            SC Rewards
          </button>
        </div>

        <!-- Transfers by Type Tab -->
        <template v-if="activeTab === 'transfers'">
          <div v-if="!transfersByType?.length" class="text-center py-8 text-foreground-muted">
            No transfer data available for this epoch.
          </div>
          <div v-else class="table-wrapper">
            <table>
              <thead>
                <tr>
                  <th>Type</th>
                  <th>Count</th>
                  <th>Total Amount</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="transfer in transfersByType" :key="transfer.logType">
                  <td>
                    <span :class="['badge', getLogTypeBadgeClass(transfer.logType)]">
                      {{ transfer.logTypeName }}
                    </span>
                  </td>
                  <td>{{ transfer.count.toLocaleString() }}</td>
                  <td>{{ formatVolume(transfer.totalAmount) }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </template>

        <!-- SC Rewards Tab -->
        <template v-if="activeTab === 'rewards'">
          <div v-if="!rewardsByContract.length" class="text-center py-8 text-foreground-muted">
            No reward distributions found for this epoch.
          </div>
          <template v-else>
            <!-- Grouped by contract -->
            <div class="table-wrapper">
              <table>
                <thead>
                  <tr>
                    <th>Contract</th>
                    <th>Total Amount</th>
                    <th>Per Share</th>
                    <th>Distributions</th>
                    <th>Transfers</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="group in rewardsByContract" :key="group.contractAddress">
                    <td>
                      <AddressDisplay :address="group.contractAddress" :label="getLabel(group.contractAddress)" />
                    </td>
                    <td class="text-success font-semibold">{{ formatVolume(group.totalAmount) }} QU</td>
                    <td>{{ formatVolume(group.totalPerShare) }} QU</td>
                    <td>{{ group.distributions.length }}</td>
                    <td>{{ group.totalTransfers.toLocaleString() }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </template>
        </template>
      </div>
    </template>
  </div>
</template>
