<script setup lang="ts">
import { Landmark, ArrowUpRight, Vote, CalendarClock, Table, ExternalLink, PieChart, Users } from 'lucide-vue-next'
import { lookupCcfMeta, CCF_CATEGORY_COLORS, type CcfCategory } from '~/data/ccfMetadata'
import type { CcfProposalDto } from '~/composables/useApi'

const api = useApi()

const { data: ccfStats, status } = await useAsyncData('ccf-stats', () => api.getCcfStats())

const loading = computed(() => status.value === 'pending')

const { formatVolume, formatEpochDate, formatEpochDateShort } = useFormatting()

// Enrich transfers with metadata
const enrichedTransfers = computed(() =>
  (ccfStats.value?.transfers ?? []).map(t => ({
    ...t,
    meta: lookupCcfMeta(t.url, t.destination),
  }))
)

// Enrich proposals with metadata
const enrichProposal = (p: CcfProposalDto) => ({
  ...p,
  meta: lookupCcfMeta(p.url, p.transferDestination ?? undefined),
})

// Spending by category — uses persisted transfers if available, falls back to passed proposals
const spendingByCategory = computed(() => {
  const map = new Map<CcfCategory, number>()
  const transfers = ccfStats.value?.transfers ?? []
  if (transfers.length > 0) {
    // Use persisted transfer data (most accurate)
    for (const t of transfers) {
      if (!t.success) continue
      const meta = lookupCcfMeta(t.url, t.destination)
      const cat = meta?.category ?? 'Tech'
      map.set(cat, (map.get(cat) ?? 0) + t.amount)
    }
    for (const p of ccfStats.value?.regularPayments ?? []) {
      if (!p.success) continue
      const meta = lookupCcfMeta(p.url, p.destination)
      const cat = meta?.category ?? 'Tech'
      map.set(cat, (map.get(cat) ?? 0) + p.amount)
    }
  } else {
    // Fallback: derive from passed proposals
    for (const p of ccfStats.value?.pastProposals ?? []) {
      if (!p.passed || !p.transferAmount || !p.transferDestination) continue
      const meta = lookupCcfMeta(p.url, p.transferDestination)
      const cat = meta?.category ?? 'Tech'
      map.set(cat, (map.get(cat) ?? 0) + p.transferAmount)
    }
  }
  return [...map.entries()].sort((a, b) => b[1] - a[1])
})

const categoryChartLabels = computed(() => spendingByCategory.value.map(([cat]) => cat))
const categoryChartData = computed(() => spendingByCategory.value.map(([, v]) => v))
const categoryChartColors = computed(() =>
  spendingByCategory.value.map(([cat]) => CCF_CATEGORY_COLORS[cat] ?? 'rgba(139, 149, 168, 0.8)')
)

// Spending by recipient — same fallback logic
const spendingByRecipient = computed(() => {
  const map = new Map<string, number>()
  const transfers = ccfStats.value?.transfers ?? []
  if (transfers.length > 0) {
    for (const t of transfers) {
      if (!t.success) continue
      const meta = lookupCcfMeta(t.url, t.destination)
      const name = meta?.recipient ?? truncateAddress(t.destination)
      map.set(name, (map.get(name) ?? 0) + t.amount)
    }
    for (const p of ccfStats.value?.regularPayments ?? []) {
      if (!p.success) continue
      const meta = lookupCcfMeta(p.url, p.destination)
      const name = meta?.recipient ?? truncateAddress(p.destination)
      map.set(name, (map.get(name) ?? 0) + p.amount)
    }
  } else {
    for (const p of ccfStats.value?.pastProposals ?? []) {
      if (!p.passed || !p.transferAmount || !p.transferDestination) continue
      const meta = lookupCcfMeta(p.url, p.transferDestination)
      const name = meta?.recipient ?? truncateAddress(p.transferDestination)
      map.set(name, (map.get(name) ?? 0) + p.transferAmount)
    }
  }
  return [...map.entries()].sort((a, b) => b[1] - a[1])
})

// Epoch chart data — falls back to passed proposals when no persisted data
const spendingByEpoch = computed(() => {
  const apiData = ccfStats.value?.spendingByEpoch ?? []
  if (apiData.length > 0) return apiData

  // Derive from passed proposals
  const map = new Map<number, { epoch: number; totalSpent: number; transferCount: number }>()
  for (const p of ccfStats.value?.pastProposals ?? []) {
    if (!p.passed || !p.transferAmount) continue
    const existing = map.get(p.epoch)
    if (existing) {
      existing.totalSpent += p.transferAmount
      existing.transferCount++
    } else {
      map.set(p.epoch, { epoch: p.epoch, totalSpent: p.transferAmount, transferCount: 1 })
    }
  }
  return [...map.values()].sort((a, b) => a.epoch - b.epoch)
})

const chartLabels = computed(() =>
  spendingByEpoch.value.map(e => `E${e.epoch} (${formatEpochDateShort(e.epoch)})`)
)

const chartData = computed(() =>
  spendingByEpoch.value.map(e => e.totalSpent)
)

// Quorum constants
const QUORUM = 452
const QUORUM_HALF = Math.ceil(QUORUM / 2)

const truncateAddress = (addr: string) =>
  addr.length > 16 ? `${addr.slice(0, 8)}...${addr.slice(-8)}` : addr

const truncateUrl = (url: string) =>
  url.length > 60 ? `${url.slice(0, 57)}...` : url

const getCategoryColor = (cat: CcfCategory | undefined) =>
  cat ? CCF_CATEGORY_COLORS[cat] : 'rgba(139, 149, 168, 0.8)'
</script>

<template>
  <div class="space-y-6">
    <!-- Overview -->
    <div class="card">
      <h2 class="section-title mb-4">
        <Landmark class="h-5 w-5 text-accent" />
        CCF Overview
      </h2>

      <div v-if="loading" class="loading">Loading...</div>
      <template v-else-if="ccfStats">
        <!-- Summary cards -->
        <div class="grid grid-cols-2 md:grid-cols-5 gap-4 mb-6">
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold text-accent">{{ formatVolume(ccfStats.totalSpent) }}</div>
            <div class="text-sm text-foreground-muted">Total Spent</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold">{{ ccfStats.totalTransferCount }}</div>
            <div class="text-sm text-foreground-muted">Total Transfers</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold text-success">{{ ccfStats.activeProposals.length }}</div>
            <div class="text-sm text-foreground-muted">Active Proposals</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold text-info">{{ ccfStats.activeSubscriptions.length }}</div>
            <div class="text-sm text-foreground-muted">Active Subscriptions</div>
          </div>
          <div class="card-elevated text-center">
            <div class="text-2xl font-bold">{{ formatVolume(ccfStats.proposalFee) }}</div>
            <div class="text-sm text-foreground-muted">Proposal Fee</div>
          </div>
        </div>

        <!-- Spending by epoch chart -->
        <ClientOnly v-if="spendingByEpoch.length">
          <ChartsEpochBarChart
            :labels="chartLabels"
            :datasets="[
              {
                label: 'Spending',
                data: chartData,
                backgroundColor: 'rgba(108, 140, 204, 0.7)',
                borderColor: 'rgb(108, 140, 204)'
              }
            ]"
            :height="250"
            y-axis-label="QU"
          />
          <template #fallback>
            <div class="h-[250px] flex items-center justify-center text-foreground-muted">
              Loading chart...
            </div>
          </template>
        </ClientOnly>
        <p v-if="spendingByEpoch.length" class="text-xs text-foreground-muted mt-2">
          Total QU spent by the CCF per epoch (one-time transfers + subscription payments).
        </p>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        <Landmark class="h-12 w-12 mx-auto mb-4 opacity-50" />
        <p>No CCF data available yet.</p>
      </div>
    </div>

    <!-- Spending Breakdown: Category + Recipients -->
    <div class="card" v-if="spendingByCategory.length">
      <h2 class="section-title mb-4">
        <PieChart class="h-5 w-5 text-accent" />
        Spending Breakdown
      </h2>

      <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
        <!-- Category doughnut chart -->
        <div>
          <h3 class="text-sm font-semibold mb-3">By Category</h3>
          <ClientOnly>
            <ChartsDoughnutChart
              :labels="categoryChartLabels"
              :data="categoryChartData"
              :colors="categoryChartColors"
              :height="260"
            />
            <template #fallback>
              <div class="h-[260px] flex items-center justify-center text-foreground-muted">
                Loading chart...
              </div>
            </template>
          </ClientOnly>
        </div>

        <!-- Recipient breakdown table -->
        <div>
          <h3 class="text-sm font-semibold mb-3">By Recipient</h3>
          <div class="space-y-2 max-h-[280px] overflow-y-auto">
            <div v-for="[name, amount] in spendingByRecipient" :key="name"
              class="flex items-center justify-between text-sm">
              <span class="truncate mr-2">{{ name }}</span>
              <span class="font-mono text-accent whitespace-nowrap">{{ formatVolume(amount) }}</span>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Active Proposals -->
    <div class="card" v-if="ccfStats?.activeProposals?.length">
      <h2 class="section-title mb-4">
        <Vote class="h-5 w-5 text-accent" />
        Active Proposals
      </h2>

      <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div v-for="p in ccfStats.activeProposals.map(enrichProposal)" :key="p.proposalIndex" class="card-elevated">
          <div class="flex items-center justify-between mb-2">
            <div class="flex items-center gap-2">
              <span class="text-sm font-semibold">Proposal #{{ p.proposalIndex }}</span>
              <span v-if="p.meta?.category" class="text-[0.625rem] px-1.5 py-0.5 rounded-full font-medium"
                :style="{ backgroundColor: getCategoryColor(p.meta.category) + '33', color: getCategoryColor(p.meta.category).replace('0.8', '1') }">
                {{ p.meta.category }}
              </span>
            </div>
            <span class="text-xs px-2 py-0.5 rounded-full"
              :class="p.passed ? 'bg-success/20 text-success' : 'bg-warning/20 text-warning'">
              {{ p.passed ? 'Passing' : 'Pending' }}
            </span>
          </div>

          <div v-if="p.meta?.description" class="text-xs text-foreground-muted mb-1">
            {{ p.meta.description }}
          </div>

          <div v-if="p.url" class="text-xs text-foreground-muted mb-2 break-all">
            <a :href="p.url" target="_blank" rel="noopener" class="hover:text-accent flex items-center gap-1">
              <ExternalLink class="h-3 w-3 shrink-0" />
              {{ truncateUrl(p.url) }}
            </a>
          </div>

          <div v-if="p.transferDestination" class="text-xs mb-2">
            <span class="text-foreground-muted">To:</span>
            <span v-if="p.meta?.recipient" class="font-semibold ml-1">{{ p.meta.recipient }}</span>
            <NuxtLink :to="`/address/${p.transferDestination}`" class="font-mono text-accent ml-1">
              {{ truncateAddress(p.transferDestination) }}
            </NuxtLink>
            <span v-if="p.transferAmount" class="ml-2">{{ formatVolume(p.transferAmount) }} QU</span>
          </div>

          <!-- Voting progress -->
          <div class="space-y-1 mt-3">
            <div class="flex justify-between text-xs text-foreground-muted">
              <span>Yes: {{ p.yesVotes }}</span>
              <span>No: {{ p.noVotes }}</span>
              <span>Cast: {{ p.totalVotesCast }}/{{ p.totalVotesAuthorized }}</span>
            </div>
            <div class="h-2 rounded-full bg-background overflow-hidden flex">
              <div class="bg-success h-full" :style="{ width: `${(p.yesVotes / p.totalVotesAuthorized) * 100}%` }" />
              <div class="bg-destructive h-full" :style="{ width: `${(p.noVotes / p.totalVotesAuthorized) * 100}%` }" />
            </div>
            <div class="flex justify-between text-xs text-foreground-muted">
              <span>Quorum: {{ QUORUM }} (need {{ QUORUM_HALF }}+ yes)</span>
              <span>{{ Math.round((p.totalVotesCast / p.totalVotesAuthorized) * 100) }}% voted</span>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Active Subscriptions -->
    <div class="card" v-if="ccfStats?.activeSubscriptions?.length">
      <h2 class="section-title mb-4">
        <CalendarClock class="h-5 w-5 text-accent" />
        Active Subscriptions
      </h2>

      <div class="overflow-x-auto">
        <table class="data-table w-full">
          <thead>
            <tr>
              <th>Recipient</th>
              <th>Destination</th>
              <th class="text-right">Amount/Period</th>
              <th class="text-right">Period</th>
              <th class="text-right">Weeks/Period</th>
              <th class="text-right">Start Date</th>
              <th>URL</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(sub, i) in ccfStats.activeSubscriptions" :key="i">
              <td>
                <span class="text-sm font-medium">
                  {{ lookupCcfMeta(sub.url, sub.destination)?.recipient ?? '-' }}
                </span>
              </td>
              <td>
                <NuxtLink :to="`/address/${sub.destination}`" class="font-mono text-accent text-xs">
                  {{ truncateAddress(sub.destination) }}
                </NuxtLink>
              </td>
              <td class="text-right">{{ formatVolume(sub.amountPerPeriod) }}</td>
              <td class="text-right">{{ sub.currentPeriod >= 0 ? sub.currentPeriod : '-' }}/{{ sub.numberOfPeriods }}</td>
              <td class="text-right">{{ sub.weeksPerPeriod }}</td>
              <td class="text-right text-xs whitespace-nowrap">E{{ sub.startEpoch }} <span class="text-foreground-muted">{{ formatEpochDate(sub.startEpoch) }}</span></td>
              <td>
                <a v-if="sub.url" :href="sub.url" target="_blank" rel="noopener"
                   class="text-xs text-accent hover:underline flex items-center gap-1">
                  <ExternalLink class="h-3 w-3" />
                  Link
                </a>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- Transfer History -->
    <div class="card" v-if="enrichedTransfers.length">
      <h2 class="section-title mb-4">
        <ArrowUpRight class="h-5 w-5 text-accent" />
        Transfer History
      </h2>

      <div class="overflow-x-auto">
        <table class="data-table w-full">
          <thead>
            <tr>
              <th>Epoch</th>
              <th>Recipient</th>
              <th>Category</th>
              <th>Description</th>
              <th>Destination</th>
              <th class="text-right">Amount</th>
              <th>Status</th>
              <th>Proposal</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(t, i) in enrichedTransfers" :key="i">
              <td class="text-xs whitespace-nowrap">E{{ t.epoch }} <span class="text-foreground-muted">{{ formatEpochDate(t.epoch) }}</span></td>
              <td>
                <span class="text-sm font-medium">{{ t.meta?.recipient ?? '-' }}</span>
              </td>
              <td>
                <span v-if="t.meta?.category" class="text-[0.625rem] px-1.5 py-0.5 rounded-full font-medium whitespace-nowrap"
                  :style="{ backgroundColor: getCategoryColor(t.meta.category) + '33', color: getCategoryColor(t.meta.category).replace('0.8', '1') }">
                  {{ t.meta.category }}
                </span>
                <span v-else class="text-foreground-muted">-</span>
              </td>
              <td class="text-xs text-foreground-muted max-w-[200px] truncate">
                {{ t.meta?.description ?? '-' }}
              </td>
              <td>
                <NuxtLink :to="`/address/${t.destination}`" class="font-mono text-accent text-xs">
                  {{ truncateAddress(t.destination) }}
                </NuxtLink>
              </td>
              <td class="text-right">{{ formatVolume(t.amount) }}</td>
              <td>
                <span class="text-xs px-1.5 py-0.5 rounded"
                  :class="t.success ? 'bg-success/20 text-success' : 'bg-destructive/20 text-destructive'">
                  {{ t.success ? 'OK' : 'Failed' }}
                </span>
              </td>
              <td>
                <a v-if="t.url" :href="t.url" target="_blank" rel="noopener"
                   class="text-xs text-accent hover:underline flex items-center gap-1">
                  <ExternalLink class="h-3 w-3" />
                  Link
                </a>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- Regular Payments -->
    <div class="card" v-if="ccfStats?.regularPayments?.length">
      <h2 class="section-title mb-4">
        <CalendarClock class="h-5 w-5 text-accent" />
        Regular Payments
      </h2>

      <div class="overflow-x-auto">
        <table class="data-table w-full">
          <thead>
            <tr>
              <th>Epoch</th>
              <th>Recipient</th>
              <th>Destination</th>
              <th class="text-right">Amount</th>
              <th class="text-right">Period</th>
              <th>Status</th>
              <th>Proposal</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(p, i) in ccfStats.regularPayments" :key="i">
              <td class="text-xs whitespace-nowrap">E{{ p.epoch }} <span class="text-foreground-muted">{{ formatEpochDate(p.epoch) }}</span></td>
              <td>
                <span class="text-sm font-medium">
                  {{ lookupCcfMeta(p.url, p.destination)?.recipient ?? '-' }}
                </span>
              </td>
              <td>
                <NuxtLink :to="`/address/${p.destination}`" class="font-mono text-accent text-xs">
                  {{ truncateAddress(p.destination) }}
                </NuxtLink>
              </td>
              <td class="text-right">{{ formatVolume(p.amount) }}</td>
              <td class="text-right font-mono">{{ p.periodIndex }}</td>
              <td>
                <span class="text-xs px-1.5 py-0.5 rounded"
                  :class="p.success ? 'bg-success/20 text-success' : 'bg-destructive/20 text-destructive'">
                  {{ p.success ? 'OK' : 'Failed' }}
                </span>
              </td>
              <td>
                <a v-if="p.url" :href="p.url" target="_blank" rel="noopener"
                   class="text-xs text-accent hover:underline flex items-center gap-1">
                  <ExternalLink class="h-3 w-3" />
                  Link
                </a>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- Past Proposals -->
    <div class="card" v-if="ccfStats?.pastProposals?.length">
      <h2 class="section-title mb-4">
        <Table class="h-5 w-5 text-accent" />
        Past Proposals
      </h2>

      <div class="overflow-x-auto">
        <table class="data-table w-full">
          <thead>
            <tr>
              <th>#</th>
              <th>Epoch</th>
              <th>Recipient</th>
              <th>Category</th>
              <th>Description</th>
              <th class="text-right">Amount</th>
              <th class="text-right">Yes</th>
              <th class="text-right">No</th>
              <th>Result</th>
              <th>Proposal</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="p in ccfStats.pastProposals.map(enrichProposal)" :key="p.proposalIndex">
              <td class="font-mono">{{ p.proposalIndex }}</td>
              <td class="text-xs whitespace-nowrap">E{{ p.epoch }} <span class="text-foreground-muted">{{ formatEpochDate(p.epoch) }}</span></td>
              <td>
                <span class="text-sm font-medium">{{ p.meta?.recipient ?? '-' }}</span>
              </td>
              <td>
                <span v-if="p.meta?.category" class="text-[0.625rem] px-1.5 py-0.5 rounded-full font-medium whitespace-nowrap"
                  :style="{ backgroundColor: getCategoryColor(p.meta.category) + '33', color: getCategoryColor(p.meta.category).replace('0.8', '1') }">
                  {{ p.meta.category }}
                </span>
                <span v-else class="text-foreground-muted">-</span>
              </td>
              <td class="text-xs text-foreground-muted max-w-[200px] truncate">
                {{ p.meta?.description ?? '-' }}
              </td>
              <td class="text-right">{{ p.transferAmount ? formatVolume(p.transferAmount) : '-' }}</td>
              <td class="text-right text-success">{{ p.yesVotes }}</td>
              <td class="text-right text-destructive">{{ p.noVotes }}</td>
              <td>
                <span class="text-xs px-1.5 py-0.5 rounded"
                  :class="p.passed ? 'bg-success/20 text-success' : 'bg-destructive/20 text-destructive'">
                  {{ p.passed ? 'Passed' : 'Failed' }}
                </span>
              </td>
              <td>
                <a v-if="p.url" :href="p.url" target="_blank" rel="noopener"
                   class="text-xs text-accent hover:underline flex items-center gap-1">
                  <ExternalLink class="h-3 w-3" />
                  Link
                </a>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>
</template>
