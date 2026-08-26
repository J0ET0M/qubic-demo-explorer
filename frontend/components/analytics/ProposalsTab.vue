<script setup lang="ts">
import { CheckCircle2, XCircle, Clock, ExternalLink, ArrowRight, Ban, History } from 'lucide-vue-next'

const api = useApi()
const route = useRoute()
const router = useRouter()

const { data: epochsData } = await useAsyncData('proposals-epochs', () => api.getProposalsEpochs())
const { data: countdown } = await useAsyncData('proposals-countdown', () => api.getEpochCountdown())

// Initial state from URL, else the current epoch. If the current epoch has no
// proposals yet the list will just be empty — the selector still shows every
// epoch that ever had activity so the user can switch.
type ContractFilter = 0 | 6 | 8
const initialEpoch = (() => {
  const q = Number(route.query.epoch)
  if (Number.isFinite(q) && q > 0) return q
  return countdown.value?.currentEpoch ?? epochsData.value?.epochs?.[0] ?? 1
})()
const selectedEpoch = ref<number>(initialEpoch)
const contractFilter = ref<ContractFilter>((Number(route.query.contract) as ContractFilter) || 0)

// Epoch selector union: current epoch + every epoch that has proposals. Sorted desc.
const epochOptions = computed<number[]>(() => {
  const set = new Set<number>()
  const cur = countdown.value?.currentEpoch
  if (cur) set.add(cur)
  for (const e of (epochsData.value?.epochs ?? [])) set.add(e)
  // Also always allow the currently-selected one so we don't drop it if it's stale
  set.add(selectedEpoch.value)
  return [...set].sort((a, b) => b - a)
})

watch([selectedEpoch, contractFilter], ([e, c]) => {
  router.replace({ query: { ...route.query, epoch: String(e), contract: c === 0 ? undefined : String(c) } })
})

const { data: proposals, pending, refresh } = await useAsyncData(
  () => `proposals-${selectedEpoch.value}-${contractFilter.value}`,
  () => api.getProposalsForEpoch(selectedEpoch.value, contractFilter.value),
  { watch: [selectedEpoch, contractFilter] }
)

const formatAmount = (amount: number, contract: number): string => {
  // GQMPROP transfers use millionths of revenue (0..1_000_000 = 0..100%).
  // CCF transfers use raw qus.
  if (contract === 6) return `${(amount / 10_000).toFixed(2)}%`
  return new Intl.NumberFormat().format(amount) + ' QUBIC'
}

const shortAddr = (a: string | null | undefined): string => {
  if (!a) return ''
  return a.length > 20 ? `${a.slice(0, 6)}…${a.slice(-6)}` : a
}

const proposalHref = (p: { epoch: number; contractIndex: number; proposalTick: string }) =>
  `/analytics/proposals/${p.epoch}/${p.contractIndex}/${p.proposalTick}`

const statusChip = (result: { totalCasted: number; thresholdMet: boolean; isCommitted: boolean; isLiveTally: boolean } | null) => {
  if (!result) return { icon: Clock, label: 'no result', color: 'text-muted' }
  if (result.isLiveTally) return { icon: Clock, label: `${result.totalCasted}/676 votes (open)`, color: 'text-accent' }
  if (result.isCommitted) return { icon: CheckCircle2, label: 'committed', color: 'text-green-500' }
  if (result.thresholdMet) return { icon: XCircle, label: 'quorum reached, rejected', color: 'text-orange-500' }
  return { icon: XCircle, label: `${result.totalCasted}/451 (no quorum)`, color: 'text-red-500' }
}

const barSegments = (result: { optionCounts: number[]; totalAuthorized: number } | null) => {
  if (!result) return []
  const total = result.totalAuthorized || 676
  const colors = ['#ef4444', '#22c55e', '#3b82f6', '#eab308', '#a855f7']
  const segs: { pct: number; color: string; label: string; count: number }[] = []
  result.optionCounts.forEach((c, i) => {
    if (c > 0) segs.push({ pct: (c / total) * 100, color: colors[i % colors.length], label: `Option ${i}`, count: c })
  })
  return segs
}
</script>

<template>
  <div class="space-y-4">
    <!-- Controls -->
    <div class="card p-4 flex flex-wrap items-center gap-4">
      <div>
        <label class="text-xs text-muted block mb-1">Epoch</label>
        <select
          v-model.number="selectedEpoch"
          class="bg-background border border-border rounded px-3 py-1.5 text-sm"
        >
          <option v-for="e in epochOptions" :key="e" :value="e">
            {{ e }}{{ e === countdown?.currentEpoch ? ' (current)' : '' }}
          </option>
        </select>
      </div>
      <div>
        <label class="text-xs text-muted block mb-1">Contract</label>
        <div class="flex gap-1">
          <button
            v-for="opt in [{ v: 0, l: 'Both' }, { v: 6, l: 'GQMPROP' }, { v: 8, l: 'CCF' }]"
            :key="opt.v"
            @click="contractFilter = opt.v as ContractFilter"
            class="px-3 py-1.5 text-sm rounded border"
            :class="contractFilter === opt.v
              ? 'bg-accent/20 border-accent text-accent'
              : 'border-border text-muted hover:text-fg'"
          >{{ opt.l }}</button>
        </div>
      </div>
      <div class="ml-auto text-xs text-muted">
        {{ proposals?.totalCount ?? 0 }} proposal(s)
      </div>
    </div>

    <!-- Loading -->
    <div v-if="pending" class="card"><div class="loading py-12">Loading proposals…</div></div>

    <!-- Empty -->
    <div v-else-if="!proposals?.items?.length" class="card p-8 text-center text-muted">
      No proposals found in epoch {{ selectedEpoch }}
      {{ contractFilter === 6 ? 'for GQMPROP' : contractFilter === 8 ? 'for CCF' : '' }}.
    </div>

    <!-- List -->
    <div v-else class="space-y-3">
      <NuxtLink
        v-for="p in proposals.items"
        :key="p.contractIndex + '-' + p.proposalTick"
        :to="proposalHref(p)"
        class="card p-4 hover:border-accent transition-colors block"
      >
        <div class="flex items-start justify-between gap-4">
          <div class="flex-1 min-w-0">
            <div class="flex flex-wrap items-center gap-2">
              <span class="px-2 py-0.5 text-xs rounded font-medium"
                :class="p.contractIndex === 6 ? 'bg-purple-500/20 text-purple-400' : 'bg-blue-500/20 text-blue-400'">
                {{ p.contractName }}
              </span>
              <span class="px-2 py-0.5 text-xs rounded bg-muted/20 text-muted">
                {{ p.proposalClassName }}<span v-if="p.optionCount > 0"> · {{ p.optionCount }} opts</span>
              </span>
              <span v-if="p.isSubscription" class="px-2 py-0.5 text-xs rounded bg-green-500/20 text-green-400">
                Subscription
              </span>
              <span v-if="p.isCancelled" class="px-2 py-0.5 text-xs rounded bg-red-500/20 text-red-400 flex items-center gap-1">
                <Ban class="h-3 w-3" /> Cancelled by proposer
              </span>
              <span v-if="p.revisionCount > 1" class="px-2 py-0.5 text-xs rounded bg-amber-500/20 text-amber-400 flex items-center gap-1"
                    :title="`First submitted ${new Date(p.firstProposalTime).toLocaleString()}; ${p.revisionCount - 1} subsequent edit(s)`">
                <History class="h-3 w-3" /> Revised {{ p.revisionCount - 1 }}×
              </span>
              <template v-if="p.result && !p.isCancelled">
                <component :is="statusChip(p.result).icon" class="h-4 w-4" :class="statusChip(p.result).color" />
                <span class="text-xs" :class="statusChip(p.result).color">{{ statusChip(p.result).label }}</span>
              </template>
            </div>

            <div class="mt-2 text-sm">
              <div v-if="p.url" class="text-muted truncate">{{ p.url }}</div>

              <div v-if="p.transferDestination" class="mt-1 flex items-center gap-1 text-xs">
                <span class="text-muted">Transfer to</span>
                <code class="text-accent">{{ p.transferDestinationLabel || shortAddr(p.transferDestination) }}</code>
                <ArrowRight class="h-3 w-3 text-muted" />
                <span>
                  {{ p.transferAmounts.map(a => formatAmount(a, p.contractIndex)).join(' / ') }}
                </span>
                <span v-if="p.transferInEpochTargetEpoch > 0" class="text-muted">
                  · in epoch {{ p.transferInEpochTargetEpoch }}
                </span>
              </div>

              <div v-if="p.isSubscription" class="mt-1 text-xs text-muted">
                Every {{ p.subscriptionWeeksPerPeriod }} week(s),
                {{ new Intl.NumberFormat().format(Number(p.subscriptionAmountPerPeriod)) }} QUBIC ×
                {{ p.subscriptionNumberOfPeriods }} periods,
                starting epoch {{ p.subscriptionStartEpoch }}
              </div>

              <div class="mt-2 flex items-center gap-3 text-xs text-muted">
                <span>Proposer: <code class="text-fg/80">{{ p.proposerOwner || shortAddr(p.proposerIdentity) }}</code></span>
                <span>Tick {{ p.proposalTick }}</span>
                <span>{{ new Date(p.proposalTime).toLocaleString() }}</span>
              </div>
            </div>

            <!-- Result bar -->
            <div v-if="p.result" class="mt-3">
              <div class="w-full h-2 rounded overflow-hidden bg-muted/20 flex">
                <div v-for="(seg, i) in barSegments(p.result)" :key="i"
                     :style="{ width: seg.pct + '%', backgroundColor: seg.color }"
                     :title="`${seg.label}: ${seg.count} votes`" />
              </div>
              <div class="mt-1 flex flex-wrap gap-3 text-xs text-muted">
                <span v-for="(seg, i) in barSegments(p.result)" :key="i">
                  <span class="inline-block w-2 h-2 rounded mr-1" :style="{ backgroundColor: seg.color }"></span>
                  {{ seg.label }}: {{ seg.count }}
                </span>
                <span v-if="p.result.transferVerified" class="text-green-500 flex items-center gap-1">
                  <CheckCircle2 class="h-3 w-3" />
                  Transfer verified on-chain
                </span>
              </div>
            </div>
          </div>
          <ExternalLink class="h-4 w-4 text-muted shrink-0" />
        </div>
      </NuxtLink>
    </div>
  </div>
</template>
