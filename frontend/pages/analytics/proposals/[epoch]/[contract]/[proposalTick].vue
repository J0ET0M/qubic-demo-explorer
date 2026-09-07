<script setup lang="ts">
import { ArrowLeft, CheckCircle2, XCircle, Clock, ExternalLink, Ban, History } from 'lucide-vue-next'

const route = useRoute()
const api = useApi()

const epoch = Number(route.params.epoch)
const contract = Number(route.params.contract) as 6 | 8
const proposalTick = String(route.params.proposalTick)

useHead({ title: `${contract === 6 ? 'GQMPROP' : 'CCF'} Proposal · Epoch ${epoch} · Tick ${proposalTick}` })

const { data, pending } = await useAsyncData(
  () => `proposal-${epoch}-${contract}-${proposalTick}`,
  () => api.getProposalDetail(epoch, contract, proposalTick)
)

const contractName = computed(() => contract === 6 ? 'GQMPROP' : 'CCF')
const optionColors = ['#ef4444', '#22c55e', '#3b82f6', '#eab308', '#a855f7']

const shortAddr = (a: string | null | undefined): string => {
  if (!a) return ''
  return a.length > 20 ? `${a.slice(0, 6)}…${a.slice(-6)}` : a
}

const formatAmount = (amount: number): string => {
  if (contract === 6) return `${(amount / 10_000).toFixed(2)}% of revenue (${amount} millionths)`
  return `${new Intl.NumberFormat().format(amount)} QUBIC`
}

const optionLabel = (i: number): string => {
  // CCF is always YesNo: 0=No, 1=Yes.
  if (contract === 8) return i === 0 ? 'No' : i === 1 ? 'Yes' : `Option ${i}`
  // GQMPROP: option 0 = "no change"; 1..N map to amounts[0..N-1] for transfers
  if (i === 0) return 'No change'
  return `Option ${i}`
}

const statusChip = computed(() => {
  const r = data.value?.summary.result
  if (!r) return { icon: Clock, label: 'no result', color: 'text-muted' }
  if (r.isLiveTally) return { icon: Clock, label: `voting open (${r.totalCasted}/676 cast)`, color: 'text-accent' }
  if (r.isCommitted) return { icon: CheckCircle2, label: 'passed & committed', color: 'text-green-500' }
  if (r.thresholdMet) return { icon: XCircle, label: 'quorum reached, rejected', color: 'text-orange-500' }
  return { icon: XCircle, label: 'no quorum', color: 'text-red-500' }
})

// ── Vote analysis ────────────────────────────────────────────────────
// For each voter, count how many votes they cast. The 1st is a "first vote";
// each subsequent one is a "correction" (i.e. the voter changed their mind).
// The vote list is already ordered by tick asc from the API.
interface EnrichedVote {
  voterIdentity: string
  computorIndex: number | null
  owner: string | null
  tickNumber: string
  timestamp: string
  voteValue: number
  isWithdraw: boolean
  option: number
  txHash: string
  seq: number              // 1 = first vote, 2+ = correction
  previousOption: number   // what they previously voted for (-1 if this was their first)
  changedOption: boolean   // seq > 1 AND option differs from previousOption
}
const enrichedVotes = computed<EnrichedVote[]>(() => {
  const rows = data.value?.votes ?? []
  const seen: Record<string, { seq: number; lastOption: number }> = {}
  return rows.map(v => {
    const s = seen[v.voterIdentity]
    const seq = (s?.seq ?? 0) + 1
    const previousOption = s?.lastOption ?? -1
    const currentOpt = v.isWithdraw ? -1 : v.option
    const changedOption = seq > 1 && previousOption !== currentOpt
    seen[v.voterIdentity] = { seq, lastOption: currentOpt }
    return { ...v, seq, previousOption, changedOption }
  })
})

// ── Timeline: two side-by-side sub-bars per hour, each stacked by option ──
// Left sub-bar = first votes (solid), right sub-bar = corrections (hatched).
// Uses ALL votes so the correction stream is visible. Bucket = 1 hour UTC.
interface BarSegment { y: number; h: number; color: string; option: number; count: number }
interface Bar {
  x: number
  halfW: number
  firstSegs: BarSegment[]
  correctionSegs: BarSegment[]
  label: string
  dateLabel: string
  firstCount: number
  correctionCount: number
}
const bars = computed<Bar[]>(() => {
  const votes = enrichedVotes.value
  if (!votes.length) return []
  // How many option slots to track — bumped to at least 2 for CCF YesNo,
  // but wide enough to cover multi-option GQMPROP transfers.
  const optionCount = Math.max(2, data.value?.summary.optionCount ?? 2, ...votes.map(v => v.option + 1))

  interface RawBucket { date: Date; first: number[]; correction: number[] }
  const buckets = new Map<string, RawBucket>()
  for (const v of votes) {
    if (v.isWithdraw) continue  // withdrawn votes: not attributable to an option
    const d = new Date(v.timestamp)
    const bucketDate = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), d.getUTCHours()))
    const key = bucketDate.toISOString()
    let b = buckets.get(key)
    if (!b) {
      b = { date: bucketDate, first: new Array(optionCount).fill(0), correction: new Array(optionCount).fill(0) }
      buckets.set(key, b)
    }
    ;(v.seq === 1 ? b.first : b.correction)[v.option]++
  }
  const sorted = [...buckets.values()].sort((a, b) => a.date.getTime() - b.date.getTime())

  const width = 800, height = 180, padding = 32
  // Shared y-scale across sub-bars so first / correction heights are comparable.
  const maxSub = Math.max(1, ...sorted.map(b =>
    Math.max(b.first.reduce((s, x) => s + x, 0), b.correction.reduce((s, x) => s + x, 0))
  ))
  const bucketW = (width - padding * 2) / sorted.length
  const halfW = Math.max(1, bucketW / 2 - 1)
  const usable = height - padding * 2

  const stack = (counts: number[]): BarSegment[] => {
    let cursorY = height - padding
    const segs: BarSegment[] = []
    counts.forEach((c, i) => {
      if (!c) return
      const h = (c / maxSub) * usable
      cursorY -= h
      segs.push({ y: cursorY, h, color: optionColors[i % optionColors.length], option: i, count: c })
    })
    return segs
  }

  return sorted.map((b, i) => {
    const x = padding + i * bucketW
    return {
      x,
      halfW,
      firstSegs: stack(b.first),
      correctionSegs: stack(b.correction),
      label: b.date.toISOString().slice(11, 13) + 'h',
      dateLabel: b.date.toISOString().slice(0, 10),
      firstCount: b.first.reduce((s, x) => s + x, 0),
      correctionCount: b.correction.reduce((s, x) => s + x, 0),
    }
  })
})

// ── Owner matrix: latest vote per voter, grouped by owner, with "Other" collapse ──
interface OwnerRow {
  owner: string
  computorCount: number
  optionCounts: number[]
  withdrawn: number
  switched: number         // voters within this owner that changed their vote at least once
}
const OWNER_TOP_N = 12
const enrichedOwnerMatrix = computed<{ rows: OwnerRow[]; other: OwnerRow | null; optionCount: number }>(() => {
  const votes = enrichedVotes.value
  const optionCount = data.value?.ownerMatrix.optionCount ?? Math.max(2, (data.value?.summary.optionCount ?? 2))

  // Latest vote per voter + a "switched" flag
  const perVoter = new Map<string, { latest: EnrichedVote; switched: boolean }>()
  for (const v of votes) {
    const prev = perVoter.get(v.voterIdentity)
    const alreadySwitched = prev?.switched ?? false
    perVoter.set(v.voterIdentity, {
      latest: v,
      switched: alreadySwitched || v.changedOption,
    })
  }

  const byOwner = new Map<string, OwnerRow>()
  for (const { latest, switched } of perVoter.values()) {
    const key = latest.owner || latest.voterIdentity
    let row = byOwner.get(key)
    if (!row) {
      row = { owner: key, computorCount: 0, optionCounts: new Array(optionCount).fill(0), withdrawn: 0, switched: 0 }
      byOwner.set(key, row)
    }
    row.computorCount++
    if (latest.isWithdraw) row.withdrawn++
    else if (latest.option < row.optionCounts.length) row.optionCounts[latest.option]++
    if (switched) row.switched++
  }
  const all = [...byOwner.values()].sort((a, b) => b.computorCount - a.computorCount)
  if (all.length <= OWNER_TOP_N) return { rows: all, other: null, optionCount }

  const top = all.slice(0, OWNER_TOP_N)
  const rest = all.slice(OWNER_TOP_N)
  const other: OwnerRow = {
    owner: `Other (${rest.length} owner${rest.length === 1 ? '' : 's'})`,
    computorCount: rest.reduce((s, r) => s + r.computorCount, 0),
    optionCounts: new Array(optionCount).fill(0),
    withdrawn: rest.reduce((s, r) => s + r.withdrawn, 0),
    switched: rest.reduce((s, r) => s + r.switched, 0),
  }
  for (const r of rest)
    for (let i = 0; i < optionCount; i++)
      other.optionCounts[i] += r.optionCounts[i] || 0
  return { rows: top, other, optionCount }
})
</script>

<template>
  <div class="space-y-6">
    <div>
      <NuxtLink :to="`/analytics/proposals?epoch=${epoch}&contract=${contract}`" class="text-xs text-muted flex items-center gap-1 hover:text-fg">
        <ArrowLeft class="h-3 w-3" /> back to proposals · epoch {{ epoch }}
      </NuxtLink>
      <h1 class="page-title mt-2 flex items-center gap-2 flex-wrap">
        <span class="px-2 py-0.5 text-xs rounded font-medium"
          :class="contract === 6 ? 'bg-purple-500/20 text-purple-400' : 'bg-blue-500/20 text-blue-400'">
          {{ contractName }}
        </span>
        Proposal
        <span class="text-muted text-sm font-normal">at tick {{ proposalTick }}</span>
      </h1>
    </div>

    <div v-if="pending" class="card"><div class="loading py-12">Loading…</div></div>
    <div v-else-if="!data" class="card p-8 text-center text-muted">Proposal not found.</div>

    <template v-else>
      <!-- Summary card -->
      <div class="card p-5 space-y-4">
        <div class="flex items-center gap-2 flex-wrap">
          <span v-if="data.summary.isCancelled" class="px-2 py-0.5 text-xs rounded bg-red-500/20 text-red-400 flex items-center gap-1">
            <Ban class="h-3 w-3" /> Cancelled — proposer cleared their slot
          </span>
          <span v-if="data.summary.revisionCount > 1"
                class="px-2 py-0.5 text-xs rounded bg-amber-500/20 text-amber-400 flex items-center gap-1"
                :title="`First submitted ${new Date(data.summary.firstProposalTime).toLocaleString()}`">
            <History class="h-3 w-3" /> Revised {{ data.summary.revisionCount - 1 }}× since first submission
          </span>
          <template v-if="!data.summary.isCancelled">
            <component :is="statusChip.icon" class="h-5 w-5" :class="statusChip.color" />
            <span class="text-sm" :class="statusChip.color">{{ statusChip.label }}</span>
          </template>
          <span v-if="data.summary.result?.transferVerified" class="ml-auto flex items-center gap-1 text-xs text-green-500">
            <CheckCircle2 class="h-3 w-3" />
            Transfer verified · tx {{ shortAddr(data.summary.result.transferVerificationTx) }}
          </span>
        </div>

        <div>
          <div class="text-xs text-muted mb-1">Type · Class</div>
          <div class="text-sm">{{ data.summary.proposalClassName }}
            <span class="text-muted">(type 0x{{ data.summary.proposalType.toString(16).padStart(4, '0') }},
              {{ data.summary.optionCount }} options)</span>
          </div>
        </div>

        <div v-if="data.summary.url">
          <div class="text-xs text-muted mb-1">URL / description</div>
          <div class="text-sm break-all">{{ data.summary.url }}</div>
        </div>

        <div v-if="data.summary.transferDestination">
          <div class="text-xs text-muted mb-1">Transfer target</div>
          <div class="text-sm">
            <span v-if="data.summary.transferDestinationLabel" class="font-semibold">{{ data.summary.transferDestinationLabel }} · </span>
            <code>{{ data.summary.transferDestination }}</code>
          </div>
          <div class="mt-2 space-y-1">
            <div v-for="(a, i) in data.summary.transferAmounts" :key="i" class="text-sm flex items-center gap-2">
              <span class="text-xs text-muted w-20">{{ optionLabel(i + 1) }}</span>
              <span>{{ formatAmount(a) }}</span>
            </div>
          </div>
          <div v-if="data.summary.transferInEpochTargetEpoch > 0" class="mt-1 text-xs text-muted">
            Executes in epoch {{ data.summary.transferInEpochTargetEpoch }}
          </div>
        </div>

        <div v-if="data.summary.isSubscription">
          <div class="text-xs text-muted mb-1">Subscription</div>
          <div class="text-sm">
            {{ new Intl.NumberFormat().format(Number(data.summary.subscriptionAmountPerPeriod)) }} QUBIC every
            {{ data.summary.subscriptionWeeksPerPeriod }} week(s)
            × {{ data.summary.subscriptionNumberOfPeriods }} periods,
            starting epoch {{ data.summary.subscriptionStartEpoch }}
          </div>
        </div>

        <div class="grid grid-cols-2 md:grid-cols-4 gap-3 text-xs pt-3 border-t border-border">
          <div>
            <div class="text-muted">Proposer</div>
            <div class="mt-0.5">
              <div v-if="data.summary.proposerOwner" class="font-semibold text-fg">{{ data.summary.proposerOwner }}</div>
              <code class="text-xs">{{ shortAddr(data.summary.proposerIdentity) }}</code>
            </div>
          </div>
          <div>
            <div class="text-muted">Submitted</div>
            <div class="mt-0.5">{{ new Date(data.summary.proposalTime).toLocaleString() }}</div>
          </div>
          <div>
            <div class="text-muted">Tx</div>
            <div class="mt-0.5"><code>{{ shortAddr(data.summary.txHash) }}</code></div>
          </div>
          <div>
            <div class="text-muted">Contract slot</div>
            <div class="mt-0.5">
              {{ data.summary.proposalIndex >= 0 ? '#' + data.summary.proposalIndex : 'unassigned' }}
            </div>
          </div>
        </div>
      </div>

      <!-- Tally -->
      <div v-if="data.summary.result" class="card p-5">
        <div class="flex items-baseline justify-between mb-3">
          <h2 class="text-lg font-semibold">Vote tally</h2>
          <span class="text-xs text-muted">
            {{ data.summary.result.totalCasted }} / {{ data.summary.result.totalAuthorized }} computors voted
            <span v-if="data.summary.result.isLiveTally">(live)</span>
          </span>
        </div>
        <div class="space-y-2">
          <div
            v-for="(count, i) in data.summary.result.optionCounts"
            :key="i"
            class="flex items-center gap-3 text-sm"
          >
            <span class="w-24 shrink-0 text-xs" :style="{ color: optionColors[i % optionColors.length] }">
              {{ optionLabel(i) }}
            </span>
            <div class="flex-1 h-6 bg-muted/10 rounded overflow-hidden">
              <div class="h-full rounded"
                :style="{
                  width: (count / data.summary.result.totalAuthorized * 100) + '%',
                  backgroundColor: optionColors[i % optionColors.length]
                }" />
            </div>
            <span class="w-20 text-right text-xs">{{ count }} votes</span>
          </div>
        </div>
        <div class="mt-3 pt-3 border-t border-border text-xs text-muted flex gap-4">
          <span>Quorum: 451 / 676 · Majority-half: 225</span>
          <span>Winning: {{ data.summary.result.winningOption != null ? optionLabel(data.summary.result.winningOption) : '—' }}</span>
        </div>
      </div>

      <!-- Timeline: two sub-bars per hour (first / correction), each stacked by option -->
      <div v-if="bars.length" class="card p-5">
        <div class="flex items-baseline justify-between mb-3 flex-wrap gap-2">
          <h2 class="text-lg font-semibold">When did votes arrive?</h2>
          <div class="flex items-center gap-3 text-xs flex-wrap">
            <span v-for="i in Math.max(2, data.summary.optionCount || 2)" :key="i" class="flex items-center gap-1.5">
              <span class="inline-block w-3 h-3 rounded" :style="{ background: optionColors[(i - 1) % optionColors.length] }"></span>
              {{ optionLabel(i - 1) }}
            </span>
            <span class="text-muted">·</span>
            <span class="flex items-center gap-1.5 text-muted">
              <svg width="14" height="14" viewBox="0 0 14 14"><rect width="14" height="14" fill="url(#legend-hatch)"/></svg>
              hatched = correction
            </span>
          </div>
        </div>
        <div class="overflow-x-auto">
          <svg viewBox="0 0 800 200" class="w-full h-40">
            <defs>
              <!-- Diagonal-line pattern used on correction sub-bars. Rendered on
                   top of the coloured segment so option colour still shows through. -->
              <pattern id="hatch" width="4" height="4" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
                <rect width="4" height="4" fill="transparent" />
                <line x1="0" y1="0" x2="0" y2="4" stroke="rgba(255,255,255,0.55)" stroke-width="1.2" />
              </pattern>
              <pattern id="legend-hatch" width="4" height="4" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
                <rect width="4" height="4" fill="#94a3b8" />
                <line x1="0" y1="0" x2="0" y2="4" stroke="rgba(255,255,255,0.55)" stroke-width="1.2" />
              </pattern>
            </defs>

            <line x1="32" y1="148" x2="768" y2="148" stroke="currentColor" class="opacity-20" />

            <g v-for="(b, i) in bars" :key="i">
              <!-- Left sub-bar: first votes (solid colour) -->
              <g>
                <rect v-for="(seg, si) in b.firstSegs" :key="'f' + si"
                      :x="b.x" :y="seg.y" :width="b.halfW" :height="seg.h" :fill="seg.color">
                  <title>{{ b.dateLabel }} {{ b.label }} · first {{ optionLabel(seg.option) }}: {{ seg.count }}</title>
                </rect>
              </g>
              <!-- Right sub-bar: corrections (colour + hatched overlay) -->
              <g>
                <template v-for="(seg, si) in b.correctionSegs" :key="'c' + si">
                  <rect :x="b.x + b.halfW" :y="seg.y" :width="b.halfW" :height="seg.h" :fill="seg.color" />
                  <rect :x="b.x + b.halfW" :y="seg.y" :width="b.halfW" :height="seg.h" fill="url(#hatch)">
                    <title>{{ b.dateLabel }} {{ b.label }} · correction to {{ optionLabel(seg.option) }}: {{ seg.count }}</title>
                  </rect>
                </template>
              </g>
            </g>

            <g class="text-[10px] fill-current opacity-60">
              <text v-for="(b, i) in bars.filter((_, i) => i % Math.max(1, Math.floor(bars.length / 8)) === 0)"
                    :key="i" :x="b.x" :y="195" text-anchor="start">{{ b.label }}</text>
            </g>
          </svg>
        </div>
        <div class="mt-2 text-xs text-muted">
          Each hour has two sub-bars: <strong>left</strong> = first-time votes, <strong>right</strong> = corrections
          (voter changed their mind). Colours = option chosen. A tall hatched stack means many computors
          revised their vote in that hour.
        </div>
      </div>

      <!-- Owner matrix -->
      <div v-if="enrichedOwnerMatrix.rows.length" class="card p-5">
        <div class="flex items-baseline justify-between mb-3">
          <h2 class="text-lg font-semibold">Voting by owner</h2>
          <span class="text-xs text-muted">
            Latest vote per computor, grouped by owner (from fattydoge). Anonymous computors show as their identity.
            Top {{ enrichedOwnerMatrix.rows.length }} shown{{ enrichedOwnerMatrix.other ? '; rest collapsed into “Other”' : '' }}.
          </span>
        </div>
        <div class="overflow-x-auto">
          <table class="w-full text-sm">
            <thead>
              <tr class="text-xs text-muted text-left">
                <th class="py-2 pr-4">Owner</th>
                <th class="py-2 pr-4 text-right">Computors</th>
                <th
                  v-for="i in enrichedOwnerMatrix.optionCount"
                  :key="i"
                  class="py-2 pr-4 text-right"
                  :style="{ color: optionColors[(i - 1) % optionColors.length] }"
                >{{ optionLabel(i - 1) }}</th>
                <th class="py-2 pr-4 text-right text-muted">Withdrawn</th>
                <th class="py-2 pr-4 text-right text-orange-500">Changed</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="row in enrichedOwnerMatrix.rows" :key="row.owner" class="border-t border-border">
                <td class="py-2 pr-4">
                  <code v-if="row.owner.length > 30" class="text-xs">{{ shortAddr(row.owner) }}</code>
                  <span v-else class="font-semibold">{{ row.owner }}</span>
                </td>
                <td class="py-2 pr-4 text-right">{{ row.computorCount }}</td>
                <td v-for="(c, ci) in row.optionCounts.slice(0, enrichedOwnerMatrix.optionCount)" :key="ci" class="py-2 pr-4 text-right">
                  <span v-if="c > 0" class="font-semibold" :style="{ color: optionColors[ci % optionColors.length] }">{{ c }}</span>
                  <span v-else class="text-muted">·</span>
                </td>
                <td class="py-2 pr-4 text-right text-muted">{{ row.withdrawn || '·' }}</td>
                <td class="py-2 pr-4 text-right">
                  <span v-if="row.switched > 0" class="text-orange-500 font-semibold">{{ row.switched }}</span>
                  <span v-else class="text-muted">·</span>
                </td>
              </tr>
              <tr v-if="enrichedOwnerMatrix.other" class="border-t border-border bg-muted/5">
                <td class="py-2 pr-4 italic text-muted">{{ enrichedOwnerMatrix.other.owner }}</td>
                <td class="py-2 pr-4 text-right">{{ enrichedOwnerMatrix.other.computorCount }}</td>
                <td v-for="(c, ci) in enrichedOwnerMatrix.other.optionCounts.slice(0, enrichedOwnerMatrix.optionCount)" :key="ci" class="py-2 pr-4 text-right">
                  <span v-if="c > 0" :style="{ color: optionColors[ci % optionColors.length] }">{{ c }}</span>
                  <span v-else class="text-muted">·</span>
                </td>
                <td class="py-2 pr-4 text-right text-muted">{{ enrichedOwnerMatrix.other.withdrawn || '·' }}</td>
                <td class="py-2 pr-4 text-right">
                  <span v-if="enrichedOwnerMatrix.other.switched > 0" class="text-orange-500">{{ enrichedOwnerMatrix.other.switched }}</span>
                  <span v-else class="text-muted">·</span>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
        <div class="mt-2 text-xs text-muted">
          <span class="text-orange-500 font-semibold">Changed</span> = number of computors under this owner
          that revised their vote at least once (e.g. switched Yes→No).
        </div>
      </div>

      <!-- Vote list -->
      <details class="card p-5">
        <summary class="cursor-pointer flex items-center justify-between">
          <h2 class="text-lg font-semibold">All votes ({{ enrichedVotes.length }})</h2>
          <ExternalLink class="h-4 w-4 text-muted" />
        </summary>
        <div class="mt-3 overflow-x-auto">
          <table class="w-full text-xs">
            <thead>
              <tr class="text-muted text-left">
                <th class="py-1 pr-3">#</th>
                <th class="py-1 pr-3">Time</th>
                <th class="py-1 pr-3">Tick</th>
                <th class="py-1 pr-3">Voter</th>
                <th class="py-1 pr-3">Owner</th>
                <th class="py-1 pr-3">Vote</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="v in enrichedVotes" :key="v.txHash"
                  class="border-t border-border"
                  :class="v.seq > 1 ? 'bg-orange-500/5' : ''">
                <td class="py-1 pr-3">
                  <span v-if="v.seq === 1" class="text-muted">1st</span>
                  <span v-else class="text-orange-500 font-semibold">#{{ v.seq }}</span>
                </td>
                <td class="py-1 pr-3">{{ new Date(v.timestamp).toISOString().slice(0, 19).replace('T', ' ') }}</td>
                <td class="py-1 pr-3">{{ v.tickNumber }}</td>
                <td class="py-1 pr-3">
                  <code>{{ shortAddr(v.voterIdentity) }}</code>
                  <span v-if="v.computorIndex != null" class="text-muted"> · c#{{ v.computorIndex }}</span>
                </td>
                <td class="py-1 pr-3">{{ v.owner || '—' }}</td>
                <td class="py-1 pr-3">
                  <span v-if="v.isWithdraw" class="text-muted italic">withdrawn</span>
                  <span v-else :style="{ color: optionColors[v.option % optionColors.length] }" class="font-semibold">
                    {{ optionLabel(v.option) }}
                  </span>
                  <span v-if="v.changedOption" class="ml-2 text-[10px] text-orange-500">
                    (was {{ v.previousOption === -1 ? 'withdrawn' : optionLabel(v.previousOption) }})
                  </span>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </details>
    </template>
  </div>
</template>
