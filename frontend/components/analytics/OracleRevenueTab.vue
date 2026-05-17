<script setup lang="ts">
import { Trophy, ListChecks } from 'lucide-vue-next'

const api = useApi()
const route = useRoute()
const router = useRouter()

// queryId encoding: ((tick << 31) | txIndex)
// Use BigInt because queryId can exceed 2^53 (JS safe-int boundary).
const queryTickFromId = (queryId: string | number | bigint): number => {
  try { return Number(BigInt(queryId) >> 31n) } catch { return 0 }
}

const { data: epochCountdown } = await useAsyncData(
  'oracle-epoch-countdown',
  () => api.getEpochCountdown()
)

// Allow ?epoch=N in the URL so back-links from detail pages reopen the same epoch.
const initialEpoch = (() => {
  const v = Number(route.query.epoch)
  if (Number.isFinite(v) && v > 0) return v
  return epochCountdown.value?.currentEpoch ?? 211
})()
const selectedEpoch = ref<number>(initialEpoch)

watch(selectedEpoch, (v) => {
  // Keep URL in sync so refresh / back-link returns to this epoch
  router.replace({ query: { ...route.query, epoch: String(v) } })
})

// Tabs: leaderboard (default) | queries
type OracleTab = 'leaderboard' | 'queries'
const activeTab = ref<OracleTab>(
  (route.query.view as OracleTab) === 'queries' ? 'queries' : 'leaderboard'
)
const switchTab = (t: OracleTab) => {
  activeTab.value = t
  router.replace({
    query: { ...route.query, view: t === 'leaderboard' ? undefined : t }
  })
}

const onEpochChange = (e: Event) => {
  const v = Number((e.target as HTMLInputElement).value)
  if (!Number.isNaN(v) && v > 0) selectedEpoch.value = v
}

// ── Epoch summary (leaderboard) ──
const { data: epochSummary, pending: summaryLoading, refresh: refreshSummary } = await useAsyncData(
  () => `oracle-epoch-${selectedEpoch.value}`,
  () => api.getOracleEpochSummary(selectedEpoch.value),
  { watch: [selectedEpoch] }
)

// Heatmap colored by estimatedPoints
interface HeatmapCell {
  computorIndex: number
  points: number
  color: string
}

const heatmapData = computed((): HeatmapCell[] => {
  const data = epochSummary.value
  if (!data?.computors.length) return []
  const byIdx = new Map<number, number>()
  for (const c of data.computors) byIdx.set(c.computorIndex, c.estimatedPoints)
  const allPoints = data.computors.map(c => c.estimatedPoints)
  const max = Math.max(...allPoints, 1)
  const min = Math.min(...allPoints)
  const range = max - min || 1

  const cells: HeatmapCell[] = []
  for (let i = 0; i < 676; i++) {
    const pts = byIdx.get(i) ?? 0
    if (pts === 0) {
      cells.push({ computorIndex: i, points: 0, color: 'rgb(40, 44, 56)' })
    } else {
      const intensity = (pts - min) / range
      // green (low points) → yellow → red (max points) — winners are red/hot
      const r = Math.round(intensity * 255)
      const g = Math.round((1 - Math.abs(intensity - 0.5) * 2) * 200 + 55)
      const b = Math.round((1 - intensity) * 80)
      cells.push({ computorIndex: i, points: pts, color: `rgb(${r}, ${g}, ${b})` })
    }
  }
  return cells
})

const heatmapHover = ref<{ computorIndex: number; points: number } | null>(null)

const heatmapStats = computed(() => {
  const data = epochSummary.value
  if (!data?.computors.length) return null
  const pts = data.computors.map(c => c.estimatedPoints)
  return {
    min: Math.min(...pts),
    max: Math.max(...pts),
    avg: Math.round(pts.reduce((a, b) => a + b, 0) / pts.length),
    active: data.computors.filter(c => c.estimatedPoints > 0).length
  }
})

// Leaderboard table sorting
const leaderboardSort = ref<'points' | 'factor' | 'commits' | 'reveals' | 'avgOffset' | 'index'>('factor')
const leaderboardAsc = ref(false)
const sortedLeaderboard = computed(() => {
  if (!epochSummary.value?.computors) return []
  const arr = [...epochSummary.value.computors]
  arr.sort((a, b) => {
    const dir = leaderboardAsc.value ? 1 : -1
    switch (leaderboardSort.value) {
      case 'points': return (a.estimatedPoints - b.estimatedPoints) * dir
      case 'factor': {
        const cmp = (a.oracleFactor - b.oracleFactor) * dir
        if (cmp !== 0) return cmp
        // Tie-break by points so the table is stable when many computors are at S=1024
        return (a.estimatedPoints - b.estimatedPoints) * dir
      }
      case 'commits': return (a.commits - b.commits) * dir
      case 'reveals': return (a.reveals - b.reveals) * dir
      case 'avgOffset': return (a.avgTickOffset - b.avgTickOffset) * dir
      case 'index': return (a.computorIndex - b.computorIndex) * dir
    }
    return 0
  })
  return arr
})

const setSort = (key: typeof leaderboardSort.value) => {
  if (leaderboardSort.value === key) leaderboardAsc.value = !leaderboardAsc.value
  else { leaderboardSort.value = key; leaderboardAsc.value = key !== 'avgOffset' ? false : true }
}

const sortIndicator = (key: typeof leaderboardSort.value) =>
  leaderboardSort.value === key ? (leaderboardAsc.value ? ' ↑' : ' ↓') : ''

// ── Per-query view ──
const queryListPage = ref(0)
const queryListPageSize = 50
const queryListOffset = computed(() => queryListPage.value * queryListPageSize)

const { data: queryList, pending: queryListLoading } = await useAsyncData(
  () => `oracle-queries-${selectedEpoch.value}-${queryListPage.value}`,
  () => api.getOracleQueryList(selectedEpoch.value, queryListPageSize, queryListOffset.value),
  { watch: [selectedEpoch, queryListPage] }
)

const queryListTotalPages = computed(() =>
  queryList.value ? Math.ceil(queryList.value.totalCount / queryListPageSize) : 0
)

watch(selectedEpoch, () => { queryListPage.value = 0 })

const queryHref = (queryId: string | number | bigint) =>
  `/analytics/oracle-revenue/query/${queryId.toString()}?epoch=${selectedEpoch.value}`
const computorHref = (idx: number) =>
  `/analytics/oracle-revenue/computor/${idx}?epoch=${selectedEpoch.value}`
</script>

<template>
  <div class="space-y-6">
    <!-- Epoch selector -->
    <div class="card">
      <div class="flex flex-wrap items-center justify-between gap-3">
        <div class="flex items-center gap-2">
          <label class="text-xs text-foreground-muted">Epoch:</label>
          <input
            type="number"
            :value="selectedEpoch"
            @change="onEpochChange"
            min="138"
            class="w-24 px-2 py-1 bg-surface-elevated border border-border rounded text-sm text-foreground"
          />
          <button
            @click="refreshSummary()"
            class="px-2 py-1 text-xs rounded bg-surface-elevated hover:bg-surface-hover text-foreground"
          >
            Refresh
          </button>
        </div>
        <div v-if="epochSummary" class="text-xs text-foreground-muted flex items-center gap-2">
          <span v-if="epochSummary.dataFromAggregates" class="px-1.5 py-0.5 rounded bg-success/15 text-success">aggregated</span>
          <span v-else class="px-1.5 py-0.5 rounded bg-info/15 text-info">live</span>
          {{ epochSummary.totalQueries.toLocaleString() }} queries ·
          {{ epochSummary.totalCommits.toLocaleString() }} commits ·
          {{ epochSummary.totalReveals.toLocaleString() }} reveals
        </div>
      </div>
    </div>

    <!-- Tabs -->
    <div class="card">
      <div class="tabs mb-4">
        <button
          :class="{ active: activeTab === 'leaderboard' }"
          @click="switchTab('leaderboard')"
        >
          <Trophy class="h-4 w-4 inline mr-1" />
          Leaderboard
        </button>
        <button
          :class="{ active: activeTab === 'queries' }"
          @click="switchTab('queries')"
        >
          <ListChecks class="h-4 w-4 inline mr-1" />
          Queries this epoch
        </button>
      </div>

    <!-- Leaderboard tab -->
    <div v-if="activeTab === 'leaderboard'">
      <p class="text-xs text-foreground-muted mb-4">
        Each computor's <strong>estimated points</strong> in this epoch — i.e. queries where their commit landed within the in-quorum cutoff (first 451 commits + ties at the 451st tick). Click a row to drill into that computor.
        Note: this is an upper bound — the core also requires K12 knowledge proof to count, which we don't replicate.
      </p>

      <div v-if="summaryLoading" class="loading py-8">Loading...</div>
      <template v-else-if="epochSummary?.computors.length">
        <!-- Heatmap (676 cells) -->
        <div class="grid grid-cols-1 xl:grid-cols-[1fr_minmax(280px,360px)] gap-6 items-start">
          <div class="min-w-0">
            <div class="text-xs text-foreground-muted mb-2 h-5">
              <template v-if="heatmapHover">
                <span class="text-foreground font-mono">Computor #{{ heatmapHover.computorIndex }}:</span>
                <span class="ml-1 font-mono">{{ heatmapHover.points.toLocaleString() }} pts</span>
              </template>
              <span v-else>Hover a cell for details · click for the computor's profile.</span>
            </div>
            <div
              class="grid gap-[2px] w-full"
              style="grid-template-columns: repeat(38, minmax(0, 1fr));"
              @mouseleave="heatmapHover = null"
            >
              <NuxtLink
                v-for="cell in heatmapData"
                :key="cell.computorIndex"
                :to="computorHref(cell.computorIndex)"
                class="aspect-square rounded-sm cursor-pointer hover:ring-2 hover:ring-accent transition-all"
                :style="{ backgroundColor: cell.color }"
                @mouseenter="heatmapHover = { computorIndex: cell.computorIndex, points: cell.points }"
              />
            </div>
            <p class="text-xs text-foreground-muted mt-3">
              Color = estimated points (green→red, red = top scorers). Grey = no commits this epoch.
            </p>
          </div>

          <!-- Stats panel -->
          <div v-if="heatmapStats" class="min-w-0 space-y-2 text-sm">
            <div class="rounded p-3 bg-surface-elevated">
              <div class="text-xs text-foreground-muted uppercase mb-1">Top score</div>
              <div class="text-2xl font-bold text-success">{{ heatmapStats.max.toLocaleString() }}</div>
            </div>
            <div class="rounded p-3 bg-surface-elevated">
              <div class="text-xs text-foreground-muted uppercase mb-1">Average</div>
              <div class="text-xl font-semibold">{{ heatmapStats.avg.toLocaleString() }}</div>
            </div>
            <div class="rounded p-3 bg-surface-elevated">
              <div class="text-xs text-foreground-muted uppercase mb-1">Min</div>
              <div class="text-xl font-semibold text-destructive">{{ heatmapStats.min.toLocaleString() }}</div>
            </div>
            <div class="rounded p-3 bg-surface-elevated">
              <div class="text-xs text-foreground-muted uppercase mb-1">Active computors</div>
              <div class="text-xl font-semibold">{{ heatmapStats.active }} / 676</div>
            </div>
          </div>
        </div>

        <!-- Leaderboard table -->
        <div class="table-wrapper mt-6">
          <table>
            <thead>
              <tr>
                <th class="cursor-pointer select-none" @click="setSort('index')">#{{ sortIndicator('index') }}</th>
                <th
                  class="cursor-pointer select-none text-right"
                  @click="setSort('factor')"
                  title="V2 oracle revenue factor (0..1024). 1024 = at-or-above the rank-451 quorum threshold; otherwise proportional. Mirrors qubic core."
                >Factor{{ sortIndicator('factor') }}</th>
                <th class="cursor-pointer select-none text-right" @click="setSort('points')">Est. Points{{ sortIndicator('points') }}</th>
                <th class="cursor-pointer select-none text-right" @click="setSort('commits')">Commits{{ sortIndicator('commits') }}</th>
                <th class="cursor-pointer select-none text-right" @click="setSort('reveals')">Reveals{{ sortIndicator('reveals') }}</th>
                <th class="text-right">Win rate</th>
                <th class="cursor-pointer select-none text-right" @click="setSort('avgOffset')">Avg ticks late{{ sortIndicator('avgOffset') }}</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="c in sortedLeaderboard.slice(0, 100)" :key="c.computorIndex">
                <td>{{ c.computorIndex }}</td>
                <td class="text-right font-mono">
                  <span :class="c.oracleFactor === 1024 ? 'text-success font-bold' : c.oracleFactor === 0 ? 'text-foreground-muted' : 'text-foreground'">
                    {{ c.oracleFactor.toLocaleString() }}
                  </span>
                  <span class="text-foreground-muted text-xs ml-1">/ 1024</span>
                </td>
                <td class="text-right font-bold">{{ c.estimatedPoints.toLocaleString() }}</td>
                <td class="text-right">{{ c.commits.toLocaleString() }}</td>
                <td class="text-right">{{ c.reveals.toLocaleString() }}</td>
                <td class="text-right text-foreground-muted">
                  {{ c.participations === 0 ? '—' : ((c.estimatedPoints / c.participations) * 100).toFixed(1) + '%' }}
                </td>
                <td class="text-right text-foreground-muted">{{ c.avgTickOffset.toFixed(2) }}</td>
                <td class="text-right">
                  <NuxtLink
                    :to="computorHref(c.computorIndex)"
                    class="px-2 py-0.5 text-xs rounded bg-surface-elevated hover:bg-surface-hover text-foreground inline-block"
                  >
                    Profile
                  </NuxtLink>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
        <p v-if="sortedLeaderboard.length > 100" class="text-xs text-foreground-muted mt-2">
          Showing top 100 of {{ sortedLeaderboard.length }} computors.
        </p>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        No oracle activity in epoch {{ selectedEpoch }} yet.
      </div>
    </div>

    <!-- Queries tab -->
    <div v-if="activeTab === 'queries'">
      <div v-if="queryListLoading" class="loading py-8">Loading queries...</div>
      <template v-else-if="queryList?.items.length">
        <div class="text-xs text-foreground-muted mb-2">
          Page {{ queryListPage + 1 }} / {{ queryListTotalPages }} · {{ queryList.totalCount.toLocaleString() }} queries total
          <span v-if="queryList.dataFromAggregates" class="px-1.5 py-0.5 rounded bg-success/15 text-success ml-2">aggregated</span>
          <span v-else class="px-1.5 py-0.5 rounded bg-info/15 text-info ml-2">live</span>
        </div>
        <div class="table-wrapper">
          <table>
            <thead>
              <tr>
                <th>Query ID</th>
                <th class="text-right">First commit tick</th>
                <th class="text-right">Cutoff tick</th>
                <th class="text-right">Commits</th>
                <th class="text-right">In quorum</th>
                <th class="text-right">Reveals</th>
                <th class="text-right">Unique committors</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="q in queryList.items" :key="q.queryId.toString()">
                <td class="font-mono text-xs">
                  {{ q.queryId.toString() }}
                  <span class="text-foreground-muted ml-1">
                    (T{{ queryTickFromId(q.queryId).toLocaleString() }})
                  </span>
                </td>
                <td class="text-right">{{ q.firstCommitTick.toLocaleString() }}</td>
                <td class="text-right">
                  <span v-if="q.quorumCutoffTick > 0">{{ q.quorumCutoffTick.toLocaleString() }}</span>
                  <span v-else class="text-foreground-muted italic">no quorum</span>
                </td>
                <td class="text-right">{{ q.totalCommits.toLocaleString() }}</td>
                <td class="text-right text-success">{{ q.commitsInQuorum.toLocaleString() }}</td>
                <td class="text-right">{{ q.totalReveals.toLocaleString() }}</td>
                <td class="text-right text-foreground-muted">{{ q.uniqueCommittors.toLocaleString() }}</td>
                <td class="text-right">
                  <NuxtLink
                    :to="queryHref(q.queryId)"
                    class="px-2 py-0.5 text-xs rounded bg-surface-elevated hover:bg-surface-hover text-foreground inline-block"
                  >
                    Detail
                  </NuxtLink>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
        <div class="flex items-center gap-2 mt-3">
          <button
            @click="queryListPage = Math.max(0, queryListPage - 1)"
            :disabled="queryListPage === 0"
            class="px-2 py-1 text-xs rounded bg-surface-elevated hover:bg-surface-hover text-foreground disabled:opacity-40"
          >
            ← Prev
          </button>
          <button
            @click="queryListPage++"
            :disabled="queryListPage + 1 >= queryListTotalPages"
            class="px-2 py-1 text-xs rounded bg-surface-elevated hover:bg-surface-hover text-foreground disabled:opacity-40"
          >
            Next →
          </button>
        </div>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">No queries.</div>
    </div>
    </div>
  </div>
</template>
