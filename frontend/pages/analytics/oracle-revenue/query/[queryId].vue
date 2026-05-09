<script setup lang="ts">
import { Database, ArrowLeft } from 'lucide-vue-next'

const route = useRoute()
const router = useRouter()
const api = useApi()

const goBack = () => {
  if (window.history.length > 1) router.back()
  else router.push(`/analytics/oracle-revenue?epoch=${epochParam.value}`)
}

const queryIdParam = computed(() => String(route.params.queryId))
const epochParam = computed(() => {
  const v = Number(route.query.epoch)
  return Number.isFinite(v) && v > 0 ? v : 0
})

// queryId encoding: ((tick << 31) | txIndex). Use BigInt — values can exceed 2^53.
const queryTickFromId = (queryId: string | number | bigint): number => {
  try { return Number(BigInt(queryId) >> 31n) } catch { return 0 }
}
const queryTxIndexFromId = (queryId: string | number | bigint): number => {
  try { return Number(BigInt(queryId) & 0x7FFFFFFFn) } catch { return 0 }
}

useHead({ title: () => `Oracle Query ${queryIdParam.value} - Analytics` })

const { data: queryDetail, pending } = await useAsyncData(
  () => `oracle-query-${epochParam.value}-${queryIdParam.value}`,
  () => api.getOracleQueryDetail(epochParam.value, queryIdParam.value),
  { watch: [queryIdParam, epochParam] }
)

interface DetailHeatmapCell {
  computorIndex: number
  rank: number | null
  isInQuorum: boolean
  commitTick: number | null
  color: string
  link: string | null
}

const detailComputorRank = computed(() => {
  const m = new Map<number, NonNullable<typeof queryDetail.value>['computors'][number]>()
  if (!queryDetail.value) return m
  for (const c of queryDetail.value.computors) m.set(c.computorIndex, c)
  return m
})

const detailHeatmap = computed((): DetailHeatmapCell[] => {
  const cells: DetailHeatmapCell[] = []
  if (!queryDetail.value) return cells
  const map = detailComputorRank.value
  const total = queryDetail.value.computors.filter(c => c.rank !== null).length || 1

  for (let i = 0; i < 676; i++) {
    const e = map.get(i)
    if (!e || e.rank === null) {
      cells.push({
        computorIndex: i, rank: null, isInQuorum: false,
        commitTick: null, color: 'rgb(40, 44, 56)', link: null
      })
      continue
    }
    const intensity = (e.rank - 1) / total
    const r = Math.round(intensity * 255)
    const g = Math.round((1 - intensity) * 200 + 55)
    const b = 30
    cells.push({
      computorIndex: i,
      rank: e.rank,
      isInQuorum: e.isInQuorum,
      commitTick: e.commitTick,
      color: `rgb(${r}, ${g}, ${b})`,
      link: e.commitTxHash ? `/tx/${e.commitTxHash}` : null
    })
  }
  return cells
})

const detailHover = ref<{ idx: number; rank: number | null; tick: number | null; inQuorum: boolean } | null>(null)

const computorHref = (idx: number) =>
  `/analytics/oracle-revenue/computor/${idx}?epoch=${epochParam.value}`
</script>

<template>
  <div class="space-y-6">
    <div class="flex flex-wrap items-center gap-3">
      <button
        type="button"
        @click="goBack"
        class="inline-flex items-center gap-1 px-2 py-1 text-xs rounded bg-surface-elevated hover:bg-surface-hover text-foreground"
      >
        <ArrowLeft class="h-3 w-3" /> Back
      </button>
      <h1 class="page-title flex items-center gap-2">
        <Database class="h-5 w-5 text-accent" />
        Query Detail
      </h1>
    </div>

    <div class="card">
      <div class="text-sm text-foreground-muted">
        Query ID:
        <span class="font-mono text-foreground">{{ queryIdParam }}</span>
        <span class="ml-2">
          (T<span class="font-mono">{{ queryTickFromId(queryIdParam).toLocaleString() }}</span>,
          tx#<span class="font-mono">{{ queryTxIndexFromId(queryIdParam) }}</span>)
        </span>
        <span class="ml-2">· Epoch <span class="font-mono">{{ epochParam }}</span></span>
      </div>
    </div>

    <div v-if="pending" class="card">
      <div class="loading py-12">Loading query detail...</div>
    </div>

    <template v-else-if="queryDetail">
      <div class="card">
        <div class="grid grid-cols-2 md:grid-cols-4 gap-2 mb-4">
          <div class="rounded p-2 bg-surface-elevated">
            <div class="text-xs text-foreground-muted">First commit</div>
            <div class="font-mono">{{ queryDetail.firstCommitTick.toLocaleString() }}</div>
          </div>
          <div class="rounded p-2 bg-surface-elevated">
            <div class="text-xs text-foreground-muted">Quorum cutoff</div>
            <div class="font-mono">
              <span v-if="queryDetail.quorumCutoffTick > 0">{{ queryDetail.quorumCutoffTick.toLocaleString() }}</span>
              <span v-else class="text-foreground-muted italic">not reached</span>
            </div>
          </div>
          <div class="rounded p-2 bg-surface-elevated">
            <div class="text-xs text-foreground-muted">Commits / In-quorum</div>
            <div class="font-mono">
              {{ queryDetail.totalCommits }}
              <span class="text-foreground-muted">/</span>
              <span class="text-success">{{ queryDetail.commitsInQuorum }}</span>
            </div>
          </div>
          <div class="rounded p-2 bg-surface-elevated">
            <div class="text-xs text-foreground-muted">Reveals</div>
            <div class="font-mono">{{ queryDetail.totalReveals }}</div>
          </div>
        </div>

        <div v-if="!queryDetail.rawEventsAvailable" class="rounded p-3 bg-warning/10 border border-warning/30 text-sm">
          Raw events for epoch {{ queryDetail.epoch }} have been pruned — only summary data is available.
        </div>

        <template v-else>
          <h3 class="text-sm font-semibold mb-2">Commit timing</h3>
          <ClientOnly>
            <ChartsEpochBarChart
              :labels="queryDetail.tickHistogram.map(h => h.tick.toLocaleString())"
              :datasets="[{
                label: 'Commits at this tick',
                data: queryDetail.tickHistogram.map(h => h.commitCount),
                backgroundColor: 'rgba(99, 102, 241, 0.7)'
              }]"
              :height="220"
            />
            <template #fallback>
              <div class="h-[220px] flex items-center justify-center text-foreground-muted">Loading chart...</div>
            </template>
          </ClientOnly>

          <div class="grid grid-cols-1 xl:grid-cols-[1fr_minmax(280px,360px)] gap-6 items-start mt-4">
            <div class="min-w-0">
              <h3 class="text-sm font-semibold mb-2">Computors (color = commit rank)</h3>
              <div class="text-xs text-foreground-muted mb-2 h-5">
                <template v-if="detailHover && detailHover.rank !== null">
                  <span class="font-mono">#{{ detailHover.idx }}: rank {{ detailHover.rank }} · tick {{ detailHover.tick?.toLocaleString() }}</span>
                  <span v-if="detailHover.inQuorum" class="ml-2 px-1.5 py-0.5 rounded bg-success/15 text-success text-[10px]">in quorum</span>
                  <span v-else class="ml-2 px-1.5 py-0.5 rounded bg-destructive/15 text-destructive text-[10px]">missed</span>
                </template>
                <template v-else-if="detailHover">
                  <span class="font-mono italic text-foreground-muted">#{{ detailHover.idx }}: no commit</span>
                </template>
                <span v-else>Hover for details · click for the commit transaction.</span>
              </div>
              <div
                class="grid gap-[2px] w-full"
                style="grid-template-columns: repeat(38, minmax(0, 1fr));"
                @mouseleave="detailHover = null"
              >
                <template v-for="cell in detailHeatmap" :key="cell.computorIndex">
                  <NuxtLink
                    v-if="cell.link"
                    :to="cell.link"
                    class="aspect-square rounded-sm cursor-pointer hover:ring-2 hover:ring-accent transition-all"
                    :style="{
                      backgroundColor: cell.color,
                      boxShadow: cell.isInQuorum ? 'inset 0 0 0 1px rgba(16,185,129,0.7)' : 'inset 0 0 0 1px rgba(239,68,68,0.5)'
                    }"
                    @mouseenter="detailHover = { idx: cell.computorIndex, rank: cell.rank, tick: cell.commitTick, inQuorum: cell.isInQuorum }"
                  />
                  <div
                    v-else
                    class="aspect-square rounded-sm cursor-help"
                    :style="{ backgroundColor: cell.color }"
                    @mouseenter="detailHover = { idx: cell.computorIndex, rank: null, tick: null, inQuorum: false }"
                  />
                </template>
              </div>
              <p class="text-xs text-foreground-muted mt-3">
                Color = commit rank (green = early winner, red = late). Border green = in quorum, red = missed. Grey = no commit.
              </p>
            </div>

            <div class="min-w-0 max-h-[480px] overflow-y-auto">
              <h3 class="text-sm font-semibold mb-2">Top 50 by rank</h3>
              <div class="space-y-1 text-xs">
                <div
                  v-for="c in queryDetail.computors.filter(c => c.rank !== null).slice(0, 50)"
                  :key="c.computorIndex"
                  class="flex items-center justify-between gap-2 p-1.5 rounded bg-surface-elevated"
                >
                  <NuxtLink :to="computorHref(c.computorIndex)" class="font-mono text-accent hover:underline">
                    #{{ c.rank }} → {{ c.computorIndex }}
                  </NuxtLink>
                  <span class="font-mono">T{{ c.commitTick?.toLocaleString() }}</span>
                  <span v-if="c.isInQuorum" class="text-success text-[10px]">✓</span>
                  <span v-else class="text-destructive text-[10px]">✗</span>
                  <NuxtLink
                    v-if="c.commitTxHash"
                    :to="`/tx/${c.commitTxHash}`"
                    class="text-accent hover:underline text-[10px]"
                  >
                    tx
                  </NuxtLink>
                </div>
              </div>
            </div>
          </div>
        </template>
      </div>
    </template>

    <div v-else class="card">
      <div class="text-center py-8 text-foreground-muted">Query not found.</div>
    </div>
  </div>
</template>
