<script setup lang="ts">
import { User, ArrowLeft } from 'lucide-vue-next'

const route = useRoute()
const router = useRouter()
const api = useApi()

// Prefer browser history so cross-navigation (e.g. query-detail → computor-profile)
// returns to the previous page rather than the overview. Falls back to overview
// when there's no in-app history (direct link / fresh tab).
const goBack = () => {
  if (window.history.length > 1) router.back()
  else router.push(`/analytics/oracle-revenue?epoch=${epochParam.value}`)
}

const computorIndex = computed(() => {
  const v = Number(route.params.computorIndex)
  return Number.isFinite(v) && v >= 0 && v < 676 ? v : 0
})
const epochParam = computed(() => {
  const v = Number(route.query.epoch)
  return Number.isFinite(v) && v > 0 ? v : 0
})

const queryTickFromId = (queryId: string | number | bigint): number => {
  try { return Number(BigInt(queryId) >> 31n) } catch { return 0 }
}

const { formatDateTime } = useFormatting()

useHead({ title: () => `Computor #${computorIndex.value} Oracle Profile - Analytics` })

const PAGE_SIZE = 50
const page = ref(0)
const offset = computed(() => page.value * PAGE_SIZE)

const { data: profile, pending } = await useAsyncData(
  () => `oracle-computor-${epochParam.value}-${computorIndex.value}-${page.value}`,
  () => api.getOracleComputorProfile(epochParam.value, computorIndex.value, PAGE_SIZE, offset.value),
  { watch: [computorIndex, epochParam, page] }
)

const totalPages = computed(() =>
  profile.value ? Math.ceil(profile.value.totalQueries / PAGE_SIZE) : 0
)

const queryHref = (queryId: string | number) =>
  `/analytics/oracle-revenue/query/${queryId}?epoch=${epochParam.value}`

watch([computorIndex, epochParam], () => { page.value = 0 })
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
        <User class="h-5 w-5 text-accent" />
        Computor #{{ computorIndex }}
      </h1>
    </div>

    <div class="card">
      <div class="text-sm text-foreground-muted">
        Computor index: <span class="font-mono text-foreground">{{ computorIndex }}</span>
        <span class="ml-2">· Epoch <span class="font-mono">{{ epochParam }}</span></span>
      </div>
    </div>

    <div v-if="pending && !profile" class="card">
      <div class="loading py-12">Loading profile...</div>
    </div>

    <template v-else-if="profile">
      <div class="card">
        <div class="grid grid-cols-2 md:grid-cols-5 gap-2 mb-4">
          <div class="rounded p-2 bg-surface-elevated">
            <div class="text-xs text-foreground-muted">Estimated points</div>
            <div class="font-mono text-success font-bold text-lg">{{ profile.estimatedPoints.toLocaleString() }}</div>
          </div>
          <div class="rounded p-2 bg-surface-elevated">
            <div class="text-xs text-foreground-muted">Participations</div>
            <div class="font-mono">{{ profile.participations.toLocaleString() }}</div>
          </div>
          <div class="rounded p-2 bg-surface-elevated">
            <div class="text-xs text-foreground-muted">Win rate</div>
            <div class="font-mono">
              {{ profile.participations === 0
                  ? '—'
                  : ((profile.estimatedPoints / profile.participations) * 100).toFixed(1) + '%' }}
            </div>
          </div>
          <div class="rounded p-2 bg-surface-elevated">
            <div class="text-xs text-foreground-muted">Avg ticks late</div>
            <div class="font-mono">{{ profile.avgTickOffset.toFixed(2) }}</div>
          </div>
          <div class="rounded p-2 bg-surface-elevated">
            <div class="text-xs text-foreground-muted">Commits / Reveals</div>
            <div class="font-mono">{{ profile.commits }} / {{ profile.reveals }}</div>
          </div>
        </div>

        <div v-if="!profile.rawEventsAvailable" class="rounded p-3 bg-warning/10 border border-warning/30 text-sm">
          Raw events for epoch {{ profile.epoch }} have been pruned — per-query breakdown not available.
        </div>
        <template v-else-if="profile.totalQueries > 0">
          <div class="flex items-center justify-between flex-wrap gap-2 mb-2">
            <h3 class="text-sm font-semibold">Per-query performance</h3>
            <div class="text-xs text-foreground-muted">
              Page {{ page + 1 }} / {{ totalPages || 1 }} ·
              {{ profile.totalQueries.toLocaleString() }} queries total
            </div>
          </div>
          <div class="table-wrapper">
            <table>
              <thead>
                <tr>
                  <th>Query ID</th>
                  <th class="text-right">Commit tick</th>
                  <th>Time</th>
                  <th class="text-right">Rank</th>
                  <th class="text-right">Ticks late</th>
                  <th>In quorum</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="q in profile.queries" :key="q.queryId.toString()">
                  <td class="font-mono text-xs">
                    <NuxtLink :to="queryHref(q.queryId)" class="text-accent hover:underline">
                      {{ q.queryId.toString() }}
                    </NuxtLink>
                    <span class="text-foreground-muted ml-1">
                      (T{{ queryTickFromId(q.queryId).toLocaleString() }})
                    </span>
                  </td>
                  <td class="text-right">{{ q.commitTick.toLocaleString() }}</td>
                  <td class="text-foreground-muted text-xs">{{ formatDateTime(q.commitTimestamp) }}</td>
                  <td class="text-right">{{ q.rank }}</td>
                  <td class="text-right">{{ q.ticksAfterFirst }}</td>
                  <td>
                    <span v-if="q.isInQuorum" class="text-success text-xs">✓ in quorum</span>
                    <span v-else class="text-destructive text-xs">✗ missed</span>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
          <div class="flex items-center gap-2 mt-3">
            <button
              @click="page = Math.max(0, page - 1)"
              :disabled="page === 0 || pending"
              class="px-2 py-1 text-xs rounded bg-surface-elevated hover:bg-surface-hover text-foreground disabled:opacity-40"
            >
              ← Prev
            </button>
            <button
              @click="page++"
              :disabled="page + 1 >= totalPages || pending"
              class="px-2 py-1 text-xs rounded bg-surface-elevated hover:bg-surface-hover text-foreground disabled:opacity-40"
            >
              Next →
            </button>
            <span v-if="pending" class="text-xs text-foreground-muted">Loading...</span>
          </div>
        </template>
        <div v-else class="text-center py-6 text-foreground-muted text-sm">
          No commits from this computor in epoch {{ profile.epoch }}.
        </div>
      </div>
    </template>

    <div v-else class="card">
      <div class="text-center py-8 text-foreground-muted">Profile not found.</div>
    </div>
  </div>
</template>
