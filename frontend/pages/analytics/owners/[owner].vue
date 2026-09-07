<script setup lang="ts">
import { Users, ArrowLeft, Info } from 'lucide-vue-next'

const api = useApi()
const route = useRoute()
const router = useRouter()
const { formatAmount, formatVolume, truncateAddress } = useFormatting()

const owner = computed(() => decodeURIComponent(route.params.owner as string))

const epochParam = computed(() => {
  const v = route.query.epoch
  if (typeof v !== 'string') return undefined
  const n = parseInt(v, 10)
  return Number.isFinite(n) && n > 0 ? n : undefined
})

const { data: epochList } = await useAsyncData('owner-detail-epochs', () => api.getOwnerEpochs())

// We need the epoch to call /owners/{epoch}/{owner}. If no ?epoch=, resolve
// the current epoch by hitting the by-owner endpoint first (it returns the
// epoch it ran against), then drill into the owner.
const { data: byOwnerData } = await useAsyncData(
  () => `owner-overview-${epochParam.value ?? 'current'}`,
  () => api.getComputorRevenueByOwner(epochParam.value),
  { watch: [epochParam] }
)

const resolvedEpoch = computed(() => epochParam.value ?? byOwnerData.value?.epoch)

const { data: detail, status } = await useAsyncData(
  () => `owner-${resolvedEpoch.value}-${owner.value}`,
  () => resolvedEpoch.value
    ? api.getOwnerDetail(resolvedEpoch.value, owner.value)
    : Promise.resolve(null),
  { watch: [resolvedEpoch, owner] }
)

useHead(() => ({ title: `${owner.value} · Owner` }))

const loading = computed(() => status.value === 'pending')

// Find the same owner in the by-owner roll-up so we can show the
// network-share fields (which the drill-down endpoint doesn't carry).
const ownerSummary = computed(() =>
  byOwnerData.value?.owners?.find(o => o.owner === owner.value)
)

// Sort the per-computor table
type SortKey = 'computorIndex' | 'revenue' | 'miningFactor' | 'dogePoints' | 'qubicSolutions'
const sortKey = ref<SortKey>('revenue')
const sortAsc = ref(false)

function toggleSort(key: SortKey) {
  if (sortKey.value === key) sortAsc.value = !sortAsc.value
  else { sortKey.value = key; sortAsc.value = false }
}

const sortedComputors = computed(() => {
  const list = detail.value?.computors?.slice() ?? []
  list.sort((a, b) => {
    const av = a[sortKey.value]
    const bv = b[sortKey.value]
    return sortAsc.value ? av - bv : bv - av
  })
  return list
})

const formatPercent = (n: number) => `${n.toFixed(2)}%`

// Epoch dropdown binding
const selectedEpoch = computed<string>({
  get: () => (epochParam.value !== undefined ? String(epochParam.value) : ''),
  set: (val: string) => {
    const query = { ...route.query }
    if (val) query.epoch = val
    else delete query.epoch
    router.push({ query })
  },
})
</script>

<template>
  <div class="space-y-6">
    <div>
      <NuxtLink to="/analytics/owners" class="inline-flex items-center gap-1 text-xs text-foreground-muted hover:text-accent mb-2">
        <ArrowLeft class="h-3 w-3" /> All owners
      </NuxtLink>
      <div class="flex items-start justify-between gap-4 flex-wrap">
        <h1 class="page-title flex items-center gap-2">
          <Users class="h-5 w-5 text-accent" />
          {{ owner }}
          <span v-if="resolvedEpoch" class="text-sm font-normal text-foreground-muted">
            — epoch {{ resolvedEpoch }}
          </span>
        </h1>

        <div class="flex items-center gap-2">
          <label class="text-xs text-foreground-muted">Epoch:</label>
          <select
            v-model="selectedEpoch"
            class="input w-auto h-8"
          >
            <option value="">Current</option>
            <option
              v-for="e in epochList ?? []"
              :key="e"
              :value="String(e)"
            >{{ e }}</option>
          </select>
        </div>
      </div>
    </div>

    <!-- Attribution + accuracy disclaimer -->
    <div class="card bg-accent/5 border-accent/30 flex items-start gap-3">
      <Info class="h-4 w-4 text-accent shrink-0 mt-0.5" />
      <div class="text-xs leading-relaxed space-y-1">
        <p>
          Owner labels sourced from
          <a href="https://fattydoge.top/" target="_blank" rel="noopener" class="text-accent underline">
            fattydoge.top
          </a>
          — all credit for the labelling work goes to him. We mirror his data per epoch.
        </p>
        <p class="text-foreground-muted">
          Ownership mapping is <strong>approximate</strong> (heuristics + self-declaration). Treat as
          directional, not authoritative.
        </p>
      </div>
    </div>

    <div v-if="loading" class="card">
      <div class="loading py-12">Loading owner data…</div>
    </div>

    <div v-else-if="!detail" class="card">
      <div class="text-center py-12 text-foreground-muted">
        Owner <span class="font-mono">{{ owner }}</span> not found for this epoch.
      </div>
    </div>

    <template v-else>
      <!-- Headline tiles -->
      <div class="grid grid-cols-2 md:grid-cols-5 gap-3">
        <div class="card">
          <div class="text-xs text-foreground-muted mb-1">Computors owned</div>
          <div class="text-lg font-semibold">{{ detail.computorCount }}</div>
          <div v-if="ownerSummary" class="text-xs text-foreground-muted mt-1">
            {{ ownerSummary.computorsWithDogeMining }} actively mining DOGE
          </div>
        </div>
        <div class="card">
          <div class="text-xs text-foreground-muted mb-1">Total revenue</div>
          <div class="text-lg font-semibold">{{ formatVolume(detail.totalRevenue) }} QU</div>
        </div>
        <div class="card">
          <div class="text-xs text-foreground-muted mb-1">Avg revenue / computor</div>
          <div class="text-lg font-semibold">{{ formatAmount(Math.round(detail.avgRevenue)) }} QU</div>
        </div>
        <div class="card">
          <div class="text-xs text-foreground-muted mb-1">Qubic mining</div>
          <div class="text-lg font-semibold">
            <template v-if="ownerSummary">{{ formatPercent(ownerSummary.qubicSolutionsPercent) }}</template>
            <template v-else>—</template>
          </div>
          <div v-if="ownerSummary" class="text-xs text-foreground-muted mt-1">
            {{ formatVolume(ownerSummary.qubicSolutions) }} solutions
          </div>
        </div>
        <div class="card">
          <div class="text-xs text-foreground-muted mb-1">DOGE mining</div>
          <div class="text-lg font-semibold">
            <template v-if="ownerSummary">{{ formatPercent(ownerSummary.dogeParticipationPercent) }}</template>
            <template v-else>—</template>
          </div>
          <div v-if="ownerSummary" class="text-xs text-foreground-muted mt-1">
            {{ formatVolume(ownerSummary.dogePoints) }} DOGE points
          </div>
        </div>
      </div>

      <!-- Per-computor table -->
      <div class="card">
        <h2 class="text-sm font-semibold mb-3">Owned computors</h2>
        <div class="overflow-x-auto">
          <table class="w-full text-sm">
            <thead>
              <tr class="text-left border-b border-border-muted text-xs uppercase text-foreground-muted">
                <th class="py-2 px-2 cursor-pointer" @click="toggleSort('computorIndex')">
                  Index
                </th>
                <th class="py-2 px-2">Address</th>
                <th class="py-2 px-2 text-right cursor-pointer" @click="toggleSort('revenue')">
                  Revenue
                </th>
                <th class="py-2 px-2 text-right cursor-pointer" @click="toggleSort('miningFactor')">
                  Mining factor
                </th>
                <th class="py-2 px-2 text-right cursor-pointer" @click="toggleSort('qubicSolutions')">
                  Qubic solutions
                </th>
                <th class="py-2 px-2 text-right cursor-pointer" @click="toggleSort('dogePoints')">
                  DOGE points
                </th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="c in sortedComputors"
                :key="c.computorIndex"
                class="border-b border-border-muted hover:bg-bg-elevated/40 transition-colors"
              >
                <td class="py-2 px-2 tabular-nums">{{ c.computorIndex }}</td>
                <td class="py-2 px-2">
                  <NuxtLink
                    :to="`/address/${c.address}`"
                    class="font-mono text-xs text-accent hover:underline"
                  >
                    {{ truncateAddress(c.address) }}
                  </NuxtLink>
                </td>
                <td class="py-2 px-2 text-right tabular-nums">{{ formatAmount(c.revenue) }}</td>
                <td class="py-2 px-2 text-right tabular-nums">
                  {{ c.miningFactor }}
                  <span class="text-xs text-foreground-muted">/ 1024</span>
                </td>
                <td class="py-2 px-2 text-right tabular-nums">{{ formatAmount(c.qubicSolutions) }}</td>
                <td class="py-2 px-2 text-right tabular-nums">{{ formatAmount(c.dogePoints) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </template>
  </div>
</template>
