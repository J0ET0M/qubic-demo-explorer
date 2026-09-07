<script setup lang="ts">
import { Users, ArrowUpDown, Info } from 'lucide-vue-next'

useHead({ title: 'Owners' })

const api = useApi()
const router = useRouter()
const route = useRoute()
const { formatAmount, formatVolume } = useFormatting()

// Epoch from ?epoch=, empty = current
const epochParam = computed(() => {
  const v = route.query.epoch
  if (typeof v !== 'string') return undefined
  const n = parseInt(v, 10)
  return Number.isFinite(n) && n > 0 ? n : undefined
})

// Epochs we actually have owner-summary data for. Driven by the server so
// the user can't pick an empty epoch.
const { data: epochList } = await useAsyncData('owners-epochs', () => api.getOwnerEpochs())

const { data, status } = await useAsyncData(
  () => `owners-${epochParam.value ?? 'current'}`,
  () => api.getComputorRevenueByOwner(epochParam.value),
  { watch: [epochParam] }
)

const loading = computed(() => status.value === 'pending')

// Bind dropdown to URL — empty string = use current epoch
const selectedEpoch = computed<string>({
  get: () => (epochParam.value !== undefined ? String(epochParam.value) : ''),
  set: (val: string) => {
    const query = { ...route.query }
    if (val) query.epoch = val
    else delete query.epoch
    router.push({ query })
  },
})

// Sort state
type SortKey = 'totalRevenue' | 'avgRevenue' | 'computorCount' | 'dogeParticipationPercent' | 'dogePoints' | 'qubicSolutions' | 'qubicSolutionsPercent' | 'owner'
const sortKey = ref<SortKey>('dogeParticipationPercent')
const sortAsc = ref(false)

function toggleSort(key: SortKey) {
  if (sortKey.value === key) sortAsc.value = !sortAsc.value
  else { sortKey.value = key; sortAsc.value = false }
}

const sortedOwners = computed(() => {
  const list = data.value?.owners?.slice() ?? []
  list.sort((a, b) => {
    const av = a[sortKey.value] as number | string
    const bv = b[sortKey.value] as number | string
    if (typeof av === 'string' && typeof bv === 'string') {
      return sortAsc.value ? av.localeCompare(bv) : bv.localeCompare(av)
    }
    return sortAsc.value ? (av as number) - (bv as number) : (bv as number) - (av as number)
  })
  return list
})

// Participation bar widths — normalized against the largest in this epoch
// so small operators are still visible (instead of being squashed).
const maxDogePct = computed(() => {
  if (!data.value?.owners?.length) return 0
  return Math.max(...data.value.owners.map(o => o.dogeParticipationPercent))
})
const maxQubicPct = computed(() => {
  if (!data.value?.owners?.length) return 0
  return Math.max(...data.value.owners.map(o => o.qubicSolutionsPercent))
})

const dogeBarWidth = (pct: number) => maxDogePct.value > 0
  ? Math.max(2, Math.round((pct / maxDogePct.value) * 100))
  : 0
const qubicBarWidth = (pct: number) => maxQubicPct.value > 0
  ? Math.max(2, Math.round((pct / maxQubicPct.value) * 100))
  : 0

const formatPercent = (n: number) => `${n.toFixed(2)}%`
</script>

<template>
  <div class="space-y-6">
    <div class="flex items-start justify-between gap-4 flex-wrap">
      <h1 class="page-title flex items-center gap-2">
        <Users class="h-5 w-5 text-accent" />
        Owners
        <span v-if="data?.epoch" class="text-sm font-normal text-foreground-muted">
          — epoch {{ data.epoch }}
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

    <!-- Attribution + accuracy disclaimer -->
    <div class="card bg-accent/5 border-accent/30 flex items-start gap-3">
      <Info class="h-4 w-4 text-accent shrink-0 mt-0.5" />
      <div class="text-xs leading-relaxed space-y-1">
        <p>
          Owner labels are sourced from
          <a href="https://fattydoge.top/" target="_blank" rel="noopener" class="text-accent underline">
            fattydoge.top
          </a>
          — all credit for the labelling work goes to him.
        </p>
        <p class="text-foreground-muted">
          The ownership mapping is <strong>approximate</strong>. Operators are identified heuristically
          (deposit patterns, peer self-declaration, etc.) and may be wrong or missing for some computors,
          and can change between epochs. Use the figures as a directional signal, not as ground truth.
        </p>
      </div>
    </div>

    <p class="text-sm text-foreground-muted max-w-3xl">
      Computor operators aggregated by total revenue and DOGE custom-mining participation for the selected epoch.
    </p>

    <!-- Headline tiles -->
    <div v-if="data" class="grid grid-cols-2 md:grid-cols-5 gap-3">
      <div class="card">
        <div class="text-xs text-foreground-muted mb-1">Distinct owners</div>
        <div class="text-lg font-semibold">{{ data.ownerCount }}</div>
      </div>
      <div class="card">
        <div class="text-xs text-foreground-muted mb-1">Computors mapped</div>
        <div class="text-lg font-semibold">
          {{ data.computorsWithOwner }} / {{ data.computorsWithOwner + data.computorsWithoutOwner }}
        </div>
      </div>
      <div class="card">
        <div class="text-xs text-foreground-muted mb-1">Attributed revenue</div>
        <div class="text-lg font-semibold">{{ formatVolume(data.totalAttributedRevenue) }} QU</div>
      </div>
      <div class="card">
        <div class="text-xs text-foreground-muted mb-1">Qubic solutions</div>
        <div class="text-lg font-semibold">{{ formatVolume(data.totalQubicSolutions) }}</div>
      </div>
      <div class="card">
        <div class="text-xs text-foreground-muted mb-1">DOGE points</div>
        <div class="text-lg font-semibold">{{ formatVolume(data.totalDogePoints) }}</div>
      </div>
    </div>

    <!-- Owners table -->
    <div class="card">
      <div v-if="loading" class="loading py-12">Loading owner data…</div>

      <div v-else-if="!data?.owners?.length" class="text-center py-12 text-foreground-muted">
        No owner data available yet. The fattydoge sync runs on the analytics cycle (~4 h).
      </div>

      <div v-else class="overflow-x-auto">
        <table class="w-full text-sm">
          <thead>
            <tr class="text-left border-b border-border-muted text-xs uppercase text-foreground-muted">
              <th class="py-2 px-2 cursor-pointer" @click="toggleSort('owner')">
                <span class="inline-flex items-center gap-1">Owner <ArrowUpDown class="h-3 w-3" /></span>
              </th>
              <th
                class="py-2 px-2 text-right cursor-pointer"
                @click="toggleSort('computorCount')"
                title="Computors owned this epoch — and in parens the subset actively submitting DOGE shares (miningFactor > 0)"
              >
                <span class="inline-flex items-center gap-1">Owned (mining) <ArrowUpDown class="h-3 w-3" /></span>
              </th>
              <th class="py-2 px-2 text-right cursor-pointer" @click="toggleSort('totalRevenue')">
                <span class="inline-flex items-center gap-1">Total revenue <ArrowUpDown class="h-3 w-3" /></span>
              </th>
              <th class="py-2 px-2 text-right cursor-pointer" @click="toggleSort('avgRevenue')">
                <span class="inline-flex items-center gap-1">Avg revenue <ArrowUpDown class="h-3 w-3" /></span>
              </th>
              <!-- Qubic mining = puzzle solutions (input_type=2 txs to computor) -->
              <th
                class="py-2 px-2 text-right cursor-pointer"
                @click="toggleSort('qubicSolutions')"
                title="Number of solution transactions (input_type=2) directed to this owner's computors"
              >
                <span class="inline-flex items-center gap-1">Qubic solutions <ArrowUpDown class="h-3 w-3" /></span>
              </th>
              <th class="py-2 px-2 cursor-pointer" @click="toggleSort('qubicSolutionsPercent')">
                <span class="inline-flex items-center gap-1">Qubic share <ArrowUpDown class="h-3 w-3" /></span>
              </th>
              <!-- DOGE mining = merged-mining shares (raw MiningScore) -->
              <th
                class="py-2 px-2 text-right cursor-pointer"
                @click="toggleSort('dogePoints')"
                title="DOGE merged-mining points (raw share count) summed across the owner's computors"
              >
                <span class="inline-flex items-center gap-1">DOGE points <ArrowUpDown class="h-3 w-3" /></span>
              </th>
              <th class="py-2 px-2 cursor-pointer" @click="toggleSort('dogeParticipationPercent')">
                <span class="inline-flex items-center gap-1">DOGE share <ArrowUpDown class="h-3 w-3" /></span>
              </th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="o in sortedOwners"
              :key="o.owner"
              class="border-b border-border-muted hover:bg-bg-elevated/40 transition-colors"
              :class="{ 'opacity-70': o.owner === 'Unknown' }"
            >
              <td class="py-2 px-2">
                <NuxtLink
                  :to="`/analytics/owners/${encodeURIComponent(o.owner)}${data?.epoch ? `?epoch=${data.epoch}` : ''}`"
                  class="font-medium hover:underline"
                  :class="o.owner === 'Unknown' ? 'text-foreground-muted italic' : 'text-accent'"
                >
                  {{ o.owner }}
                </NuxtLink>
              </td>
              <td class="py-2 px-2 text-right">
                {{ o.computorCount }}
                <span class="text-xs text-foreground-muted ml-1">
                  ({{ o.computorsWithDogeMining }} mining)
                </span>
              </td>
              <td class="py-2 px-2 text-right tabular-nums">{{ formatAmount(o.totalRevenue) }}</td>
              <td class="py-2 px-2 text-right tabular-nums">{{ formatAmount(Math.round(o.avgRevenue)) }}</td>
              <td class="py-2 px-2 text-right tabular-nums">{{ formatVolume(o.qubicSolutions) }}</td>
              <td class="py-2 px-2">
                <div class="flex items-center gap-2 min-w-[140px]">
                  <div class="flex-1 h-1.5 bg-bg-elevated rounded-full overflow-hidden">
                    <div
                      class="h-full bg-accent-light"
                      :style="{ width: `${qubicBarWidth(o.qubicSolutionsPercent)}%` }"
                    ></div>
                  </div>
                  <div class="text-xs tabular-nums w-14 text-right">
                    {{ formatPercent(o.qubicSolutionsPercent) }}
                  </div>
                </div>
              </td>
              <td class="py-2 px-2 text-right tabular-nums">{{ formatVolume(o.dogePoints) }}</td>
              <td class="py-2 px-2">
                <div class="flex items-center gap-2 min-w-[140px]">
                  <div class="flex-1 h-1.5 bg-bg-elevated rounded-full overflow-hidden">
                    <div
                      class="h-full bg-accent"
                      :style="{ width: `${dogeBarWidth(o.dogeParticipationPercent)}%` }"
                    ></div>
                  </div>
                  <div class="text-xs tabular-nums w-14 text-right">
                    {{ formatPercent(o.dogeParticipationPercent) }}
                  </div>
                </div>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>
</template>
