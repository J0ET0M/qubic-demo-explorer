<script setup lang="ts">
import { Receipt, ListOrdered, Activity, Layers, Search, X } from 'lucide-vue-next'
import { getProcedureSchema, getContractSchema } from '~/utils/contractInputDecoder'

const api = useApi()
const { formatAmount, truncateAddress } = useFormatting()
const { getLabel, fetchLabels } = useAddressLabels()

// Default to current epoch
const { data: epochCountdown } = await useAsyncData(
  'exec-fees-epoch-countdown',
  () => api.getEpochCountdown()
)
const selectedEpoch = ref<number>(epochCountdown.value?.currentEpoch ?? 211)

const onEpochChange = (e: Event) => {
  const v = Number((e.target as HTMLInputElement).value)
  if (!Number.isNaN(v) && v > 0) selectedEpoch.value = v
}

// Summary view: per-phase per-contract aggregated data
const { data: summary, pending: summaryLoading, refresh: refreshSummary } = await useAsyncData(
  () => `exec-fees-summary-${selectedEpoch.value}`,
  () => api.getExecutionFeeSummary(selectedEpoch.value),
  { watch: [selectedEpoch] }
)

// Resolve contract index → address/label using known addresses
const { data: knownContracts } = await useAsyncData(
  'known-contracts',
  () => api.getAllKnownAddresses('smartcontract')
)

const contractLabel = (idx: number): string => {
  const found = knownContracts.value?.find(c => c.contractIndex === idx)
  return found?.label ? `${found.label} (#${idx})` : `Contract #${idx}`
}

// Index the summary for O(1) lookups by (phase, contract)
const summaryIndex = computed(() => {
  const m = new Map<string, number>()
  if (!summary.value) return m
  for (const row of summary.value) m.set(`${row.phaseNumber}:${row.contractIndex}`, row.agreedFee)
  return m
})

// All distinct contract indices that appear in this epoch
const distinctContracts = computed(() => {
  if (!summary.value) return [] as number[]
  return Array.from(new Set(summary.value.map(s => s.contractIndex))).sort((a, b) => a - b)
})

// All distinct phase numbers and a lookup of phase -> tick
const distinctPhases = computed(() => {
  if (!summary.value) return [] as number[]
  return Array.from(new Set(summary.value.map(s => s.phaseNumber))).sort((a, b) => a - b)
})

const phaseTickByPhase = computed(() => {
  const m = new Map<number, number>()
  if (!summary.value) return m
  for (const row of summary.value) m.set(row.phaseNumber, row.phaseTick)
  return m
})

const phaseTimestampByPhase = computed(() => {
  const m = new Map<number, string | null>()
  if (!summary.value) return m
  for (const row of summary.value) m.set(row.phaseNumber, row.phaseTimestamp)
  return m
})

// Core input-type labels (matching the tick-page list). Used to label the
// "tx counts by input type" panel in the per-phase detail view.
const coreInputTypeLabels: Record<number, string> = {
  0: 'Transfer',
  1: 'Vote Counter',
  2: 'Mining Solution',
  3: 'File Header',
  4: 'File Fragment',
  5: 'File Trailer',
  6: 'Oracle Reply Commit',
  7: 'Oracle Reply Reveal',
  8: 'Mining Share Counter',
  9: 'Execution Fee Report',
  10: 'Oracle User Query',
}

// Qubic contract addresses self-encode the contract index in their first byte.
// A contract address has the form: <single non-A first char> + 55 A's + 4-char
// checksum (e.g. "BAAAAA...AARMID" = QX, contract index 1). Index 0 produces an
// all-A public-key portion which is also the BURN address; we treat that as
// "no contract" so the core-type fallback kicks in. Doesn't rely on label bundle.
const contractIndexFromAddress = (addr: string): number | null => {
  if (!addr || addr.length !== 60) return null
  // chars 1..55 must all be 'A' for it to be a contract-form address
  for (let i = 1; i < 56; i++) {
    if (addr.charCodeAt(i) !== 65 /* 'A' */) return null
  }
  const c = addr.charCodeAt(0)
  if (c < 65 || c > 90) return null
  const idx = c - 65
  return idx > 0 ? idx : null
}

// Resolve (toAddress, inputType) to a human-readable type. Priority:
//   1. type 0 → Transfer (regardless of destination)
//   2. destination is a contract address → contract.procedure (e.g. QX.RemoveFromAskOrder).
//      Resolved via the address bytes; falls back to label bundle's contractIndex
//      if the address shape isn't the canonical contract form.
//   3. otherwise (destination is the burn address or unknown) → core type name
//   4. fallback → "Type N"
const phaseTypeInfo = (toAddress: string, inputType: number): { label: string; sub: string } => {
  if (inputType === 0) return { label: 'Transfer', sub: '' }
  const idx = contractIndexFromAddress(toAddress)
            ?? (getLabel(toAddress)?.contractIndex ?? null)
  if (idx !== null && idx !== undefined) {
    const proc = getProcedureSchema(idx, inputType)
    const contract = getContractSchema(idx)
    if (proc) {
      return { label: proc.name, sub: `${contract?.name ?? `Contract ${idx}`} · type ${inputType}` }
    }
    if (contract) {
      return { label: `${contract.name} #${inputType}`, sub: `unknown procedure` }
    }
    return { label: `Type ${inputType}`, sub: `contract ${idx}` }
  }
  if (inputType in coreInputTypeLabels) {
    return { label: coreInputTypeLabels[inputType], sub: `core · type ${inputType}` }
  }
  return { label: `Type ${inputType}`, sub: getLabel(toAddress)?.label ?? '' }
}

const formatTimestamp = (iso: string | null | undefined): string => {
  if (!iso) return ''
  const d = new Date(iso)
  return d.toLocaleString(undefined, {
    month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false
  })
}

// Phase window: how many recent phases to render. Defaults to 50.
const phaseLimit = ref<number>(50)
const visiblePhases = computed(() => {
  const all = distinctPhases.value
  if (phaseLimit.value <= 0 || all.length <= phaseLimit.value) return all
  return all.slice(all.length - phaseLimit.value)
})

const formatPhase = (phase: number): string => {
  const ts = phaseTimestampByPhase.value.get(phase)
  if (ts) return formatTimestamp(ts)
  const tick = phaseTickByPhase.value.get(phase)
  if (tick === undefined) return `P${phase}`
  return `T${tick.toLocaleString()}`
}

// Top N contracts by total agreed fee
const topNContracts = computed(() => {
  if (!summary.value) return [] as number[]
  const totals = new Map<number, number>()
  for (const row of summary.value) {
    totals.set(row.contractIndex, (totals.get(row.contractIndex) ?? 0) + row.agreedFee)
  }
  return Array.from(totals.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([idx]) => idx)
})

const visibleContracts = ref<number[]>([])
watch(topNContracts, (newTop) => {
  if (visibleContracts.value.length === 0) visibleContracts.value = newTop
}, { immediate: true })

// Searchable contract filter
const contractSearch = ref('')
const filteredContracts = computed(() => {
  if (!contractSearch.value.trim()) return distinctContracts.value
  const q = contractSearch.value.trim().toLowerCase()
  return distinctContracts.value.filter(idx => contractLabel(idx).toLowerCase().includes(q))
})

const toggleContract = (idx: number) => {
  if (visibleContracts.value.includes(idx))
    visibleContracts.value = visibleContracts.value.filter(c => c !== idx)
  else
    visibleContracts.value = [...visibleContracts.value, idx]
}

const selectAllVisible = () => {
  visibleContracts.value = filteredContracts.value
}
const clearAll = () => {
  visibleContracts.value = []
}

// Chart palette
const palette = [
  'rgb(99, 102, 241)', 'rgb(16, 185, 129)', 'rgb(239, 68, 68)',
  'rgb(245, 158, 11)', 'rgb(168, 85, 247)', 'rgb(20, 184, 166)',
  'rgb(244, 114, 182)', 'rgb(59, 130, 246)', 'rgb(132, 204, 22)',
]

const phaseLabels = computed(() => visiblePhases.value.map(p => formatPhase(p)))

const chartDatasets = computed(() => {
  if (!summary.value) return []
  const idx = summaryIndex.value
  const phases = visiblePhases.value
  return visibleContracts.value.map((contractIdx, i) => {
    const data = phases.map(phase => idx.get(`${phase}:${contractIdx}`) ?? 0)
    const color = palette[i % palette.length]
    return {
      label: contractLabel(contractIdx),
      data,
      borderColor: color,
      backgroundColor: color.replace('rgb', 'rgba').replace(')', ', 0.05)')
    }
  })
})

// ── Drill-down: per-contract detail (manual fetch on selection) ──
const selectedContract = ref<number | null>(null)
const contractSearchDD = ref('')
const showContractDD = ref(false)
const contractDetail = ref<Awaited<ReturnType<typeof api.getExecutionFeeContract>> | null>(null)
const contractLoading = ref(false)

const filteredContractsDD = computed(() => {
  if (!contractSearchDD.value.trim()) return distinctContracts.value
  const q = contractSearchDD.value.trim().toLowerCase()
  return distinctContracts.value.filter(idx => contractLabel(idx).toLowerCase().includes(q))
})

const pickContract = async (idx: number) => {
  selectedContract.value = idx
  contractSearchDD.value = contractLabel(idx)
  showContractDD.value = false
  await loadContractDetail()
}

const clearContract = () => {
  selectedContract.value = null
  contractSearchDD.value = ''
  contractDetail.value = null
}

const loadContractDetail = async () => {
  if (selectedContract.value === null) {
    contractDetail.value = null
    return
  }
  contractLoading.value = true
  try {
    contractDetail.value = await api.getExecutionFeeContract(selectedEpoch.value, selectedContract.value)
  } catch (e) {
    console.error('Failed to load contract detail', e)
    contractDetail.value = null
  } finally {
    contractLoading.value = false
  }
}

watch(selectedEpoch, () => {
  if (selectedContract.value !== null) loadContractDetail()
})

// Independent window for the drill-down chart so user can zoom in there.
const contractPhaseLimit = ref<number>(50)
const visibleContractPhases = computed(() => {
  const all = contractDetail.value?.phases ?? []
  if (contractPhaseLimit.value <= 0 || all.length <= contractPhaseLimit.value) return all
  return all.slice(all.length - contractPhaseLimit.value)
})

const contractDetailChartLabels = computed(() =>
  visibleContractPhases.value.map(p =>
    p.phaseTimestamp ? formatTimestamp(p.phaseTimestamp) : `T${p.phaseTick.toLocaleString()}`
  )
)

const contractDetailChartDatasets = computed(() => {
  const phases = visibleContractPhases.value
  if (phases.length === 0) return []
  return [
    {
      label: 'Agreed Fee (2/3 percentile)',
      data: phases.map(p => p.agreedFee),
      borderColor: 'rgb(239, 68, 68)',
      backgroundColor: 'rgba(239, 68, 68, 0.1)'
    },
    {
      label: 'Min Reported',
      data: phases.map(p =>
        p.reports.length ? Math.min(...p.reports.map(r => r.reportedFee)) : 0),
      borderColor: 'rgb(245, 158, 11)',
      backgroundColor: 'rgba(245, 158, 11, 0.05)'
    },
    {
      label: 'Max Reported',
      data: phases.map(p =>
        p.reports.length ? Math.max(...p.reports.map(r => r.reportedFee)) : 0),
      borderColor: 'rgb(16, 185, 129)',
      backgroundColor: 'rgba(16, 185, 129, 0.05)'
    },
    {
      label: 'Average',
      data: phases.map(p =>
        p.reports.length ? Math.round(p.reports.reduce((a, b) => a + b.reportedFee, 0) / p.reports.length) : 0),
      borderColor: 'rgb(99, 102, 241)',
      backgroundColor: 'rgba(99, 102, 241, 0.05)'
    }
  ]
})

// ── Per-phase distribution view (manual fetch on selection) ──
const selectedPhase = ref<number | null>(null)
const phaseSearch = ref('')
const showPhaseDD = ref(false)
const phaseDetail = ref<Awaited<ReturnType<typeof api.getExecutionFeePhase>> | null>(null)
const phaseLoading = ref(false)

// Fetch labels for the toAddresses appearing in the phase tx-counts table so
// the contract resolver in phaseTypeInfo (above) can map address → contract index.
watch(() => phaseDetail.value?.txCountsByInputType, async (rows) => {
  if (rows?.length) {
    const addrs = Array.from(new Set(rows.map(r => r.toAddress).filter(Boolean)))
    if (addrs.length) await fetchLabels(addrs)
  }
}, { immediate: true })

const filteredPhases = computed(() => {
  if (!phaseSearch.value.trim()) return distinctPhases.value
  const q = phaseSearch.value.trim().toLowerCase()
  return distinctPhases.value.filter(p => {
    const tick = phaseTickByPhase.value.get(p)
    const ts = phaseTimestampByPhase.value.get(p)
    if (p.toString().includes(q)) return true
    if (tick !== undefined && tick.toString().includes(q)) return true
    if (ts) {
      // Match against the ISO and the locale-formatted display ("May 04 22:35")
      if (ts.toLowerCase().includes(q)) return true
      if (formatTimestamp(ts).toLowerCase().includes(q)) return true
    }
    return false
  })
})

const phaseDisplayValue = (p: number | null): string => {
  if (p === null) return ''
  const tick = phaseTickByPhase.value.get(p)
  return tick !== undefined ? `T${tick.toLocaleString()} (phase ${p})` : `Phase ${p}`
}

const pickPhase = async (phase: number | null) => {
  selectedPhase.value = phase
  showPhaseDD.value = false
  if (phase === null) {
    phaseDetail.value = null
    phaseSearch.value = ''
    return
  }
  phaseSearch.value = phaseDisplayValue(phase)
  phaseLoading.value = true
  try {
    phaseDetail.value = await api.getExecutionFeePhase(selectedEpoch.value, phase)
  } catch (e) {
    console.error('Failed to load phase detail', e)
    phaseDetail.value = null
  } finally {
    phaseLoading.value = false
  }
}

const clearPhase = () => {
  selectedPhase.value = null
  phaseSearch.value = ''
  phaseDetail.value = null
}

// Prev/next navigation in the single-phase viewer (uses ALL phases, not the windowed view)
const phaseNavIndex = computed(() => {
  if (selectedPhase.value === null) return -1
  return distinctPhases.value.indexOf(selectedPhase.value)
})

const hasPrevPhase = computed(() => phaseNavIndex.value > 0)
const hasNextPhase = computed(() => phaseNavIndex.value >= 0 && phaseNavIndex.value < distinctPhases.value.length - 1)

const prevPhase = () => {
  if (hasPrevPhase.value) pickPhase(distinctPhases.value[phaseNavIndex.value - 1])
}
const nextPhase = () => {
  if (hasNextPhase.value) pickPhase(distinctPhases.value[phaseNavIndex.value + 1])
}

// Chart-click handlers — translate X-axis index back to a phase number
const onMainChartClick = (idx: number) => {
  const phase = visiblePhases.value[idx]
  if (phase !== undefined) {
    pickPhase(phase)
    // Scroll the phase detail card into view
    setTimeout(() => {
      const el = document.getElementById('phase-detail-card')
      if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' })
    }, 50)
  }
}

const onContractChartClick = (idx: number) => {
  const phaseEntry = visibleContractPhases.value[idx]
  if (phaseEntry) {
    pickPhase(phaseEntry.phaseNumber)
    setTimeout(() => {
      const el = document.getElementById('phase-detail-card')
      if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' })
    }, 50)
  }
}

watch(selectedEpoch, () => {
  if (selectedPhase.value !== null) pickPhase(selectedPhase.value)
})

// Heatmap: which contract is currently expanded in the per-phase view
const heatmapContractIdx = ref<number | null>(null)
const heatmapHover = ref<{ computorIndex: number; fee: number | null } | null>(null)

// Map computorIndex → txHash for the currently selected (phase, contract) so
// each colored cell can link directly to the publishing transaction. The fee
// report is published in the tick where that computor is leader, not at the
// phase boundary, so a per-tx link is the right level of precision.
const heatmapTxByComputor = computed(() => {
  const m = new Map<number, string>()
  if (heatmapContractIdx.value === null || !phaseDetail.value) return m
  const contract = phaseDetail.value.contracts.find(c => c.contractIndex === heatmapContractIdx.value)
  if (!contract?.reports) return m
  for (const r of contract.reports) {
    if (r.txHash) m.set(r.computorIndex, r.txHash)
  }
  return m
})

const heatmapCellLink = (computorIndex: number, fee: number | null): string | null => {
  if (fee === null) return null
  const txHash = heatmapTxByComputor.value.get(computorIndex)
  return txHash ? `/tx/${txHash}` : null
}

const toggleHeatmap = (contractIdx: number) => {
  heatmapContractIdx.value = heatmapContractIdx.value === contractIdx ? null : contractIdx
  heatmapHover.value = null
}

// Build heatmap cells (676 entries) for the currently expanded contract.
// Color scale: green (low) → yellow (mid) → red (high), relative to this contract's min/max.
interface HeatmapCell {
  computorIndex: number
  fee: number | null  // null = no report from that computor
  color: string
  intensity: number  // 0..1 for tooltip
}

const heatmapData = computed((): HeatmapCell[] | null => {
  if (heatmapContractIdx.value === null || !phaseDetail.value) return null
  const contract = phaseDetail.value.contracts.find(c => c.contractIndex === heatmapContractIdx.value)
  if (!contract?.reports?.length) return null

  // Build computor → fee lookup
  const byComputor = new Map<number, number>()
  for (const r of contract.reports) byComputor.set(r.computorIndex, r.reportedFee)

  const fees = contract.reports.map(r => r.reportedFee)
  const min = Math.min(...fees)
  const max = Math.max(...fees)
  const range = max - min || 1

  const cells: HeatmapCell[] = []
  for (let i = 0; i < 676; i++) {
    const fee = byComputor.get(i) ?? null
    if (fee === null) {
      cells.push({ computorIndex: i, fee: null, color: 'rgb(40, 44, 56)', intensity: 0 })
    } else {
      const intensity = (fee - min) / range
      // green (low) → yellow (~0.5) → red (high)
      const r = Math.round(intensity * 255)
      const g = Math.round((1 - Math.abs(intensity - 0.5) * 2) * 200 + 55)
      const b = Math.round((1 - intensity) * 80)
      cells.push({
        computorIndex: i,
        fee,
        color: `rgb(${r}, ${g}, ${b})`,
        intensity
      })
    }
  }
  return cells
})

const heatmapStats = computed(() => {
  if (heatmapContractIdx.value === null || !phaseDetail.value) return null
  const contract = phaseDetail.value.contracts.find(c => c.contractIndex === heatmapContractIdx.value)
  if (!contract?.reports?.length) return null
  const fees = contract.reports.map(r => r.reportedFee)
  return {
    contractIndex: contract.contractIndex,
    min: Math.min(...fees),
    max: Math.max(...fees),
    agreed: contract.agreedFee,
    reporters: fees.length,
    silent: 676 - fees.length
  }
})

// 1D gap-based clustering: sort values, split where the gap between consecutive
// values exceeds GAP_THRESHOLD × (range/N). Tunable but produces sensible clusters
// for fee voting where most computors agree but a few outliers stand apart.
interface FeeCluster {
  index: number
  count: number
  min: number
  max: number
  mean: number
  computors: number[]      // sorted ASC
  isMajority: boolean      // largest cluster
  containsAgreed: boolean  // contains the network-agreed value
}

const heatmapClusters = computed((): FeeCluster[] | null => {
  if (heatmapContractIdx.value === null || !phaseDetail.value) return null
  const contract = phaseDetail.value.contracts.find(c => c.contractIndex === heatmapContractIdx.value)
  if (!contract?.reports?.length || contract.reports.length < 2) return null

  // Pair reports with their computor index, sort by fee ASC
  const sorted = [...contract.reports].sort((a, b) => a.reportedFee - b.reportedFee)
  const fees = sorted.map(r => r.reportedFee)
  const min = fees[0]
  const max = fees[fees.length - 1]
  const range = max - min

  // If everyone reported the same → single cluster
  if (range === 0) {
    return [{
      index: 0, count: sorted.length, min, max, mean: min,
      computors: sorted.map(r => r.computorIndex).sort((a, b) => a - b),
      isMajority: true,
      containsAgreed: contract.agreedFee >= min && contract.agreedFee <= max
    }]
  }

  // Threshold = 5% of total range — a "natural break"
  const threshold = range * 0.05
  const splits: number[] = []
  for (let i = 1; i < fees.length; i++) {
    if (fees[i] - fees[i - 1] > threshold) splits.push(i)
  }

  // Build clusters
  const clusters: FeeCluster[] = []
  let start = 0
  splits.push(fees.length)
  for (const end of splits) {
    if (end <= start) continue
    const slice = sorted.slice(start, end)
    const sliceFees = slice.map(r => r.reportedFee)
    const cMin = sliceFees[0]
    const cMax = sliceFees[sliceFees.length - 1]
    const sum = sliceFees.reduce((a, b) => a + b, 0)
    clusters.push({
      index: clusters.length,
      count: slice.length,
      min: cMin,
      max: cMax,
      mean: Math.round(sum / slice.length),
      computors: slice.map(r => r.computorIndex).sort((a, b) => a - b),
      isMajority: false,
      containsAgreed: contract.agreedFee >= cMin && contract.agreedFee <= cMax
    })
    start = end
  }

  // Mark majority cluster
  if (clusters.length > 0) {
    const biggest = clusters.reduce((max, c) => c.count > max.count ? c : max, clusters[0])
    biggest.isMajority = true
  }

  // Sort clusters by member count desc for display, but keep stable index
  return clusters.sort((a, b) => b.count - a.count)
})

// Map: computor index → cluster index (for ring highlighting on the heatmap)
const computorToCluster = computed(() => {
  const m = new Map<number, number>()
  if (!heatmapClusters.value) return m
  for (const cluster of heatmapClusters.value) {
    for (const cmp of cluster.computors) m.set(cmp, cluster.index)
  }
  return m
})

const clusterPalette = [
  '#10b981', '#f59e0b', '#ef4444', '#a855f7',
  '#3b82f6', '#ec4899', '#14b8a6', '#84cc16'
]
const clusterColor = (index: number) => clusterPalette[index % clusterPalette.length]

// Show cluster outlines on heatmap?
const showClusters = ref(true)
</script>

<template>
  <div class="space-y-6">
    <!-- Epoch selector + status -->
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
        <div class="text-xs text-foreground-muted" v-if="summary">
          {{ summary.length }} (phase × contract) rows · {{ distinctPhases.length }} phases · {{ distinctContracts.length }} contracts
        </div>
      </div>
    </div>

    <!-- Main chart: agreed fee per contract over phases -->
    <div class="card">
      <h2 class="section-title mb-2">
        <Activity class="h-4 w-4 text-accent" />
        Agreed Fees Per Contract Over Phases
      </h2>
      <p class="text-xs text-foreground-muted mb-4">
        Each line is the network's agreed (2/3 ascending percentile) execution fee for one contract,
        per 676-tick phase, in <strong>qus</strong>. This is the value actually deducted from the contract's fee reserve.
        X-axis labels show the phase publication time. Click a point to drill into that phase.
      </p>

      <div v-if="summaryLoading" class="loading py-12">Loading summary...</div>
      <template v-else-if="summary?.length">
        <!-- Phase window selector -->
        <div class="flex flex-wrap items-center gap-2 mb-3">
          <span class="text-xs text-foreground-muted">Show last</span>
          <button
            v-for="n in [20, 50, 100, 200, 500]"
            :key="n"
            @click="phaseLimit = n"
            :class="[
              'px-2 py-1 text-xs rounded-md font-medium transition-colors',
              phaseLimit === n
                ? 'bg-accent text-white'
                : 'bg-surface-elevated text-foreground-muted hover:text-foreground'
            ]"
          >
            {{ n }}
          </button>
          <button
            @click="phaseLimit = 0"
            :class="[
              'px-2 py-1 text-xs rounded-md font-medium transition-colors',
              phaseLimit === 0
                ? 'bg-accent text-white'
                : 'bg-surface-elevated text-foreground-muted hover:text-foreground'
            ]"
          >
            All ({{ distinctPhases.length }})
          </button>
          <span class="text-xs text-foreground-muted">phases · showing {{ visiblePhases.length }}</span>
        </div>

        <ClientOnly>
          <ChartsEpochLineChart
            :labels="phaseLabels"
            :datasets="chartDatasets"
            :height="320"
            :clickable="true"
            @point-click="(idx: number) => onMainChartClick(idx)"
          />
          <template #fallback>
            <div class="h-[320px] flex items-center justify-center text-foreground-muted">Loading chart...</div>
          </template>
        </ClientOnly>

        <p class="text-xs text-foreground-muted mt-2">
          Hover for details · click a point to jump to that phase's detail view below.
        </p>

        <!-- Contract filter + toggle pills -->
        <div class="mt-4 space-y-3">
          <div class="flex flex-wrap items-center gap-2">
            <div class="relative flex-1 min-w-[200px] max-w-md">
              <Search class="absolute left-2.5 top-1/2 -translate-y-1/2 h-4 w-4 text-foreground-muted pointer-events-none" />
              <input
                v-model="contractSearch"
                type="text"
                placeholder="Filter contracts..."
                class="w-full pl-8 pr-3 py-1.5 bg-surface-elevated border border-border rounded text-sm text-foreground placeholder:text-foreground-muted"
              />
            </div>
            <button @click="selectAllVisible" class="px-2 py-1 text-xs rounded bg-surface-elevated hover:bg-surface-hover text-foreground">
              Show all visible
            </button>
            <button @click="clearAll" class="px-2 py-1 text-xs rounded bg-surface-elevated hover:bg-surface-hover text-foreground">
              Clear
            </button>
            <span class="text-xs text-foreground-muted">
              {{ visibleContracts.length }} / {{ distinctContracts.length }} shown
            </span>
          </div>

          <div class="flex flex-wrap gap-1.5 max-h-48 overflow-y-auto p-1">
            <button
              v-for="idx in filteredContracts"
              :key="idx"
              @click="toggleContract(idx)"
              :class="[
                'px-2.5 py-1 text-xs rounded-md font-medium transition-colors',
                visibleContracts.includes(idx)
                  ? 'bg-accent text-white'
                  : 'bg-surface-elevated text-foreground-muted hover:text-foreground'
              ]"
            >
              {{ contractLabel(idx) }}
            </button>
          </div>
        </div>
      </template>
      <div v-else class="text-center py-8 text-foreground-muted">
        No execution fee reports for epoch {{ selectedEpoch }} yet.
      </div>
    </div>

    <!-- Per-contract drill-down -->
    <div class="card">
      <h2 class="section-title mb-2">
        <ListOrdered class="h-4 w-4 text-accent" />
        Per-Contract Drill-down
      </h2>

      <div class="flex flex-wrap items-center gap-2 mb-4 relative">
        <label class="text-xs text-foreground-muted">Contract:</label>
        <div class="relative w-72">
          <Search class="absolute left-2.5 top-1/2 -translate-y-1/2 h-4 w-4 text-foreground-muted pointer-events-none" />
          <input
            v-model="contractSearchDD"
            @focus="showContractDD = true"
            @input="showContractDD = true"
            type="text"
            placeholder="Search by name or index..."
            class="w-full pl-8 pr-8 py-1.5 bg-surface-elevated border border-border rounded text-sm text-foreground placeholder:text-foreground-muted"
          />
          <button
            v-if="selectedContract !== null"
            @click="clearContract"
            class="absolute right-2 top-1/2 -translate-y-1/2 text-foreground-muted hover:text-foreground"
            title="Clear"
          >
            <X class="h-4 w-4" />
          </button>

          <!-- Searchable dropdown -->
          <div
            v-if="showContractDD && filteredContractsDD.length > 0"
            class="absolute z-10 mt-1 w-full max-h-64 overflow-y-auto rounded border border-border bg-surface-elevated shadow-lg"
          >
            <button
              v-for="idx in filteredContractsDD.slice(0, 50)"
              :key="idx"
              @click="pickContract(idx)"
              class="w-full text-left px-3 py-1.5 text-sm text-foreground hover:bg-surface-hover"
            >
              {{ contractLabel(idx) }}
            </button>
            <div v-if="filteredContractsDD.length > 50" class="px-3 py-1.5 text-xs text-foreground-muted">
              ... and {{ filteredContractsDD.length - 50 }} more — refine search
            </div>
          </div>
        </div>
        <button
          v-if="showContractDD"
          @click="showContractDD = false"
          class="px-2 py-1 text-xs rounded bg-surface-elevated hover:bg-surface-hover text-foreground"
        >
          Close
        </button>
      </div>

      <div v-if="contractLoading" class="loading py-8">Loading contract data...</div>
      <template v-else-if="contractDetail?.phases.length">
        <!-- Phase window for the drill-down chart -->
        <div class="flex flex-wrap items-center gap-2 mb-3">
          <span class="text-xs text-foreground-muted">Show last</span>
          <button
            v-for="n in [20, 50, 100, 200, 500]"
            :key="n"
            @click="contractPhaseLimit = n"
            :class="[
              'px-2 py-1 text-xs rounded-md font-medium transition-colors',
              contractPhaseLimit === n
                ? 'bg-accent text-white'
                : 'bg-surface-elevated text-foreground-muted hover:text-foreground'
            ]"
          >
            {{ n }}
          </button>
          <button
            @click="contractPhaseLimit = 0"
            :class="[
              'px-2 py-1 text-xs rounded-md font-medium transition-colors',
              contractPhaseLimit === 0
                ? 'bg-accent text-white'
                : 'bg-surface-elevated text-foreground-muted hover:text-foreground'
            ]"
          >
            All ({{ contractDetail.phases.length }})
          </button>
          <span class="text-xs text-foreground-muted">phases · showing {{ visibleContractPhases.length }}</span>
        </div>

        <ClientOnly>
          <ChartsEpochLineChart
            :labels="contractDetailChartLabels"
            :datasets="contractDetailChartDatasets"
            :height="320"
            :clickable="true"
            @point-click="(idx: number) => onContractChartClick(idx)"
          />
          <template #fallback>
            <div class="h-[320px] flex items-center justify-center text-foreground-muted">Loading chart...</div>
          </template>
        </ClientOnly>
        <p class="text-xs text-foreground-muted mt-3">
          The red <strong>Agreed Fee</strong> line is what the network actually deducted.
          Values between Min and Max show the disagreement among the 676 computors per phase.
          Click a point to jump to that phase's detail view.
        </p>
      </template>
      <div v-else-if="selectedContract !== null" class="text-center py-8 text-foreground-muted">
        No data for this contract in epoch {{ selectedEpoch }}.
      </div>
      <div v-else class="text-center py-8 text-foreground-muted text-sm">
        Pick a contract to see all 676 reported values per phase.
      </div>
    </div>

    <!-- Per-phase view -->
    <div id="phase-detail-card" class="card">
      <h2 class="section-title mb-2">
        <Layers class="h-4 w-4 text-accent" />
        Single Phase: All Contracts
      </h2>
      <div class="flex flex-wrap items-center gap-2 mb-4 relative">
        <button
          @click="prevPhase"
          :disabled="!hasPrevPhase"
          class="px-2 py-1 text-xs rounded bg-surface-elevated text-foreground hover:bg-surface-hover disabled:opacity-40 disabled:cursor-not-allowed"
          title="Previous phase"
        >
          ← Prev
        </button>
        <label class="text-xs text-foreground-muted">Phase:</label>
        <div class="relative w-80">
          <Search class="absolute left-2.5 top-1/2 -translate-y-1/2 h-4 w-4 text-foreground-muted pointer-events-none" />
          <input
            v-model="phaseSearch"
            @focus="showPhaseDD = true"
            @input="showPhaseDD = true"
            type="text"
            placeholder="Search by date, tick, or phase number..."
            class="w-full pl-8 pr-8 py-1.5 bg-surface-elevated border border-border rounded text-sm text-foreground placeholder:text-foreground-muted"
          />
          <button
            v-if="selectedPhase !== null"
            @click="clearPhase"
            class="absolute right-2 top-1/2 -translate-y-1/2 text-foreground-muted hover:text-foreground"
            title="Clear"
          >
            <X class="h-4 w-4" />
          </button>

          <div
            v-if="showPhaseDD && filteredPhases.length > 0"
            class="absolute z-10 mt-1 w-full max-h-64 overflow-y-auto rounded border border-border bg-surface-elevated shadow-lg"
          >
            <button
              v-for="p in filteredPhases.slice(0, 100)"
              :key="p"
              @click="pickPhase(p)"
              class="w-full text-left px-3 py-1.5 text-sm text-foreground hover:bg-surface-hover"
            >
              <span v-if="phaseTimestampByPhase.get(p)">
                {{ formatTimestamp(phaseTimestampByPhase.get(p)) }}
              </span>
              <span class="text-foreground-muted text-xs ml-2">
                T{{ phaseTickByPhase.get(p)?.toLocaleString() }} · phase {{ p }}
              </span>
            </button>
            <div v-if="filteredPhases.length > 100" class="px-3 py-1.5 text-xs text-foreground-muted">
              ... and {{ filteredPhases.length - 100 }} more — refine search
            </div>
          </div>
        </div>
        <button
          v-if="showPhaseDD"
          @click="showPhaseDD = false"
          class="px-2 py-1 text-xs rounded bg-surface-elevated hover:bg-surface-hover text-foreground"
        >
          Close
        </button>
        <button
          @click="nextPhase"
          :disabled="!hasNextPhase"
          class="px-2 py-1 text-xs rounded bg-surface-elevated text-foreground hover:bg-surface-hover disabled:opacity-40 disabled:cursor-not-allowed"
          title="Next phase"
        >
          Next →
        </button>
        <span v-if="selectedPhase !== null && distinctPhases.length > 0" class="text-xs text-foreground-muted ml-2">
          {{ phaseNavIndex + 1 }} / {{ distinctPhases.length }}
        </span>
      </div>

      <div v-if="phaseLoading" class="loading py-8">Loading phase data...</div>
      <template v-else-if="phaseDetail?.contracts.length">
        <div class="text-xs text-foreground-muted mb-2">
          Phase {{ phaseDetail.phaseNumber }} (publication tick {{ phaseDetail.phaseTick.toLocaleString() }}) ·
          {{ phaseDetail.contracts.length }} contracts
        </div>

        <!-- Tx counts by input_type for the 676-tick phase window -->
        <div v-if="phaseDetail.txCountsByInputType?.length" class="mb-4 p-3 bg-surface-elevated rounded-lg">
          <div class="text-xs font-semibold text-foreground mb-2">
            Transactions in this phase window
            <span class="text-foreground-muted font-normal">
              (ticks {{ (phaseDetail.phaseTick - 675).toLocaleString() }}..{{ phaseDetail.phaseTick.toLocaleString() }})
            </span>
          </div>
          <div class="table-wrapper">
            <table>
              <thead>
                <tr>
                  <th>Destination</th>
                  <th>Type</th>
                  <th class="text-right">Total</th>
                  <th class="text-right">Executed</th>
                  <th class="text-right">Failed</th>
                  <th class="text-right">Exec %</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="row in phaseDetail.txCountsByInputType" :key="`${row.toAddress}-${row.inputType}`">
                  <td class="font-mono text-xs">
                    <NuxtLink v-if="row.toAddress" :to="`/address/${row.toAddress}`" class="text-accent hover:underline">
                      {{ getLabel(row.toAddress)?.label || truncateAddress(row.toAddress, 6) }}
                    </NuxtLink>
                    <span v-else class="text-foreground-muted italic">various</span>
                  </td>
                  <td>
                    <div class="text-sm">{{ phaseTypeInfo(row.toAddress, row.inputType).label }}</div>
                    <div v-if="phaseTypeInfo(row.toAddress, row.inputType).sub" class="text-[10px] text-foreground-muted">
                      {{ phaseTypeInfo(row.toAddress, row.inputType).sub }}
                    </div>
                  </td>
                  <td class="text-right">{{ row.totalCount.toLocaleString() }}</td>
                  <td class="text-right text-success">{{ row.executedCount.toLocaleString() }}</td>
                  <td class="text-right text-destructive">
                    {{ (row.totalCount - row.executedCount).toLocaleString() }}
                  </td>
                  <td class="text-right text-foreground-muted">
                    {{ row.totalCount === 0 ? '—' : ((row.executedCount / row.totalCount) * 100).toFixed(1) + '%' }}
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
        <div class="table-wrapper">
          <table>
            <thead>
              <tr>
                <th>Contract</th>
                <th class="text-right">Reports</th>
                <th class="text-right">Min</th>
                <th class="text-right">Avg</th>
                <th class="text-right">Median</th>
                <th class="text-right">Max</th>
                <th class="text-right">Agreed</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="c in phaseDetail.contracts" :key="c.contractIndex">
                <td>{{ contractLabel(c.contractIndex) }}</td>
                <td class="text-right text-foreground-muted">{{ c.reportCount }}</td>
                <td class="text-right">{{ formatAmount(c.minFee) }}</td>
                <td class="text-right">{{ formatAmount(c.avgFee) }}</td>
                <td class="text-right">{{ formatAmount(c.medianFee) }}</td>
                <td class="text-right">{{ formatAmount(c.maxFee) }}</td>
                <td class="text-right font-bold text-destructive">{{ formatAmount(c.agreedFee) }}</td>
                <td class="text-right">
                  <button
                    @click="toggleHeatmap(c.contractIndex)"
                    :class="[
                      'px-2 py-0.5 text-xs rounded',
                      heatmapContractIdx === c.contractIndex
                        ? 'bg-accent text-white'
                        : 'bg-surface-elevated text-foreground-muted hover:text-foreground'
                    ]"
                  >
                    Heatmap
                  </button>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <!-- Computor heatmap for the selected contract -->
        <div v-if="heatmapData && heatmapStats" class="mt-6 p-4 rounded-lg bg-surface-elevated">
          <div class="flex flex-wrap items-center justify-between gap-2 mb-3">
            <div class="text-sm">
              <strong>{{ contractLabel(heatmapStats.contractIndex) }}</strong>
              <span class="text-foreground-muted ml-2">
                — {{ heatmapStats.reporters }} reports · {{ heatmapStats.silent }} silent
              </span>
            </div>
            <div class="flex items-center gap-3 text-xs text-foreground-muted">
              <div class="flex items-center gap-1">
                <span class="inline-block w-3 h-3 rounded" style="background: rgb(0, 255, 80)"></span>
                Min {{ formatAmount(heatmapStats.min) }}
              </div>
              <div class="flex items-center gap-1">
                <span class="inline-block w-3 h-3 rounded" style="background: rgb(127, 195, 40)"></span>
                Mid
              </div>
              <div class="flex items-center gap-1">
                <span class="inline-block w-3 h-3 rounded" style="background: rgb(255, 55, 0)"></span>
                Max {{ formatAmount(heatmapStats.max) }}
              </div>
              <div class="flex items-center gap-1">
                <span class="inline-block w-3 h-3 rounded" style="background: rgb(40, 44, 56)"></span>
                No report
              </div>
            </div>
          </div>

          <!-- Side-by-side on xl screens, stacked below.
               Heatmap takes ~2x the width of the cluster panel. -->
          <div class="grid grid-cols-1 xl:grid-cols-[1fr_minmax(280px,360px)] gap-6 items-start">
            <!-- Left: heatmap (larger, fills available space) -->
            <div class="min-w-0">
              <!-- Custom hover read-out (instant, no native title delay) -->
              <div class="text-xs text-foreground-muted mb-2 h-5">
                <template v-if="heatmapHover">
                  <span class="text-foreground font-mono">
                    Computor #{{ heatmapHover.computorIndex }}:
                  </span>
                  <span v-if="heatmapHover.fee !== null" class="ml-1 font-mono">
                    {{ heatmapHover.fee.toLocaleString() }} qus (click to open transaction)
                  </span>
                  <span v-else class="ml-1 italic">no report</span>
                </template>
                <span v-else>Hover a cell for details.</span>
              </div>

              <div
                class="grid gap-[2px] w-full"
                style="grid-template-columns: repeat(38, minmax(0, 1fr));"
                @mouseleave="heatmapHover = null"
              >
                <template v-for="cell in heatmapData" :key="cell.computorIndex">
                  <NuxtLink
                    v-if="heatmapCellLink(cell.computorIndex, cell.fee)"
                    :to="heatmapCellLink(cell.computorIndex, cell.fee)!"
                    class="aspect-square rounded-sm cursor-pointer hover:ring-2 hover:ring-accent transition-all"
                    :style="{
                      backgroundColor: cell.color,
                      boxShadow: showClusters && computorToCluster.get(cell.computorIndex) !== undefined
                        ? `inset 0 0 0 1px ${clusterColor(computorToCluster.get(cell.computorIndex)!)}`
                        : undefined
                    }"
                    @mouseenter="heatmapHover = { computorIndex: cell.computorIndex, fee: cell.fee }"
                  />
                  <div
                    v-else
                    class="aspect-square rounded-sm cursor-help"
                    :style="{ backgroundColor: cell.color }"
                    @mouseenter="heatmapHover = { computorIndex: cell.computorIndex, fee: cell.fee }"
                  />
                </template>
              </div>
              <p class="text-xs text-foreground-muted mt-3 max-w-xl">
                One cell per computor (0..675). Color = fee value (green→red). Grey = no report.
                <strong>Click a colored cell</strong> to view that computor's fee-report transaction.
              </p>
            </div>

            <!-- Right: cluster analysis -->
            <div v-if="heatmapClusters && heatmapClusters.length > 1" class="min-w-0">
              <div class="flex items-center justify-between mb-2 flex-wrap gap-2">
                <h3 class="text-sm font-semibold">
                  Vote Clusters
                  <span class="text-foreground-muted font-normal text-xs ml-1">
                    · gap-based grouping (5% of range)
                  </span>
                </h3>
                <label class="flex items-center gap-1.5 text-xs text-foreground-muted cursor-pointer">
                  <input type="checkbox" v-model="showClusters" class="rounded" />
                  Outline on heatmap
                </label>
              </div>
              <div class="grid grid-cols-1 gap-2">
                <div
                  v-for="c in heatmapClusters"
                  :key="c.index"
                  class="rounded p-2 border"
                  :style="{ borderColor: clusterColor(c.index), borderLeftWidth: '4px' }"
                >
                  <div class="flex items-center justify-between gap-2">
                    <div class="flex items-center gap-2 flex-wrap">
                      <span
                        class="inline-block w-2 h-2 rounded"
                        :style="{ background: clusterColor(c.index) }"
                      ></span>
                      <span class="font-semibold text-sm">{{ c.count }} computors</span>
                      <span v-if="c.isMajority" class="text-[10px] px-1.5 py-0.5 rounded bg-accent text-white">majority</span>
                      <span v-if="c.containsAgreed" class="text-[10px] px-1.5 py-0.5 rounded bg-destructive text-white">agreed</span>
                    </div>
                    <span class="text-xs text-foreground-muted">
                      {{ ((c.count / heatmapStats!.reporters) * 100).toFixed(1) }}%
                    </span>
                  </div>
                  <div class="mt-1 font-mono text-xs">
                    <template v-if="c.min === c.max">
                      {{ c.min.toLocaleString() }} qus
                    </template>
                    <template v-else>
                      {{ c.min.toLocaleString() }} — {{ c.max.toLocaleString() }} qus
                      <span class="text-foreground-muted ml-1">(mean {{ c.mean.toLocaleString() }})</span>
                    </template>
                  </div>
                  <div v-if="c.count <= 12" class="mt-1 text-[11px] text-foreground-muted">
                    Computors:
                    <span v-for="(cmp, i) in c.computors" :key="cmp">
                      <span v-if="i > 0">, </span>#{{ cmp }}
                    </span>
                  </div>
                </div>
              </div>
            </div>
            <div v-else-if="heatmapClusters?.length === 1" class="text-xs text-foreground-muted self-center">
              All {{ heatmapClusters[0].count }} reporting computors agree (single cluster).
            </div>
          </div>
        </div>
      </template>
      <div v-else-if="selectedPhase !== null" class="text-center py-8 text-foreground-muted">
        No data for this phase.
      </div>
      <div v-else class="text-center py-8 text-foreground-muted text-sm">
        Pick a phase tick to see how computors voted across all contracts.
      </div>
    </div>
  </div>
</template>
