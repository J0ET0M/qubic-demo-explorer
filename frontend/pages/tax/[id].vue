<script setup lang="ts">
import { ArrowLeft, FileSpreadsheet, Download, ArrowDownLeft, ArrowUpRight, AlertTriangle } from 'lucide-vue-next'

const route = useRoute()
const router = useRouter()
const api = useApi()
const { formatAmount, truncateAddress, copyToClipboard: doCopy } = useFormatting()

const address = String(route.params.id)

const currentYear = new Date().getUTCFullYear()
const minYear = 2024
const initialYear = (() => {
  const v = Number(route.query.year)
  return Number.isFinite(v) && v >= minYear && v <= currentYear ? v : currentYear
})()

const selectedYear = ref<number>(initialYear)
const availableYears = computed(() => {
  const years: number[] = []
  for (let y = currentYear; y >= minYear; y--) years.push(y)
  return years
})

useHead({ title: () => `Tax report ${selectedYear.value} · ${truncateAddress(address, 6)} - Analytics` })

const { data: report, pending } = await useAsyncData(
  () => `tax-${address}-${selectedYear.value}`,
  () => api.getAddressTaxReport(address, selectedYear.value),
  { watch: [selectedYear] }
)

watch(selectedYear, (v) => {
  router.replace({ query: { ...route.query, year: String(v) } })
})

const downloadCsv = () => api.downloadAddressTaxReportCsv(address, selectedYear.value)

const formatDateTime = (iso: string): string => {
  const d = new Date(iso)
  return d.toLocaleString(undefined, {
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    timeZone: 'UTC', hour12: false
  }) + ' UTC'
}

const formatDate = (iso: string): string => {
  return new Date(iso).toLocaleDateString(undefined, {
    year: 'numeric', month: 'short', day: '2-digit', timeZone: 'UTC'
  })
}

// Group transfers by month for display
const transfersByMonth = computed(() => {
  const groups: Record<number, ReturnType<typeof Object>[]> = {}
  if (!report.value) return groups
  for (const t of report.value.transfers) {
    const m = new Date(t.timestamp).getUTCMonth() + 1
    ;(groups[m] = groups[m] || []).push(t)
  }
  return groups
})

const expandedMonths = ref<Set<number>>(new Set())
const toggleMonth = (m: number) => {
  if (expandedMonths.value.has(m)) expandedMonths.value.delete(m)
  else expandedMonths.value.add(m)
}

const copyToClipboard = async (text: string) => {
  await doCopy(text)
}
</script>

<template>
  <div class="space-y-6">
    <div class="flex flex-wrap items-center gap-3">
      <NuxtLink
        :to="`/address/${address}`"
        class="inline-flex items-center gap-1 px-2 py-1 text-xs rounded bg-surface-elevated hover:bg-surface-hover text-foreground"
      >
        <ArrowLeft class="h-3 w-3" /> Back to address
      </NuxtLink>
      <h1 class="page-title flex items-center gap-2">
        <FileSpreadsheet class="h-5 w-5 text-accent" />
        Tax report
      </h1>
    </div>

    <!-- Header card -->
    <div class="card">
      <div class="flex flex-wrap items-center justify-between gap-4 mb-4">
        <div>
          <div class="text-xs text-foreground-muted">Address</div>
          <div class="font-mono text-sm break-all">
            <span v-if="report?.addressLabel" class="text-accent font-semibold mr-2">{{ report.addressLabel }}</span>
            <span class="text-foreground-muted">{{ address }}</span>
            <button
              type="button"
              class="ml-2 text-xs text-accent hover:underline"
              @click="copyToClipboard(address)"
            >copy</button>
          </div>
        </div>
        <div class="flex items-center gap-2">
          <label for="tax-year" class="text-xs text-foreground-muted">Year</label>
          <select
            id="tax-year"
            v-model.number="selectedYear"
            class="px-3 py-1.5 text-sm rounded-md bg-surface-elevated border border-border focus:outline-none focus:ring-1 focus:ring-accent"
          >
            <option v-for="y in availableYears" :key="y" :value="y">{{ y }}</option>
          </select>
          <button
            type="button"
            class="inline-flex items-center gap-1 px-3 py-1.5 text-xs rounded-md bg-accent text-white hover:bg-accent/90"
            @click="downloadCsv"
          >
            <Download class="h-3.5 w-3.5" /> CSV
          </button>
        </div>
      </div>

      <div v-if="pending && !report" class="loading py-12">Loading tax report…</div>
      <template v-else-if="report">
        <!-- Year summary -->
        <div class="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
          <div class="rounded p-3 bg-surface-elevated">
            <div class="text-xs text-foreground-muted">Opening balance</div>
            <div class="text-base font-mono">{{ formatAmount(report.openingBalance) }}</div>
            <div class="text-[10px] text-foreground-muted mt-0.5">{{ formatDate(report.periodStart) }}</div>
          </div>
          <div class="rounded p-3 bg-surface-elevated">
            <div class="text-xs text-foreground-muted">Closing balance</div>
            <div class="text-base font-mono"
                 :class="report.closingBalance >= report.openingBalance ? 'text-success' : 'text-destructive'">
              {{ formatAmount(report.closingBalance) }}
            </div>
            <div class="text-[10px] text-foreground-muted mt-0.5">{{ formatDate(report.periodEnd) }}</div>
          </div>
          <div class="rounded p-3 bg-surface-elevated">
            <div class="text-xs text-foreground-muted">Total received</div>
            <div class="text-base font-mono text-success">+{{ formatAmount(report.totalIn) }}</div>
            <div class="text-[10px] text-foreground-muted mt-0.5">{{ report.inboundCount.toLocaleString() }} transfers</div>
          </div>
          <div class="rounded p-3 bg-surface-elevated">
            <div class="text-xs text-foreground-muted">Total sent</div>
            <div class="text-base font-mono text-destructive">−{{ formatAmount(report.totalOut) }}</div>
            <div class="text-[10px] text-foreground-muted mt-0.5">{{ report.outboundCount.toLocaleString() }} transfers</div>
          </div>
        </div>

        <div v-if="report.truncated" class="rounded p-3 bg-warning/10 border border-warning/30 text-sm flex items-start gap-2 mb-4">
          <AlertTriangle class="h-4 w-4 text-warning shrink-0 mt-0.5" />
          <div>
            Transfer list was truncated at {{ report.maxTransfers.toLocaleString() }} rows.
            Monthly totals below cover only the rows we returned — for a complete year-end picture,
            use the CSV export (no per-call cap there).
          </div>
        </div>

        <!-- Monthly breakdown -->
        <h2 class="text-sm font-semibold mb-2">Monthly summary</h2>
        <div class="table-wrapper">
          <table class="text-sm">
            <thead>
              <tr>
                <th>Month</th>
                <th class="text-right">Opening</th>
                <th class="text-right">In</th>
                <th class="text-right">Out</th>
                <th class="text-right">Net change</th>
                <th class="text-right">Closing</th>
                <th class="text-right">Tx count</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="m in report.months" :key="m.month" class="hover:bg-surface-hover/30">
                <td class="font-semibold">{{ m.monthName }}</td>
                <td class="text-right font-mono">{{ formatAmount(m.openingBalance) }}</td>
                <td class="text-right font-mono text-success">
                  <template v-if="m.totalIn > 0">+{{ formatAmount(m.totalIn) }}</template>
                  <span v-else class="text-foreground-muted/50">—</span>
                </td>
                <td class="text-right font-mono text-destructive">
                  <template v-if="m.totalOut > 0">−{{ formatAmount(m.totalOut) }}</template>
                  <span v-else class="text-foreground-muted/50">—</span>
                </td>
                <td class="text-right font-mono"
                    :class="m.netChange > 0 ? 'text-success' : m.netChange < 0 ? 'text-destructive' : 'text-foreground-muted'">
                  {{ m.netChange === 0 ? '0' : (m.netChange > 0 ? '+' : '') + formatAmount(m.netChange) }}
                </td>
                <td class="text-right font-mono">{{ formatAmount(m.closingBalance) }}</td>
                <td class="text-right text-foreground-muted">{{ m.netCount }}</td>
                <td>
                  <button
                    v-if="m.netCount > 0"
                    type="button"
                    class="text-xs text-accent hover:underline"
                    @click="toggleMonth(m.month)"
                  >
                    {{ expandedMonths.has(m.month) ? 'hide' : 'show' }}
                  </button>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <!-- Per-month transfer detail (collapsible) -->
        <div v-for="m in report.months.filter(mm => expandedMonths.has(mm.month) && mm.netCount > 0)"
             :key="`detail-${m.month}`"
             class="mt-4">
          <h3 class="text-sm font-semibold mb-1">{{ m.monthName }} {{ report.year }}</h3>
          <div class="table-wrapper">
            <table class="text-xs">
              <thead>
                <tr>
                  <th>Date (UTC)</th>
                  <th>Dir</th>
                  <th>Counterparty</th>
                  <th class="text-right">Amount</th>
                  <th class="text-right">Balance</th>
                  <th>Tx</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(t, i) in (transfersByMonth[m.month] as any[])" :key="t.txHash + '-' + i">
                  <td class="font-mono whitespace-nowrap">{{ formatDateTime(t.timestamp) }}</td>
                  <td>
                    <span :class="t.direction === 'in' ? 'text-success' : 'text-destructive'" class="font-medium inline-flex items-center gap-0.5">
                      <ArrowDownLeft v-if="t.direction === 'in'" class="h-3 w-3" />
                      <ArrowUpRight v-else class="h-3 w-3" />
                      {{ t.direction === 'in' ? 'IN' : 'OUT' }}
                    </span>
                  </td>
                  <td class="font-mono">
                    <NuxtLink :to="`/address/${t.counterparty}`" class="text-accent hover:underline">
                      <span v-if="t.counterpartyLabel" class="font-semibold mr-1">{{ t.counterpartyLabel }}</span>
                      <span class="text-foreground-muted">{{ t.counterparty.slice(0, 8) }}…{{ t.counterparty.slice(-4) }}</span>
                    </NuxtLink>
                  </td>
                  <td class="text-right font-mono"
                      :class="t.direction === 'in' ? 'text-success' : 'text-destructive'">
                    {{ t.direction === 'in' ? '+' : '−' }}{{ formatAmount(t.amount) }}
                  </td>
                  <td class="text-right font-mono">{{ formatAmount(t.runningBalance) }}</td>
                  <td class="font-mono">
                    <NuxtLink v-if="t.txHash" :to="`/tx/${t.txHash}`" class="text-accent hover:underline">
                      {{ t.txHash.slice(0, 8) }}…
                    </NuxtLink>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

        <div v-if="report.totalCount === 0" class="text-center py-8 text-foreground-muted">
          No transfers in {{ report.year }} for this address.
        </div>

        <!-- Tax-filing notes -->
        <div class="mt-6 p-4 rounded bg-surface-elevated border border-border text-xs text-foreground-muted space-y-1.5">
          <div class="font-semibold text-foreground">Notes for tax filing</div>
          <ul class="list-disc list-inside space-y-1">
            <li>All amounts are in QU. To get fiat values, combine with daily QU/USD or QU/EUR prices from an external source (we don't keep historical price data).</li>
            <li>Timestamps are UTC. Balances are exact, reconstructed by summing every transfer that touched this address since chain start.</li>
            <li>The CSV is a flat list of every QU transfer in the year — open in Excel/LibreOffice or import into your tax tool.</li>
            <li>Asset transfers (non-QU) and contract execution-fee deductions are not included. This report is QU-only.</li>
          </ul>
        </div>
      </template>
    </div>
  </div>
</template>
