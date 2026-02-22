<script setup lang="ts">
import type { ParsedInputData } from '~/composables/useApi'
import { ChevronDown, ChevronUp, Code, Hash, Pickaxe, FileText, Radio, Cpu, HelpCircle, BarChart3 } from 'lucide-vue-next'

const props = defineProps<{
  parsed: ParsedInputData
  rawHex?: string | null
}>()

const showRaw = ref(false)
const showVotes = ref(false)
const showScores = ref(false)

const typeIcon = computed(() => {
  switch (props.parsed.typeName) {
    case 'VOTE_COUNTER': return BarChart3
    case 'MINING_SOLUTION': return Pickaxe
    case 'CUSTOM_MINING_SHARE_COUNTER': return Pickaxe
    case 'FILE_HEADER':
    case 'FILE_FRAGMENT':
    case 'FILE_TRAILER': return FileText
    case 'ORACLE_REPLY_COMMIT':
    case 'ORACLE_REPLY_REVEAL':
    case 'ORACLE_USER_QUERY': return Radio
    case 'EXECUTION_FEE_REPORT': return Cpu
    default: return HelpCircle
  }
})

const typeLabel = computed(() => {
  switch (props.parsed.typeName) {
    case 'VOTE_COUNTER': return 'Vote Counter'
    case 'MINING_SOLUTION': return 'Mining Solution'
    case 'CUSTOM_MINING_SHARE_COUNTER': return 'Custom Mining Share Counter'
    case 'FILE_HEADER': return 'File Header'
    case 'FILE_FRAGMENT': return 'File Fragment'
    case 'FILE_TRAILER': return 'File Trailer'
    case 'ORACLE_REPLY_COMMIT': return 'Oracle Reply Commit'
    case 'ORACLE_REPLY_REVEAL': return 'Oracle Reply Reveal'
    case 'ORACLE_USER_QUERY': return 'Oracle User Query'
    case 'EXECUTION_FEE_REPORT': return 'Execution Fee Report'
    default: return props.parsed.typeName
  }
})

const truncateHex = (hex: string, max = 16) => {
  if (hex.length <= max) return hex
  return `${hex.slice(0, max / 2)}...${hex.slice(-max / 2)}`
}

const formatFileSize = (bytes: number) => {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1048576) return `${(bytes / 1024).toFixed(1)} KB`
  if (bytes < 1073741824) return `${(bytes / 1048576).toFixed(1)} MB`
  return `${(bytes / 1073741824).toFixed(2)} GB`
}
</script>

<template>
  <div class="space-y-4">
    <!-- Type header badge -->
    <div class="flex items-center gap-2">
      <component :is="typeIcon" class="h-4 w-4 text-accent" />
      <span class="badge badge-accent">{{ typeLabel }}</span>
    </div>

    <!-- ========================================= -->
    <!-- VOTE_COUNTER -->
    <!-- ========================================= -->
    <template v-if="parsed.typeName === 'VOTE_COUNTER'">
      <div class="space-y-0">
        <div class="detail-row">
          <span class="detail-label">Total Votes</span>
          <span class="detail-value font-mono">{{ parsed.totalVotes?.toLocaleString() }}</span>
        </div>
        <div class="detail-row">
          <span class="detail-label">Computors with Votes</span>
          <span class="detail-value font-mono">{{ parsed.nonZeroCount }} / 676</span>
        </div>
        <div class="detail-row">
          <span class="detail-label">Data Lock</span>
          <span class="detail-value font-mono text-xs break-all">{{ parsed.dataLock }}</span>
        </div>
      </div>
      <!-- Expandable vote list -->
      <button
        @click="showVotes = !showVotes"
        class="btn btn-ghost btn-sm flex items-center gap-1"
      >
        <BarChart3 class="h-3.5 w-3.5" />
        Vote Data (676 computors)
        <ChevronUp v-if="showVotes" class="h-3 w-3" />
        <ChevronDown v-else class="h-3 w-3" />
      </button>
      <div v-if="showVotes && parsed.votes" class="bg-background-secondary rounded-lg p-3 overflow-x-auto">
        <div class="grid grid-cols-[repeat(auto-fill,minmax(5rem,1fr))] gap-1 text-xs font-mono">
          <div
            v-for="(vote, i) in parsed.votes"
            :key="i"
            class="px-1.5 py-0.5 rounded text-center"
            :class="vote > 0 ? 'bg-accent/10 text-accent-light' : 'text-foreground-muted/40'"
          >
            {{ i }}: {{ vote }}
          </div>
        </div>
      </div>
    </template>

    <!-- ========================================= -->
    <!-- MINING_SOLUTION -->
    <!-- ========================================= -->
    <template v-else-if="parsed.typeName === 'MINING_SOLUTION'">
      <div class="space-y-0">
        <div class="detail-row">
          <span class="detail-label">Mining Seed</span>
          <span class="detail-value font-mono text-xs break-all">{{ parsed.miningSeed }}</span>
        </div>
        <div class="detail-row">
          <span class="detail-label">Nonce</span>
          <span class="detail-value font-mono text-xs break-all">{{ parsed.nonce }}</span>
        </div>
      </div>
    </template>

    <!-- ========================================= -->
    <!-- CUSTOM_MINING_SHARE_COUNTER -->
    <!-- ========================================= -->
    <template v-else-if="parsed.typeName === 'CUSTOM_MINING_SHARE_COUNTER'">
      <div class="space-y-0">
        <div class="detail-row">
          <span class="detail-label">Total Score</span>
          <span class="detail-value font-mono">{{ parsed.totalScore?.toLocaleString() }}</span>
        </div>
        <div class="detail-row">
          <span class="detail-label">Active Computors</span>
          <span class="detail-value font-mono">{{ parsed.nonZeroCount }} / 676</span>
        </div>
        <div class="detail-row">
          <span class="detail-label">Data Lock</span>
          <span class="detail-value font-mono text-xs break-all">{{ parsed.dataLock }}</span>
        </div>
      </div>
      <button
        @click="showScores = !showScores"
        class="btn btn-ghost btn-sm flex items-center gap-1"
      >
        <BarChart3 class="h-3.5 w-3.5" />
        Score Data (676 computors)
        <ChevronUp v-if="showScores" class="h-3 w-3" />
        <ChevronDown v-else class="h-3 w-3" />
      </button>
      <div v-if="showScores && parsed.scores" class="bg-background-secondary rounded-lg p-3 overflow-x-auto">
        <div class="grid grid-cols-[repeat(auto-fill,minmax(5rem,1fr))] gap-1 text-xs font-mono">
          <div
            v-for="(score, i) in parsed.scores"
            :key="i"
            class="px-1.5 py-0.5 rounded text-center"
            :class="score > 0 ? 'bg-accent/10 text-accent-light' : 'text-foreground-muted/40'"
          >
            {{ i }}: {{ score }}
          </div>
        </div>
      </div>
    </template>

    <!-- ========================================= -->
    <!-- FILE_HEADER -->
    <!-- ========================================= -->
    <template v-else-if="parsed.typeName === 'FILE_HEADER'">
      <div class="space-y-0">
        <div class="detail-row">
          <span class="detail-label">File Size</span>
          <span class="detail-value font-mono">{{ formatFileSize(parsed.fileSize!) }} ({{ parsed.fileSize?.toLocaleString() }} bytes)</span>
        </div>
        <div class="detail-row">
          <span class="detail-label">Fragments</span>
          <span class="detail-value font-mono">{{ parsed.numberOfFragments?.toLocaleString() }}</span>
        </div>
        <div class="detail-row">
          <span class="detail-label">File Format</span>
          <span class="detail-value font-mono">{{ parsed.fileFormat || '(empty)' }}</span>
        </div>
      </div>
    </template>

    <!-- ========================================= -->
    <!-- FILE_FRAGMENT -->
    <!-- ========================================= -->
    <template v-else-if="parsed.typeName === 'FILE_FRAGMENT'">
      <div class="space-y-0">
        <div class="detail-row">
          <span class="detail-label">Fragment Index</span>
          <span class="detail-value font-mono">{{ parsed.fragmentIndex?.toLocaleString() }}</span>
        </div>
        <div class="detail-row">
          <span class="detail-label">Prev Fragment Digest</span>
          <span class="detail-value font-mono text-xs break-all">{{ parsed.prevFileFragmentTransactionDigest }}</span>
        </div>
        <div class="detail-row">
          <span class="detail-label">Payload Size</span>
          <span class="detail-value font-mono">{{ formatFileSize(parsed.payloadSize!) }}</span>
        </div>
      </div>
    </template>

    <!-- ========================================= -->
    <!-- FILE_TRAILER -->
    <!-- ========================================= -->
    <template v-else-if="parsed.typeName === 'FILE_TRAILER'">
      <div class="space-y-0">
        <div class="detail-row">
          <span class="detail-label">File Size</span>
          <span class="detail-value font-mono">{{ formatFileSize(parsed.fileSize!) }} ({{ parsed.fileSize?.toLocaleString() }} bytes)</span>
        </div>
        <div class="detail-row">
          <span class="detail-label">Fragments</span>
          <span class="detail-value font-mono">{{ parsed.numberOfFragments?.toLocaleString() }}</span>
        </div>
        <div class="detail-row">
          <span class="detail-label">File Format</span>
          <span class="detail-value font-mono">{{ parsed.fileFormat || '(empty)' }}</span>
        </div>
        <div class="detail-row">
          <span class="detail-label">Last Fragment Digest</span>
          <span class="detail-value font-mono text-xs break-all">{{ parsed.lastFileFragmentTransactionDigest }}</span>
        </div>
      </div>
    </template>

    <!-- ========================================= -->
    <!-- ORACLE_REPLY_COMMIT -->
    <!-- ========================================= -->
    <template v-else-if="parsed.typeName === 'ORACLE_REPLY_COMMIT'">
      <div class="text-sm text-foreground-muted mb-2">{{ parsed.items?.length }} commit item(s)</div>
      <div v-for="(item, i) in parsed.items" :key="i" class="space-y-0 mb-3">
        <div v-if="(parsed.items?.length ?? 0) > 1" class="text-xs font-semibold text-foreground-muted/70 uppercase tracking-wider mb-1">
          Item {{ i + 1 }}
        </div>
        <div class="detail-row">
          <span class="detail-label">Query ID</span>
          <span class="detail-value font-mono">{{ item.queryId }}</span>
        </div>
        <div class="detail-row">
          <span class="detail-label">Reply Digest</span>
          <span class="detail-value font-mono text-xs break-all">{{ item.replyDigest }}</span>
        </div>
        <div class="detail-row">
          <span class="detail-label">Knowledge Proof</span>
          <span class="detail-value font-mono text-xs break-all">{{ item.replyKnowledgeProof }}</span>
        </div>
      </div>
    </template>

    <!-- ========================================= -->
    <!-- ORACLE_REPLY_REVEAL -->
    <!-- ========================================= -->
    <template v-else-if="parsed.typeName === 'ORACLE_REPLY_REVEAL'">
      <div class="space-y-0">
        <div class="detail-row">
          <span class="detail-label">Query ID</span>
          <span class="detail-value font-mono">{{ parsed.queryId }}</span>
        </div>
        <div class="detail-row">
          <span class="detail-label">Reply Data Size</span>
          <span class="detail-value font-mono">{{ parsed.replyDataSize }} bytes</span>
        </div>
        <div v-if="parsed.replyDataHex" class="detail-row">
          <span class="detail-label">Reply Data</span>
          <span class="detail-value font-mono text-xs break-all">{{ parsed.replyDataHex }}</span>
        </div>
      </div>
    </template>

    <!-- ========================================= -->
    <!-- EXECUTION_FEE_REPORT -->
    <!-- ========================================= -->
    <template v-else-if="parsed.typeName === 'EXECUTION_FEE_REPORT'">
      <div class="space-y-0">
        <div class="detail-row">
          <span class="detail-label">Phase Number</span>
          <span class="detail-value font-mono">{{ parsed.phaseNumber?.toLocaleString() }}</span>
        </div>
        <div class="detail-row">
          <span class="detail-label">Contracts Reported</span>
          <span class="detail-value font-mono">{{ parsed.numEntries }}</span>
        </div>
        <div v-if="parsed.dataLock" class="detail-row">
          <span class="detail-label">Data Lock</span>
          <span class="detail-value font-mono text-xs break-all">{{ parsed.dataLock }}</span>
        </div>
      </div>
      <div v-if="parsed.entries?.length" class="table-wrapper mt-3">
        <table>
          <thead>
            <tr>
              <th>Contract Index</th>
              <th>Execution Fee</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="entry in parsed.entries" :key="entry.contractIndex">
              <td class="font-mono">{{ entry.contractIndex }}</td>
              <td class="font-mono amount">{{ entry.executionFee.toLocaleString() }} QU</td>
            </tr>
          </tbody>
        </table>
      </div>
    </template>

    <!-- ========================================= -->
    <!-- ORACLE_USER_QUERY -->
    <!-- ========================================= -->
    <template v-else-if="parsed.typeName === 'ORACLE_USER_QUERY'">
      <div class="space-y-0">
        <div class="detail-row">
          <span class="detail-label">Oracle Interface</span>
          <span class="detail-value font-mono">{{ parsed.oracleInterfaceIndex }}</span>
        </div>
        <div class="detail-row">
          <span class="detail-label">Timeout</span>
          <span class="detail-value font-mono">{{ parsed.timeoutMilliseconds?.toLocaleString() }} ms</span>
        </div>
        <div v-if="parsed.queryDataSize && parsed.queryDataSize > 0" class="detail-row">
          <span class="detail-label">Query Data Size</span>
          <span class="detail-value font-mono">{{ parsed.queryDataSize }} bytes</span>
        </div>
        <div v-if="parsed.queryDataHex" class="detail-row">
          <span class="detail-label">Query Data</span>
          <span class="detail-value font-mono text-xs break-all">{{ parsed.queryDataHex }}</span>
        </div>
      </div>
    </template>

    <!-- ========================================= -->
    <!-- Raw hex toggle -->
    <!-- ========================================= -->
    <div v-if="rawHex" class="pt-2 border-t border-border">
      <button
        @click="showRaw = !showRaw"
        class="btn btn-ghost btn-sm flex items-center gap-1"
      >
        <Hash class="h-3.5 w-3.5" />
        Raw Hex
        <ChevronUp v-if="showRaw" class="h-3 w-3" />
        <ChevronDown v-else class="h-3 w-3" />
      </button>
      <div v-if="showRaw" class="mt-2 bg-background-secondary rounded-lg p-3 overflow-x-auto">
        <code class="text-xs font-mono text-foreground-muted break-all">{{ rawHex }}</code>
      </div>
    </div>
  </div>
</template>
