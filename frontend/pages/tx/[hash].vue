<script setup lang="ts">
import type { TransactionDetailDto, SpecialTransactionDto } from '~/composables/useApi'
import { ArrowLeftRight, Copy, Check, FileText, Zap, Code } from 'lucide-vue-next'
import { decodeContractInput, type DecodedInput } from '~/utils/contractInputDecoder'

const api = useApi()
const route = useRoute()
const { getLabel, fetchLabels, fetchLabelsForTransfers } = useAddressLabels()

const hash = computed(() => route.params.hash as string)
const copied = ref(false)

const { data: tx, pending, error } = await useAsyncData(
  () => `tx-${hash.value}`,
  () => api.getTransaction(hash.value),
  { watch: [hash] }
)

// Type guard to check if it's a special transaction
const isSpecialTx = (data: TransactionDetailDto | SpecialTransactionDto | null): data is SpecialTransactionDto => {
  return data !== null && 'specialType' in data
}

// Computed properties to access typed data
const regularTx = computed(() => !isSpecialTx(tx.value) ? tx.value as TransactionDetailDto : null)
const specialTx = computed(() => isSpecialTx(tx.value) ? tx.value : null)

// Procedure name for smart contract calls
const procedureName = ref<string | null>(null)
const contractIndex = ref<number | null>(null)

// Decoded contract input
const decodedInput = ref<DecodedInput | null>(null)

// Fetch labels for addresses in this transaction and procedure name if applicable
watch(tx, async (txData) => {
  if (txData) {
    if (!isSpecialTx(txData)) {
      const addresses = [txData.fromAddress, txData.toAddress]
      await fetchLabels(addresses)

      // Fetch procedure name and contract index if this is a smart contract call
      if (txData.inputType > 0) {
        try {
          const result = await api.getProcedureName(txData.toAddress, txData.inputType)
          procedureName.value = result.procedureName

          // Get contract index from label info
          const labelInfo = await api.getAddressLabel(txData.toAddress)
          contractIndex.value = labelInfo.contractIndex ?? null

          // Decode input data if we have contract index
          if (contractIndex.value && txData.inputData) {
            decodedInput.value = decodeContractInput(
              txData.inputData,
              contractIndex.value,
              txData.inputType
            )
          } else {
            decodedInput.value = null
          }
        } catch {
          procedureName.value = null
          contractIndex.value = null
          decodedInput.value = null
        }
      } else {
        procedureName.value = null
        contractIndex.value = null
        decodedInput.value = null
      }
    }
    if (txData.logs?.length) {
      await fetchLabelsForTransfers(txData.logs)
    }
  }
}, { immediate: true })

const formatDate = (date: string) => {
  return new Date(date).toLocaleString()
}

const formatAmount = (amount: number) => {
  // Qubic has no decimals, amount is already in QU
  return Math.floor(amount).toLocaleString()
}

const copyToClipboard = async (text: string) => {
  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(text)
    } else {
      // Fallback for non-secure contexts
      const textArea = document.createElement('textarea')
      textArea.value = text
      textArea.style.position = 'fixed'
      textArea.style.left = '-999999px'
      textArea.style.top = '-999999px'
      document.body.appendChild(textArea)
      textArea.focus()
      textArea.select()
      document.execCommand('copy')
      textArea.remove()
    }
    copied.value = true
    setTimeout(() => copied.value = false, 2000)
  } catch (err) {
    console.error('Failed to copy:', err)
  }
}
</script>

<template>
  <div class="space-y-6">
    <div v-if="pending" class="loading">Loading...</div>

    <div v-else-if="error" class="card">
      <div class="text-center py-8">
        <ArrowLeftRight class="h-12 w-12 text-foreground-muted mx-auto mb-4" />
        <h2 class="text-xl font-semibold mb-2">Transaction Not Found</h2>
        <p class="text-foreground-muted text-sm break-all max-w-md mx-auto">
          {{ hash }}
        </p>
        <NuxtLink to="/transactions" class="btn btn-primary mt-4">
          Back to Transactions
        </NuxtLink>
      </div>
    </div>

    <!-- Special Transaction (Smart Contract Lifecycle Event) -->
    <template v-else-if="specialTx">
      <div class="card">
        <h2 class="section-title mb-4">
          <Zap class="h-5 w-5 text-warning" />
          Smart Contract Event
        </h2>

        <div class="space-y-0">
          <div class="detail-row">
            <span class="detail-label">Event ID</span>
            <span class="detail-value flex items-center gap-2 flex-wrap">
              <span class="hash break-all">{{ specialTx.txHash }}</span>
              <button
                @click="copyToClipboard(specialTx.txHash)"
                class="btn btn-ghost p-1"
                :title="copied ? 'Copied!' : 'Copy'"
              >
                <Check v-if="copied" class="h-4 w-4 text-success" />
                <Copy v-else class="h-4 w-4" />
              </button>
            </span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Type</span>
            <span class="detail-value">
              <span class="badge badge-warning">{{ specialTx.specialTypeName }}</span>
            </span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Tick</span>
            <span class="detail-value">
              <NuxtLink :to="`/ticks/${specialTx.tickNumber}`" class="font-medium">
                {{ specialTx.tickNumber.toLocaleString() }}
              </NuxtLink>
            </span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Timestamp</span>
            <span class="detail-value">{{ formatDate(specialTx.timestamp) }}</span>
          </div>
        </div>

        <div class="mt-4 p-3 bg-background-secondary rounded-lg text-sm text-foreground-muted">
          This is a virtual transaction generated by the Qubic protocol for smart contract lifecycle events.
          It is not a regular transaction with a sender and receiver.
        </div>
      </div>

      <!-- Logs for Special Transaction -->
      <div class="card" v-if="specialTx.logs?.length">
        <h2 class="section-title mb-4">
          <FileText class="h-5 w-5 text-accent" />
          Associated Logs ({{ specialTx.logs.length }})
        </h2>

        <div class="table-wrapper">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Type</th>
                <th class="hide-mobile">Source</th>
                <th class="hide-mobile">Destination</th>
                <th>Amount</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="log in specialTx.logs" :key="log.logId">
                <td>{{ log.logId }}</td>
                <td>
                  <span class="badge badge-info">{{ log.logTypeName }}</span>
                </td>
                <td class="hide-mobile address">
                  <AddressDisplay
                    v-if="log.sourceAddress"
                    :address="log.sourceAddress"
                    :label="getLabel(log.sourceAddress)"
                  />
                  <span v-else class="text-foreground-muted">-</span>
                </td>
                <td class="hide-mobile address">
                  <AddressDisplay
                    v-if="log.destAddress"
                    :address="log.destAddress"
                    :label="getLabel(log.destAddress)"
                  />
                  <span v-else class="text-foreground-muted">-</span>
                </td>
                <td class="amount">{{ formatAmount(log.amount) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <div class="card" v-else>
        <div class="text-center py-4 text-foreground-muted">
          No logs associated with this event.
        </div>
      </div>
    </template>

    <!-- Regular Transaction -->
    <template v-else-if="regularTx">
      <div class="card">
        <h2 class="section-title mb-4">
          <ArrowLeftRight class="h-5 w-5 text-accent" />
          Transaction Overview
        </h2>

        <div class="space-y-0">
          <div class="detail-row">
            <span class="detail-label">Hash</span>
            <span class="detail-value flex items-center gap-2 flex-wrap">
              <span class="hash break-all">{{ regularTx.hash }}</span>
              <button
                @click="copyToClipboard(regularTx.hash)"
                class="btn btn-ghost p-1"
                :title="copied ? 'Copied!' : 'Copy'"
              >
                <Check v-if="copied" class="h-4 w-4 text-success" />
                <Copy v-else class="h-4 w-4" />
              </button>
            </span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Status</span>
            <span class="detail-value">
              <span :class="['badge', regularTx.executed ? 'badge-success' : 'badge-error']">
                {{ regularTx.executed ? 'Executed' : 'Failed' }}
              </span>
            </span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Tick</span>
            <span class="detail-value">
              <NuxtLink :to="`/ticks/${regularTx.tickNumber}`" class="font-medium">
                {{ regularTx.tickNumber.toLocaleString() }}
              </NuxtLink>
            </span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Epoch</span>
            <span class="detail-value">{{ regularTx.epoch }}</span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Timestamp</span>
            <span class="detail-value">{{ formatDate(regularTx.timestamp) }}</span>
          </div>
          <div class="detail-row">
            <span class="detail-label">From</span>
            <span class="detail-value">
              <AddressDisplay :address="regularTx.fromAddress" :label="getLabel(regularTx.fromAddress)" />
            </span>
          </div>
          <div class="detail-row">
            <span class="detail-label">To</span>
            <span class="detail-value">
              <AddressDisplay :address="regularTx.toAddress" :label="getLabel(regularTx.toAddress)" />
            </span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Amount</span>
            <span class="detail-value amount text-accent font-semibold">
              {{ formatAmount(regularTx.amount) }} QU
            </span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Input Type</span>
            <span class="detail-value">
              <template v-if="regularTx.inputType === 0">Transfer</template>
              <template v-else-if="regularTx.inputTypeName">
                <span class="badge badge-warning mr-2">{{ regularTx.inputTypeName }}</span>
                <span class="text-foreground-muted text-sm">(Type {{ regularTx.inputType }})</span>
              </template>
              <template v-else>
                <span class="badge badge-info mr-2">{{ procedureName || `Procedure #${regularTx.inputType}` }}</span>
                <span class="text-foreground-muted text-sm">(ID: {{ regularTx.inputType }})</span>
              </template>
            </span>
          </div>
        </div>
      </div>

      <!-- Core System Transaction Input Data -->
      <div class="card" v-if="regularTx.parsedInput">
        <h2 class="section-title mb-4">
          <Code class="h-5 w-5 text-accent" />
          Input Data
        </h2>
        <CoreInputDataViewer
          :parsed="regularTx.parsedInput"
          :raw-hex="regularTx.inputData"
        />
      </div>

      <!-- Smart Contract Input Data -->
      <div class="card" v-else-if="regularTx.inputType > 0 && regularTx.inputData">
        <h2 class="section-title mb-4">
          <Code class="h-5 w-5 text-accent" />
          Input Data
        </h2>
        <ContractInputDecoder
          :decoded="decodedInput"
          :raw-hex="regularTx.inputData"
        />
      </div>

      <!-- Logs -->
      <div class="card" v-if="regularTx.logs?.length">
        <h2 class="section-title mb-4">
          <FileText class="h-5 w-5 text-accent" />
          Logs ({{ regularTx.logs.length }})
        </h2>

        <div class="table-wrapper">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Type</th>
                <th class="hide-mobile">Source</th>
                <th class="hide-mobile">Destination</th>
                <th>Amount</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="log in regularTx.logs" :key="log.logId">
                <td>{{ log.logId }}</td>
                <td>
                  <span class="badge badge-info">{{ log.logTypeName }}</span>
                </td>
                <td class="hide-mobile address">
                  <AddressDisplay
                    v-if="log.sourceAddress"
                    :address="log.sourceAddress"
                    :label="getLabel(log.sourceAddress)"
                  />
                  <span v-else class="text-foreground-muted">-</span>
                </td>
                <td class="hide-mobile address">
                  <AddressDisplay
                    v-if="log.destAddress"
                    :address="log.destAddress"
                    :label="getLabel(log.destAddress)"
                  />
                  <span v-else class="text-foreground-muted">-</span>
                </td>
                <td class="amount">{{ formatAmount(log.amount) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </template>
  </div>
</template>
