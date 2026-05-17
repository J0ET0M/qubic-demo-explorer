<script setup lang="ts">
import type { TransactionDto } from '~/composables/useApi'
import { getProcedureSchema, getContractSchema } from '~/utils/contractInputDecoder'

const props = defineProps<{
  transactions: TransactionDto[]
  highlightAddress?: string
}>()

const { getLabel, fetchLabelsForTransactions } = useAddressLabels()

// Fetch labels when transactions change
watch(() => props.transactions, async (txs) => {
  if (txs?.length) {
    await fetchLabelsForTransactions(txs)
  }
}, { immediate: true })

const { formatVolume, truncateHash, formatDateTime } = useFormatting()
const truncateHashShort = (hash: string) => hash.slice(0, 6) + '...'

// Decode a contract index directly from the address bytes. Qubic contract
// addresses have the canonical shape: <single non-A first char> + 55 A's + 4
// checksum chars. Index 0 = burn address → treated as "no contract" so the
// core-type fallback kicks in. Doesn't rely on the address-label bundle.
const contractIndexFromAddress = (addr: string): number | null => {
  if (!addr || addr.length !== 60) return null
  for (let i = 1; i < 56; i++) {
    if (addr.charCodeAt(i) !== 65) return null
  }
  const c = addr.charCodeAt(0)
  if (c < 65 || c > 90) return null
  const idx = c - 65
  return idx > 0 ? idx : null
}

// Resolve a human-readable name for a transaction. Priority:
//   1. inputType === 0 → "Transfer".
//   2. toAddress is a contract address → contract.procedure (resolved from address).
//   3. Backend-provided inputTypeName (only set for core txs to the burn address).
//   4. Fallback: "Type N".
// Returns { label, title } so callers can render a short label with a longer tooltip.
const txTypeInfo = (tx: TransactionDto): { label: string; title: string } => {
  if (tx.inputType === 0) {
    return { label: 'Transfer', title: 'Standard QU transfer' }
  }
  const idx = contractIndexFromAddress(tx.toAddress)
           ?? (getLabel(tx.toAddress)?.contractIndex ?? null)
  if (idx !== null && idx !== undefined) {
    const proc = getProcedureSchema(idx, tx.inputType)
    const contract = getContractSchema(idx)
    if (proc) {
      return {
        label: proc.name,
        title: `${contract?.name ?? `Contract ${idx}`}.${proc.name} (type ${tx.inputType})`
      }
    }
    if (contract) {
      return {
        label: `${contract.name} #${tx.inputType}`,
        title: `${contract.name} unknown procedure ${tx.inputType}`
      }
    }
  }
  if (tx.inputTypeName) {
    return { label: tx.inputTypeName, title: `Core: ${tx.inputTypeName} (type ${tx.inputType})` }
  }
  return { label: `Type ${tx.inputType}`, title: `Unknown input type ${tx.inputType}` }
}
</script>

<template>
  <div class="table-wrapper">
    <table>
      <thead>
        <tr>
          <th>Hash</th>
          <th class="hide-mobile">Epoch</th>
          <th class="hide-mobile">Tick</th>
          <th class="hide-mobile">Time</th>
          <th>From</th>
          <th>To</th>
          <th class="hide-mobile">Type</th>
          <th>Amount</th>
          <th class="hide-mobile">Status</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="tx in transactions" :key="tx.hash">
          <td class="hash">
            <NuxtLink :to="`/tx/${tx.hash}`">
              <span class="hide-mobile">{{ truncateHash(tx.hash) }}</span>
              <span class="show-mobile-only">{{ truncateHashShort(tx.hash) }}</span>
            </NuxtLink>
          </td>
          <td class="hide-mobile">
            <NuxtLink :to="`/epochs/${tx.epoch}`" class="text-accent">
              {{ tx.epoch }}
            </NuxtLink>
          </td>
          <td class="hide-mobile">
            <NuxtLink :to="`/ticks/${tx.tickNumber}`">
              {{ tx.tickNumber.toLocaleString() }}
            </NuxtLink>
          </td>
          <td class="hide-mobile text-foreground-muted text-sm">
            {{ formatDateTime(tx.timestamp) }}
          </td>
          <td class="address">
            <span class="hide-mobile">
              <AddressDisplay
                :address="tx.fromAddress"
                :label="getLabel(tx.fromAddress)"
                :highlight="tx.fromAddress === highlightAddress ? 'negative' : null"
              />
            </span>
            <span class="show-mobile-only">
              <AddressDisplay
                :address="tx.fromAddress"
                :label="getLabel(tx.fromAddress)"
                :highlight="tx.fromAddress === highlightAddress ? 'negative' : null"
                short
              />
            </span>
          </td>
          <td class="address">
            <span class="hide-mobile">
              <AddressDisplay
                :address="tx.toAddress"
                :label="getLabel(tx.toAddress)"
                :highlight="tx.toAddress === highlightAddress ? 'positive' : null"
              />
            </span>
            <span class="show-mobile-only">
              <AddressDisplay
                :address="tx.toAddress"
                :label="getLabel(tx.toAddress)"
                :highlight="tx.toAddress === highlightAddress ? 'positive' : null"
                short
              />
            </span>
          </td>
          <td class="hide-mobile text-foreground-muted text-xs">
            <span :title="txTypeInfo(tx).title" class="font-mono">{{ txTypeInfo(tx).label }}</span>
          </td>
          <td class="amount">{{ formatVolume(tx.amount) }}</td>
          <td class="hide-mobile">
            <span :class="['badge', tx.executed ? 'badge-success' : 'badge-error']">
              {{ tx.executed ? 'Success' : 'Failed' }}
            </span>
          </td>
        </tr>
      </tbody>
    </table>
  </div>
</template>
