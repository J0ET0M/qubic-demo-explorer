<script setup lang="ts">
import type { TransactionDto } from '~/composables/useApi'

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

const formatAmount = (amount: number) => {
  // Qubic has no decimals, amount is already in QU
  const qu = Math.floor(amount)
  if (qu >= 1_000_000) return Math.floor(qu / 1_000_000).toLocaleString() + 'M'
  if (qu >= 1_000) return Math.floor(qu / 1_000).toLocaleString() + 'K'
  return qu.toLocaleString()
}

const truncateHash = (hash: string) => {
  return hash.slice(0, 8) + '...' + hash.slice(-8)
}

const truncateHashShort = (hash: string) => {
  return hash.slice(0, 6) + '...'
}

const formatDateTime = (dateStr: string) => {
  const date = new Date(dateStr)
  return date.toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  })
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
          <td class="amount">{{ formatAmount(tx.amount) }}</td>
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
