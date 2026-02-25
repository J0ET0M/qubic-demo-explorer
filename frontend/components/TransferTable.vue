<script setup lang="ts">
import type { TransferDto } from '~/composables/useApi'

const props = defineProps<{
  transfers: TransferDto[]
  highlightAddress?: string
  hideTick?: boolean
}>()

const { getLabel, fetchLabelsForTransfers } = useAddressLabels()

// Fetch labels when transfers change
watch(() => props.transfers, async (transfers) => {
  if (transfers?.length) {
    await fetchLabelsForTransfers(transfers)
  }
}, { immediate: true })

const { formatVolume, getLogTypeBadgeClass, formatDateTime } = useFormatting()
</script>

<template>
  <div class="table-wrapper">
    <table>
      <thead>
        <tr>
          <th v-if="!hideTick" class="hide-mobile">Epoch</th>
          <th v-if="!hideTick" class="hide-mobile">Tick</th>
          <th class="hide-mobile">Time</th>
          <th class="hide-mobile">TX</th>
          <th>Type</th>
          <th>From</th>
          <th>To</th>
          <th>Amount</th>
          <th class="hide-mobile">Asset</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="transfer in transfers" :key="`${transfer.tickNumber}-${transfer.logId}`">
          <td v-if="!hideTick" class="hide-mobile">
            <NuxtLink :to="`/epochs/${transfer.epoch}`" class="text-accent">
              {{ transfer.epoch }}
            </NuxtLink>
          </td>
          <td v-if="!hideTick" class="hide-mobile">
            <NuxtLink :to="`/ticks/${transfer.tickNumber}`">
              {{ transfer.tickNumber.toLocaleString() }}
            </NuxtLink>
          </td>
          <td class="hide-mobile text-foreground-muted text-sm">
            {{ formatDateTime(transfer.timestamp) }}
          </td>
          <td class="hide-mobile">
            <NuxtLink
              v-if="transfer.txHash"
              :to="`/tx/${transfer.txHash}`"
              class="hash text-accent"
              :title="transfer.txHash"
            >
              {{ transfer.txHash.slice(0, 8) }}...
            </NuxtLink>
            <span v-else class="text-foreground-muted">-</span>
          </td>
          <td>
            <span :class="['badge', getLogTypeBadgeClass(transfer.logType)]">
              {{ transfer.logTypeName }}
            </span>
          </td>
          <td class="address">
            <template v-if="transfer.sourceAddress">
              <span class="hide-mobile">
                <AddressDisplay
                  :address="transfer.sourceAddress"
                  :label="getLabel(transfer.sourceAddress)"
                  :highlight="transfer.sourceAddress === highlightAddress ? 'negative' : null"
                />
              </span>
              <span class="show-mobile-only">
                <AddressDisplay
                  :address="transfer.sourceAddress"
                  :label="getLabel(transfer.sourceAddress)"
                  :highlight="transfer.sourceAddress === highlightAddress ? 'negative' : null"
                  short
                />
              </span>
            </template>
            <span v-else class="text-foreground-muted">-</span>
          </td>
          <td class="address">
            <template v-if="transfer.destAddress">
              <span class="hide-mobile">
                <AddressDisplay
                  :address="transfer.destAddress"
                  :label="getLabel(transfer.destAddress)"
                  :highlight="transfer.destAddress === highlightAddress ? 'positive' : null"
                />
              </span>
              <span class="show-mobile-only">
                <AddressDisplay
                  :address="transfer.destAddress"
                  :label="getLabel(transfer.destAddress)"
                  :highlight="transfer.destAddress === highlightAddress ? 'positive' : null"
                  short
                />
              </span>
            </template>
            <span v-else class="text-foreground-muted">-</span>
          </td>
          <td class="amount">{{ formatVolume(transfer.amount) }}</td>
          <td class="hide-mobile">{{ transfer.assetName || 'QU' }}</td>
        </tr>
      </tbody>
    </table>
  </div>
</template>
