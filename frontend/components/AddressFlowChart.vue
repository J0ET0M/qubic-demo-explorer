<script setup lang="ts">
import { ArrowRight, ArrowLeft } from 'lucide-vue-next'
import type { AddressFlowDto, FlowNodeDto } from '~/composables/useApi'

interface Props {
  flow: AddressFlowDto
}

const props = defineProps<Props>()

const { formatVolume, truncateAddress, getBadgeClass } = useFormatting()

const getDisplayName = (node: FlowNodeDto) => node.label || truncateAddress(node.address)

// Calculate max values for bar widths
const maxInbound = computed(() => {
  if (!props.flow.inbound.length) return 1
  return Math.max(...props.flow.inbound.map(n => n.totalAmount))
})

const maxOutbound = computed(() => {
  if (!props.flow.outbound.length) return 1
  return Math.max(...props.flow.outbound.map(n => n.totalAmount))
})

const getBarWidth = (amount: number, max: number) => {
  return Math.max(10, (amount / max) * 100)
}
</script>

<template>
  <div class="space-y-6">
    <!-- Sankey-style Flow Visualization -->
    <div class="grid grid-cols-1 lg:grid-cols-3 gap-4 items-start">
      <!-- Inbound (Left) -->
      <div class="space-y-2">
        <div class="text-sm font-medium text-foreground-muted flex items-center gap-2 mb-3">
          <ArrowRight class="h-4 w-4 text-success" />
          Inbound (Top Senders)
        </div>
        <div v-if="flow.inbound.length === 0" class="text-sm text-foreground-muted py-4">
          No inbound transactions
        </div>
        <div
          v-for="node in flow.inbound"
          :key="node.address"
          class="relative group"
        >
          <NuxtLink
            :to="`/address/${node.address}?tab=flow`"
            class="block p-2 rounded-lg border border-border hover:border-success/50 transition-colors"
          >
            <div class="flex items-center justify-between gap-2">
              <div class="min-w-0 flex-1">
                <div class="font-medium text-sm truncate">
                  {{ getDisplayName(node) }}
                </div>
                <div v-if="node.label" class="text-xs text-foreground-muted truncate">
                  {{ truncateAddress(node.address) }}
                </div>
              </div>
              <div class="text-right flex-shrink-0">
                <div class="text-sm font-semibold text-success">{{ formatVolume(node.totalAmount) }}</div>
                <div class="text-xs text-foreground-muted">{{ node.transactionCount }} tx</div>
              </div>
            </div>
            <!-- Flow bar -->
            <div class="mt-2 h-1 bg-surface-elevated rounded-full overflow-hidden">
              <div
                class="h-full bg-success rounded-full transition-all"
                :style="{ width: `${getBarWidth(node.totalAmount, maxInbound)}%` }"
              ></div>
            </div>
          </NuxtLink>
          <span
            v-if="node.type && node.type !== 'unknown'"
            :class="['badge text-xs absolute -top-2 -right-2', getBadgeClass(node.type)]"
          >
            {{ node.type }}
          </span>
        </div>
      </div>

      <!-- Center (Current Address) -->
      <div class="flex items-center justify-center py-8 lg:py-0">
        <div class="text-center p-4 rounded-xl bg-accent/10 border-2 border-accent">
          <div class="text-sm text-foreground-muted mb-1">Current Address</div>
          <div class="font-bold text-accent">
            {{ flow.label || truncateAddress(flow.address) }}
          </div>
          <div v-if="flow.label" class="text-xs text-foreground-muted mt-1">
            {{ truncateAddress(flow.address) }}
          </div>
          <div v-if="flow.type && flow.type !== 'unknown'" class="mt-2">
            <span :class="['badge text-xs', getBadgeClass(flow.type)]">
              {{ flow.type }}
            </span>
          </div>
        </div>
      </div>

      <!-- Outbound (Right) -->
      <div class="space-y-2">
        <div class="text-sm font-medium text-foreground-muted flex items-center gap-2 mb-3">
          <ArrowLeft class="h-4 w-4 text-warning" />
          Outbound (Top Receivers)
        </div>
        <div v-if="flow.outbound.length === 0" class="text-sm text-foreground-muted py-4">
          No outbound transactions
        </div>
        <div
          v-for="node in flow.outbound"
          :key="node.address"
          class="relative group"
        >
          <NuxtLink
            :to="`/address/${node.address}?tab=flow`"
            class="block p-2 rounded-lg border border-border hover:border-warning/50 transition-colors"
          >
            <div class="flex items-center justify-between gap-2">
              <div class="min-w-0 flex-1">
                <div class="font-medium text-sm truncate">
                  {{ getDisplayName(node) }}
                </div>
                <div v-if="node.label" class="text-xs text-foreground-muted truncate">
                  {{ truncateAddress(node.address) }}
                </div>
              </div>
              <div class="text-right flex-shrink-0">
                <div class="text-sm font-semibold text-warning">{{ formatVolume(node.totalAmount) }}</div>
                <div class="text-xs text-foreground-muted">{{ node.transactionCount }} tx</div>
              </div>
            </div>
            <!-- Flow bar -->
            <div class="mt-2 h-1 bg-surface-elevated rounded-full overflow-hidden">
              <div
                class="h-full bg-warning rounded-full transition-all"
                :style="{ width: `${getBarWidth(node.totalAmount, maxOutbound)}%` }"
              ></div>
            </div>
          </NuxtLink>
          <span
            v-if="node.type && node.type !== 'unknown'"
            :class="['badge text-xs absolute -top-2 -right-2', getBadgeClass(node.type)]"
          >
            {{ node.type }}
          </span>
        </div>
      </div>
    </div>

    <!-- Summary Stats -->
    <div class="grid grid-cols-2 lg:grid-cols-4 gap-4 pt-4 border-t border-border">
      <div class="text-center">
        <div class="text-2xl font-bold text-success">{{ flow.inbound.length }}</div>
        <div class="text-xs text-foreground-muted">Top Senders</div>
      </div>
      <div class="text-center">
        <div class="text-2xl font-bold text-success">
          {{ formatVolume(flow.inbound.reduce((sum, n) => sum + n.totalAmount, 0)) }}
        </div>
        <div class="text-xs text-foreground-muted">Inbound Volume</div>
      </div>
      <div class="text-center">
        <div class="text-2xl font-bold text-warning">{{ flow.outbound.length }}</div>
        <div class="text-xs text-foreground-muted">Top Receivers</div>
      </div>
      <div class="text-center">
        <div class="text-2xl font-bold text-warning">
          {{ formatVolume(flow.outbound.reduce((sum, n) => sum + n.totalAmount, 0)) }}
        </div>
        <div class="text-xs text-foreground-muted">Outbound Volume</div>
      </div>
    </div>
  </div>
</template>
