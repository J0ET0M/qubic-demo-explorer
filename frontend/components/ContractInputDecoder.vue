<script setup lang="ts">
import { Code, AlertCircle, ChevronDown, ChevronUp } from 'lucide-vue-next'
import type { DecodedInput } from '~/utils/contractInputDecoder'

const props = defineProps<{
  decoded: DecodedInput | null
  rawHex?: string | null
}>()

const showRaw = ref(false)
const showJson = ref(false)

const jsonOutput = computed(() => {
  if (!props.decoded) return '{}'
  const obj: Record<string, unknown> = {}
  for (const field of props.decoded.fields) {
    obj[field.name] = field.value
  }
  return JSON.stringify(obj, null, 2)
})

const truncateAddress = (addr: string) => {
  if (addr.length <= 16) return addr
  return `${addr.slice(0, 8)}...${addr.slice(-8)}`
}

const isAddress = (value: unknown): boolean => {
  return typeof value === 'string' && value.length === 60
}
</script>

<template>
  <div v-if="decoded" class="space-y-4">
    <!-- Decoded Fields -->
    <div class="space-y-2">
      <div
        v-for="field in decoded.fields"
        :key="field.name"
        class="detail-row"
      >
        <span class="detail-label flex items-center gap-2">
          {{ field.name }}
          <span v-if="field.description" class="text-foreground-muted text-xs" :title="field.description">
            ({{ field.type }})
          </span>
        </span>
        <span class="detail-value">
          <!-- Address type - make it a link -->
          <template v-if="field.type === 'id' && isAddress(field.value)">
            <AddressDisplay :address="field.value as string" />
          </template>
          <!-- Asset type - show both parts -->
          <template v-else-if="field.type === 'Asset' && typeof field.value === 'object'">
            <span class="font-mono">
              {{ (field.value as { assetName: string }).assetName }}
            </span>
            <span class="text-foreground-muted text-sm ml-2">
              by <AddressDisplay :address="(field.value as { issuer: string }).issuer" short />
            </span>
          </template>
          <!-- Asset name - highlight it -->
          <template v-else-if="field.type === 'assetName'">
            <span class="badge badge-accent">{{ field.displayValue }}</span>
          </template>
          <!-- Numeric types -->
          <template v-else>
            <span class="font-mono">{{ field.displayValue }}</span>
          </template>
        </span>
      </div>
    </div>

    <!-- Error message if any -->
    <div v-if="decoded.error" class="flex items-center gap-2 text-warning text-sm">
      <AlertCircle class="h-4 w-4" />
      {{ decoded.error }}
    </div>

    <!-- Toggle buttons -->
    <div class="flex gap-2 pt-2 border-t border-border">
      <button
        @click="showJson = !showJson"
        class="btn btn-ghost btn-sm flex items-center gap-1"
      >
        <Code class="h-4 w-4" />
        JSON
        <ChevronUp v-if="showJson" class="h-3 w-3" />
        <ChevronDown v-else class="h-3 w-3" />
      </button>
      <button
        v-if="rawHex"
        @click="showRaw = !showRaw"
        class="btn btn-ghost btn-sm flex items-center gap-1"
      >
        Raw Hex
        <ChevronUp v-if="showRaw" class="h-3 w-3" />
        <ChevronDown v-else class="h-3 w-3" />
      </button>
    </div>

    <!-- JSON Output -->
    <div v-if="showJson" class="bg-background-secondary rounded-lg p-3 overflow-x-auto">
      <pre class="text-xs font-mono text-foreground-muted">{{ jsonOutput }}</pre>
    </div>

    <!-- Raw Hex Output -->
    <div v-if="showRaw && rawHex" class="bg-background-secondary rounded-lg p-3 overflow-x-auto">
      <code class="text-xs font-mono text-foreground-muted break-all">{{ rawHex }}</code>
    </div>
  </div>

  <!-- Fallback when not decoded -->
  <div v-else-if="rawHex" class="space-y-2">
    <div class="text-sm text-foreground-muted">
      Unable to decode input data. Schema not available for this contract/procedure.
    </div>
    <button
      @click="showRaw = !showRaw"
      class="btn btn-ghost btn-sm flex items-center gap-1"
    >
      Raw Hex
      <ChevronUp v-if="showRaw" class="h-3 w-3" />
      <ChevronDown v-else class="h-3 w-3" />
    </button>
    <div v-if="showRaw" class="bg-background-secondary rounded-lg p-3 overflow-x-auto">
      <code class="text-xs font-mono text-foreground-muted break-all">{{ rawHex }}</code>
    </div>
  </div>
</template>
