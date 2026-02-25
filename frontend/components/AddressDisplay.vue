<script setup lang="ts">
import { Copy, Check, Star } from 'lucide-vue-next'
import type { AddressLabelDto } from '~/composables/useApi'

const props = withDefaults(defineProps<{
  address: string
  label?: AddressLabelDto | null
  short?: boolean
  link?: boolean
  highlight?: 'positive' | 'negative' | null
  actions?: boolean
}>(), {
  link: true,
  actions: true
})

const { isInPortfolio, addAddress, removeAddress } = usePortfolio()
const { show: showToast } = useToast()

const copied = ref(false)

const copyAddress = async () => {
  try {
    await navigator.clipboard.writeText(props.address)
    copied.value = true
    setTimeout(() => { copied.value = false }, 1500)
  } catch {}
}

const togglePortfolio = () => {
  if (isInPortfolio(props.address)) {
    removeAddress(props.address)
    showToast('Removed from portfolio', { type: 'info' })
  } else {
    addAddress(props.address)
    showToast('Added to portfolio', {
      type: 'success',
      action: { label: 'View Portfolio', to: '/portfolio' },
    })
  }
}

const truncateAddress = (address: string, short: boolean) => {
  if (short) {
    return address.slice(0, 4) + '...' + address.slice(-4)
  }
  return address.slice(0, 6) + '...' + address.slice(-6)
}

const displayText = computed(() => {
  const shortAddr = truncateAddress(props.address, props.short ?? false)
  if (props.label?.label) {
    return `${props.label.label} (${shortAddr})`
  }
  return shortAddr
})

const badgeClass = computed(() => {
  if (!props.label?.type || props.label.type === 'unknown') return ''
  switch (props.label.type) {
    case 'exchange':
      return 'address-badge-exchange'
    case 'smartcontract':
      return 'address-badge-contract'
    case 'tokenissuer':
      return 'address-badge-token'
    case 'burn':
      return 'address-badge-burn'
    default:
      return 'address-badge-known'
  }
})

const highlightClass = computed(() => {
  if (props.highlight === 'positive') return 'amount-positive'
  if (props.highlight === 'negative') return 'amount-negative'
  return ''
})
</script>

<template>
  <span class="address-display">
    <NuxtLink
      v-if="link"
      :to="`/address/${address}`"
      :class="[badgeClass, highlightClass]"
      :title="address"
    >
      {{ displayText }}
    </NuxtLink>
    <span
      v-else
      :class="[badgeClass, highlightClass]"
      :title="address"
    >
      {{ displayText }}
    </span>
    <span v-if="actions" class="address-actions">
      <button
        @click.prevent.stop="copyAddress"
        class="action-btn"
        :title="copied ? 'Copied!' : 'Copy address'"
      >
        <Check v-if="copied" class="h-3 w-3 text-success" />
        <Copy v-else class="h-3 w-3" />
      </button>
      <button
        @click.prevent.stop="togglePortfolio"
        class="action-btn"
        :class="{ 'text-warning': isInPortfolio(address) }"
        :title="isInPortfolio(address) ? 'Remove from portfolio' : 'Add to portfolio'"
      >
        <Star class="h-3 w-3" :class="{ 'fill-current': isInPortfolio(address) }" />
      </button>
    </span>
  </span>
</template>

<style scoped>
.address-display {
  display: inline-flex;
  align-items: center;
  position: relative;
}

.address-actions {
  display: inline-flex;
  align-items: center;
  gap: 0.125rem;
  opacity: 0;
  pointer-events: none;
  position: absolute;
  left: 100%;
  margin-left: 0.25rem;
  transition: opacity 0.15s;
}

.address-display:hover .address-actions {
  opacity: 1;
  pointer-events: auto;
}

.action-btn {
  padding: 0.125rem;
  border-radius: 0.25rem;
  color: var(--color-foreground-muted);
  cursor: pointer;
  transition: color 0.15s;
}

.action-btn:hover {
  color: var(--color-foreground);
}

/* Compact icons inside table cells */
:global(td) .address-actions {
  gap: 0;
}

:global(td) .action-btn {
  padding: 0.0625rem;
}

:global(td) .action-btn :deep(svg) {
  width: 0.625rem;
  height: 0.625rem;
}

.address-badge-exchange {
  color: var(--color-warning);
}

.address-badge-contract {
  color: var(--color-info);
}

.address-badge-token {
  color: var(--color-accent);
}

.address-badge-burn {
  color: var(--color-error);
}

.address-badge-known {
  color: var(--color-success);
}
</style>
