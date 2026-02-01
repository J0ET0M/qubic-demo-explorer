<script setup lang="ts">
import type { AddressLabelDto } from '~/composables/useApi'

const props = withDefaults(defineProps<{
  address: string
  label?: AddressLabelDto | null
  short?: boolean
  link?: boolean
  highlight?: 'positive' | 'negative' | null
}>(), {
  link: true
})

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
</template>

<style scoped>
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
