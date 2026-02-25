<script setup lang="ts">
import { GitCompare, Search } from 'lucide-vue-next'

useHead({ title: 'Address Comparison - QLI Explorer' })

const api = useApi()
const { getLabel, fetchLabels } = useAddressLabels()

const address1 = ref('')
const address2 = ref('')
const address3 = ref('')
const loading = ref(false)
const results = ref<any[]>([])

const handleCompare = async () => {
  const addrs = [address1.value, address2.value, address3.value]
    .map(a => a.trim())
    .filter(a => a.length >= 50)

  if (addrs.length < 2) return

  loading.value = true
  try {
    const data = await api.getAddressesBatch(addrs)
    results.value = data
    await fetchLabels(addrs)
  } catch (e) {
    console.error('Failed to compare:', e)
  } finally {
    loading.value = false
  }
}

const formatAmount = (amount: number) => {
  if (amount >= 1_000_000_000_000) return (amount / 1_000_000_000_000).toFixed(2) + 'T'
  if (amount >= 1_000_000_000) return (amount / 1_000_000_000).toFixed(2) + 'B'
  if (amount >= 1_000_000) return (amount / 1_000_000).toFixed(2) + 'M'
  if (amount >= 1_000) return (amount / 1_000).toFixed(2) + 'K'
  return amount.toLocaleString()
}

const truncateAddress = (addr: string) =>
  addr.length > 16 ? addr.slice(0, 8) + '...' + addr.slice(-8) : addr

const metrics = [
  { key: 'balance', label: 'Balance', format: (v: number) => formatAmount(v) + ' QU', color: 'text-accent' },
  { key: 'incomingAmount', label: 'Incoming', format: (v: number) => formatAmount(v) + ' QU', color: 'text-success' },
  { key: 'outgoingAmount', label: 'Outgoing', format: (v: number) => formatAmount(v) + ' QU', color: 'text-destructive' },
  { key: 'txCount', label: 'Transactions', format: (v: number) => v.toLocaleString(), color: 'text-accent' },
  { key: 'transferCount', label: 'Transfers', format: (v: number) => v.toLocaleString(), color: 'text-accent' },
]
</script>

<template>
  <div class="space-y-6">
    <h1 class="page-title flex items-center gap-2">
      <GitCompare class="h-5 w-5 text-accent" />
      Address Comparison
    </h1>

    <!-- Input -->
    <div class="card space-y-3">
      <div>
        <label class="block text-xs font-medium mb-1">Address 1</label>
        <input v-model="address1" type="text" class="input w-full" placeholder="Enter first address..." />
      </div>
      <div>
        <label class="block text-xs font-medium mb-1">Address 2</label>
        <input v-model="address2" type="text" class="input w-full" placeholder="Enter second address..." />
      </div>
      <div>
        <label class="block text-xs font-medium mb-1">Address 3 (optional)</label>
        <input v-model="address3" type="text" class="input w-full" placeholder="Enter third address..." />
      </div>
      <button
        @click="handleCompare"
        :disabled="loading || !address1.trim() || !address2.trim()"
        class="btn btn-primary flex items-center gap-1.5"
      >
        <Search class="h-4 w-4" />
        Compare
      </button>
    </div>

    <!-- Loading -->
    <div v-if="loading" class="card">
      <div class="loading py-12">Comparing addresses...</div>
    </div>

    <!-- Results -->
    <template v-else-if="results.length >= 2">
      <!-- Side by side cards -->
      <div :class="`grid grid-cols-1 md:grid-cols-${results.length} gap-4`">
        <div v-for="addr in results" :key="addr.address" class="card-elevated">
          <NuxtLink :to="`/address/${addr.address}`" class="text-accent font-mono text-xs block mb-3">
            {{ getLabel(addr.address)?.label || truncateAddress(addr.address) }}
          </NuxtLink>
          <div class="space-y-2">
            <div v-for="m in metrics" :key="m.key">
              <div class="text-xs text-foreground-muted">{{ m.label }}</div>
              <div class="font-semibold" :class="m.color">{{ m.format(addr[m.key]) }}</div>
            </div>
          </div>
        </div>
      </div>

      <!-- Comparison Table -->
      <div class="card">
        <h2 class="section-title mb-4">Detailed Comparison</h2>
        <div class="table-wrapper">
          <table>
            <thead>
              <tr>
                <th>Metric</th>
                <th v-for="addr in results" :key="addr.address" class="text-right">
                  {{ getLabel(addr.address)?.label || truncateAddress(addr.address) }}
                </th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="m in metrics" :key="m.key">
                <td class="font-medium">{{ m.label }}</td>
                <td v-for="addr in results" :key="addr.address" class="text-right" :class="m.color">
                  {{ m.format(addr[m.key]) }}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </template>
  </div>
</template>
