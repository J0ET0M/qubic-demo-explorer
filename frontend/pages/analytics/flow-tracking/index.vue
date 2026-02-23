<script setup lang="ts">
import { Plus, Minus, Radar, Loader2, ExternalLink, Copy, Check } from 'lucide-vue-next'

const api = useApi()
const router = useRouter()

const alias = ref('')
const startTick = ref<number | null>(null)
const addresses = ref<{ value: string; balance: number | null }[]>([
  { value: '', balance: null }
])
const maxHops = ref(10)
const isSubmitting = ref(false)
const error = ref('')
const copied = ref(false)

// Recent trackings from localStorage
const recentTrackings = ref<{ jobId: string; alias: string; addresses: string[]; createdAt: string }[]>([])

onMounted(() => {
  const stored = localStorage.getItem('customFlowTrackings')
  if (stored) {
    try { recentTrackings.value = JSON.parse(stored) } catch {}
  }
})

const addAddress = () => {
  if (addresses.value.length < 5) {
    addresses.value.push({ value: '', balance: null })
  }
}

const removeAddress = (index: number) => {
  if (addresses.value.length > 1) {
    addresses.value.splice(index, 1)
  }
}

const submit = async () => {
  error.value = ''
  isSubmitting.value = true

  try {
    const validAddresses = addresses.value
      .map(a => a.value.trim().toUpperCase())
      .filter(a => a.length === 60)

    if (validAddresses.length === 0) {
      error.value = 'At least one valid 60-character Qubic address is required'
      return
    }

    if (!startTick.value || startTick.value <= 0) {
      error.value = 'Start tick must be greater than 0'
      return
    }

    const balances = addresses.value
      .filter(a => a.value.trim().length === 60)
      .map(a => a.balance && a.balance > 0 ? a.balance : 0)

    const hasCustomBalances = balances.some(b => b > 0)

    const result = await api.createCustomFlow({
      addresses: validAddresses,
      startTick: startTick.value,
      balances: hasCustomBalances ? balances : undefined,
      alias: alias.value.trim() || undefined,
      maxHops: maxHops.value,
    })

    // Save to localStorage
    const tracking = {
      jobId: result.jobId,
      alias: alias.value.trim() || `Tracking ${validAddresses.length} address${validAddresses.length > 1 ? 'es' : ''}`,
      addresses: validAddresses,
      createdAt: new Date().toISOString(),
    }
    recentTrackings.value.unshift(tracking)
    if (recentTrackings.value.length > 20) recentTrackings.value = recentTrackings.value.slice(0, 20)
    localStorage.setItem('customFlowTrackings', JSON.stringify(recentTrackings.value))

    router.push(`/analytics/flow-tracking/${result.jobId}`)
  } catch (e: any) {
    error.value = e?.message || 'Failed to create flow tracking'
  } finally {
    isSubmitting.value = false
  }
}

const copyJobId = (jobId: string) => {
  navigator.clipboard.writeText(`${window.location.origin}/analytics/flow-tracking/${jobId}`)
  copied.value = true
  setTimeout(() => copied.value = false, 2000)
}

const shortenAddress = (addr: string) => addr.slice(0, 8) + '...' + addr.slice(-8)

useHead({ title: 'Flow Tracking' })
</script>

<template>
  <div class="space-y-6">
    <div>
      <h1 class="text-2xl font-bold">Flow Tracking</h1>
      <p class="text-muted-foreground mt-1">
        Track where funds from specific addresses end up. Enter up to 5 addresses and a start tick.
      </p>
    </div>

    <!-- Create form -->
    <div class="bg-card rounded-lg border p-6 space-y-4">
      <h2 class="text-lg font-semibold flex items-center gap-2">
        <Radar class="h-5 w-5" />
        New Flow Tracking
      </h2>

      <div>
        <label class="block text-sm font-medium mb-1">Alias (optional)</label>
        <input
          v-model="alias"
          type="text"
          maxlength="100"
          placeholder="e.g. My wallet tracking"
          class="w-full bg-background border rounded-md px-3 py-2 text-sm"
        />
      </div>

      <div>
        <label class="block text-sm font-medium mb-1">Start Tick</label>
        <input
          v-model.number="startTick"
          type="number"
          min="1"
          placeholder="Tick number to start tracking from"
          class="w-full bg-background border rounded-md px-3 py-2 text-sm"
        />
      </div>

      <div>
        <div class="flex items-center justify-between mb-1">
          <label class="text-sm font-medium">Addresses (1-5)</label>
          <button
            v-if="addresses.length < 5"
            @click="addAddress"
            class="text-xs flex items-center gap-1 text-primary hover:underline"
          >
            <Plus class="h-3 w-3" /> Add address
          </button>
        </div>
        <div v-for="(addr, i) in addresses" :key="i" class="flex gap-2 mb-2">
          <div class="flex-1 space-y-1">
            <input
              v-model="addr.value"
              type="text"
              maxlength="60"
              :placeholder="`Qubic address ${i + 1} (60 uppercase letters)`"
              class="w-full bg-background border rounded-md px-3 py-2 text-sm font-mono"
            />
            <input
              v-model.number="addr.balance"
              type="number"
              min="0"
              :placeholder="`Balance (optional, leave empty to use current balance)`"
              class="w-full bg-background border rounded-md px-3 py-2 text-xs text-muted-foreground"
            />
          </div>
          <button
            v-if="addresses.length > 1"
            @click="removeAddress(i)"
            class="text-muted-foreground hover:text-destructive self-start mt-2"
          >
            <Minus class="h-4 w-4" />
          </button>
        </div>
      </div>

      <div>
        <label class="block text-sm font-medium mb-1">Max Hops: {{ maxHops }}</label>
        <input
          v-model.number="maxHops"
          type="range"
          min="1"
          max="20"
          class="w-full"
        />
        <div class="flex justify-between text-xs text-muted-foreground">
          <span>1</span>
          <span>20</span>
        </div>
      </div>

      <div v-if="error" class="text-destructive text-sm bg-destructive/10 rounded-md px-3 py-2">
        {{ error }}
      </div>

      <button
        @click="submit"
        :disabled="isSubmitting"
        class="w-full bg-primary text-primary-foreground rounded-md px-4 py-2 text-sm font-medium hover:bg-primary/90 disabled:opacity-50 flex items-center justify-center gap-2"
      >
        <Loader2 v-if="isSubmitting" class="h-4 w-4 animate-spin" />
        <Radar v-else class="h-4 w-4" />
        {{ isSubmitting ? 'Creating...' : 'Start Tracking' }}
      </button>
    </div>

    <!-- Recent trackings -->
    <div v-if="recentTrackings.length > 0" class="bg-card rounded-lg border p-6">
      <h2 class="text-lg font-semibold mb-4">Recent Trackings</h2>
      <div class="space-y-2">
        <NuxtLink
          v-for="tracking in recentTrackings"
          :key="tracking.jobId"
          :to="`/analytics/flow-tracking/${tracking.jobId}`"
          class="flex items-center justify-between p-3 rounded-md border hover:bg-accent/50 transition-colors"
        >
          <div class="min-w-0">
            <div class="font-medium text-sm truncate">{{ tracking.alias || 'Untitled' }}</div>
            <div class="text-xs text-muted-foreground">
              {{ tracking.addresses.map(shortenAddress).join(', ') }}
            </div>
            <div class="text-xs text-muted-foreground">
              {{ new Date(tracking.createdAt).toLocaleDateString() }}
            </div>
          </div>
          <div class="flex items-center gap-2 ml-2 shrink-0">
            <button
              @click.prevent="copyJobId(tracking.jobId)"
              class="text-muted-foreground hover:text-foreground"
              title="Copy link"
            >
              <Check v-if="copied" class="h-4 w-4 text-success" />
              <Copy v-else class="h-4 w-4" />
            </button>
            <ExternalLink class="h-4 w-4 text-muted-foreground" />
          </div>
        </NuxtLink>
      </div>
    </div>
  </div>
</template>
