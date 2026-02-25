<script setup lang="ts">
import { Timer } from 'lucide-vue-next'

const api = useApi()

const { data: countdown } = await useAsyncData(
  'epoch-countdown',
  () => api.getEpochCountdown(),
  { lazy: true }
)

const now = ref(Date.now())
let timer: ReturnType<typeof setInterval> | null = null

onMounted(() => {
  timer = setInterval(() => {
    now.value = Date.now()
  }, 1000)
})

onUnmounted(() => {
  if (timer) clearInterval(timer)
})

const remaining = computed(() => {
  if (!countdown.value) return null
  const end = new Date(countdown.value.estimatedEpochEnd).getTime()
  const diff = end - now.value
  if (diff <= 0) return { days: 0, hours: 0, minutes: 0, seconds: 0, total: 0 }
  return {
    days: Math.floor(diff / 86400000),
    hours: Math.floor((diff % 86400000) / 3600000),
    minutes: Math.floor((diff % 3600000) / 60000),
    seconds: Math.floor((diff % 60000) / 1000),
    total: diff,
  }
})

const progress = computed(() => {
  if (!countdown.value || !remaining.value) return 0
  const start = new Date(countdown.value.currentEpochStart).getTime()
  const end = new Date(countdown.value.estimatedEpochEnd).getTime()
  const total = end - start
  if (total <= 0) return 100
  const elapsed = now.value - start
  return Math.min(100, Math.max(0, (elapsed / total) * 100))
})

const pad = (n: number) => String(n).padStart(2, '0')
</script>

<template>
  <div v-if="countdown && remaining" class="card-elevated">
    <div class="flex items-center gap-2 mb-3">
      <Timer class="h-4 w-4 text-accent" />
      <span class="text-sm font-medium">Epoch {{ countdown.currentEpoch }} Progress</span>
      <span class="text-xs text-foreground-muted ml-auto">
        Tick {{ countdown.currentTick.toLocaleString() }}
      </span>
    </div>

    <!-- Progress bar -->
    <div class="w-full h-2 bg-surface-elevated rounded-full overflow-hidden mb-3">
      <div
        class="h-full bg-accent rounded-full transition-all duration-1000"
        :style="{ width: `${progress}%` }"
      />
    </div>

    <!-- Countdown display -->
    <div v-if="remaining.total > 0" class="flex items-center justify-center gap-3 text-center">
      <div v-if="remaining.days > 0">
        <div class="text-xl font-bold text-accent">{{ remaining.days }}</div>
        <div class="text-[10px] text-foreground-muted uppercase">Days</div>
      </div>
      <div>
        <div class="text-xl font-bold text-accent">{{ pad(remaining.hours) }}</div>
        <div class="text-[10px] text-foreground-muted uppercase">Hours</div>
      </div>
      <div class="text-foreground-muted text-xl">:</div>
      <div>
        <div class="text-xl font-bold text-accent">{{ pad(remaining.minutes) }}</div>
        <div class="text-[10px] text-foreground-muted uppercase">Min</div>
      </div>
      <div class="text-foreground-muted text-xl">:</div>
      <div>
        <div class="text-xl font-bold text-accent">{{ pad(remaining.seconds) }}</div>
        <div class="text-[10px] text-foreground-muted uppercase">Sec</div>
      </div>
    </div>
    <div v-else class="text-center text-sm text-foreground-muted">
      Epoch transition expected soon...
    </div>
  </div>
</template>
