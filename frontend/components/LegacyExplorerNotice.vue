<script setup lang="ts">
import { ArrowRight, Sparkles } from 'lucide-vue-next'

const props = defineProps<{ newUrl: string }>()

useHead({
  title: 'Moved to analytics.qubic.li',
  meta: [
    { name: 'robots', content: 'noindex,nofollow' },
    {
      name: 'description',
      content: 'The Qubic explorer has moved to analytics.qubic.li.'
    }
  ]
})

// Preserve the current path/query so the deep link still works after redirect.
const targetUrl = computed(() => {
  if (!import.meta.client) return props.newUrl
  const { pathname, search, hash } = window.location
  return `${props.newUrl.replace(/\/$/, '')}${pathname}${search}${hash}`
})

// Auto-redirect after 8s — gives the user time to read but doesn't leave
// bookmark-followers stranded forever.
const COUNTDOWN_SECONDS = 8
const remaining = ref(COUNTDOWN_SECONDS)
let timer: ReturnType<typeof setInterval> | null = null

onMounted(() => {
  timer = setInterval(() => {
    remaining.value -= 1
    if (remaining.value <= 0) {
      if (timer) clearInterval(timer)
      window.location.href = targetUrl.value
    }
  }, 1000)
})

onUnmounted(() => {
  if (timer) clearInterval(timer)
})
</script>

<template>
  <div class="min-h-screen bg-background flex flex-col items-center justify-center px-4">
    <div class="w-full max-w-xl">
      <div class="flex items-center gap-3 mb-10 justify-center">
        <img src="/logo.svg" alt="QLI" class="h-9 w-auto" />
        <span class="text-lg font-semibold text-foreground tracking-tight">QLI Analytics</span>
      </div>

      <div class="bg-surface border border-border rounded-xl p-8 text-center shadow-lg">
        <div class="inline-flex items-center justify-center w-14 h-14 rounded-full bg-accent/15 text-accent mb-5">
          <Sparkles class="h-7 w-7" />
        </div>

        <h1 class="text-2xl font-bold text-foreground mb-2">We&rsquo;ve moved</h1>
        <p class="text-base text-foreground-muted mb-6 leading-relaxed">
          The Qubic explorer has a new home with more features and faster analytics.
          <br />
          Please update your bookmarks to
          <span class="font-mono text-foreground font-semibold">analytics.qubic.li</span>.
        </p>

        <a
          :href="targetUrl"
          class="inline-flex items-center gap-2 px-5 py-3 rounded-lg bg-accent text-accent-foreground hover:bg-accent/90 transition-colors text-sm font-semibold"
        >
          Open QLI Analytics
          <ArrowRight class="h-4 w-4" />
        </a>

        <p class="text-xs text-foreground-muted/70 mt-6">
          Redirecting automatically in {{ remaining }}s…
        </p>
      </div>

      <p class="text-center text-xs text-foreground-muted/50 mt-6">
        This page (explorer-demo.qubic.tools) is no longer maintained.
      </p>
    </div>
  </div>
</template>
