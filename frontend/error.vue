<script setup lang="ts">
import { AlertTriangle, Home, RefreshCw } from 'lucide-vue-next'
import type { NuxtError } from '#app'

const props = defineProps<{ error: NuxtError }>()

useHead({ title: () => `Error ${props.error.statusCode} — QLI Analytics` })

const headline = computed(() => {
  switch (props.error.statusCode) {
    case 404: return 'Page not found'
    case 403: return 'Forbidden'
    case 500: return 'Something went wrong'
    default: return 'Error'
  }
})

const goHome = () => clearError({ redirect: '/' })
const tryAgain = () => clearError({ redirect: useRoute().fullPath })
</script>

<template>
  <div class="min-h-screen bg-background flex flex-col items-center justify-center px-4">
    <div class="w-full max-w-lg">
      <NuxtLink to="/" class="flex items-center gap-3 mb-10 justify-center group">
        <img src="/logo.svg" alt="QLI" class="h-9 w-auto" />
        <span class="text-lg font-semibold text-foreground tracking-tight">QLI Analytics</span>
      </NuxtLink>

      <div class="bg-surface border border-border rounded-xl p-8 text-center shadow-lg">
        <div class="inline-flex items-center justify-center w-14 h-14 rounded-full bg-destructive/15 text-destructive mb-5">
          <AlertTriangle class="h-7 w-7" />
        </div>

        <div class="text-5xl font-bold text-foreground mb-2 tabular-nums">
          {{ error.statusCode || 500 }}
        </div>
        <div class="text-base font-semibold text-foreground mb-3">{{ headline }}</div>

        <p v-if="error.message" class="text-sm text-foreground-muted break-words mb-6 font-mono">
          {{ error.message }}
        </p>

        <div class="flex flex-wrap gap-2 justify-center">
          <button
            type="button"
            @click="goHome"
            class="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-accent text-accent-foreground hover:bg-accent/90 transition-colors text-sm font-medium"
          >
            <Home class="h-4 w-4" />
            Go home
          </button>
          <button
            type="button"
            @click="tryAgain"
            class="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-surface-elevated hover:bg-surface-hover text-foreground transition-colors text-sm font-medium"
          >
            <RefreshCw class="h-4 w-4" />
            Try again
          </button>
        </div>
      </div>

      <p class="text-center text-xs text-foreground-muted/60 mt-6">
        QLI Analytics
      </p>
    </div>
  </div>
</template>
