<script setup lang="ts">
import { X } from 'lucide-vue-next'

const { toasts, dismiss } = useToast()
</script>

<template>
  <Teleport to="body">
    <div class="fixed bottom-4 right-4 z-50 flex flex-col gap-2 max-w-sm">
      <TransitionGroup name="toast">
        <div
          v-for="toast in toasts"
          :key="toast.id"
          class="toast-item"
          :class="{
            'border-success/30 bg-success/10': toast.type === 'success',
            'border-accent/30 bg-accent/10': toast.type === 'info',
            'border-destructive/30 bg-destructive/10': toast.type === 'error',
          }"
        >
          <div class="flex items-start gap-2">
            <span class="text-sm flex-1">{{ toast.message }}</span>
            <button @click="dismiss(toast.id)" class="text-foreground-muted hover:text-foreground shrink-0">
              <X class="h-3.5 w-3.5" />
            </button>
          </div>
          <NuxtLink
            v-if="toast.action"
            :to="toast.action.to"
            class="text-xs text-accent hover:underline mt-1 inline-block"
            @click="dismiss(toast.id)"
          >
            {{ toast.action.label }}
          </NuxtLink>
        </div>
      </TransitionGroup>
    </div>
  </Teleport>
</template>

<style scoped>
.toast-item {
  padding: 0.75rem 1rem;
  border-radius: 0.5rem;
  border: 1px solid;
  backdrop-filter: blur(8px);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
}

.toast-enter-active {
  transition: all 0.3s ease;
}
.toast-leave-active {
  transition: all 0.2s ease;
}
.toast-enter-from {
  opacity: 0;
  transform: translateX(100%);
}
.toast-leave-to {
  opacity: 0;
  transform: translateX(100%);
}
</style>
