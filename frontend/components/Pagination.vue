<script setup lang="ts">
import { ChevronLeft, ChevronRight } from 'lucide-vue-next'

interface Props {
  currentPage: number
  totalPages: number
  hasNext: boolean
  hasPrevious: boolean
}

const props = defineProps<Props>()
const emit = defineEmits<{
  'update:currentPage': [page: number]
}>()

const goToPage = (page: number) => {
  if (page >= 1 && page <= props.totalPages) {
    emit('update:currentPage', page)
  }
}

// Calculate visible page numbers
const visiblePages = computed(() => {
  const pages: number[] = []
  const total = props.totalPages
  const current = props.currentPage

  if (total <= 5) {
    for (let i = 1; i <= total; i++) {
      pages.push(i)
    }
  } else {
    // Always show first page
    pages.push(1)

    if (current > 3) {
      pages.push(-1) // Ellipsis
    }

    // Show pages around current
    const start = Math.max(2, current - 1)
    const end = Math.min(total - 1, current + 1)

    for (let i = start; i <= end; i++) {
      pages.push(i)
    }

    if (current < total - 2) {
      pages.push(-2) // Ellipsis
    }

    // Always show last page
    if (total > 1) {
      pages.push(total)
    }
  }

  return pages
})
</script>

<template>
  <div class="pagination" v-if="totalPages > 1">
    <button
      :disabled="!hasPrevious"
      @click="goToPage(currentPage - 1)"
      class="flex items-center gap-1"
    >
      <ChevronLeft class="h-4 w-4" />
      <span class="hidden sm:inline">Previous</span>
    </button>

    <template v-for="page in visiblePages" :key="page">
      <span v-if="page < 0" class="px-2 text-foreground-muted">...</span>
      <button
        v-else
        :class="{ current: page === currentPage }"
        @click="goToPage(page)"
      >
        {{ page }}
      </button>
    </template>

    <button
      :disabled="!hasNext"
      @click="goToPage(currentPage + 1)"
      class="flex items-center gap-1"
    >
      <span class="hidden sm:inline">Next</span>
      <ChevronRight class="h-4 w-4" />
    </button>
  </div>
</template>
