<script setup lang="ts">
const api = useApi()
const route = useRoute()
const router = useRouter()

const page = ref(Number(route.query.page) || 1)
const limit = 20

const { data, pending, refresh } = await useAsyncData(
  () => `ticks-${page.value}`,
  () => api.getTicks(page.value, limit),
  { watch: [page] }
)

const updatePage = async (newPage: number) => {
  page.value = newPage
  await router.push({ query: { page: newPage } })
}
</script>

<template>
  <div class="space-y-6">
    <div v-if="pending" class="loading">Loading...</div>

    <template v-else-if="data">
      <div class="card">
        <div class="flex items-center justify-between mb-4">
          <span class="text-sm text-foreground-muted">
            Showing {{ data.items.length }} of {{ data.totalCount.toLocaleString() }} ticks
          </span>
        </div>
        <TickTable :ticks="data.items" />
      </div>

      <Pagination
        :current-page="page"
        :total-pages="data.totalPages"
        :has-next="data.hasNextPage"
        :has-previous="data.hasPreviousPage"
        @update:current-page="updatePage"
      />
    </template>
  </div>
</template>
