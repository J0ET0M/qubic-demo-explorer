<script setup lang="ts">
const router = useRouter()
const query = ref('')

const handleSearch = async () => {
  const q = query.value.trim()
  if (!q) return

  // Check if it's a number (tick)
  if (/^\d+$/.test(q)) {
    await router.push(`/ticks/${q}`)
    return
  }

  // Check if it's 60 characters (hash or address)
  if (q.length === 60) {
    // Lowercase = transaction hash
    if (q === q.toLowerCase()) {
      await router.push(`/tx/${q}`)
    } else {
      // Uppercase = address
      await router.push(`/address/${q}`)
    }
    return
  }

  // Otherwise, go to search results
  await router.push(`/search?q=${encodeURIComponent(q)}`)
}
</script>

<template>
  <form @submit.prevent="handleSearch">
    <input
      v-model="query"
      type="text"
      placeholder="Search tick, tx, address, or name..."
      class="input w-full"
    />
  </form>
</template>
