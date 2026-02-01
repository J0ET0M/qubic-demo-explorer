<script setup lang="ts">
import AppSidebar from '~/components/layout/AppSidebar.vue'
import AppHeader from '~/components/layout/AppHeader.vue'

const sidebarOpen = ref(false)

function toggleSidebar() {
  sidebarOpen.value = !sidebarOpen.value
}

function closeSidebar() {
  sidebarOpen.value = false
}
</script>

<template>
  <div class="min-h-screen bg-background">
    <!-- Sidebar -->
    <AppSidebar :is-open="sidebarOpen" @close="closeSidebar" />

    <!-- Backdrop for mobile -->
    <div
      v-if="sidebarOpen"
      class="fixed inset-0 z-40 bg-black/50 lg:hidden"
      @click="closeSidebar"
    />

    <!-- Main content -->
    <div class="lg:pl-64">
      <AppHeader @toggle-sidebar="toggleSidebar" />

      <main class="p-4 lg:p-6">
        <slot />
      </main>
    </div>
  </div>
</template>
