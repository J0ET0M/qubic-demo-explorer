<script setup lang="ts">
import AppSidebar from '~/components/layout/AppSidebar.vue'
import AppHeader from '~/components/layout/AppHeader.vue'

const sidebarOpen = ref(false)
const sidebarCollapsed = ref(false)

function toggleSidebar() {
  sidebarOpen.value = !sidebarOpen.value
}

function closeSidebar() {
  sidebarOpen.value = false
}

function toggleCollapse() {
  sidebarCollapsed.value = !sidebarCollapsed.value
}
</script>

<template>
  <div class="min-h-screen bg-background">
    <!-- Sidebar -->
    <AppSidebar
      :is-open="sidebarOpen"
      :is-collapsed="sidebarCollapsed"
      @close="closeSidebar"
      @toggle-collapse="toggleCollapse"
    />

    <!-- Backdrop for mobile -->
    <div
      v-if="sidebarOpen"
      class="fixed inset-0 z-40 bg-black/50 lg:hidden"
      @click="closeSidebar"
    />

    <!-- Main content -->
    <div
      class="transition-[padding-left] duration-300"
      :class="sidebarCollapsed ? 'lg:pl-16' : 'lg:pl-64'"
    >
      <AppHeader @toggle-sidebar="toggleSidebar" />

      <main class="p-4 lg:p-6">
        <slot />
      </main>
    </div>

    <!-- Toast notifications -->
    <ToastContainer />
  </div>
</template>
