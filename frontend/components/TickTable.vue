<script setup lang="ts">
import type { TickDto } from '~/composables/useApi'

defineProps<{
  ticks: TickDto[]
}>()

const formatDate = (date: string) => {
  return new Date(date).toLocaleString()
}

const formatDateShort = (date: string) => {
  const d = new Date(date)
  return d.toLocaleDateString()
}
</script>

<template>
  <div class="table-wrapper">
    <table>
      <thead>
        <tr>
          <th>Tick</th>
          <th class="hide-mobile">Epoch</th>
          <th>Time</th>
          <th>TXs</th>
          <th class="hide-mobile">Logs</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="tick in ticks" :key="tick.tickNumber">
          <td>
            <NuxtLink :to="`/ticks/${tick.tickNumber}`">
              {{ tick.tickNumber.toLocaleString() }}
            </NuxtLink>
          </td>
          <td class="hide-mobile">{{ tick.epoch }}</td>
          <td>
            <span class="hide-mobile">{{ formatDate(tick.timestamp) }}</span>
            <span class="show-mobile-only">{{ formatDateShort(tick.timestamp) }}</span>
          </td>
          <td>{{ tick.txCount }}</td>
          <td class="hide-mobile">{{ tick.logCount }}</td>
        </tr>
      </tbody>
    </table>
  </div>
</template>
