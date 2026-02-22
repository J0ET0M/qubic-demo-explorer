<script setup lang="ts">
import { Doughnut } from 'vue-chartjs'
import {
  Chart as ChartJS,
  ArcElement,
  Title,
  Tooltip,
  Legend
} from 'chart.js'

ChartJS.register(
  ArcElement,
  Title,
  Tooltip,
  Legend
)

interface Props {
  labels: string[]
  data: number[]
  title?: string
  height?: number
}

const props = withDefaults(defineProps<Props>(), {
  height: 300
})

const colors = [
  'rgba(108, 140, 204, 0.8)',  // accent blue
  'rgba(102, 187, 154, 0.8)',  // success green
  'rgba(240, 184, 90, 0.8)',   // warning amber
  'rgba(229, 115, 115, 0.8)',  // error red
  'rgba(169, 142, 218, 0.8)',  // secondary purple
  'rgba(236, 120, 170, 0.8)',  // pink
  'rgba(80, 190, 210, 0.8)',   // cyan
  'rgba(140, 200, 80, 0.8)'   // lime
]

const chartData = computed(() => ({
  labels: props.labels,
  datasets: [{
    data: props.data,
    backgroundColor: colors.slice(0, props.data.length),
    borderColor: colors.slice(0, props.data.length).map(c => c.replace('0.8', '1')),
    borderWidth: 2,
    hoverOffset: 4
  }]
}))

const chartOptions = computed(() => ({
  responsive: true,
  maintainAspectRatio: false,
  plugins: {
    legend: {
      position: 'right' as const,
      labels: {
        color: 'rgb(139, 149, 168)',
        usePointStyle: true,
        padding: 15
      }
    },
    title: {
      display: !!props.title,
      text: props.title,
      color: 'rgb(230, 234, 240)',
      font: {
        size: 14,
        weight: 'bold' as const
      }
    },
    tooltip: {
      backgroundColor: 'rgb(24, 27, 36)',
      titleColor: 'rgb(230, 234, 240)',
      bodyColor: 'rgb(139, 149, 168)',
      borderColor: 'rgb(42, 48, 64)',
      borderWidth: 1,
      padding: 12,
      callbacks: {
        label: (context: { label?: string, parsed: number, dataset: { data: number[] } }) => {
          const label = context.label || ''
          const value = context.parsed
          const total = context.dataset.data.reduce((a, b) => a + b, 0)
          const percentage = ((value / total) * 100).toFixed(1)
          return `${label}: ${value.toLocaleString()} (${percentage}%)`
        }
      }
    }
  },
  cutout: '60%'
}))
</script>

<template>
  <div :style="{ height: `${height}px` }">
    <Doughnut :data="chartData" :options="chartOptions" />
  </div>
</template>
