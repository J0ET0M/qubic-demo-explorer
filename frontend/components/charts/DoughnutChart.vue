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
  'rgba(59, 130, 246, 0.8)',   // blue
  'rgba(16, 185, 129, 0.8)',   // green
  'rgba(245, 158, 11, 0.8)',   // amber
  'rgba(239, 68, 68, 0.8)',    // red
  'rgba(139, 92, 246, 0.8)',   // purple
  'rgba(236, 72, 153, 0.8)',   // pink
  'rgba(6, 182, 212, 0.8)',    // cyan
  'rgba(132, 204, 22, 0.8)'    // lime
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
        color: 'rgb(156, 163, 175)',
        usePointStyle: true,
        padding: 15
      }
    },
    title: {
      display: !!props.title,
      text: props.title,
      color: 'rgb(229, 231, 235)',
      font: {
        size: 14,
        weight: 'bold' as const
      }
    },
    tooltip: {
      backgroundColor: 'rgb(31, 41, 55)',
      titleColor: 'rgb(229, 231, 235)',
      bodyColor: 'rgb(156, 163, 175)',
      borderColor: 'rgb(75, 85, 99)',
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
