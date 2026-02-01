<script setup lang="ts">
import { Bar } from 'vue-chartjs'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend
} from 'chart.js'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend
)

interface Props {
  labels: string[]
  datasets: {
    label: string
    data: number[]
    backgroundColor?: string
    borderColor?: string
    borderWidth?: number
  }[]
  title?: string
  yAxisLabel?: string
  height?: number
  stacked?: boolean
}

const props = withDefaults(defineProps<Props>(), {
  height: 300,
  stacked: false
})

const colors = [
  'rgba(59, 130, 246, 0.8)',   // blue
  'rgba(16, 185, 129, 0.8)',   // green
  'rgba(245, 158, 11, 0.8)',   // amber
  'rgba(239, 68, 68, 0.8)',    // red
  'rgba(139, 92, 246, 0.8)',   // purple
  'rgba(236, 72, 153, 0.8)'    // pink
]

const chartData = computed(() => ({
  labels: props.labels,
  datasets: props.datasets.map((ds, index) => ({
    ...ds,
    backgroundColor: ds.backgroundColor || colors[index % colors.length],
    borderColor: ds.borderColor || colors[index % colors.length].replace('0.8', '1'),
    borderWidth: ds.borderWidth ?? 1,
    borderRadius: 4
  }))
}))

const chartOptions = computed(() => ({
  responsive: true,
  maintainAspectRatio: false,
  interaction: {
    intersect: false,
    mode: 'index' as const
  },
  plugins: {
    legend: {
      display: props.datasets.length > 1,
      position: 'top' as const,
      labels: {
        color: 'rgb(156, 163, 175)',
        usePointStyle: true
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
        label: (context: { dataset: { label?: string }, parsed: { y: number | null } }) => {
          const label = context.dataset.label || ''
          const value = context.parsed.y ?? 0
          return `${label}: ${value.toLocaleString()}`
        }
      }
    }
  },
  scales: {
    x: {
      stacked: props.stacked,
      grid: {
        display: false
      },
      ticks: {
        color: 'rgb(156, 163, 175)'
      }
    },
    y: {
      stacked: props.stacked,
      grid: {
        color: 'rgba(75, 85, 99, 0.3)'
      },
      ticks: {
        color: 'rgb(156, 163, 175)',
        callback: (value: number | string) => {
          const numValue = typeof value === 'string' ? parseFloat(value) : value
          if (numValue >= 1_000_000_000) return (numValue / 1_000_000_000).toFixed(1) + 'B'
          if (numValue >= 1_000_000) return (numValue / 1_000_000).toFixed(1) + 'M'
          if (numValue >= 1_000) return (numValue / 1_000).toFixed(1) + 'K'
          return numValue.toLocaleString()
        }
      },
      title: {
        display: !!props.yAxisLabel,
        text: props.yAxisLabel,
        color: 'rgb(156, 163, 175)'
      }
    }
  }
}))
</script>

<template>
  <div :style="{ height: `${height}px` }">
    <Bar :data="chartData" :options="chartOptions" />
  </div>
</template>
