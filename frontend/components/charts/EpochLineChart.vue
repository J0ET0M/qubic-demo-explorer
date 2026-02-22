<script setup lang="ts">
import { Line } from 'vue-chartjs'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler
} from 'chart.js'

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler
)

interface Props {
  labels: string[]
  datasets: {
    label: string
    data: number[]
    borderColor?: string
    backgroundColor?: string
    fill?: boolean
    tension?: number
  }[]
  title?: string
  yAxisLabel?: string
  height?: number
}

const props = withDefaults(defineProps<Props>(), {
  height: 300
})

const chartData = computed(() => ({
  labels: props.labels,
  datasets: props.datasets.map(ds => ({
    ...ds,
    borderColor: ds.borderColor || 'rgb(108, 140, 204)',
    backgroundColor: ds.backgroundColor || 'rgba(108, 140, 204, 0.08)',
    fill: ds.fill ?? true,
    tension: ds.tension ?? 0.3,
    pointRadius: 3,
    pointHoverRadius: 5
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
        color: 'rgb(139, 149, 168)',
        usePointStyle: true
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
      grid: {
        color: 'rgba(42, 48, 64, 0.5)'
      },
      ticks: {
        color: 'rgb(139, 149, 168)'
      }
    },
    y: {
      grid: {
        color: 'rgba(42, 48, 64, 0.5)'
      },
      ticks: {
        color: 'rgb(139, 149, 168)',
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
        color: 'rgb(139, 149, 168)'
      }
    }
  }
}))
</script>

<template>
  <div :style="{ height: `${height}px` }">
    <Line :data="chartData" :options="chartOptions" />
  </div>
</template>
