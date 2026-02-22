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
  'rgba(108, 140, 204, 0.75)',  // accent blue
  'rgba(102, 187, 154, 0.75)',  // success green
  'rgba(240, 184, 90, 0.75)',   // warning amber
  'rgba(229, 115, 115, 0.75)',  // error red
  'rgba(169, 142, 218, 0.75)',  // secondary purple
  'rgba(236, 120, 170, 0.75)'   // pink
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
      stacked: props.stacked,
      grid: {
        display: false
      },
      ticks: {
        color: 'rgb(139, 149, 168)'
      }
    },
    y: {
      stacked: props.stacked,
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
    <Bar :data="chartData" :options="chartOptions" />
  </div>
</template>
