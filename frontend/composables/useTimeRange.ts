export type TimeRangePreset = '24h' | '7d' | '30d' | '90d' | 'all' | 'custom'

export interface TimeRange {
  preset: TimeRangePreset
  from: string | null  // ISO 8601 string
  to: string | null    // ISO 8601 string
}

export const useTimeRange = () => {
  const timeRange = useState<TimeRange>('analytics-time-range', () => ({
    preset: '7d',
    from: null,
    to: null,
  }))

  const setPreset = (preset: TimeRangePreset) => {
    const now = new Date()
    let from: Date | null = null

    switch (preset) {
      case '24h': from = new Date(now.getTime() - 24 * 60 * 60 * 1000); break
      case '7d':  from = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000); break
      case '30d': from = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000); break
      case '90d': from = new Date(now.getTime() - 90 * 24 * 60 * 60 * 1000); break
      case 'all': from = null; break
    }

    timeRange.value = {
      preset,
      from: from?.toISOString() ?? null,
      to: preset === 'all' ? null : now.toISOString(),
    }
  }

  const setCustomRange = (from: string, to: string) => {
    timeRange.value = { preset: 'custom', from, to }
  }

  // Initialize with default if not yet set
  if (!timeRange.value.from && timeRange.value.preset !== 'all') {
    setPreset('7d')
  }

  return { timeRange: readonly(timeRange), setPreset, setCustomRange }
}
