/**
 * Shared formatting utilities used across the explorer.
 * Consolidates duplicated format functions from 20+ components.
 */
export const useFormatting = () => {
  /**
   * Format large numbers with T/B/M/K suffixes.
   * Handles negative values (e.g. net flows).
   */
  const formatVolume = (volume: number, decimals = 1): string => {
    const abs = Math.abs(volume)
    const sign = volume < 0 ? '-' : ''
    if (abs >= 1_000_000_000_000) return sign + (abs / 1_000_000_000_000).toFixed(decimals) + 'T'
    if (abs >= 1_000_000_000) return sign + (abs / 1_000_000_000).toFixed(decimals) + 'B'
    if (abs >= 1_000_000) return sign + (abs / 1_000_000).toFixed(decimals) + 'M'
    if (abs >= 1_000) return sign + (abs / 1_000).toFixed(decimals) + 'K'
    return sign + abs.toLocaleString()
  }

  /**
   * Format QU amount as full number with locale separators.
   * Qubic has no decimals — amount is already in QU.
   */
  const formatAmount = (amount: number): string => {
    return Math.floor(amount).toLocaleString()
  }

  /**
   * Format a number with locale separators.
   */
  const formatNumber = (num: number | undefined): string => {
    if (!num) return '0'
    return num.toLocaleString()
  }

  /**
   * Truncate a 60-char Qubic address for display.
   */
  const truncateAddress = (address: string, chars = 8): string => {
    if (!address || address.length <= chars * 2) return address
    return `${address.slice(0, chars)}...${address.slice(-chars)}`
  }

  /**
   * Truncate a transaction hash for display.
   */
  const truncateHash = (hash: string, chars = 8): string => {
    if (!hash || hash.length <= chars * 2) return hash
    return `${hash.slice(0, chars)}...${hash.slice(-chars)}`
  }

  /**
   * Format a date string to locale string (full date + time).
   */
  const formatDate = (dateStr: string): string => {
    return new Date(dateStr).toLocaleString()
  }

  /**
   * Format a date string to locale date only (no time).
   */
  const formatDateShort = (dateStr: string): string => {
    return new Date(dateStr).toLocaleDateString()
  }

  /**
   * Format a date string to compact date + time (e.g. "Jan 5, 14:30").
   */
  const formatDateTime = (dateStr: string): string => {
    return new Date(dateStr).toLocaleString(undefined, {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    })
  }

  /**
   * Get badge CSS class for an address type string (from API labels).
   */
  const getBadgeClass = (type: string | null | undefined): string => {
    switch (type) {
      case 'exchange': return 'badge-warning'
      case 'smartcontract': return 'badge-info'
      case 'tokenissuer': return 'badge-accent'
      case 'burn': return 'badge-error'
      default: return 'badge-secondary'
    }
  }

  /**
   * Get badge CSS class for a log_type number (transfer type).
   */
  const getLogTypeBadgeClass = (logType: number): string => {
    switch (logType) {
      case 0: return 'badge-success'   // QU_TRANSFER
      case 1: return 'badge-info'      // ASSET_ISSUANCE
      case 2:
      case 3: return 'badge-info'      // ASSET_OWNERSHIP/POSSESSION
      default: return 'badge-warning'
    }
  }

  /**
   * Get text color class for an address type.
   */
  const getTypeClass = (type: string | null | undefined): string => {
    if (type === 'exchange') return 'text-warning'
    if (type === 'smartcontract') return 'text-info'
    if (type === 'burn') return 'text-destructive'
    return ''
  }

  /**
   * Format duration in milliseconds to human-readable string.
   */
  const formatDuration = (ms: number): string => {
    if (ms < 1000) return `${ms}ms`
    const seconds = Math.floor(ms / 1000)
    if (seconds < 60) return `${seconds}s`
    const minutes = Math.floor(seconds / 60)
    const remainingSeconds = seconds % 60
    if (minutes < 60) return `${minutes}m ${remainingSeconds}s`
    const hours = Math.floor(minutes / 60)
    const remainingMinutes = minutes % 60
    return `${hours}h ${remainingMinutes}m`
  }

  /**
   * Copy text to clipboard with fallback for non-secure contexts.
   */
  const copyToClipboard = async (text: string): Promise<boolean> => {
    try {
      if (navigator.clipboard && window.isSecureContext) {
        await navigator.clipboard.writeText(text)
      } else {
        const textArea = document.createElement('textarea')
        textArea.value = text
        textArea.style.position = 'fixed'
        textArea.style.left = '-999999px'
        textArea.style.top = '-999999px'
        document.body.appendChild(textArea)
        textArea.focus()
        textArea.select()
        document.execCommand('copy')
        textArea.remove()
      }
      return true
    } catch (err) {
      console.error('Failed to copy:', err)
      return false
    }
  }

  return {
    formatVolume,
    formatAmount,
    formatNumber,
    truncateAddress,
    truncateHash,
    formatDate,
    formatDateShort,
    formatDateTime,
    getBadgeClass,
    getLogTypeBadgeClass,
    getTypeClass,
    formatDuration,
    copyToClipboard,
  }
}
