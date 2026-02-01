import type { AddressLabelDto } from './useApi'

// Global cache for address labels
const labelCache = reactive(new Map<string, AddressLabelDto>())
const pendingRequests = new Map<string, Promise<AddressLabelDto[]>>()

export const useAddressLabels = () => {
  const api = useApi()

  // Reactive version - returns the label or undefined, triggers reactivity
  const getLabel = (address: string): AddressLabelDto | undefined => {
    return labelCache.get(address)
  }

  // Fetch labels for a list of addresses (with deduplication and caching)
  const fetchLabels = async (addresses: string[]): Promise<void> => {
    // Filter out empty and already cached addresses
    const uncached = addresses.filter(addr => addr && !labelCache.has(addr))

    if (uncached.length === 0) return

    // Batch into chunks of 100 (API limit)
    const chunks: string[][] = []
    for (let i = 0; i < uncached.length; i += 100) {
      chunks.push(uncached.slice(i, i + 100))
    }

    for (const chunk of chunks) {
      const cacheKey = chunk.sort().join(',')
      let promise = pendingRequests.get(cacheKey)

      if (!promise) {
        promise = api.getAddressLabels(chunk)
        pendingRequests.set(cacheKey, promise)

        try {
          const labels = await promise
          for (const label of labels) {
            labelCache.set(label.address, label)
          }
        } finally {
          pendingRequests.delete(cacheKey)
        }
      } else {
        await promise
      }
    }
  }

  // Helper to extract all addresses from transactions
  const fetchLabelsForTransactions = async (transactions: { fromAddress: string; toAddress: string }[]) => {
    const addresses = new Set<string>()
    for (const tx of transactions) {
      if (tx.fromAddress) addresses.add(tx.fromAddress)
      if (tx.toAddress) addresses.add(tx.toAddress)
    }
    await fetchLabels(Array.from(addresses))
  }

  // Helper to extract all addresses from transfers/logs
  const fetchLabelsForTransfers = async (transfers: { sourceAddress?: string; destAddress?: string }[]) => {
    const addresses = new Set<string>()
    for (const t of transfers) {
      if (t.sourceAddress) addresses.add(t.sourceAddress)
      if (t.destAddress) addresses.add(t.destAddress)
    }
    await fetchLabels(Array.from(addresses))
  }

  const formatAddress = (address: string, short = true): string => {
    const label = labelCache.get(address)
    if (label?.label) {
      return label.label
    }
    if (short && address.length > 12) {
      return `${address.slice(0, 6)}...${address.slice(-4)}`
    }
    return address
  }

  const getAddressType = (address: string): string => {
    return labelCache.get(address)?.type ?? 'unknown'
  }

  const isExchange = (address: string): boolean => {
    return labelCache.get(address)?.type === 'exchange'
  }

  const isSmartContract = (address: string): boolean => {
    return labelCache.get(address)?.type === 'smartcontract'
  }

  const isBurn = (address: string): boolean => {
    return labelCache.get(address)?.type === 'burn'
  }

  const getTypeIcon = (address: string): string => {
    const type = getAddressType(address)
    switch (type) {
      case 'exchange':
        return '🏦'
      case 'smartcontract':
        return '📜'
      case 'tokenissuer':
        return '🪙'
      case 'burn':
        return '🔥'
      default:
        return ''
    }
  }

  const getTypeBadgeClass = (address: string): string => {
    const type = getAddressType(address)
    switch (type) {
      case 'exchange':
        return 'badge-exchange'
      case 'smartcontract':
        return 'badge-contract'
      case 'tokenissuer':
        return 'badge-token'
      case 'burn':
        return 'badge-burn'
      default:
        return ''
    }
  }

  return {
    getLabel,
    fetchLabels,
    fetchLabelsForTransactions,
    fetchLabelsForTransfers,
    formatAddress,
    getAddressType,
    isExchange,
    isSmartContract,
    isBurn,
    getTypeIcon,
    getTypeBadgeClass,
  }
}
