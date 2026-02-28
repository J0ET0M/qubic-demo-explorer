export const useApi = () => {
  const config = useRuntimeConfig()

  // Use server-side URL during SSR, client-side URL in browser
  const getBaseUrl = () => {
    if (import.meta.server) {
      // Server-side: use internal Docker network URL
      return config.apiUrl || 'http://api:8080'
    }
    // Client-side: use public URL (empty = same origin, goes through nginx)
    return config.public.apiUrl || ''
  }

  const fetchApi = async <T>(
    endpoint: string,
    options: RequestInit = {}
  ): Promise<T> => {
    const baseUrl = getBaseUrl()
    const response = await fetch(`${baseUrl}${endpoint}`, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
    })

    if (!response.ok) {
      throw new Error(`API error: ${response.status}`)
    }

    return response.json()
  }

  // Ticks
  const getTicks = (page = 1, limit = 20) =>
    fetchApi<PaginatedResponse<TickDto>>(`/api/ticks?page=${page}&limit=${limit}`)

  const getTick = (tickNumber: number) =>
    fetchApi<TickDetailDto>(`/api/ticks/${tickNumber}`)

  const getTickTransactions = (tickNumber: number, page = 1, limit = 20, options?: {
    address?: string
    direction?: 'from' | 'to'
    minAmount?: number
    executed?: boolean
  }) => {
    const params = new URLSearchParams({ page: String(page), limit: String(limit) })
    if (options?.address) params.set('address', options.address)
    if (options?.direction) params.set('direction', options.direction)
    if (options?.minAmount !== undefined) params.set('minAmount', String(options.minAmount))
    if (options?.executed !== undefined) params.set('executed', String(options.executed))
    return fetchApi<PaginatedResponse<TransactionDto>>(`/api/ticks/${tickNumber}/transactions?${params}`)
  }

  const getTickLogs = (tickNumber: number, page = 1, limit = 20, options?: {
    address?: string
    type?: number
    direction?: 'in' | 'out'
    minAmount?: number
  }) => {
    const params = new URLSearchParams({ page: String(page), limit: String(limit) })
    if (options?.address) params.set('address', options.address)
    if (options?.type !== undefined) params.set('type', String(options.type))
    if (options?.direction) params.set('direction', options.direction)
    if (options?.minAmount !== undefined) params.set('minAmount', String(options.minAmount))
    return fetchApi<PaginatedResponse<TransferDto>>(`/api/ticks/${tickNumber}/logs?${params}`)
  }

  // Transactions
  const getTransactions = (page = 1, limit = 20, options?: {
    address?: string
    direction?: 'from' | 'to'
    minAmount?: number
    executed?: boolean
    inputType?: number
    toAddress?: string
  }) => {
    const params = new URLSearchParams({ page: String(page), limit: String(limit) })
    if (options?.address) params.set('address', options.address)
    if (options?.direction) params.set('direction', options.direction)
    if (options?.minAmount !== undefined) params.set('minAmount', String(options.minAmount))
    if (options?.executed !== undefined) params.set('executed', String(options.executed))
    if (options?.inputType !== undefined) params.set('inputType', String(options.inputType))
    if (options?.toAddress) params.set('toAddress', options.toAddress)
    return fetchApi<PaginatedResponse<TransactionDto>>(`/api/transactions?${params}`)
  }

  const getTransaction = (hash: string) =>
    fetchApi<TransactionDetailDto | SpecialTransactionDto>(`/api/transactions/${hash}`)

  // Transfers
  const getTransfers = (page = 1, limit = 20, options?: {
    address?: string
    type?: number
    types?: number[]
    direction?: 'in' | 'out'
    minAmount?: number
  }) => {
    const params = new URLSearchParams({ page: String(page), limit: String(limit) })
    if (options?.address) params.set('address', options.address)
    if (options?.type !== undefined) params.set('type', String(options.type))
    if (options?.types && options.types.length > 0) params.set('types', options.types.join(','))
    if (options?.direction) params.set('direction', options.direction)
    if (options?.minAmount !== undefined) params.set('minAmount', String(options.minAmount))
    return fetchApi<PaginatedResponse<TransferDto>>(`/api/transfers?${params}`)
  }

  // Address
  const getAddress = (address: string) =>
    fetchApi<AddressDto>(`/api/address/${address}`)

  const getAddressTransactions = (address: string, page = 1, limit = 20, options?: {
    direction?: 'from' | 'to'
    minAmount?: number
    executed?: boolean
  }) => {
    const params = new URLSearchParams({ page: String(page), limit: String(limit) })
    if (options?.direction) params.set('direction', options.direction)
    if (options?.minAmount !== undefined) params.set('minAmount', String(options.minAmount))
    if (options?.executed !== undefined) params.set('executed', String(options.executed))
    return fetchApi<PaginatedResponse<TransactionDto>>(`/api/address/${address}/transactions?${params}`)
  }

  const getAddressTransfers = (address: string, page = 1, limit = 20, options?: {
    direction?: 'in' | 'out'
    type?: number
    minAmount?: number
  }) => {
    const params = new URLSearchParams({ page: String(page), limit: String(limit) })
    if (options?.direction) params.set('direction', options.direction)
    if (options?.type !== undefined) params.set('type', String(options.type))
    if (options?.minAmount !== undefined) params.set('minAmount', String(options.minAmount))
    return fetchApi<PaginatedResponse<TransferDto>>(`/api/address/${address}/transfers?${params}`)
  }

  // Search
  const search = (query: string) =>
    fetchApi<SearchResponse>(`/api/search?q=${encodeURIComponent(query)}`)

  // Stats
  const getStats = () =>
    fetchApi<NetworkStatsDto>('/api/stats')

  const getTxVolumeChart = (period: 'day' | 'week' | 'month' = 'week') =>
    fetchApi<ChartDataPointDto[]>(`/api/stats/chart/tx-volume?period=${period}`)

  // Epochs
  const getEpochs = (limit = 50) =>
    fetchApi<EpochSummaryDto[]>(`/api/epoch?limit=${limit}`)

  const getEpoch = (epoch: number) =>
    fetchApi<EpochStatsDto>(`/api/epoch/${epoch}`)

  const getEpochTransfersByType = (epoch: number) =>
    fetchApi<EpochTransferByTypeDto[]>(`/api/epoch/${epoch}/transfers-by-type`)

  const getEpochRewards = (epoch: number) =>
    fetchApi<EpochRewardSummaryDto>(`/api/epoch/${epoch}/rewards`)

  // Epoch Metadata
  const getEpochMeta = (epoch: number) =>
    fetchApi<EpochMetaDto>(`/api/epoch/${epoch}/meta`)

  const getAllEpochMeta = (limit = 100) =>
    fetchApi<EpochMetaDto[]>(`/api/epoch/meta?limit=${limit}`)

  const getCurrentEpochMeta = () =>
    fetchApi<EpochMetaDto>(`/api/epoch/meta/current`)

  const upsertEpochMeta = (epoch: number, data: {
    initialTick: number
    endTick?: number
    endTickStartLogId?: number
    endTickEndLogId?: number
  }) =>
    fetchApi<{ success: boolean; epoch: number }>(`/api/epoch/${epoch}/meta`, {
      method: 'POST',
      body: JSON.stringify(data),
    })

  // Address Rewards
  const getAddressRewards = (address: string, page = 1, limit = 20) =>
    fetchApi<ContractRewardHistoryDto>(`/api/address/${address}/rewards?page=${page}&limit=${limit}`)

  // Address Labels
  const getAddressLabel = (address: string) =>
    fetchApi<AddressLabelDto>(`/api/labels/${address}`)

  const getAddressLabels = (addresses: string[]) =>
    fetchApi<AddressLabelDto[]>(`/api/labels/batch`, {
      method: 'POST',
      body: JSON.stringify(addresses),
    })

  const getAllKnownAddresses = (type?: string) => {
    const params = type ? `?type=${type}` : ''
    return fetchApi<KnownAddressDto[]>(`/api/labels${params}`)
  }

  const getLabelStats = () =>
    fetchApi<LabelStatsDto>('/api/labels/stats')

  const getProcedureName = (contractAddress: string, inputType: number) =>
    fetchApi<ProcedureLookupDto>(`/api/labels/procedure/${contractAddress}/${inputType}`)

  // Analytics
  const getTopAddresses = (limit = 20, epoch?: number) => {
    const params = new URLSearchParams({ limit: String(limit) })
    if (epoch !== undefined) params.set('epoch', String(epoch))
    return fetchApi<TopAddressDto[]>(`/api/stats/top-addresses?${params}`)
  }

  const getSmartContractUsage = (epoch?: number) => {
    const params = epoch !== undefined ? `?epoch=${epoch}` : ''
    return fetchApi<SmartContractUsageDto[]>(`/api/stats/smart-contract-usage${params}`)
  }

  const getAddressActivityRange = (address: string) =>
    fetchApi<AddressActivityRangeDto>(`/api/address/${address}/activity-range`)

  const getAddressFlow = (address: string, limit = 10) =>
    fetchApi<AddressFlowDto>(`/api/address/${address}/flow?limit=${limit}`)

  // Rich list
  const getRichList = (page = 1, limit = 50) =>
    fetchApi<RichListDto>(`/api/stats/rich-list?page=${page}&limit=${limit}`)

  // Supply dashboard
  const getSupplyDashboard = () =>
    fetchApi<SupplyDashboardDto>('/api/stats/supply')

  // Whale alerts
  const getWhaleAlerts = (threshold = 10_000_000_000, limit = 50) =>
    fetchApi<WhaleAlertDto[]>(`/api/stats/whale-alerts?threshold=${threshold}&limit=${limit}`)

  // CSV export helper (opens download in new tab)
  const exportAddressData = (address: string, type = 'transfers', epoch?: number) => {
    const baseUrl = getBaseUrl()
    const epochParam = epoch ? `&epoch=${epoch}` : ''
    window.open(`${baseUrl}/api/address/${address}/export?format=csv&type=${type}${epochParam}`, '_blank')
  }

  // Address batch (for portfolio)
  const getAddressesBatch = (addresses: string[]) =>
    fetchApi<AddressDto[]>('/api/address/batch', {
      method: 'POST',
      body: JSON.stringify({ addresses }),
    })

  // Assets
  const getAssets = () =>
    fetchApi<AssetSummaryDto[]>('/api/assets')

  const getAsset = (name: string, issuer?: string) => {
    const issuerParam = issuer ? `?issuer=${issuer}` : ''
    return fetchApi<AssetDetailDto>(`/api/assets/${name}${issuerParam}`)
  }

  const getAssetHolders = (name: string, page = 1, limit = 50, issuer?: string) => {
    const issuerParam = issuer ? `&issuer=${issuer}` : ''
    return fetchApi<AssetHoldersPageDto>(`/api/assets/${name}/holders?page=${page}&limit=${limit}${issuerParam}`)
  }

  // Transaction graph
  const getAddressGraph = (address: string, hops = 1, limit = 20) =>
    fetchApi<TransactionGraphDto>(`/api/address/${address}/graph?hops=${hops}&limit=${limit}`)

  // Epoch countdown
  const getEpochCountdown = () =>
    fetchApi<EpochCountdownDto>('/api/epoch/countdown')

  // Glassnode-style Analytics
  const getActiveAddressTrends = (period = 'epoch', limit = 50) =>
    fetchApi<ActiveAddressTrendDto[]>(`/api/stats/active-addresses?period=${period}&limit=${limit}`)

  const getNewVsReturningAddresses = (limit = 50) =>
    fetchApi<NewVsReturningDto[]>(`/api/stats/new-vs-returning?limit=${limit}`)

  const getExchangeFlows = (limit = 50) =>
    fetchApi<ExchangeFlowDto>(`/api/stats/exchange-flows?limit=${limit}`)

  const getHolderDistribution = () =>
    fetchApi<HolderDistributionDto>('/api/stats/holder-distribution')

  const getAvgTxSizeTrends = (period = 'epoch', limit = 50) =>
    fetchApi<AvgTxSizeTrendDto[]>(`/api/stats/avg-tx-size?period=${period}&limit=${limit}`)

  // Extended holder distribution with history
  const getHolderDistributionExtended = (historyLimit = 500, from?: string, to?: string) => {
    const params = new URLSearchParams({ historyLimit: String(historyLimit) })
    if (from) params.set('from', from)
    if (to) params.set('to', to)
    return fetchApi<HolderDistributionExtendedDto>(`/api/stats/holder-distribution/extended?${params}`)
  }

  const getHolderDistributionHistory = (limit = 500, from?: string, to?: string) => {
    const params = new URLSearchParams({ limit: String(limit) })
    if (from) params.set('from', from)
    if (to) params.set('to', to)
    return fetchApi<HolderDistributionHistoryDto[]>(`/api/stats/holder-distribution/history?${params}`)
  }

  // Network stats history
  const getNetworkStatsHistory = (limit = 500, from?: string, to?: string) => {
    const params = new URLSearchParams({ limit: String(limit) })
    if (from) params.set('from', from)
    if (to) params.set('to', to)
    return fetchApi<NetworkStatsHistoryDto[]>(`/api/stats/network-stats/history?${params}`)
  }

  const getNetworkStatsExtended = (historyLimit = 500, from?: string, to?: string) => {
    const params = new URLSearchParams({ historyLimit: String(historyLimit) })
    if (from) params.set('from', from)
    if (to) params.set('to', to)
    return fetchApi<NetworkStatsExtendedDto>(`/api/stats/network-stats/extended?${params}`)
  }

  // Burn stats
  const getBurnStatsHistory = (limit = 500, from?: string, to?: string) => {
    const params = new URLSearchParams({ limit: String(limit) })
    if (from) params.set('from', from)
    if (to) params.set('to', to)
    return fetchApi<BurnStatsHistoryDto[]>(`/api/stats/burn-stats/history?${params}`)
  }

  const getBurnStatsExtended = (historyLimit = 500, from?: string, to?: string) => {
    const params = new URLSearchParams({ historyLimit: String(historyLimit) })
    if (from) params.set('from', from)
    if (to) params.set('to', to)
    return fetchApi<BurnStatsExtendedDto>(`/api/stats/burn-stats/extended?${params}`)
  }

  // Qearn stats
  const getQearnStats = () =>
    fetchApi<QearnStatsDto>('/api/stats/qearn')

  // Miner/Computor Flow
  const getMinerFlowStats = (limit = 500, from?: string, to?: string) => {
    const params = new URLSearchParams({ limit: String(limit) })
    if (from) params.set('from', from)
    if (to) params.set('to', to)
    return fetchApi<MinerFlowSummaryDto>(`/api/miner-flow/stats?${params}`)
  }

  const getComputors = (epoch: number) =>
    fetchApi<ComputorListDto>(`/api/miner-flow/computors/${epoch}`)

  const getFlowVisualization = (emissionEpoch: number, maxDepth = 10) => {
    const params = new URLSearchParams({ maxDepth: String(maxDepth) })
    return fetchApi<FlowVisualizationDto>(`/api/miner-flow/visualization/${emissionEpoch}?${params}`)
  }

  const getFlowHops = (epoch: number, tickStart?: number, tickEnd?: number, maxDepth = 5, limit = 1000) => {
    const params = new URLSearchParams({ maxDepth: String(maxDepth), limit: String(limit) })
    if (tickStart !== undefined) params.set('tickStart', String(tickStart))
    if (tickEnd !== undefined) params.set('tickEnd', String(tickEnd))
    return fetchApi<FlowHopsResponseDto>(`/api/miner-flow/hops/${epoch}?${params}`)
  }

  // Emissions
  const getEmissionSummary = (epoch: number) =>
    fetchApi<EmissionSummaryDto>(`/api/miner-flow/emissions/${epoch}`)

  const getEmissionDetails = (epoch: number) =>
    fetchApi<EmissionDetailsDto>(`/api/miner-flow/emissions/${epoch}/details`)

  const getComputorEmission = (epoch: number, address: string) =>
    fetchApi<ComputorEmissionResponseDto>(`/api/miner-flow/emissions/${epoch}/address/${address}`)

  // Custom flow tracking
  const createCustomFlow = (request: { addresses: string[], startTick: number, balances?: number[], alias?: string, maxHops?: number }) =>
    fetchApi<any>('/api/custom-flow', { method: 'POST', body: JSON.stringify(request) })

  const getCustomFlowJob = (jobId: string) =>
    fetchApi<any>(`/api/custom-flow/${jobId}`)

  const getCustomFlowVisualization = (jobId: string) =>
    fetchApi<any>(`/api/custom-flow/${jobId}/visualization`)

  const getCustomFlowHops = (jobId: string, maxDepth = 10) =>
    fetchApi<any>(`/api/custom-flow/${jobId}/hops?maxDepth=${maxDepth}`)

  const getCustomFlowState = (jobId: string) =>
    fetchApi<any>(`/api/custom-flow/${jobId}/state`)

  return {
    getTicks,
    getTick,
    getTickTransactions,
    getTickLogs,
    getTransactions,
    getTransaction,
    getTransfers,
    getAddress,
    getAddressTransactions,
    getAddressTransfers,
    getAddressActivityRange,
    search,
    getStats,
    getTxVolumeChart,
    getEpochs,
    getEpoch,
    getEpochTransfersByType,
    getEpochRewards,
    getEpochCountdown,
    getEpochMeta,
    getAllEpochMeta,
    getCurrentEpochMeta,
    upsertEpochMeta,
    getAddressRewards,
    getAddressLabel,
    getAddressLabels,
    getAllKnownAddresses,
    getLabelStats,
    getProcedureName,
    getRichList,
    getSupplyDashboard,
    getWhaleAlerts,
    exportAddressData,
    getAddressesBatch,
    getAddressGraph,
    getAssets,
    getAsset,
    getAssetHolders,
    getTopAddresses,
    getSmartContractUsage,
    getAddressFlow,
    getActiveAddressTrends,
    getNewVsReturningAddresses,
    getExchangeFlows,
    getHolderDistribution,
    getAvgTxSizeTrends,
    getHolderDistributionExtended,
    getHolderDistributionHistory,
    getNetworkStatsHistory,
    getNetworkStatsExtended,
    getBurnStatsHistory,
    getBurnStatsExtended,
    getQearnStats,
    getMinerFlowStats,
    getComputors,
    getFlowVisualization,
    getFlowHops,
    getEmissionSummary,
    getEmissionDetails,
    getComputorEmission,
    createCustomFlow,
    getCustomFlowJob,
    getCustomFlowVisualization,
    getCustomFlowHops,
    getCustomFlowState,
  }
}

// Types
interface PaginatedResponse<T> {
  items: T[]
  page: number
  limit: number
  totalCount: number
  totalPages: number
  hasNextPage: boolean
  hasPreviousPage: boolean
}

interface TickDto {
  tickNumber: number
  epoch: number
  timestamp: string
  txCount: number
  logCount: number
}

interface TickDetailDto extends TickDto {
  transactions: TransactionDto[]
}

interface TransactionDto {
  hash: string
  tickNumber: number
  epoch: number
  fromAddress: string
  toAddress: string
  amount: number
  inputType: number
  inputTypeName?: string
  executed: boolean
  timestamp: string
}

interface TransactionDetailDto extends TransactionDto {
  epoch: number
  inputData?: string
  parsedInput?: ParsedInputData
  logs: LogDto[]
}

// Parsed input data for core/system transactions sent to the burn address
interface ParsedInputData {
  typeName: string
  // VoteCounter / CustomMiningShareCounter
  votes?: number[]
  scores?: number[]
  dataLock?: string
  totalVotes?: number
  totalScore?: number
  nonZeroCount?: number
  // MiningSolution
  miningSeed?: string
  nonce?: string
  // FileHeader / FileTrailer
  fileSize?: number
  numberOfFragments?: number
  fileFormat?: string
  lastFileFragmentTransactionDigest?: string
  // FileFragment
  fragmentIndex?: number
  prevFileFragmentTransactionDigest?: string
  payloadSize?: number
  // OracleReplyCommit
  items?: Array<{ queryId: number; replyDigest: string; replyKnowledgeProof: string }>
  // OracleReplyReveal
  queryId?: number
  replyDataHex?: string
  replyDataSize?: number
  // ExecutionFeeReport
  phaseNumber?: number
  numEntries?: number
  entries?: Array<{ contractIndex: number; executionFee: number }>
  // OracleUserQuery
  oracleInterfaceIndex?: number
  oracleInterfaceName?: string
  timeoutMilliseconds?: number
  queryDataHex?: string
  queryDataSize?: number
  parsedQueryFields?: Array<{ name: string; value: string; type: string }>
}

interface SpecialTransactionDto {
  txHash: string
  specialType: string
  specialTypeName: string
  tickNumber: number
  timestamp: string
  logs: LogDto[]
}

interface LogDto {
  tickNumber: number
  logId: number
  logType: number
  logTypeName: string
  txHash?: string
  sourceAddress?: string
  destAddress?: string
  amount: number
  assetName?: string
  timestamp: string
}

interface TransferDto {
  tickNumber: number
  epoch: number
  logId: number
  logType: number
  logTypeName: string
  txHash?: string
  sourceAddress: string
  destAddress: string
  amount: number
  assetName?: string
  timestamp: string
}

interface AddressDto {
  address: string
  balance: number
  incomingAmount: number
  outgoingAmount: number
  txCount: number
  transferCount: number
}

interface SearchResponse {
  query: string
  results: SearchResultDto[]
}

interface SearchResultDto {
  type: number  // 0=Tick, 1=Transaction, 2=Address, 3=Asset
  value: string
  displayName?: string
}

interface NetworkStatsDto {
  latestTick: number
  currentEpoch: number
  totalTransactions: number
  totalTransfers: number
  totalVolume: number
  lastUpdated: string
}

interface ChartDataPointDto {
  date: string
  txCount: number
  volume: number
}

interface EpochSummaryDto {
  epoch: number
  tickCount: number
  txCount: number
  totalVolume: number
  activeAddresses: number
  startTime: string
  endTime: string
  firstTick: number
  lastTick: number
}

interface EpochStatsDto {
  epoch: number
  tickCount: number
  firstTick: number
  lastTick: number
  startTime: string
  endTime: string
  txCount: number
  totalVolume: number
  uniqueSenders: number
  uniqueReceivers: number
  activeAddresses: number
  transferCount: number
  quTransferred: number
  assetTransferCount: number
}

interface EpochTransferByTypeDto {
  epoch: number
  logType: number
  logTypeName: string
  count: number
  totalAmount: number
}

interface RewardDistributionDto {
  epoch: number
  contractAddress: string
  contractName: string | null
  tickNumber: number
  totalAmount: number
  amountPerShare: number
  transferCount: number
  timestamp: string
}

interface EpochRewardSummaryDto {
  epoch: number
  distributions: RewardDistributionDto[]
  totalRewardsDistributed: number
}

interface ContractRewardHistoryDto {
  contractAddress: string
  contractName: string | null
  distributions: RewardDistributionDto[]
  totalAllTimeDistributed: number
  page: number
  limit: number
  totalCount: number
  totalPages: number
  hasNextPage: boolean
  hasPreviousPage: boolean
}

interface AddressLabelDto {
  address: string
  label: string | null
  type: 'unknown' | 'known' | 'exchange' | 'smartcontract' | 'tokenissuer' | 'burn'
  contractIndex?: number | null
  website?: string | null
}

interface KnownAddressDto {
  address: string
  label: string
  type: string
  contractIndex?: number | null
  website?: string | null
}

interface LabelStatsDto {
  totalLabels: number
  byType: Record<string, number>
}

interface ProcedureLookupDto {
  contractAddress: string
  inputType: number
  procedureName: string | null
}

// Analytics DTOs
interface TopAddressDto {
  address: string
  label: string | null
  type: string | null
  sentVolume: number
  receivedVolume: number
  totalVolume: number
  sentCount: number
  receivedCount: number
  totalCount: number
}

interface FlowNodeDto {
  address: string
  label: string | null
  type: string | null
  totalAmount: number
  transactionCount: number
}

interface AddressFlowDto {
  address: string
  label: string | null
  type: string | null
  inbound: FlowNodeDto[]
  outbound: FlowNodeDto[]
}

interface SmartContractUsageDto {
  address: string
  name: string
  contractIndex: number | null
  callCount: number
  totalAmount: number
  uniqueCallers: number
}

// Glassnode-style Analytics Types
interface ActiveAddressTrendDto {
  epoch: number | null
  date: string | null
  uniqueSenders: number
  uniqueReceivers: number
  totalActive: number
}

interface NewVsReturningDto {
  epoch: number
  newAddresses: number
  returningAddresses: number
  totalAddresses: number
}

interface ExchangeFlowDataPointDto {
  epoch: number
  inflowVolume: number
  inflowCount: number
  outflowVolume: number
  outflowCount: number
  netFlow: number
}

interface ExchangeFlowDto {
  dataPoints: ExchangeFlowDataPointDto[]
  totalInflow: number
  totalOutflow: number
}

interface HolderBracketDto {
  name: string
  count: number
  balance: number
  percentageOfSupply: number
}

interface HolderDistributionDto {
  brackets: HolderBracketDto[]
  totalHolders: number
  totalBalance: number
}

interface AvgTxSizeTrendDto {
  epoch: number | null
  date: string | null
  txCount: number
  totalVolume: number
  avgTxSize: number
  medianTxSize: number
}

// Concentration metrics for holder distribution
interface ConcentrationMetricsDto {
  top10Balance: number
  top10Percent: number
  top50Balance: number
  top50Percent: number
  top100Balance: number
  top100Percent: number
}

// Historical snapshot of holder distribution
interface HolderDistributionHistoryDto {
  epoch: number
  snapshotAt: string
  tickStart: number
  tickEnd: number
  brackets: HolderBracketDto[]
  totalHolders: number
  totalBalance: number
  concentration: ConcentrationMetricsDto
  dataSource: string
}

// Extended holder distribution with history
interface HolderDistributionExtendedDto {
  current: HolderDistributionDto
  history: HolderDistributionHistoryDto[]
}

// Network stats history snapshot (for a specific tick window)
interface NetworkStatsHistoryDto {
  epoch: number
  snapshotAt: string
  tickStart: number
  tickEnd: number
  totalTransactions: number
  totalTransfers: number
  totalVolume: number
  uniqueSenders: number
  uniqueReceivers: number
  totalActiveAddresses: number
  newAddresses: number
  returningAddresses: number
  exchangeInflowVolume: number
  exchangeInflowCount: number
  exchangeOutflowVolume: number
  exchangeOutflowCount: number
  exchangeNetFlow: number
  scCallCount: number
  scUniqueCallers: number
  avgTxSize: number
  medianTxSize: number
  newUsers100MPlus: number
  newUsers1BPlus: number
  newUsers10BPlus: number
}

// Extended network stats with history
interface NetworkStatsExtendedDto {
  current: NetworkStatsHistoryDto | null
  history: NetworkStatsHistoryDto[]
}

// Burn stats history snapshot
interface BurnStatsHistoryDto {
  epoch: number
  snapshotAt: string
  tickStart: number
  tickEnd: number
  totalBurned: number
  burnCount: number
  burnAmount: number
  dustBurnCount: number
  dustBurned: number
  transferBurnCount: number
  transferBurned: number
  uniqueBurners: number
  largestBurn: number
  cumulativeBurned: number
}

// Extended burn stats with history
interface BurnStatsExtendedDto {
  current: BurnStatsHistoryDto | null
  history: BurnStatsHistoryDto[]
  allTimeTotalBurned: number
}

// Qearn stats
interface QearnEpochStatsDto {
  epoch: number
  totalBurned: number
  burnCount: number
  totalInput: number
  inputCount: number
  totalOutput: number
  outputCount: number
  uniqueLockers: number
  uniqueUnlockers: number
}

interface QearnStatsDto {
  epochs: QearnEpochStatsDto[]
  allTimeTotalBurned: number
  allTimeTotalInput: number
  allTimeTotalOutput: number
}

// Rich list
interface RichListEntryDto {
  rank: number
  address: string
  label: string | null
  type: string | null
  balance: number
  balanceFormatted: string
  percentageOfSupply: number
}

interface RichListDto {
  entries: RichListEntryDto[]
  page: number
  limit: number
  totalCount: number
  totalPages: number
  totalBalance: number
  snapshotEpoch: number
}

// Supply dashboard
interface EmissionDonationDto {
  address: string
  label: string | null
  amount: number
}

interface EmissionDataPointDto {
  epoch: number
  computorEmission: number
  arbRevenue: number
  donations: EmissionDonationDto[]
  donationTotal: number
  computorCount: number
}

interface BurnDataPointDto {
  epoch: number
  snapshotAt: string
  burnAmount: number
  burnCount: number
}

interface SupplyDashboardDto {
  circulatingSupply: number
  totalEmitted: number
  totalBurned: number
  latestEpochEmission: number
  epochCount: number
  snapshotEpoch: number
  supplyCap: number
  supplyCapProgress: number
  emissionHistory: EmissionDataPointDto[]
  burnHistory: BurnDataPointDto[]
}

// Whale alerts
interface WhaleAlertDto {
  tickNumber: number
  epoch: number
  txHash: string
  sourceAddress: string
  sourceLabel: string | null
  sourceType: string | null
  destAddress: string
  destLabel: string | null
  destType: string | null
  amount: number
  amountFormatted: string
  timestamp: string
}

// Assets
interface AssetSummaryDto {
  assetName: string
  issuerAddress: string
  issuerLabel: string | null
  numberOfDecimalPlaces: number
  totalSupply: number
  holderCount: number
}

interface AssetHolderDetailDto {
  address: string
  label: string | null
  type: string | null
  ownedShares: number
  possessedShares: number
}

interface AssetDetailDto {
  assetName: string
  issuerAddress: string
  issuerLabel: string | null
  numberOfDecimalPlaces: number
  totalSupply: number
  holderCount: number
  snapshotEpoch: number
  topHolders: AssetHolderDetailDto[]
}

interface AssetHoldersPageDto {
  holders: AssetHolderDetailDto[]
  page: number
  limit: number
  totalCount: number
  totalPages: number
}

// Transaction graph
interface GraphNodeDto {
  address: string
  label: string | null
  type: string | null
  totalVolume: number
  depth: number
}

interface GraphLinkDto {
  source: string
  target: string
  amount: number
  txCount: number
}

interface TransactionGraphDto {
  nodes: GraphNodeDto[]
  links: GraphLinkDto[]
}

// Address activity range (first/last seen)
interface AddressActivityRangeDto {
  firstTick: number | null
  firstTimestamp: string | null
  firstEpoch: number | null
  lastTick: number | null
  lastTimestamp: string | null
  lastEpoch: number | null
}

// Epoch countdown
interface EpochCountdownDto {
  currentEpoch: number
  currentEpochStart: string
  averageEpochDurationMs: number
  estimatedEpochEnd: string
  currentTick: number
}

// Epoch metadata
interface EpochMetaDto {
  epoch: number
  initialTick: number
  endTick: number
  endTickStartLogId: number
  endTickEndLogId: number
  isComplete: boolean
  updatedAt: string
}

// Miner/Computor Flow Types
interface ComputorDto {
  epoch: number
  address: string
  index: number
  label: string | null
}

interface ComputorListDto {
  epoch: number
  computors: ComputorDto[]
  count: number
  importedAt: string | null
}

interface MinerFlowStatsDto {
  epoch: number
  snapshotAt: string
  tickStart: number
  tickEnd: number
  emissionEpoch: number
  totalEmission: number
  computorCount: number
  totalOutflow: number
  outflowTxCount: number
  flowToExchangeDirect: number
  flowToExchange1Hop: number
  flowToExchange2Hop: number
  flowToExchange3Plus: number
  flowToExchangeTotal: number
  flowToExchangeCount: number
  flowToOther: number
  minerNetPosition: number
  hop1Volume: number
  hop2Volume: number
  hop3Volume: number
  hop4PlusVolume: number
}

interface MinerFlowSummaryDto {
  latest: MinerFlowStatsDto | null
  history: MinerFlowStatsDto[]
  totalEmissionTracked: number
  totalFlowToExchange: number
  averageExchangeFlowPercent: number
}

interface FlowVisualizationNodeDto {
  id: string
  address: string
  label: string | null
  type: string
  totalInflow: number
  totalOutflow: number
  depth: number
}

interface FlowVisualizationLinkDto {
  sourceId: string
  targetId: string
  amount: number
  transactionCount: number
}

interface FlowVisualizationDto {
  epoch: number
  tickStart: number
  tickEnd: number
  nodes: FlowVisualizationNodeDto[]
  links: FlowVisualizationLinkDto[]
  maxDepth: number
  totalTrackedVolume: number
}

interface FlowHopDto {
  epoch: number
  tickNumber: number
  timestamp: string
  txHash: string
  sourceAddress: string
  sourceLabel: string | null
  sourceType: string | null
  destAddress: string
  destLabel: string | null
  destType: string | null
  amount: number
  originAddress: string
  originType: string
  hopLevel: number
}

interface FlowHopsResponseDto {
  epoch: number
  tickStart: number
  tickEnd: number
  maxDepth: number
  totalHops: number
  hops: FlowHopDto[]
}

// Emission Types
interface ComputorEmissionDto {
  epoch: number
  computorIndex: number
  address: string
  label: string | null
  emissionAmount: number
  emissionTick: number
  emissionTimestamp: string
}

interface EmissionSummaryDto {
  epoch: number
  computorCount: number
  totalEmission: number
  emissionTick: number
  importedAt: string
}

interface EmissionDetailsDto {
  epoch: number
  computorCount: number
  totalEmission: number
  emissionTick: number
  importedAt: string
  emissions: ComputorEmissionDto[]
}

interface ComputorEmissionResponseDto {
  epoch: number
  address: string
  emission: number
}

export type {
  PaginatedResponse,
  TickDto,
  TickDetailDto,
  TransactionDto,
  TransactionDetailDto,
  SpecialTransactionDto,
  LogDto,
  TransferDto,
  AddressDto,
  SearchResponse,
  SearchResultDto,
  NetworkStatsDto,
  ChartDataPointDto,
  EpochSummaryDto,
  EpochStatsDto,
  EpochTransferByTypeDto,
  RewardDistributionDto,
  EpochRewardSummaryDto,
  ContractRewardHistoryDto,
  AddressLabelDto,
  KnownAddressDto,
  LabelStatsDto,
  ProcedureLookupDto,
  TopAddressDto,
  FlowNodeDto,
  AddressFlowDto,
  SmartContractUsageDto,
  ActiveAddressTrendDto,
  NewVsReturningDto,
  ExchangeFlowDataPointDto,
  ExchangeFlowDto,
  HolderBracketDto,
  HolderDistributionDto,
  AvgTxSizeTrendDto,
  ConcentrationMetricsDto,
  HolderDistributionHistoryDto,
  HolderDistributionExtendedDto,
  NetworkStatsHistoryDto,
  NetworkStatsExtendedDto,
  RichListEntryDto,
  RichListDto,
  SupplyDashboardDto,
  AddressActivityRangeDto,
  EpochCountdownDto,
  EpochMetaDto,
  ComputorDto,
  ComputorListDto,
  MinerFlowStatsDto,
  MinerFlowSummaryDto,
  FlowVisualizationNodeDto,
  FlowVisualizationLinkDto,
  FlowVisualizationDto,
  FlowHopDto,
  FlowHopsResponseDto,
  ComputorEmissionDto,
  EmissionSummaryDto,
  EmissionDetailsDto,
  ComputorEmissionResponseDto,
  ParsedInputData,
  WhaleAlertDto,
  AssetSummaryDto,
  AssetDetailDto,
  AssetHolderDetailDto,
  AssetHoldersPageDto,
  GraphNodeDto,
  GraphLinkDto,
  TransactionGraphDto,
}
