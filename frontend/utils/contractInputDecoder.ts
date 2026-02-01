/**
 * Qubic Smart Contract Input Decoder
 *
 * Decodes binary input data from Qubic smart contract transactions into human-readable JSON.
 *
 * Type definitions based on:
 * - https://github.com/qubic/core/blob/main/src/contracts/qpi.h
 * - https://github.com/qubic/core/tree/main/src/contracts
 */

// =============================================================================
// Type Definitions
// =============================================================================

/** Field types supported by the decoder */
export type FieldType =
  | 'uint8'
  | 'sint8'
  | 'uint16'
  | 'sint16'
  | 'uint32'
  | 'sint32'
  | 'uint64'
  | 'sint64'
  | 'id' // 32 bytes - public key
  | 'Asset' // 40 bytes - { issuer: id, assetName: uint64 }
  | 'assetName' // uint64 displayed as ASCII string

/** Field definition for struct parsing */
export interface FieldDef {
  name: string
  type: FieldType
  /** For array types, the count of elements */
  count?: number
  /** Optional description for display */
  description?: string
}

/** Procedure input schema */
export interface ProcedureSchema {
  name: string
  fields: FieldDef[]
}

/** Contract schema containing all procedures */
export interface ContractSchema {
  name: string
  index: number
  procedures: Record<number, ProcedureSchema>
}

/** Decoded field value */
export interface DecodedField {
  name: string
  type: FieldType
  value: unknown
  displayValue: string
  description?: string
}

/** Decoded input result */
export interface DecodedInput {
  contractName: string
  contractIndex: number
  procedureName: string
  procedureId: number
  fields: DecodedField[]
  rawHex: string
  /** If decoding failed or partial */
  error?: string
}

// =============================================================================
// Type Sizes
// =============================================================================

const TYPE_SIZES: Record<FieldType, number> = {
  uint8: 1,
  sint8: 1,
  uint16: 2,
  sint16: 2,
  uint32: 4,
  sint32: 4,
  uint64: 8,
  sint64: 8,
  id: 32,
  Asset: 40,
  assetName: 8,
}

// =============================================================================
// Contract Schemas
// =============================================================================

/**
 * QX Contract (Index: 1)
 * DEX for assets on Qubic
 */
const QX_SCHEMA: ContractSchema = {
  name: 'QX',
  index: 1,
  procedures: {
    1: {
      name: 'IssueAsset',
      fields: [
        { name: 'assetName', type: 'assetName', description: 'Asset name (up to 7 ASCII chars)' },
        { name: 'numberOfShares', type: 'sint64', description: 'Number of shares to issue' },
        { name: 'unitOfMeasurement', type: 'assetName', description: 'Unit of measurement' },
        { name: 'numberOfDecimalPlaces', type: 'sint8', description: 'Decimal places (0-18)' },
      ],
    },
    2: {
      name: 'TransferShareOwnershipAndPossession',
      fields: [
        { name: 'issuer', type: 'id', description: 'Asset issuer address' },
        { name: 'newOwnerAndPossessor', type: 'id', description: 'New owner address' },
        { name: 'assetName', type: 'assetName', description: 'Asset name' },
        { name: 'numberOfShares', type: 'sint64', description: 'Number of shares' },
      ],
    },
    5: {
      name: 'AddToAskOrder',
      fields: [
        { name: 'issuer', type: 'id', description: 'Asset issuer address' },
        { name: 'assetName', type: 'assetName', description: 'Asset name' },
        { name: 'price', type: 'sint64', description: 'Price per share in QU' },
        { name: 'numberOfShares', type: 'sint64', description: 'Number of shares' },
      ],
    },
    6: {
      name: 'AddToBidOrder',
      fields: [
        { name: 'issuer', type: 'id', description: 'Asset issuer address' },
        { name: 'assetName', type: 'assetName', description: 'Asset name' },
        { name: 'price', type: 'sint64', description: 'Price per share in QU' },
        { name: 'numberOfShares', type: 'sint64', description: 'Number of shares' },
      ],
    },
    7: {
      name: 'RemoveFromAskOrder',
      fields: [
        { name: 'issuer', type: 'id', description: 'Asset issuer address' },
        { name: 'assetName', type: 'assetName', description: 'Asset name' },
        { name: 'price', type: 'sint64', description: 'Price per share in QU' },
        { name: 'numberOfShares', type: 'sint64', description: 'Number of shares' },
      ],
    },
    8: {
      name: 'RemoveFromBidOrder',
      fields: [
        { name: 'issuer', type: 'id', description: 'Asset issuer address' },
        { name: 'assetName', type: 'assetName', description: 'Asset name' },
        { name: 'price', type: 'sint64', description: 'Price per share in QU' },
        { name: 'numberOfShares', type: 'sint64', description: 'Number of shares' },
      ],
    },
    9: {
      name: 'TransferShareManagementRights',
      fields: [
        { name: 'asset', type: 'Asset', description: 'Asset (issuer + name)' },
        { name: 'numberOfShares', type: 'sint64', description: 'Number of shares' },
        { name: 'newManagingContractIndex', type: 'uint32', description: 'New managing contract index' },
      ],
    },
  },
}

/**
 * Quottery Contract (Index: 2)
 * Betting/prediction market
 */
const QUOTTERY_SCHEMA: ContractSchema = {
  name: 'Quottery',
  index: 2,
  procedures: {
    1: {
      name: 'issueBet',
      fields: [
        { name: 'betDesc', type: 'id', description: 'Bet description hash' },
        // Note: id_8 and other array types would need special handling
        // Simplified for basic decoding
      ],
    },
    2: {
      name: 'joinBet',
      fields: [
        { name: 'betId', type: 'uint32', description: 'Bet ID' },
        { name: 'numberOfSlot', type: 'uint32', description: 'Number of slots' },
        { name: 'option', type: 'uint32', description: 'Option to bet on' },
        { name: '_placeHolder', type: 'uint32', description: 'Placeholder' },
      ],
    },
    3: {
      name: 'cancelBet',
      fields: [{ name: 'betId', type: 'uint32', description: 'Bet ID to cancel' }],
    },
    4: {
      name: 'publishResult',
      fields: [
        { name: 'betId', type: 'uint32', description: 'Bet ID' },
        { name: 'option', type: 'uint32', description: 'Winning option' },
      ],
    },
  },
}

/**
 * QUTIL Contract (Index: 4)
 * Utility functions including SendToMany and BurnQubic
 */
const QUTIL_SCHEMA: ContractSchema = {
  name: 'QUTIL',
  index: 4,
  procedures: {
    1: {
      name: 'SendToManyV1',
      fields: [
        // 25 destination addresses
        ...Array.from({ length: 25 }, (_, i) => ({
          name: `dst${i}`,
          type: 'id' as FieldType,
          description: `Destination address ${i}`,
        })),
        // 25 amounts
        ...Array.from({ length: 25 }, (_, i) => ({
          name: `amt${i}`,
          type: 'sint64' as FieldType,
          description: `Amount for destination ${i}`,
        })),
      ],
    },
    2: {
      name: 'BurnQubic',
      fields: [{ name: 'amount', type: 'sint64', description: 'Amount to burn' }],
    },
  },
}

/**
 * QEARN Contract (Index: 9)
 * Staking/earning contract
 */
const QEARN_SCHEMA: ContractSchema = {
  name: 'QEARN',
  index: 9,
  procedures: {
    1: {
      name: 'lock',
      fields: [], // Empty input - uses invocationReward
    },
    2: {
      name: 'unlock',
      fields: [
        { name: 'amount', type: 'uint64', description: 'Amount to unlock' },
        { name: 'lockedEpoch', type: 'uint32', description: 'Epoch when locked' },
      ],
    },
  },
}

/**
 * QVAULT Contract (Index: 10)
 * Vault management
 */
const QVAULT_SCHEMA: ContractSchema = {
  name: 'QVAULT',
  index: 10,
  procedures: {
    1: {
      name: 'submitAuthAddress',
      fields: [{ name: 'newAddress', type: 'id', description: 'New auth address' }],
    },
    2: {
      name: 'changeAuthAddress',
      fields: [{ name: 'numberOfChangedAddress', type: 'uint32', description: 'Number of changed address' }],
    },
    3: {
      name: 'submitDistributionPermille',
      fields: [
        { name: 'newQCAPHolderPermille', type: 'uint32', description: 'QCAP holder share (permille)' },
        { name: 'newReinvestingPermille', type: 'uint32', description: 'Reinvesting share (permille)' },
        { name: 'newDevPermille', type: 'uint32', description: 'Dev share (permille)' },
      ],
    },
    4: {
      name: 'changeDistributionPermille',
      fields: [
        { name: 'newQCAPHolderPermille', type: 'uint32', description: 'QCAP holder share (permille)' },
        { name: 'newReinvestingPermille', type: 'uint32', description: 'Reinvesting share (permille)' },
        { name: 'newDevPermille', type: 'uint32', description: 'Dev share (permille)' },
      ],
    },
    5: {
      name: 'submitReinvestingAddress',
      fields: [{ name: 'newAddress', type: 'id', description: 'New reinvesting address' }],
    },
    6: {
      name: 'changeReinvestingAddress',
      fields: [{ name: 'newAddress', type: 'id', description: 'New reinvesting address' }],
    },
    7: {
      name: 'submitAdminAddress',
      fields: [{ name: 'newAddress', type: 'id', description: 'New admin address' }],
    },
    8: {
      name: 'changeAdminAddress',
      fields: [{ name: 'newAddress', type: 'id', description: 'New admin address' }],
    },
    9: {
      name: 'submitBannedAddress',
      fields: [{ name: 'bannedAddress', type: 'id', description: 'Address to ban' }],
    },
    10: {
      name: 'saveBannedAddress',
      fields: [{ name: 'bannedAddress', type: 'id', description: 'Address to save as banned' }],
    },
    11: {
      name: 'submitUnbannedAddress',
      fields: [{ name: 'unbannedAddress', type: 'id', description: 'Address to unban' }],
    },
    12: {
      name: 'unblockBannedAddress',
      fields: [{ name: 'unbannedAddress', type: 'id', description: 'Address to unblock' }],
    },
  },
}

/**
 * QSWAP Contract (Index: 13)
 * AMM swap contract
 */
const QSWAP_SCHEMA: ContractSchema = {
  name: 'QSWAP',
  index: 13,
  procedures: {
    1: {
      name: 'IssueAsset',
      fields: [
        { name: 'assetName', type: 'assetName', description: 'Asset name' },
        { name: 'numberOfShares', type: 'sint64', description: 'Number of shares' },
        { name: 'unitOfMeasurement', type: 'assetName', description: 'Unit of measurement' },
        { name: 'numberOfDecimalPlaces', type: 'sint8', description: 'Decimal places' },
      ],
    },
    2: {
      name: 'CreatePool',
      fields: [
        { name: 'assetIssuer', type: 'id', description: 'Asset issuer address' },
        { name: 'assetName', type: 'assetName', description: 'Asset name' },
      ],
    },
    3: {
      name: 'AddLiquidity',
      fields: [
        { name: 'assetIssuer', type: 'id', description: 'Asset issuer address' },
        { name: 'assetName', type: 'assetName', description: 'Asset name' },
        { name: 'assetAmountDesired', type: 'sint64', description: 'Desired asset amount' },
        { name: 'quAmountMin', type: 'sint64', description: 'Minimum QU amount' },
        { name: 'assetAmountMin', type: 'sint64', description: 'Minimum asset amount' },
      ],
    },
    4: {
      name: 'RemoveLiquidity',
      fields: [
        { name: 'assetIssuer', type: 'id', description: 'Asset issuer address' },
        { name: 'assetName', type: 'assetName', description: 'Asset name' },
        { name: 'burnLiquidity', type: 'sint64', description: 'Liquidity to burn' },
        { name: 'quAmountMin', type: 'sint64', description: 'Minimum QU to receive' },
        { name: 'assetAmountMin', type: 'sint64', description: 'Minimum asset to receive' },
      ],
    },
    5: {
      name: 'SwapExactQuForAsset',
      fields: [
        { name: 'assetIssuer', type: 'id', description: 'Asset issuer address' },
        { name: 'assetName', type: 'assetName', description: 'Asset name' },
        { name: 'assetAmountOutMin', type: 'sint64', description: 'Minimum asset output' },
      ],
    },
    6: {
      name: 'SwapQuForExactAsset',
      fields: [
        { name: 'assetIssuer', type: 'id', description: 'Asset issuer address' },
        { name: 'assetName', type: 'assetName', description: 'Asset name' },
        { name: 'assetAmountOut', type: 'sint64', description: 'Exact asset output' },
      ],
    },
    7: {
      name: 'SwapExactAssetForQu',
      fields: [
        { name: 'assetIssuer', type: 'id', description: 'Asset issuer address' },
        { name: 'assetName', type: 'assetName', description: 'Asset name' },
        { name: 'assetAmountIn', type: 'sint64', description: 'Exact asset input' },
        { name: 'quAmountOutMin', type: 'sint64', description: 'Minimum QU output' },
      ],
    },
    8: {
      name: 'SwapAssetForExactQu',
      fields: [
        { name: 'assetIssuer', type: 'id', description: 'Asset issuer address' },
        { name: 'assetName', type: 'assetName', description: 'Asset name' },
        { name: 'assetAmountInMax', type: 'sint64', description: 'Maximum asset input' },
        { name: 'quAmountOut', type: 'sint64', description: 'Exact QU output' },
      ],
    },
    9: {
      name: 'TransferShareOwnershipAndPossession',
      fields: [
        { name: 'assetIssuer', type: 'id', description: 'Asset issuer address' },
        { name: 'assetName', type: 'assetName', description: 'Asset name' },
        { name: 'newOwnerAndPossessor', type: 'id', description: 'New owner address' },
        { name: 'amount', type: 'sint64', description: 'Amount to transfer' },
      ],
    },
  },
}

/**
 * RANDOM Contract (Index: 3)
 * Random number generation
 */
const RANDOM_SCHEMA: ContractSchema = {
  name: 'RANDOM',
  index: 3,
  procedures: {
    1: {
      name: 'RevealAndCommit',
      fields: [
        // bit_4096 is 512 bytes - we'll show it as raw hex
        { name: 'committedDigest', type: 'id', description: 'Committed digest' },
      ],
    },
  },
}

/**
 * MLM Contract (Index: 5)
 * My Last Match - no public procedures
 */
const MLM_SCHEMA: ContractSchema = {
  name: 'MLM',
  index: 5,
  procedures: {},
}

/**
 * GQMPROP Contract (Index: 6)
 * General Quorum Proposal
 */
const GQMPROP_SCHEMA: ContractSchema = {
  name: 'GQMPROP',
  index: 6,
  procedures: {
    // SetProposal and Vote use complex types - basic schema
    1: {
      name: 'SetProposal',
      fields: [], // Complex ProposalDataT type
    },
    2: {
      name: 'Vote',
      fields: [], // Complex ProposalSingleVoteDataV1 type
    },
  },
}

/**
 * SWATCH Contract (Index: 7)
 * Supply Watcher - no public procedures
 */
const SWATCH_SCHEMA: ContractSchema = {
  name: 'SWATCH',
  index: 7,
  procedures: {},
}

/**
 * CCF Contract (Index: 8)
 * Computor Controlled Fund
 */
const CCF_SCHEMA: ContractSchema = {
  name: 'CCF',
  index: 8,
  procedures: {
    1: {
      name: 'SetProposal',
      fields: [], // Complex ProposalDataT + subscription fields
    },
    2: {
      name: 'Vote',
      fields: [], // Complex ProposalSingleVoteDataV1 type
    },
  },
}

/**
 * MSVAULT Contract (Index: 11)
 * Multi-signature Vault
 */
const MSVAULT_SCHEMA: ContractSchema = {
  name: 'MSVAULT',
  index: 11,
  procedures: {
    1: {
      name: 'registerVault',
      fields: [
        { name: 'vaultName', type: 'id', description: 'Vault name hash' },
        // owners array (up to 5 ids) - simplified
      ],
    },
    2: {
      name: 'deposit',
      fields: [{ name: 'vaultId', type: 'uint64', description: 'Vault ID' }],
    },
    3: {
      name: 'releaseTo',
      fields: [
        { name: 'vaultId', type: 'uint64', description: 'Vault ID' },
        { name: 'amount', type: 'uint64', description: 'Amount to release' },
        { name: 'destination', type: 'id', description: 'Destination address' },
      ],
    },
    4: {
      name: 'resetRelease',
      fields: [{ name: 'vaultId', type: 'uint64', description: 'Vault ID' }],
    },
    5: {
      name: 'voteFeeChange',
      fields: [
        { name: 'newRegisteringFee', type: 'uint64', description: 'New registering fee' },
        { name: 'newReleaseFee', type: 'uint64', description: 'New release fee' },
        { name: 'newReleaseResetFee', type: 'uint64', description: 'New release reset fee' },
        { name: 'newHoldingFee', type: 'uint64', description: 'New holding fee' },
        { name: 'newDepositFee', type: 'uint64', description: 'New deposit fee' },
        { name: 'burnFee', type: 'uint64', description: 'Burn fee' },
      ],
    },
    6: {
      name: 'depositAsset',
      fields: [
        { name: 'vaultId', type: 'uint64', description: 'Vault ID' },
        { name: 'asset', type: 'Asset', description: 'Asset to deposit' },
        { name: 'amount', type: 'uint64', description: 'Amount' },
      ],
    },
    7: {
      name: 'releaseAssetTo',
      fields: [
        { name: 'vaultId', type: 'uint64', description: 'Vault ID' },
        { name: 'asset', type: 'Asset', description: 'Asset' },
        { name: 'amount', type: 'uint64', description: 'Amount' },
        { name: 'destination', type: 'id', description: 'Destination' },
      ],
    },
    8: {
      name: 'resetAssetRelease',
      fields: [{ name: 'vaultId', type: 'uint64', description: 'Vault ID' }],
    },
    9: {
      name: 'revokeAssetManagementRights',
      fields: [
        { name: 'asset', type: 'Asset', description: 'Asset' },
        { name: 'numberOfShares', type: 'sint64', description: 'Number of shares' },
      ],
    },
  },
}

/**
 * QBAY Contract (Index: 12)
 * NFT Marketplace
 */
const QBAY_SCHEMA: ContractSchema = {
  name: 'QBAY',
  index: 12,
  procedures: {
    1: {
      name: 'settingCFBAndQubicPrice',
      fields: [
        { name: 'CFBPrice', type: 'uint64', description: 'CFB price' },
        { name: 'QubicPrice', type: 'uint64', description: 'Qubic price' },
      ],
    },
    2: {
      name: 'createCollection',
      fields: [
        { name: 'priceForDropMint', type: 'uint64', description: 'Price for drop mint' },
        { name: 'volume', type: 'uint32', description: 'Collection volume' },
        { name: 'royalty', type: 'uint32', description: 'Royalty percentage' },
        { name: 'maxSizePerOneId', type: 'uint32', description: 'Max size per ID' },
        // URI is 64 bytes - skipped for now
      ],
    },
    3: {
      name: 'mint',
      fields: [
        { name: 'royalty', type: 'uint32', description: 'Royalty' },
        { name: 'collectionId', type: 'uint32', description: 'Collection ID' },
        // URI is 64 bytes
      ],
    },
    4: {
      name: 'mintOfDrop',
      fields: [
        { name: 'collectionId', type: 'uint32', description: 'Collection ID' },
        // URI is 64 bytes
      ],
    },
    5: {
      name: 'transfer',
      fields: [
        { name: 'receiver', type: 'id', description: 'Receiver address' },
        { name: 'NFTid', type: 'uint32', description: 'NFT ID' },
      ],
    },
    6: {
      name: 'listInMarket',
      fields: [
        { name: 'price', type: 'uint64', description: 'Price' },
        { name: 'NFTid', type: 'uint32', description: 'NFT ID' },
      ],
    },
    7: {
      name: 'buy',
      fields: [
        { name: 'NFTid', type: 'uint32', description: 'NFT ID' },
        { name: 'methodOfPayment', type: 'uint8', description: 'Payment method (0=QU, 1=CFB)' },
      ],
    },
    8: {
      name: 'cancelSale',
      fields: [{ name: 'NFTid', type: 'uint32', description: 'NFT ID' }],
    },
    9: {
      name: 'listInExchange',
      fields: [
        { name: 'possessedNFT', type: 'uint32', description: 'Possessed NFT ID' },
        { name: 'anotherNFT', type: 'uint32', description: 'Desired NFT ID' },
      ],
    },
    10: {
      name: 'cancelExchange',
      fields: [
        { name: 'possessedNFT', type: 'uint32', description: 'Possessed NFT ID' },
        { name: 'anotherNFT', type: 'uint32', description: 'Desired NFT ID' },
      ],
    },
    11: {
      name: 'makeOffer',
      fields: [
        { name: 'askPrice', type: 'uint64', description: 'Offer price' },
        { name: 'NFTid', type: 'uint32', description: 'NFT ID' },
        { name: 'paymentMethod', type: 'uint8', description: 'Payment method' },
      ],
    },
    12: {
      name: 'acceptOffer',
      fields: [{ name: 'NFTid', type: 'uint32', description: 'NFT ID' }],
    },
    13: {
      name: 'cancelOffer',
      fields: [{ name: 'NFTid', type: 'uint32', description: 'NFT ID' }],
    },
    14: {
      name: 'createTraditionalAuction',
      fields: [
        { name: 'minPrice', type: 'uint64', description: 'Minimum price' },
        { name: 'NFTId', type: 'uint32', description: 'NFT ID' },
        { name: 'startYear', type: 'uint32', description: 'Start year' },
        { name: 'startMonth', type: 'uint32', description: 'Start month' },
        { name: 'startDay', type: 'uint32', description: 'Start day' },
        { name: 'startHour', type: 'uint32', description: 'Start hour' },
        { name: 'endYear', type: 'uint32', description: 'End year' },
        { name: 'endMonth', type: 'uint32', description: 'End month' },
        { name: 'endDay', type: 'uint32', description: 'End day' },
        { name: 'endHour', type: 'uint32', description: 'End hour' },
        { name: 'paymentMethodOfAuction', type: 'uint8', description: 'Payment method' },
      ],
    },
    15: {
      name: 'bidOnTraditionalAuction',
      fields: [
        { name: 'price', type: 'uint64', description: 'Bid price' },
        { name: 'NFTId', type: 'uint32', description: 'NFT ID' },
        { name: 'paymentMethod', type: 'uint8', description: 'Payment method' },
      ],
    },
    16: {
      name: 'TransferShareManagementRights',
      fields: [
        { name: 'asset', type: 'Asset', description: 'Asset' },
        { name: 'numberOfShares', type: 'sint64', description: 'Number of shares' },
        { name: 'newManagingContractIndex', type: 'uint32', description: 'New contract index' },
      ],
    },
    17: {
      name: 'changeStatusOfMarketPlace',
      fields: [{ name: 'status', type: 'uint8', description: 'Status (0=closed, 1=open)' }],
    },
  },
}

/**
 * NOST Contract (Index: 14)
 * Nostromo - Launchpad
 */
const NOST_SCHEMA: ContractSchema = {
  name: 'NOST',
  index: 14,
  procedures: {
    1: {
      name: 'registerInTier',
      fields: [{ name: 'tierLevel', type: 'uint32', description: 'Tier level' }],
    },
    2: {
      name: 'logoutFromTier',
      fields: [],
    },
    3: {
      name: 'createProject',
      fields: [
        { name: 'tokenName', type: 'assetName', description: 'Token name' },
        { name: 'supply', type: 'uint64', description: 'Token supply' },
        { name: 'startYear', type: 'uint32', description: 'Start year' },
        { name: 'startMonth', type: 'uint32', description: 'Start month' },
        { name: 'startDay', type: 'uint32', description: 'Start day' },
        { name: 'startHour', type: 'uint32', description: 'Start hour' },
        { name: 'endYear', type: 'uint32', description: 'End year' },
        { name: 'endMonth', type: 'uint32', description: 'End month' },
        { name: 'endDay', type: 'uint32', description: 'End day' },
        { name: 'endHour', type: 'uint32', description: 'End hour' },
      ],
    },
    4: {
      name: 'voteInProject',
      fields: [
        { name: 'indexOfProject', type: 'uint32', description: 'Project index' },
        { name: 'decision', type: 'uint8', description: 'Vote (0=no, 1=yes)' },
      ],
    },
    5: {
      name: 'createFundraising',
      fields: [
        { name: 'tokenPrice', type: 'uint64', description: 'Token price' },
        { name: 'soldAmount', type: 'uint64', description: 'Amount to sell' },
        { name: 'requiredFunds', type: 'uint64', description: 'Required funds' },
        { name: 'indexOfProject', type: 'uint32', description: 'Project index' },
        // Many date fields follow - simplified
      ],
    },
    6: {
      name: 'investInProject',
      fields: [{ name: 'indexOfFundraising', type: 'uint32', description: 'Fundraising index' }],
    },
    7: {
      name: 'claimToken',
      fields: [
        { name: 'amount', type: 'uint64', description: 'Amount to claim' },
        { name: 'indexOfFundraising', type: 'uint32', description: 'Fundraising index' },
      ],
    },
    8: {
      name: 'upgradeTier',
      fields: [{ name: 'newTierLevel', type: 'uint32', description: 'New tier level' }],
    },
    9: {
      name: 'TransferShareManagementRights',
      fields: [
        { name: 'asset', type: 'Asset', description: 'Asset' },
        { name: 'numberOfShares', type: 'sint64', description: 'Number of shares' },
        { name: 'newManagingContractIndex', type: 'uint32', description: 'New contract index' },
      ],
    },
  },
}

/**
 * QDRAW Contract (Index: 15)
 * Lottery
 */
const QDRAW_SCHEMA: ContractSchema = {
  name: 'QDRAW',
  index: 15,
  procedures: {
    1: {
      name: 'buyTicket',
      fields: [{ name: 'ticketCount', type: 'uint64', description: 'Number of tickets' }],
    },
  },
}

/**
 * RL Contract (Index: 16)
 * Random Lottery
 */
const RL_SCHEMA: ContractSchema = {
  name: 'RL',
  index: 16,
  procedures: {
    1: {
      name: 'BuyTicket',
      fields: [], // Empty - uses invocationReward
    },
    2: {
      name: 'SetPrice',
      fields: [{ name: 'newPrice', type: 'uint64', description: 'New ticket price' }],
    },
    3: {
      name: 'SetSchedule',
      fields: [{ name: 'newSchedule', type: 'uint8', description: 'New schedule bitmask' }],
    },
  },
}

/**
 * QBOND Contract (Index: 17)
 * Bond trading
 */
const QBOND_SCHEMA: ContractSchema = {
  name: 'QBOND',
  index: 17,
  procedures: {
    1: {
      name: 'Stake',
      fields: [{ name: 'quMillions', type: 'sint64', description: 'QU millions to stake' }],
    },
    2: {
      name: 'TransferMBondOwnershipAndPossession',
      fields: [
        { name: 'newOwnerAndPossessor', type: 'id', description: 'New owner address' },
        { name: 'epoch', type: 'sint64', description: 'Bond epoch' },
        { name: 'numberOfMBonds', type: 'sint64', description: 'Number of MBonds' },
      ],
    },
    3: {
      name: 'AddAskOrder',
      fields: [
        { name: 'epoch', type: 'sint64', description: 'Bond epoch' },
        { name: 'price', type: 'sint64', description: 'Price' },
        { name: 'numberOfMBonds', type: 'sint64', description: 'Number of MBonds' },
      ],
    },
    4: {
      name: 'RemoveAskOrder',
      fields: [
        { name: 'epoch', type: 'sint64', description: 'Bond epoch' },
        { name: 'price', type: 'sint64', description: 'Price' },
        { name: 'numberOfMBonds', type: 'sint64', description: 'Number of MBonds' },
      ],
    },
    5: {
      name: 'AddBidOrder',
      fields: [
        { name: 'epoch', type: 'sint64', description: 'Bond epoch' },
        { name: 'price', type: 'sint64', description: 'Price' },
        { name: 'numberOfMBonds', type: 'sint64', description: 'Number of MBonds' },
      ],
    },
    6: {
      name: 'RemoveBidOrder',
      fields: [
        { name: 'epoch', type: 'sint64', description: 'Bond epoch' },
        { name: 'price', type: 'sint64', description: 'Price' },
        { name: 'numberOfMBonds', type: 'sint64', description: 'Number of MBonds' },
      ],
    },
    7: {
      name: 'BurnQU',
      fields: [{ name: 'amount', type: 'sint64', description: 'Amount to burn' }],
    },
    8: {
      name: 'UpdateCFA',
      fields: [
        { name: 'user', type: 'id', description: 'User address' },
        { name: 'operation', type: 'uint8', description: 'Operation (0=remove, 1=add)' },
      ],
    },
  },
}

/**
 * QIP Contract (Index: 18)
 * ICO Platform
 */
const QIP_SCHEMA: ContractSchema = {
  name: 'QIP',
  index: 18,
  procedures: {
    1: {
      name: 'createICO',
      fields: [
        { name: 'issuer', type: 'id', description: 'Token issuer' },
        // 10 distribution addresses
        ...Array.from({ length: 10 }, (_, i) => ({
          name: `address${i + 1}`,
          type: 'id' as FieldType,
          description: `Distribution address ${i + 1}`,
        })),
        { name: 'assetName', type: 'assetName', description: 'Asset name' },
        { name: 'price1', type: 'uint64', description: 'Phase 1 price' },
        { name: 'price2', type: 'uint64', description: 'Phase 2 price' },
        { name: 'price3', type: 'uint64', description: 'Phase 3 price' },
        { name: 'saleAmountForPhase1', type: 'uint64', description: 'Phase 1 sale amount' },
        { name: 'saleAmountForPhase2', type: 'uint64', description: 'Phase 2 sale amount' },
        { name: 'saleAmountForPhase3', type: 'uint64', description: 'Phase 3 sale amount' },
        // 10 percent values
        ...Array.from({ length: 10 }, (_, i) => ({
          name: `percent${i + 1}`,
          type: 'uint32' as FieldType,
          description: `Percent for address ${i + 1}`,
        })),
        { name: 'startEpoch', type: 'uint32', description: 'Start epoch' },
      ],
    },
    2: {
      name: 'buyToken',
      fields: [
        { name: 'indexOfICO', type: 'uint32', description: 'ICO index' },
        { name: 'amount', type: 'uint64', description: 'Amount to buy' },
      ],
    },
    3: {
      name: 'TransferShareManagementRights',
      fields: [
        { name: 'asset', type: 'Asset', description: 'Asset' },
        { name: 'numberOfShares', type: 'sint64', description: 'Number of shares' },
        { name: 'newManagingContractIndex', type: 'uint32', description: 'New contract index' },
      ],
    },
  },
}

/**
 * QRAFFLE Contract (Index: 19)
 * Raffle system
 */
const QRAFFLE_SCHEMA: ContractSchema = {
  name: 'QRAFFLE',
  index: 19,
  procedures: {
    1: {
      name: 'registerInSystem',
      fields: [{ name: 'useQXMR', type: 'uint8', description: 'Use QXMR (0=no, 1=yes)' }],
    },
    2: {
      name: 'logoutInSystem',
      fields: [],
    },
    3: {
      name: 'submitEntryAmount',
      fields: [{ name: 'amount', type: 'uint64', description: 'Entry amount' }],
    },
    4: {
      name: 'submitProposal',
      fields: [
        { name: 'tokenIssuer', type: 'id', description: 'Token issuer' },
        { name: 'tokenName', type: 'assetName', description: 'Token name' },
        { name: 'entryAmount', type: 'uint64', description: 'Entry amount' },
      ],
    },
    5: {
      name: 'voteInProposal',
      fields: [
        { name: 'indexOfProposal', type: 'uint32', description: 'Proposal index' },
        { name: 'yes', type: 'uint8', description: 'Vote (0=no, 1=yes)' },
      ],
    },
    6: {
      name: 'depositInQuRaffle',
      fields: [],
    },
    7: {
      name: 'depositInTokenRaffle',
      fields: [{ name: 'indexOfTokenRaffle', type: 'uint32', description: 'Token raffle index' }],
    },
    8: {
      name: 'TransferShareManagementRights',
      fields: [
        { name: 'tokenIssuer', type: 'id', description: 'Token issuer' },
        { name: 'tokenName', type: 'assetName', description: 'Token name' },
        { name: 'numberOfShares', type: 'sint64', description: 'Number of shares' },
        { name: 'newManagingContractIndex', type: 'uint32', description: 'New contract index' },
      ],
    },
  },
}

/**
 * QRWA Contract (Index: 20)
 * Real World Assets - optional contract
 */
const QRWA_SCHEMA: ContractSchema = {
  name: 'QRWA',
  index: 20,
  procedures: {
    1: {
      name: 'DonateToTreasury',
      fields: [{ name: 'amount', type: 'uint64', description: 'Amount to donate' }],
    },
    2: {
      name: 'VoteGovParams',
      fields: [], // Complex QRWAGovParams type
    },
    3: {
      name: 'CreateAssetReleasePoll',
      fields: [
        { name: 'proposalName', type: 'id', description: 'Proposal name hash' },
        { name: 'asset', type: 'Asset', description: 'Asset' },
        { name: 'amount', type: 'uint64', description: 'Amount' },
        { name: 'destination', type: 'id', description: 'Destination address' },
      ],
    },
    4: {
      name: 'VoteAssetRelease',
      fields: [
        { name: 'proposalId', type: 'uint64', description: 'Proposal ID' },
        { name: 'option', type: 'uint64', description: 'Vote option (0=no, 1=yes)' },
      ],
    },
    5: {
      name: 'DepositGeneralAsset',
      fields: [
        { name: 'asset', type: 'Asset', description: 'Asset' },
        { name: 'amount', type: 'uint64', description: 'Amount' },
      ],
    },
    6: {
      name: 'RevokeAssetManagementRights',
      fields: [
        { name: 'asset', type: 'Asset', description: 'Asset' },
        { name: 'numberOfShares', type: 'sint64', description: 'Number of shares' },
      ],
    },
  },
}

// =============================================================================
// Contract Registry
// =============================================================================

/** Map of contract index to schema */
const CONTRACT_SCHEMAS: Record<number, ContractSchema> = {
  1: QX_SCHEMA,
  2: QUOTTERY_SCHEMA,
  3: RANDOM_SCHEMA,
  4: QUTIL_SCHEMA,
  5: MLM_SCHEMA,
  6: GQMPROP_SCHEMA,
  7: SWATCH_SCHEMA,
  8: CCF_SCHEMA,
  9: QEARN_SCHEMA,
  10: QVAULT_SCHEMA,
  11: MSVAULT_SCHEMA,
  12: QBAY_SCHEMA,
  13: QSWAP_SCHEMA,
  14: NOST_SCHEMA,
  15: QDRAW_SCHEMA,
  16: RL_SCHEMA,
  17: QBOND_SCHEMA,
  18: QIP_SCHEMA,
  19: QRAFFLE_SCHEMA,
  20: QRWA_SCHEMA,
}

// =============================================================================
// Binary Parsing Utilities
// =============================================================================

/**
 * Convert hex string to Uint8Array
 */
function hexToBytes(hex: string): Uint8Array {
  const cleanHex = hex.startsWith('0x') ? hex.slice(2) : hex
  const bytes = new Uint8Array(cleanHex.length / 2)
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(cleanHex.substr(i * 2, 2), 16)
  }
  return bytes
}

/**
 * Convert bytes to hex string
 */
function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('')
}

/**
 * Read unsigned integer from buffer (little-endian)
 */
function readUint(buffer: Uint8Array, offset: number, size: number): bigint {
  let value = 0n
  for (let i = 0; i < size; i++) {
    value |= BigInt(buffer[offset + i]) << BigInt(i * 8)
  }
  return value
}

/**
 * Read signed integer from buffer (little-endian, two's complement)
 */
function readSint(buffer: Uint8Array, offset: number, size: number): bigint {
  const unsigned = readUint(buffer, offset, size)
  const signBit = 1n << BigInt(size * 8 - 1)
  if (unsigned >= signBit) {
    return unsigned - (1n << BigInt(size * 8))
  }
  return unsigned
}

/**
 * Decode asset name from uint64 to ASCII string
 * Asset names are stored as up to 7 ASCII characters packed into a uint64
 */
function decodeAssetName(value: bigint): string {
  let name = ''
  let v = value
  for (let i = 0; i < 8; i++) {
    const char = Number(v & 0xffn)
    if (char === 0) break
    name += String.fromCharCode(char)
    v >>= 8n
  }
  return name
}

/**
 * Encode bytes to Qubic base26 address format
 */
function bytesToAddress(bytes: Uint8Array): string {
  // Qubic uses a custom base26 encoding for addresses
  // For now, return hex representation - proper base26 encoding would need the full algorithm
  const hex = bytesToHex(bytes)

  // Check if it's a zero address (burn address)
  if (bytes.every((b) => b === 0)) {
    return 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
  }

  // Return hex for non-zero addresses (full base26 implementation would go here)
  return hex.toUpperCase()
}

// =============================================================================
// Main Decoder
// =============================================================================

/**
 * Decode a single field from binary data
 */
function decodeField(
  buffer: Uint8Array,
  offset: number,
  field: FieldDef
): { value: unknown; displayValue: string; bytesRead: number } {
  const { type } = field

  switch (type) {
    case 'uint8': {
      const value = Number(readUint(buffer, offset, 1))
      return { value, displayValue: value.toString(), bytesRead: 1 }
    }
    case 'sint8': {
      const value = Number(readSint(buffer, offset, 1))
      return { value, displayValue: value.toString(), bytesRead: 1 }
    }
    case 'uint16': {
      const value = Number(readUint(buffer, offset, 2))
      return { value, displayValue: value.toString(), bytesRead: 2 }
    }
    case 'sint16': {
      const value = Number(readSint(buffer, offset, 2))
      return { value, displayValue: value.toString(), bytesRead: 2 }
    }
    case 'uint32': {
      const value = Number(readUint(buffer, offset, 4))
      return { value, displayValue: value.toLocaleString(), bytesRead: 4 }
    }
    case 'sint32': {
      const value = Number(readSint(buffer, offset, 4))
      return { value, displayValue: value.toLocaleString(), bytesRead: 4 }
    }
    case 'uint64': {
      const value = readUint(buffer, offset, 8)
      return { value: value.toString(), displayValue: value.toLocaleString(), bytesRead: 8 }
    }
    case 'sint64': {
      const value = readSint(buffer, offset, 8)
      return { value: value.toString(), displayValue: value.toLocaleString(), bytesRead: 8 }
    }
    case 'id': {
      const bytes = buffer.slice(offset, offset + 32)
      const address = bytesToAddress(bytes)
      return { value: address, displayValue: address, bytesRead: 32 }
    }
    case 'Asset': {
      const issuerBytes = buffer.slice(offset, offset + 32)
      const issuer = bytesToAddress(issuerBytes)
      const assetNameValue = readUint(buffer, offset + 32, 8)
      const assetName = decodeAssetName(assetNameValue)
      const value = { issuer, assetName }
      return {
        value,
        displayValue: `${assetName} (${issuer.slice(0, 8)}...)`,
        bytesRead: 40,
      }
    }
    case 'assetName': {
      const rawValue = readUint(buffer, offset, 8)
      const name = decodeAssetName(rawValue)
      return { value: name, displayValue: name || '(empty)', bytesRead: 8 }
    }
    default:
      throw new Error(`Unknown type: ${type}`)
  }
}

/**
 * Get contract schema by contract index
 */
export function getContractSchema(contractIndex: number): ContractSchema | undefined {
  return CONTRACT_SCHEMAS[contractIndex]
}

/**
 * Get procedure schema by contract index and procedure ID
 */
export function getProcedureSchema(
  contractIndex: number,
  procedureId: number
): ProcedureSchema | undefined {
  const contract = CONTRACT_SCHEMAS[contractIndex]
  if (!contract) return undefined
  return contract.procedures[procedureId]
}

/**
 * Decode contract input from hex string
 *
 * @param inputHex - The input data as hex string
 * @param contractIndex - The contract index (derived from toAddress)
 * @param procedureId - The procedure ID (inputType from transaction)
 * @returns Decoded input or null if schema not found
 */
export function decodeContractInput(
  inputHex: string | null | undefined,
  contractIndex: number,
  procedureId: number
): DecodedInput | null {
  const contract = CONTRACT_SCHEMAS[contractIndex]
  if (!contract) {
    return null
  }

  const procedure = contract.procedures[procedureId]
  if (!procedure) {
    return null
  }

  const result: DecodedInput = {
    contractName: contract.name,
    contractIndex,
    procedureName: procedure.name,
    procedureId,
    fields: [],
    rawHex: inputHex || '',
  }

  // If no input data or empty procedure, return early
  if (!inputHex || procedure.fields.length === 0) {
    return result
  }

  try {
    const buffer = hexToBytes(inputHex)
    let offset = 0

    for (const field of procedure.fields) {
      if (offset >= buffer.length) {
        // Not enough data - might be optional fields or truncated
        break
      }

      const decoded = decodeField(buffer, offset, field)
      result.fields.push({
        name: field.name,
        type: field.type,
        value: decoded.value,
        displayValue: decoded.displayValue,
        description: field.description,
      })
      offset += decoded.bytesRead
    }

    // Check if there's remaining data
    if (offset < buffer.length) {
      result.error = `${buffer.length - offset} bytes of unread data remaining`
    }
  } catch (err) {
    result.error = err instanceof Error ? err.message : 'Unknown decoding error'
  }

  return result
}

/**
 * Get all supported contracts
 */
export function getSupportedContracts(): Array<{ index: number; name: string }> {
  return Object.entries(CONTRACT_SCHEMAS).map(([index, schema]) => ({
    index: parseInt(index),
    name: schema.name,
  }))
}

/**
 * Check if a contract is supported for input decoding
 */
export function isContractSupported(contractIndex: number): boolean {
  return contractIndex in CONTRACT_SCHEMAS
}

/**
 * Format decoded input as JSON for display
 */
export function formatDecodedInputAsJson(decoded: DecodedInput): object {
  const result: Record<string, unknown> = {}
  for (const field of decoded.fields) {
    result[field.name] = field.value
  }
  return result
}
