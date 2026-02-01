import {
  decodeContractInput,
  getContractSchema,
  getProcedureSchema,
  formatDecodedInputAsJson,
  type DecodedInput,
  type ContractSchema,
  type ProcedureSchema,
} from '~/utils/contractInputDecoder'

/**
 * Contract index mapping from known smart contract addresses
 * This maps the toAddress of a transaction to a contract index
 */
const CONTRACT_ADDRESS_TO_INDEX: Record<string, number> = {
  // These are the known smart contract addresses from the bundle
  // The mapping is populated from the AddressLabelService which has contractIndex
}

/**
 * Composable for decoding smart contract input data
 */
export function useContractInput() {
  /**
   * Decode contract input given transaction data
   *
   * @param inputHex - The input data hex string
   * @param toAddress - The destination address (contract address)
   * @param inputType - The input type / procedure ID
   * @param contractIndex - Optional contract index (if known)
   */
  const decode = (
    inputHex: string | null | undefined,
    toAddress: string,
    inputType: number,
    contractIndex?: number | null
  ): DecodedInput | null => {
    // If inputType is 0, it's a regular transfer, not a contract call
    if (inputType === 0) {
      return null
    }

    // Try to get contract index from the provided value or address mapping
    let index = contractIndex
    if (index === undefined || index === null) {
      index = CONTRACT_ADDRESS_TO_INDEX[toAddress]
    }

    // If we still don't have a contract index, we can't decode
    if (index === undefined || index === null) {
      return null
    }

    return decodeContractInput(inputHex, index, inputType)
  }

  /**
   * Get human-readable procedure name
   */
  const getProcedureName = (contractIndex: number, procedureId: number): string | null => {
    const schema = getProcedureSchema(contractIndex, procedureId)
    return schema?.name ?? null
  }

  /**
   * Get contract name by index
   */
  const getContractName = (contractIndex: number): string | null => {
    const schema = getContractSchema(contractIndex)
    return schema?.name ?? null
  }

  /**
   * Format decoded input as JSON object
   */
  const toJson = (decoded: DecodedInput): object => {
    return formatDecodedInputAsJson(decoded)
  }

  /**
   * Update contract address to index mapping
   * Call this when you have label data with contract indices
   */
  const setContractIndex = (address: string, index: number) => {
    CONTRACT_ADDRESS_TO_INDEX[address] = index
  }

  return {
    decode,
    getProcedureName,
    getContractName,
    toJson,
    setContractIndex,
  }
}
