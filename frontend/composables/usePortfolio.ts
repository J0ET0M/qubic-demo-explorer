const STORAGE_KEY = 'qli-portfolio-addresses'

const addresses = ref<string[]>([])
let initialized = false

function loadFromStorage() {
  if (initialized) return
  initialized = true
  if (typeof window === 'undefined') return
  try {
    const stored = localStorage.getItem(STORAGE_KEY)
    if (stored) {
      addresses.value = JSON.parse(stored)
    }
  } catch {
    addresses.value = []
  }
}

function saveToStorage() {
  if (typeof window === 'undefined') return
  localStorage.setItem(STORAGE_KEY, JSON.stringify(addresses.value))
}

export const usePortfolio = () => {
  loadFromStorage()

  const addAddress = (address: string) => {
    if (!addresses.value.includes(address)) {
      addresses.value.push(address)
      saveToStorage()
    }
  }

  const removeAddress = (address: string) => {
    addresses.value = addresses.value.filter(a => a !== address)
    saveToStorage()
  }

  const isInPortfolio = (address: string) => {
    return addresses.value.includes(address)
  }

  return {
    addresses: readonly(addresses),
    addAddress,
    removeAddress,
    isInPortfolio,
  }
}
