import * as signalR from '@microsoft/signalr'

let connection: signalR.HubConnection | null = null

export const useLiveUpdates = () => {
  const config = useRuntimeConfig()
  const isConnected = ref(false)

  const connect = async () => {
    if (connection) return

    const hubUrl = `${config.public.apiUrl}/hubs/live`

    connection = new signalR.HubConnectionBuilder()
      .withUrl(hubUrl)
      .withAutomaticReconnect()
      .configureLogging(signalR.LogLevel.Warning)
      .build()

    connection.onreconnecting(() => {
      isConnected.value = false
    })

    connection.onreconnected(() => {
      isConnected.value = true
    })

    connection.onclose(() => {
      isConnected.value = false
    })

    try {
      await connection.start()
      isConnected.value = true
    } catch (err) {
      console.error('SignalR connection error:', err)
      connection = null
      throw err
    }
  }

  const disconnect = async () => {
    if (connection) {
      await connection.stop()
      connection = null
      isConnected.value = false
    }
  }

  const subscribeToTicks = async () => {
    if (!connection) await connect()
    await connection?.invoke('SubscribeToTicks')
  }

  const unsubscribeFromTicks = async () => {
    await connection?.invoke('UnsubscribeFromTicks')
  }

  const subscribeToAddress = async (address: string) => {
    if (!connection) await connect()
    await connection?.invoke('SubscribeToAddress', address)
  }

  const unsubscribeFromAddress = async (address: string) => {
    await connection?.invoke('UnsubscribeFromAddress', address)
  }

  const onNewTick = (callback: (data: any) => void): (() => void) => {
    connection?.on('OnNewTick', callback)
    return () => connection?.off('OnNewTick', callback)
  }

  const onNewTransaction = (callback: (data: any) => void): (() => void) => {
    connection?.on('OnNewTransaction', callback)
    return () => connection?.off('OnNewTransaction', callback)
  }

  const onAddressUpdate = (callback: (data: any) => void): (() => void) => {
    connection?.on('OnAddressUpdate', callback)
    return () => connection?.off('OnAddressUpdate', callback)
  }

  return {
    isConnected: readonly(isConnected),
    connect,
    disconnect,
    subscribeToTicks,
    unsubscribeFromTicks,
    subscribeToAddress,
    unsubscribeFromAddress,
    onNewTick,
    onNewTransaction,
    onAddressUpdate,
  }
}
