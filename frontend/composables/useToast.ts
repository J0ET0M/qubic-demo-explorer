export interface Toast {
  id: number
  message: string
  type: 'success' | 'info' | 'error'
  action?: { label: string; to: string }
  duration?: number
}

let nextId = 0
const toasts = ref<Toast[]>([])

export const useToast = () => {
  const show = (
    message: string,
    options?: {
      type?: Toast['type']
      action?: Toast['action']
      duration?: number
    }
  ) => {
    const id = nextId++
    const toast: Toast = {
      id,
      message,
      type: options?.type ?? 'info',
      action: options?.action,
      duration: options?.duration ?? 4000,
    }
    toasts.value.push(toast)

    if (toast.duration > 0) {
      setTimeout(() => dismiss(id), toast.duration)
    }
  }

  const dismiss = (id: number) => {
    toasts.value = toasts.value.filter(t => t.id !== id)
  }

  return {
    toasts: readonly(toasts),
    show,
    dismiss,
  }
}
