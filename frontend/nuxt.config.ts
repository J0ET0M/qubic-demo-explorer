// https://nuxt.com/docs/api/configuration/nuxt-config
import tailwindcss from '@tailwindcss/vite'

export default defineNuxtConfig({
  compatibilityDate: '2024-11-01',
  devtools: { enabled: true },

  runtimeConfig: {
    // Server-side API URL (for SSR)
    apiUrl: process.env.NUXT_API_URL || 'http://api:8080',
    public: {
      // Client-side API URL (for browser) - use relative path to go through nginx
      apiUrl: process.env.NUXT_PUBLIC_API_URL || ''
    }
  },

  app: {
    head: {
      title: 'Qubic Explorer',
      htmlAttrs: {
        lang: 'en',
        class: 'dark'
      },
      meta: [
        { charset: 'utf-8' },
        { name: 'viewport', content: 'width=device-width, initial-scale=1' },
        { name: 'description', content: 'Explore Qubic blockchain - ticks, transactions, transfers and addresses' },
        { name: 'theme-color', content: '#1a1d24' }
      ],
      link: [
        { rel: 'icon', type: 'image/svg+xml', href: '/favicon.svg' }
      ]
    }
  },

  css: ['~/assets/css/main.css'],

  vite: {
    plugins: [tailwindcss()]
  }
})
