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
    pageTransition: { name: 'page', mode: 'out-in' },
    head: {
      title: 'QLI Explorer',
      htmlAttrs: {
        lang: 'en',
        class: 'dark'
      },
      meta: [
        { charset: 'utf-8' },
        { name: 'viewport', content: 'width=device-width, initial-scale=1' },
        { name: 'description', content: 'QLI Explorer - Explore Qubic blockchain ticks, transactions, transfers and addresses' },
        { name: 'theme-color', content: '#0f1117' }
      ],
      link: [
        { rel: 'icon', type: 'image/svg+xml', href: '/favicon.svg' },
        { rel: 'preconnect', href: 'https://fonts.googleapis.com' },
        { rel: 'preconnect', href: 'https://fonts.gstatic.com', crossorigin: '' },
        { rel: 'stylesheet', href: 'https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Source+Code+Pro:wght@300;400;500;600;700&display=swap' }
      ]
    }
  },

  css: ['~/assets/css/main.css'],

  vite: {
    plugins: [tailwindcss()]
  }
})
