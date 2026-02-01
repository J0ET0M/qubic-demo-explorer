# Qubic Demo Explorer Frontend

Vue 3 / Nuxt 3 web application for exploring the Qubic blockchain.

## Features

- **Dashboard**: Network statistics, latest ticks overview
- **Tick Browser**: Paginated list of ticks with details
- **Transaction Explorer**: View transactions with decoded smart contract inputs
- **Transfer Browser**: Filter transfers by address, type, direction
- **Address View**: Balance, transactions, transfers, and rewards for any address
- **Epoch Browser**: Epoch statistics, transfer breakdowns, miner flow analysis
- **Global Search**: Search by tick number, transaction hash, or address
- **Real-time Updates**: Live updates via SignalR connection
- **Analytics**: Holder distribution, exchange flows, network stats history
- **Responsive Design**: Works on desktop and mobile

## Pages

| Route | Description |
|-------|-------------|
| `/` | Dashboard with stats and recent ticks |
| `/ticks` | Paginated tick list |
| `/ticks/:tickNumber` | Tick detail with transactions |
| `/transactions` | Paginated transaction list |
| `/tx/:hash` | Transaction detail with logs |
| `/transfers` | Paginated transfer list |
| `/address/:id` | Address detail with tabs |
| `/search` | Search results page |

## Tech Stack

- **Framework**: Nuxt 3
- **UI**: Vue 3 with Composition API
- **Language**: TypeScript
- **Styling**: Tailwind CSS (dark theme)
- **Real-time**: @microsoft/signalr

## Project Structure

```
frontend/
├── app.vue                    # Root component
├── nuxt.config.ts            # Nuxt configuration
├── assets/
│   └── css/main.css          # Global styles (Tailwind)
├── components/               # Reusable UI components
├── composables/
│   ├── useApi.ts             # API client with typed methods
│   ├── useLiveUpdates.ts     # SignalR real-time connection
│   ├── useAddressLabels.ts   # Address label caching
│   └── useContractInput.ts   # Smart contract input decoder
├── layouts/
│   └── default.vue           # Main layout with header
├── pages/                    # File-based routing
└── utils/
    └── contractInputDecoder.ts  # Contract schema definitions
```

## Setup

```bash
# Install dependencies
npm install

# Development server (http://localhost:3000)
npm run dev

# Build for production
npm run build

# Preview production build
npm run preview
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NUXT_API_URL` | `http://api:8080` | Server-side API URL (used during SSR) |
| `NUXT_PUBLIC_API_URL` | _(empty)_ | Client-side API URL (empty = relative path, routed via nginx) |

### nuxt.config.ts

```typescript
export default defineNuxtConfig({
  runtimeConfig: {
    // Server-side only (SSR) — internal Docker network URL
    apiUrl: process.env.NUXT_API_URL || 'http://api:8080',
    public: {
      // Client-side (browser) — empty means relative path through nginx
      apiUrl: process.env.NUXT_PUBLIC_API_URL || ''
    }
  }
})
```

### How API Routing Works

- **Via nginx (port 80)**: The frontend uses relative URLs. Browser requests to `/api/*` are proxied by nginx to the API backend. This is the default production setup.
- **Direct access (port 3000)**: Set `NUXT_PUBLIC_API_URL=http://localhost:5000` so the browser calls the API directly.
- **SSR**: Server-side rendering always uses `NUXT_API_URL` (internal Docker URL `http://api:8080`).

### Docker Compose

```yaml
# Production (via nginx)
environment:
  - NUXT_API_URL=http://api:8080
  - NUXT_PUBLIC_API_URL=

# Development (direct API access)
environment:
  - NUXT_API_URL=http://api:8080
  - NUXT_PUBLIC_API_URL=http://localhost:5000
```

## Docker

```bash
# Build image
docker build -t qubic-frontend .

# Run container (production, behind nginx)
docker run -p 3000:3000 \
  -e NUXT_API_URL=http://api:8080 \
  qubic-frontend

# Run container (standalone, direct API access)
docker run -p 3000:3000 \
  -e NUXT_API_URL=http://localhost:5000 \
  -e NUXT_PUBLIC_API_URL=http://localhost:5000 \
  qubic-frontend
```

## Development

### API Client

The `useApi()` composable provides typed API methods:

```typescript
const api = useApi()

// Fetch ticks
const { data } = await useAsyncData(() => api.getTicks(1, 20))

// Search
const results = await api.search('ABCD...')
```

### Real-time Updates

The `useLiveUpdates()` composable manages SignalR:

```typescript
const { isConnected, subscribeToTicks, onNewTick } = useLiveUpdates()

await subscribeToTicks()
onNewTick((tick) => {
  console.log('New tick:', tick)
})
```
