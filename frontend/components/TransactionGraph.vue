<script setup lang="ts">
import * as d3 from 'd3'
import type { GraphNodeDto, GraphLinkDto } from '~/composables/useApi'

const props = defineProps<{
  nodes: GraphNodeDto[]
  links: GraphLinkDto[]
  centerAddress: string
  height?: number
}>()

const emit = defineEmits<{
  'node-click': [address: string]
}>()

const containerRef = ref<HTMLDivElement | null>(null)
const svgRef = ref<SVGSVGElement | null>(null)

const getTypeColor = (type: string | null) => {
  switch (type) {
    case 'exchange': return '#f59e0b'
    case 'smartcontract': return '#3b82f6'
    case 'burn': return '#ef4444'
    case 'known': return '#10b981'
    default: return '#8b5cf6'
  }
}

const truncateAddr = (addr: string) =>
  addr.length > 12 ? addr.slice(0, 6) + '...' + addr.slice(-4) : addr

const formatAmount = (amount: number) => {
  if (amount >= 1e12) return (amount / 1e12).toFixed(1) + 'T'
  if (amount >= 1e9) return (amount / 1e9).toFixed(1) + 'B'
  if (amount >= 1e6) return (amount / 1e6).toFixed(1) + 'M'
  if (amount >= 1e3) return (amount / 1e3).toFixed(1) + 'K'
  return amount.toString()
}

const drawGraph = () => {
  if (!svgRef.value || !containerRef.value) return

  const width = containerRef.value.clientWidth
  const height = props.height || 500

  const svg = d3.select(svgRef.value)
  svg.selectAll('*').remove()
  svg.attr('width', width).attr('height', height)

  const g = svg.append('g')

  // Zoom
  const zoom = d3.zoom<SVGSVGElement, unknown>()
    .scaleExtent([0.2, 4])
    .on('zoom', (event) => g.attr('transform', event.transform))
  svg.call(zoom)

  // Build D3 force data
  const nodeMap = new Map(props.nodes.map(n => [n.address, n]))
  const maxVolume = Math.max(...props.nodes.map(n => n.totalVolume), 1)

  const simNodes = props.nodes.map(n => ({
    id: n.address,
    ...n,
    fx: n.address === props.centerAddress ? width / 2 : undefined,
    fy: n.address === props.centerAddress ? height / 2 : undefined,
  }))

  const simLinks = props.links
    .filter(l => nodeMap.has(l.source) && nodeMap.has(l.target))
    .map(l => ({
      source: l.source,
      target: l.target,
      amount: l.amount,
      txCount: l.txCount,
    }))

  const maxLinkAmount = Math.max(...simLinks.map(l => l.amount), 1)

  // Force simulation
  const simulation = d3.forceSimulation(simNodes as any)
    .force('link', d3.forceLink(simLinks as any).id((d: any) => d.id).distance(120))
    .force('charge', d3.forceManyBody().strength(-300))
    .force('center', d3.forceCenter(width / 2, height / 2))
    .force('collision', d3.forceCollide(30))

  // Links
  const link = g.append('g')
    .selectAll('line')
    .data(simLinks)
    .join('line')
    .attr('stroke', '#666')
    .attr('stroke-opacity', 0.4)
    .attr('stroke-width', (d: any) => Math.max(1, (d.amount / maxLinkAmount) * 6))

  // Nodes
  const node = g.append('g')
    .selectAll('g')
    .data(simNodes)
    .join('g')
    .attr('cursor', 'pointer')
    .call(d3.drag<any, any>()
      .on('start', (event, d) => {
        if (!event.active) simulation.alphaTarget(0.3).restart()
        d.fx = d.x
        d.fy = d.y
      })
      .on('drag', (event, d) => {
        d.fx = event.x
        d.fy = event.y
      })
      .on('end', (event, d) => {
        if (!event.active) simulation.alphaTarget(0)
        if (d.address !== props.centerAddress) {
          d.fx = null
          d.fy = null
        }
      })
    )

  // Node circles
  node.append('circle')
    .attr('r', (d: any) => d.address === props.centerAddress ? 16 : 8 + (d.totalVolume / maxVolume) * 12)
    .attr('fill', (d: any) => getTypeColor(d.type))
    .attr('stroke', '#1a1a2e')
    .attr('stroke-width', 2)
    .on('click', (_event: any, d: any) => emit('node-click', d.address))

  // Node labels
  node.append('text')
    .text((d: any) => d.label || truncateAddr(d.address))
    .attr('dy', (d: any) => (d.address === props.centerAddress ? 28 : 22))
    .attr('text-anchor', 'middle')
    .attr('fill', '#a0a0c0')
    .attr('font-size', '10px')

  // Tooltips
  node.append('title')
    .text((d: any) => `${d.label || d.address}\nVolume: ${formatAmount(d.totalVolume)}\nType: ${d.type || 'unknown'}`)

  link.append('title')
    .text((d: any) => `Amount: ${formatAmount(d.amount)}\nTxs: ${d.txCount}`)

  // Tick
  simulation.on('tick', () => {
    link
      .attr('x1', (d: any) => d.source.x)
      .attr('y1', (d: any) => d.source.y)
      .attr('x2', (d: any) => d.target.x)
      .attr('y2', (d: any) => d.target.y)

    node.attr('transform', (d: any) => `translate(${d.x},${d.y})`)
  })
}

onMounted(() => drawGraph())
watch(() => [props.nodes, props.links], () => drawGraph(), { deep: true })
</script>

<template>
  <div ref="containerRef" class="w-full overflow-hidden rounded-lg bg-background-elevated">
    <svg ref="svgRef" :height="height || 500" class="w-full" />
  </div>
</template>
