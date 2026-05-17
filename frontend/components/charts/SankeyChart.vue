<script setup lang="ts">
import { ref, onMounted, watch, computed } from 'vue'
import * as d3 from 'd3'
import { sankey, sankeyLinkHorizontal } from 'd3-sankey'

interface FlowNode {
  id: string
  address: string
  label: string | null
  type: string
  totalInflow: number
  totalOutflow: number
  depth: number
}

interface FlowLink {
  sourceId: string
  targetId: string
  amount: number
  transactionCount: number
}

const props = defineProps<{
  nodes: FlowNode[]
  links: FlowLink[]
  width?: number
  height?: number
  filteredNodeId?: string | null
  /** Minimum width per column in pixels. Higher = more breathing room between hops. */
  minColumnWidth?: number
  /**
   * When a node is selected, hide non-connected nodes/links entirely instead of
   * just dimming them. Has no effect when no node is selected.
   */
  hideUnrelated?: boolean
}>()

const emit = defineEmits<{
  'node-click': [node: FlowNode]
  'node-filter': [nodeId: string | null]
}>()

// Track selected/filtered node - sync with prop
const selectedNodeId = computed(() => props.filteredNodeId ?? null)

const containerRef = ref<HTMLDivElement | null>(null)
const svgRef = ref<SVGSVGElement | null>(null)
const tooltipRef = ref<HTMLDivElement | null>(null)

const chartWidth = computed(() => props.width || 1000)
const chartHeight = computed(() => props.height || 600)

const { formatVolume, truncateAddress } = useFormatting()

const getNodeColor = (type: string) => {
  switch (type) {
    case 'computor': return '#22c55e' // green
    case 'exchange': return '#ef4444' // red
    case 'smartcontract': return '#f59e0b' // amber/orange for smart contracts
    default: return '#6366f1' // indigo for intermediaries
  }
}

const renderChart = () => {
  if (!svgRef.value || !props.nodes.length || !props.links.length) return

  const svg = d3.select(svgRef.value)
  svg.selectAll('*').remove()

  // Minimum column width so labels don't overlap. Default sized so a 6-column
  // flow (Computors + 5 hops) renders at ~1920px total: 80 + 6×293 + 80 ≈ 1918.
  // Caller can override via the minColumnWidth prop.
  const MIN_COLUMN_WIDTH = props.minColumnWidth && props.minColumnWidth > 0
    ? props.minColumnWidth
    : 293
  const margin = { top: 20, right: 80, bottom: 20, left: 80 }

  // Pre-compute column count from the (already-validated) nodes so we can
  // expand the SVG when the container would otherwise cram the columns.
  const provisionalMaxDepth = Math.max(...props.nodes.map(n => n.depth))
  const columnsNeeded = Math.max(1, provisionalMaxDepth + 1)
  const requiredWidth = margin.left + margin.right + columnsNeeded * MIN_COLUMN_WIDTH
  const renderWidth = Math.max(chartWidth.value, requiredWidth)
  const width = renderWidth - margin.left - margin.right
  const height = chartHeight.value - margin.top - margin.bottom

  const g = svg
    .attr('width', renderWidth)
    .attr('height', chartHeight.value)
    .append('g')
    .attr('transform', `translate(${margin.left},${margin.top})`)

  // Build node map
  const nodeMap = new Map(props.nodes.map((n, i) => [n.id, { ...n, index: i }]))

  // Filter valid links and create sankey data
  // Also detect and remove circular links to prevent d3-sankey errors
  const basicValidLinks = props.links
    .filter(l => nodeMap.has(l.sourceId) && nodeMap.has(l.targetId) && l.amount > 0)
    .map(l => ({
      source: l.sourceId,
      target: l.targetId,
      value: l.amount,
      transactionCount: l.transactionCount
    }))

  // Remove circular links: ensure source depth < target depth
  // This prevents cycles in the sankey diagram
  const validLinks = basicValidLinks.filter(l => {
    const sourceNode = nodeMap.get(l.source as string)
    const targetNode = nodeMap.get(l.target as string)
    if (!sourceNode || !targetNode) return false
    // Only allow forward flow (source depth < target depth)
    // Also filter self-loops
    return sourceNode.depth < targetNode.depth && l.source !== l.target
  })

  if (validLinks.length === 0) return

  // Get unique nodes that are actually used in links
  const usedNodeIds = new Set<string>()
  validLinks.forEach(l => {
    usedNodeIds.add(l.source as string)
    usedNodeIds.add(l.target as string)
  })

  const sankeyNodes = props.nodes
    .filter(n => usedNodeIds.has(n.id))
    .map(n => ({
      id: n.id,
      name: n.label || truncateAddress(n.address),
      address: n.address,
      type: n.type,
      depth: n.depth,
      totalInflow: n.totalInflow,
      totalOutflow: n.totalOutflow
    }))

  // Calculate the maximum depth to determine column widths
  const maxDepth = Math.max(...sankeyNodes.map(n => n.depth))
  const nodeWidth = 20
  const columnWidth = maxDepth > 0 ? (width - nodeWidth) / maxDepth : width

  // Create sankey generator with custom node alignment based on depth
  const sankeyGenerator = sankey<any, any>()
    .nodeId((d: any) => d.id)
    .nodeWidth(nodeWidth)
    .nodePadding(10)
    .nodeAlign((node: any) => node.depth) // Align nodes by their depth property
    .nodeSort((a: any, b: any) => {
      // Within same column, sort by volume (largest at top)
      return b.totalOutflow - a.totalOutflow
    })
    .extent([[0, 0], [width, height]])

  // Generate sankey layout
  const { nodes: layoutNodes, links: layoutLinks } = sankeyGenerator({
    nodes: sankeyNodes,
    links: validLinks
  })

  // Force nodes to their correct x positions based on depth
  // This ensures consistent column placement regardless of d3-sankey's calculations
  layoutNodes.forEach((node: any) => {
    const targetX = node.depth * columnWidth
    node.x0 = targetX
    node.x1 = targetX + nodeWidth
  })

  // Compute connected nodes if filtering is active
  const connectedNodeIds = new Set<string>()
  const connectedLinkIndices = new Set<number>()

  if (selectedNodeId.value) {
    // BFS to find all connected nodes (both upstream and downstream)
    const toVisitForward = [selectedNodeId.value]
    const toVisitBackward = [selectedNodeId.value]
    const visitedForward = new Set<string>()
    const visitedBackward = new Set<string>()

    // Find downstream nodes (following flow direction)
    while (toVisitForward.length > 0) {
      const currentId = toVisitForward.shift()!
      if (visitedForward.has(currentId)) continue
      visitedForward.add(currentId)
      connectedNodeIds.add(currentId)

      layoutLinks.forEach((link: any, idx: number) => {
        if (link.source.id === currentId) {
          connectedLinkIndices.add(idx)
          if (!visitedForward.has(link.target.id)) {
            toVisitForward.push(link.target.id)
          }
        }
      })
    }

    // Find upstream nodes (going against flow direction)
    while (toVisitBackward.length > 0) {
      const currentId = toVisitBackward.shift()!
      if (visitedBackward.has(currentId)) continue
      visitedBackward.add(currentId)
      connectedNodeIds.add(currentId)

      layoutLinks.forEach((link: any, idx: number) => {
        if (link.target.id === currentId) {
          connectedLinkIndices.add(idx)
          if (!visitedBackward.has(link.source.id)) {
            toVisitBackward.push(link.source.id)
          }
        }
      })
    }
  }

  const isFiltering = selectedNodeId.value !== null
  const hide = props.hideUnrelated === true

  // Draw links
  const link = g.append('g')
    .attr('class', 'links')
    .attr('fill', 'none')
    .selectAll('path')
    .data(layoutLinks)
    .join('path')
    .attr('d', sankeyLinkHorizontal())
    .attr('display', (_d: any, i: number) =>
      (isFiltering && hide && !connectedLinkIndices.has(i)) ? 'none' : null)
    .attr('stroke', (d: any, i: number) => {
      const sourceColor = getNodeColor(d.source.type)
      if (isFiltering && !connectedLinkIndices.has(i)) {
        return '#666' // Dim non-connected links
      }
      return d3.color(sourceColor)?.copy({ opacity: 0.4 })?.toString() || sourceColor
    })
    .attr('stroke-width', (d: any) => Math.max(1, d.width))
    .attr('opacity', (d: any, i: number) => {
      if (isFiltering && !connectedLinkIndices.has(i)) {
        return 0.1 // Dim non-connected links
      }
      return isFiltering ? 0.8 : 0.6
    })
    .style('cursor', 'pointer')
    .on('mouseenter', function(event: MouseEvent, d: any) {
      d3.select(this).attr('opacity', 1).attr('stroke-width', Math.max(2, d.width + 2))
      showTooltip(event, [
        { text: `${d.source.name} → ${d.target.name}`, classes: 'font-medium' },
        { text: `Amount: ${formatVolume(d.value)}`, classes: 'text-sm' },
        { text: `Transactions: ${d.transactionCount || 'N/A'}`, classes: 'text-sm' },
      ])
    })
    .on('mousemove', function(event: MouseEvent) { positionTooltip(event) })
    .on('mouseleave', function(event: MouseEvent, d: any) {
      const idx = layoutLinks.indexOf(d)
      const baseOpacity = (isFiltering && !connectedLinkIndices.has(idx)) ? 0.1 : (isFiltering ? 0.8 : 0.6)
      d3.select(this).attr('opacity', baseOpacity).attr('stroke-width', Math.max(1, d.width))
      hideTooltip()
    })

  // Draw nodes
  const node = g.append('g')
    .attr('class', 'nodes')
    .selectAll('g')
    .data(layoutNodes)
    .join('g')
    .attr('transform', (d: any) => `translate(${d.x0},${d.y0})`)
    .attr('display', (d: any) =>
      (isFiltering && hide && !connectedNodeIds.has(d.id)) ? 'none' : null)

  // Node rectangles
  node.append('rect')
    .attr('width', (d: any) => d.x1 - d.x0)
    .attr('height', (d: any) => Math.max(1, d.y1 - d.y0))
    .attr('fill', (d: any) => {
      if (isFiltering && !connectedNodeIds.has(d.id)) {
        return '#444' // Dim non-connected nodes
      }
      return getNodeColor(d.type)
    })
    .attr('stroke', (d: any) => {
      if (d.id === selectedNodeId.value) {
        return '#fff' // Highlight selected node
      }
      if (isFiltering && !connectedNodeIds.has(d.id)) {
        return '#555'
      }
      return d3.color(getNodeColor(d.type))?.darker(0.5)?.toString() || '#000'
    })
    .attr('stroke-width', (d: any) => d.id === selectedNodeId.value ? 3 : 1)
    .attr('opacity', (d: any) => {
      if (isFiltering && !connectedNodeIds.has(d.id)) {
        return 0.3 // Dim non-connected nodes
      }
      return 1
    })
    .attr('rx', 3)
    .style('cursor', 'pointer')
    .on('mouseenter', function(event: MouseEvent, d: any) {
      d3.select(this).attr('stroke-width', d.id === selectedNodeId.value ? 4 : 2)
      const filterHintText = d.id === selectedNodeId.value
        ? 'Click to clear filter'
        : 'Click to filter flow paths'
      // d3-sankey decorates each node with sourceLinks (outgoing) and
      // targetLinks (incoming). Sum the per-link transactionCount to get
      // total transactions in/out of this node.
      const inTx = (d.targetLinks ?? []).reduce(
        (sum: number, l: any) => sum + (l.transactionCount || 0), 0)
      const outTx = (d.sourceLinks ?? []).reduce(
        (sum: number, l: any) => sum + (l.transactionCount || 0), 0)
      showTooltip(event, [
        { text: d.name, classes: 'font-medium' },
        { text: d.address, classes: 'text-xs text-gray-400' },
        { text: `Type: ${d.type}`, classes: 'text-sm mt-1' },
        { text: `Inflow: ${formatVolume(d.totalInflow)}`, classes: 'text-sm' },
        { text: `Outflow: ${formatVolume(d.totalOutflow)}`, classes: 'text-sm' },
        { text: `Transactions: ${(inTx + outTx).toLocaleString()} (${inTx.toLocaleString()} in / ${outTx.toLocaleString()} out)`, classes: 'text-sm' },
        { text: filterHintText, classes: 'text-xs text-accent mt-2' },
      ])
    })
    .on('mousemove', function(event: MouseEvent) { positionTooltip(event) })
    .on('mouseleave', function(event: MouseEvent, d: any) {
      d3.select(this).attr('stroke-width', d.id === selectedNodeId.value ? 3 : 1)
      hideTooltip()
    })
    .on('click', function(_event: MouseEvent, d: any) {
      // Toggle selection - emit to parent which manages the state
      if (selectedNodeId.value === d.id) {
        emit('node-filter', null)
      } else {
        emit('node-filter', d.id)
      }
      // Find the original node data to emit
      const originalNode = props.nodes.find(n => n.id === d.id)
      if (originalNode) {
        emit('node-click', originalNode)
      }
    })

  // Node labels (left side for sources, right side for targets)
  node.append('text')
    .attr('x', (d: any) => d.x0 < width / 2 ? (d.x1 - d.x0) + 6 : -6)
    .attr('y', (d: any) => (d.y1 - d.y0) / 2)
    .attr('dy', '0.35em')
    .attr('text-anchor', (d: any) => d.x0 < width / 2 ? 'start' : 'end')
    .attr('fill', 'currentColor')
    .attr('font-size', '11px')
    .text((d: any) => {
      const name = d.name || truncateAddress(d.address)
      return name.length > 20 ? name.slice(0, 17) + '...' : name
    })

  // Add column headers
  const depths = [...new Set(layoutNodes.map((n: any) => n.depth))].sort((a, b) => a - b)
  const depthPositions = new Map<number, number>()

  layoutNodes.forEach((n: any) => {
    const existing = depthPositions.get(n.depth)
    if (existing === undefined || n.x0 < existing) {
      depthPositions.set(n.depth, n.x0)
    }
  })

  g.append('g')
    .attr('class', 'headers')
    .selectAll('text')
    .data(depths)
    .join('text')
    .attr('x', (d: number) => (depthPositions.get(d) || 0) + 10)
    .attr('y', -8)
    .attr('text-anchor', 'start')
    .attr('fill', 'currentColor')
    .attr('font-size', '12px')
    .attr('font-weight', 'bold')
    .text((d: number) => d === 0 ? 'Computors' : d === 1 ? 'Hop 1' : `Hop ${d}`)
}

interface TooltipLine {
  text: string
  classes?: string
}

const showTooltip = (event: MouseEvent, lines: TooltipLine[]) => {
  if (!tooltipRef.value) return
  // Clear previous content safely
  tooltipRef.value.textContent = ''
  for (const line of lines) {
    const div = document.createElement('div')
    if (line.classes) div.className = line.classes
    div.textContent = line.text
    tooltipRef.value.appendChild(div)
  }
  tooltipRef.value.style.display = 'block'
  positionTooltip(event)
}

// Position the (position: fixed) tooltip near the cursor using viewport coords.
// Mirrors with clamping so it doesn't disappear off the right/bottom edges.
const positionTooltip = (event: MouseEvent) => {
  if (!tooltipRef.value) return
  const tt = tooltipRef.value
  const margin = 12
  const vw = window.innerWidth
  const vh = window.innerHeight
  let x = event.clientX + margin
  let y = event.clientY + margin
  const w = tt.offsetWidth
  const h = tt.offsetHeight
  if (x + w + margin > vw) x = event.clientX - w - margin
  if (y + h + margin > vh) y = event.clientY - h - margin
  if (x < 0) x = margin
  if (y < 0) y = margin
  tt.style.left = `${x}px`
  tt.style.top = `${y}px`
}

const hideTooltip = () => {
  if (!tooltipRef.value) return
  tooltipRef.value.style.display = 'none'
}

// Click-and-drag horizontal panning. The chart can be wider than the viewport
// and the bottom scrollbar is often hidden below the fold — dragging makes the
// content reachable without scrolling the page. Clicks (mousedown without
// movement) still register as clicks on nodes via the SVG handlers.
const setupPanning = () => {
  const c = containerRef.value
  if (!c) return
  let isDown = false
  let dragged = false
  let startClientX = 0
  let startScrollLeft = 0
  const DRAG_THRESHOLD = 4 // px

  c.style.cursor = 'grab'

  const onDown = (e: MouseEvent) => {
    // Only react to primary button
    if (e.button !== 0) return
    isDown = true
    dragged = false
    startClientX = e.clientX
    startScrollLeft = c.scrollLeft
  }
  const onMove = (e: MouseEvent) => {
    if (!isDown) return
    const dx = e.clientX - startClientX
    if (!dragged && Math.abs(dx) > DRAG_THRESHOLD) {
      dragged = true
      c.style.cursor = 'grabbing'
      // Suppress text/SVG selection during drag
      document.body.style.userSelect = 'none'
    }
    if (dragged) {
      c.scrollLeft = startScrollLeft - dx
      e.preventDefault()
    }
  }
  const onUp = () => {
    if (!isDown) return
    isDown = false
    c.style.cursor = 'grab'
    document.body.style.userSelect = ''
    // If the user actually dragged, swallow the click that fires next on the
    // SVG to avoid accidentally selecting a node on drag-release.
    if (dragged) {
      const swallow = (e: MouseEvent) => { e.stopPropagation(); e.preventDefault() }
      c.addEventListener('click', swallow, { capture: true, once: true })
    }
  }

  c.addEventListener('mousedown', onDown)
  window.addEventListener('mousemove', onMove)
  window.addEventListener('mouseup', onUp)
}

onMounted(() => {
  renderChart()
  setupPanning()
})

watch(() => [props.nodes, props.links, props.width, props.height, props.filteredNodeId, props.minColumnWidth, props.hideUnrelated], () => {
  renderChart()
}, { deep: true })
</script>

<template>
  <div ref="containerRef" class="relative overflow-x-auto">
    <svg ref="svgRef" class="sankey-chart"></svg>
    <div
      ref="tooltipRef"
      class="fixed z-50 bg-surface-elevated border border-border rounded-lg p-3 shadow-lg pointer-events-none"
      style="display: none;"
    ></div>
  </div>
</template>

<style scoped>
.sankey-chart {
  font-family: inherit;
  overflow: visible;
}
</style>
