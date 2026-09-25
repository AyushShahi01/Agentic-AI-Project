// Layered left-to-right layout: a block's column is its longest distance from a root;
// blocks in a column are stacked in the order they were discovered.

export const COLUMN_WIDTH = 300
export const ROW_HEIGHT = 150

/**
 * @param {{id: string}[]} nodes
 * @param {{from: string, to: string, port?: string}[]} edges  backend-shaped edges
 * @returns {Record<string, {x: number, y: number}>}
 */
export function layeredLayout(nodes, edges) {
  const incoming = {}
  const outgoing = {}
  nodes.forEach((n) => {
    incoming[n.id] = 0
    outgoing[n.id] = []
  })
  edges.forEach((e) => {
    if (e.from in outgoing && e.to in incoming) {
      outgoing[e.from].push(e.to)
      incoming[e.to] += 1
    }
  })

  // Longest-path depth via Kahn's algorithm (graphs are acyclic; leftovers go last).
  const depth = {}
  const order = []
  const remaining = { ...incoming }
  const queue = nodes.filter((n) => remaining[n.id] === 0).map((n) => n.id)
  queue.forEach((id) => (depth[id] = 0))
  while (queue.length) {
    const id = queue.shift()
    order.push(id)
    outgoing[id].forEach((next) => {
      depth[next] = Math.max(depth[next] ?? 0, depth[id] + 1)
      remaining[next] -= 1
      if (remaining[next] === 0) queue.push(next)
    })
  }
  const maxDepth = Math.max(0, ...Object.values(depth))
  nodes.forEach((n) => {
    if (!order.includes(n.id)) {
      depth[n.id] = maxDepth + 1
      order.push(n.id)
    }
  })

  const columns = {}
  order.forEach((id) => (columns[depth[id]] ??= []).push(id))
  const tallest = Math.max(1, ...Object.values(columns).map((c) => c.length))
  const positions = {}
  Object.entries(columns).forEach(([col, ids]) => {
    const offset = ((tallest - ids.length) * ROW_HEIGHT) / 2 // centre shorter columns
    ids.forEach((id, row) => {
      positions[id] = { x: Number(col) * COLUMN_WIDTH, y: offset + row * ROW_HEIGHT }
    })
  })
  return positions
}
