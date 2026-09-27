// Convert between the backend workflow graph ({nodes, edges}) and React Flow nodes/edges,
// plus the client-side connection rules. The server remains the authority on validity.

import { layeredLayout } from './layout'

export const BLOCK = 'block'

export function isTrigger(type) {
  return type.startsWith('trigger.')
}

// Output ports taken when something went wrong or the answer was "no".
export const BAD_PORTS = new Set(['failed', 'fail', 'rejected', 'false', 'timeout', 'paused', 'busy'])

export function edgeId(from, port, to) {
  return `${from}:${port}->${to}`
}

function toEdge(from, port, to, extra = {}) {
  return {
    id: edgeId(from, port, to),
    source: from,
    sourceHandle: port,
    target: to,
    label: port === 'next' ? undefined : port,
    className: BAD_PORTS.has(port) ? 'edge-bad' : 'edge-ok',
    ...extra,
  }
}

/** Backend graph -> React Flow. Nodes without a saved position are auto-laid out. */
export function toFlow(graph, extraData = () => ({})) {
  const edges = (graph?.edges ?? []).map((e) => toEdge(e.from, e.port, e.to))
  const needsLayout = (graph?.nodes ?? []).some((n) => !n.position)
  const auto = needsLayout ? layeredLayout(graph.nodes, graph.edges) : {}
  const nodes = (graph?.nodes ?? []).map((n) => ({
    id: n.id,
    type: BLOCK,
    position: n.position ?? auto[n.id] ?? { x: 0, y: 0 },
    deletable: true,
    data: { nodeType: n.type, name: n.name ?? '', config: n.config ?? {}, ...extraData(n) },
  }))
  return { nodes, edges }
}

/** React Flow -> backend graph (positions rounded). */
export function fromFlow(nodes, edges) {
  return {
    nodes: nodes.map((n) => ({
      id: n.id,
      type: n.data.nodeType,
      ...(n.data.name?.trim() ? { name: n.data.name.trim() } : {}),
      config: n.data.config ?? {},
      position: { x: Math.round(n.position.x), y: Math.round(n.position.y) },
    })),
    edges: edges.map((e) => ({ from: e.source, port: e.sourceHandle, to: e.target })),
  }
}

export function newEdge(connection) {
  return toEdge(connection.source, connection.sourceHandle, connection.target)
}

function reaches(start, goal, edges) {
  const next = {}
  edges.forEach((e) => (next[e.source] ??= []).push(e.target))
  const stack = [start]
  const seen = new Set()
  while (stack.length) {
    const id = stack.pop()
    if (id === goal) return true
    if (seen.has(id)) continue
    seen.add(id)
    stack.push(...(next[id] ?? []))
  }
  return false
}

/**
 * Why a connection is not allowed (or null if it is). One output may link to several blocks;
 * they run one after another, top to bottom on the canvas.
 */
export function connectionProblem(connection, nodes, edges) {
  const { source, target, sourceHandle } = connection
  if (!source || !target || !sourceHandle) return 'Connect an output port to a block'
  if (source === target) return 'A block cannot connect to itself'
  const targetNode = nodes.find((n) => n.id === target)
  if (!targetNode) return 'Unknown block'
  if (isTrigger(targetNode.data.nodeType)) return 'Nothing can connect into a trigger'
  if (edges.some((e) => e.source === source && e.sourceHandle === sourceHandle && e.target === target)) {
    return 'That output is already connected to this block'
  }
  if (reaches(target, source, edges)) return 'That would create a loop'
  return null
}

/** Add an edge (an exact duplicate is ignored). */
export function connect(connection, edges) {
  const edge = newEdge(connection)
  return edges.some((e) => e.id === edge.id) ? edges : [...edges, edge]
}

/**
 * Run order of blocks linked from the same output, as shown on the canvas: top to bottom, then
 * left to right (the backend uses the same rule). Returns {edgeId: 1-based position} for
 * outputs with more than one link.
 */
export function linkOrder(nodes, edges) {
  const pos = Object.fromEntries(nodes.map((n) => [n.id, n.position]))
  const groups = {}
  edges.forEach((e) => (groups[`${e.source}:${e.sourceHandle}`] ??= []).push(e))
  const order = {}
  Object.values(groups).forEach((group) => {
    if (group.length < 2) return
    const sorted = group
      .map((e, index) => ({ e, index, p: pos[e.target] }))
      .sort((a, b) => (a.p?.y ?? 0) - (b.p?.y ?? 0) || (a.p?.x ?? 0) - (b.p?.x ?? 0) || a.index - b.index)
    sorted.forEach(({ e }, i) => (order[e.id] = i + 1))
  })
  return order
}

/** Readable unique id for a new block, e.g. `notify_2`. */
export function nextNodeId(type, nodes) {
  const base = type.split('.').at(-1).replace(/[^A-Za-z0-9_-]/g, '_')
  const taken = new Set(nodes.map((n) => n.id))
  for (let i = 1; ; i += 1) {
    const id = `${base}_${i}`
    if (!taken.has(id)) return id
  }
}

/**
 * Change a block to another type (e.g. "Run this DAG" -> "Wait for it to succeed").
 * Keeps config keys the new type also has (connection, DAG, SQL), uses defaults for the rest,
 * and drops edges leaving output ports the new type does not have.
 */
export function retypeNode(node, newDef, edges) {
  const defaults = defaultConfig(newDef.config_schema)
  const keep = Object.fromEntries(Object.entries(node.data.config ?? {}).filter(([key]) => key in defaults))
  const ports = new Set(newDef.ports ?? [])
  return {
    node: { ...node, data: { ...node.data, nodeType: newDef.type, def: newDef, config: { ...defaults, ...keep } } },
    edges: edges.filter((e) => e.source !== node.id || ports.has(e.sourceHandle)),
  }
}

/** Palette drag payload: JSON `{type, name?, config?, idBase?}` or a bare block type. */
export function parseBlockPayload(raw) {
  if (!raw) return null
  if (!raw.startsWith('{')) return { type: raw }
  try {
    const payload = JSON.parse(raw)
    return typeof payload?.type === 'string' ? payload : null
  } catch {
    return null
  }
}

/** Default config values from a Pydantic JSON schema. */
export function defaultConfig(schema) {
  const config = {}
  Object.entries(schema?.properties ?? {}).forEach(([key, prop]) => {
    if (prop.default !== undefined) config[key] = structuredClone(prop.default)
  })
  return config
}
