// Logic tests for the canvas modules. Run with `npm run test:canvas`
// (bundled by Vite so extensionless imports resolve, then run by node:test).
import assert from 'node:assert/strict'
import { test } from 'node:test'
import {
  attachBody,
  buildPipeline,
  detachBody,
  monitorsOf,
  workflowFeeds,
} from '../../pages/pipelines/pipelineModel'
import { connect, connectionProblem, defaultConfig, fromFlow, nextNodeId, toFlow } from './graph'
import { layeredLayout } from './layout'

const retryGraph = {
  nodes: [
    { id: 'trigger', type: 'trigger.incident', config: { events: ['opened'], incident_types: ['DAG_RUN_FAILED'] } },
    { id: 'classify', type: 'diagnose.classify_log', config: {} },
    { id: 'filter', type: 'condition.filter', config: { dag_ids: ['orders'], environments: ['DEV'] } },
    { id: 'retry', type: 'action.clear_failed_tasks', config: {} },
    { id: 'notify', type: 'notify', config: {} },
  ],
  edges: [
    { from: 'trigger', port: 'next', to: 'classify' },
    { from: 'classify', port: 'next', to: 'filter' },
    { from: 'filter', port: 'true', to: 'retry' },
    { from: 'retry', port: 'failed', to: 'notify' },
  ],
}

test('graph round-trips through React Flow and gets a layout', () => {
  const flow = toFlow(retryGraph)
  assert.equal(flow.nodes.length, 5)
  assert.deepEqual(flow.nodes[0].position, { x: 0, y: 0 })
  assert.equal(flow.nodes[3].position.x, 900) // depth 3
  const back = fromFlow(flow.nodes, flow.edges)
  assert.deepEqual(back.edges, retryGraph.edges)
  assert.deepEqual(back.nodes[2].config, retryGraph.nodes[2].config)
  assert.ok(back.nodes.every((n) => n.position))
})

test('saved positions are kept', () => {
  const graph = { nodes: [{ id: 't', type: 'trigger.incident', config: {}, position: { x: 7, y: 9 } }], edges: [] }
  assert.deepEqual(toFlow(graph).nodes[0].position, { x: 7, y: 9 })
})

test('connection rules', () => {
  const { nodes, edges } = toFlow(retryGraph)
  const c = (source, sourceHandle, target) => connectionProblem({ source, sourceHandle, target }, nodes, edges)
  assert.equal(c('retry', 'success', 'notify'), null)
  assert.match(c('notify', 'next', 'trigger'), /trigger/)
  assert.match(c('notify', 'next', 'notify'), /itself/)
  assert.match(c('notify', 'next', 'classify'), /loop/)
  // Re-wiring an already-used port replaces the old edge instead of adding a second one.
  const rewired = connect({ source: 'retry', sourceHandle: 'failed', target: 'classify' }, edges)
  assert.equal(rewired.filter((e) => e.source === 'retry' && e.sourceHandle === 'failed').length, 1)
})

test('ids and defaults', () => {
  const { nodes } = toFlow(retryGraph)
  assert.equal(nextNodeId('notify', nodes), 'notify_1')
  assert.equal(nextNodeId('action.clear_failed_tasks', [{ id: 'clear_failed_tasks_1' }]), 'clear_failed_tasks_2')
  const schema = { properties: { a: { default: [1] }, b: { type: 'string' }, c: { default: null } } }
  assert.deepEqual(defaultConfig(schema), { a: [1], c: null })
})

test('layout handles joins and stray nodes', () => {
  const positions = layeredLayout(
    [{ id: 'a' }, { id: 'b' }, { id: 'c' }, { id: 'lonely' }],
    [
      { from: 'a', to: 'b' },
      { from: 'a', to: 'c' },
      { from: 'b', to: 'c' },
    ],
  )
  assert.equal(positions.c.x, 600) // longest path a→b→c
  assert.equal(positions.lonely.x, 0)
})

// ---------------------------------------------------------------------- pipeline model

const dag = (over = {}) => ({
  id: 'd1',
  dag_id: 'orders',
  is_present: true,
  is_monitored: true,
  detect_failures: true,
  sla_minutes: null,
  ...over,
})

test('monitor blocks mirror DAG settings', () => {
  assert.deepEqual(monitorsOf(dag()), ['failure'])
  assert.deepEqual(monitorsOf(dag({ sla_minutes: 30 })), ['failure', 'sla'])
  assert.deepEqual(monitorsOf(dag({ detect_failures: false, sla_minutes: 30 })), ['sla'])
  assert.deepEqual(monitorsOf(dag({ is_monitored: false, sla_minutes: 30 })), [])
})

test('attach and detach bodies', () => {
  const off = dag({ is_monitored: false, sla_minutes: 45 })
  assert.deepEqual(attachBody(off, 'failure'), { is_monitored: true, detect_failures: true, sla_minutes: null })
  assert.deepEqual(attachBody(off, 'sla', 60), { is_monitored: true, sla_minutes: 60, detect_failures: false })
  assert.deepEqual(attachBody(dag(), 'sla', 60), { is_monitored: true, sla_minutes: 60 })
  assert.deepEqual(detachBody(dag(), 'failure'), { detect_failures: false, is_monitored: false })
  assert.deepEqual(detachBody(dag({ sla_minutes: 5 }), 'failure'), { detect_failures: false })
  assert.deepEqual(detachBody(dag({ sla_minutes: 5, detect_failures: false }), 'sla'), {
    sla_minutes: null,
    is_monitored: false,
  })
})

test('workflow feeds follow trigger types and leading filters', () => {
  const feeds = workflowFeeds({ graph: retryGraph })
  assert.deepEqual([...feeds.types], ['DAG_RUN_FAILED'])
  assert.deepEqual([...feeds.dagIds], ['orders'])
  assert.deepEqual([...feeds.envs], ['DEV'])
  const stale = workflowFeeds({ graph: { nodes: [{ id: 't', type: 'trigger.incident_stale', config: {} }], edges: [] } })
  assert.equal(stale.stale, true)
})

test('pipeline diagram wires connection → DAG → monitors → workflows', () => {
  const conn = { id: 'c1', name: 'dev', environment: 'DEV' }
  const { nodes, edges } = buildPipeline({
    connections: [conn],
    dagsByConnection: {
      c1: [dag({ sla_minutes: 30 }), dag({ id: 'd2', dag_id: 'other', is_monitored: false })],
    },
    workflows: [{ id: 'w1', name: 'Retry', mode: 'LIVE', graph: retryGraph }],
    counts: { 'c1:orders:DAG_RUN_FAILED': 2 },
    drafts: [{ id: 'draft:1', kind: 'sla', position: { x: 1, y: 2 } }],
    showUnmonitored: false,
    canEdit: true,
  })
  const ids = nodes.map((n) => n.id).sort()
  assert.deepEqual(ids, ['conn:c1', 'dag:d1', 'draft:1', 'mon:failure:d1', 'mon:sla:d1', 'wf:w1'])
  assert.equal(nodes.find((n) => n.id === 'mon:failure:d1').data.count, 2)
  // The retry workflow listens to failures only, so only the failure monitor feeds it.
  const feeds = edges.filter((e) => e.target === 'wf:w1').map((e) => e.source)
  assert.deepEqual(feeds, ['mon:failure:d1'])
  assert.ok(edges.some((e) => e.source === 'conn:c1' && e.target === 'dag:d1'))
})
