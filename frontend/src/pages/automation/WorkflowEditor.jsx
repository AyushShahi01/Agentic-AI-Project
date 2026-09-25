import {
  Background,
  Controls,
  MarkerType,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  useEdgesState,
  useNodesState,
  useReactFlow,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import '../../components/flow/FlowCanvas.css'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Link, useNavigate, useParams, useSearchParams } from 'react-router'
import SchemaForm from '../../components/flow/SchemaForm'
import {
  BLOCK,
  connect,
  connectionProblem,
  defaultConfig,
  fromFlow,
  isTrigger,
  nextNodeId,
  toFlow,
} from '../../components/flow/graph'
import { layeredLayout } from '../../components/flow/layout'
import { Alert, Badge, Button } from '../../components/ui'
import { useAuth } from '../../context/AuthContext'
import { useToast } from '../../context/ToastContext'
import { automationApi } from '../../services/endpoints'
import BlockNode from './BlockNode'
import { nodeIcon } from './automationText'

const NODE_TYPES = { [BLOCK]: BlockNode }
const EDGE_OPTIONS = { markerEnd: { type: MarkerType.ArrowClosed, width: 18, height: 18 } }
const DRAG_TYPE = 'application/x-workflow-block'
const CATEGORY_ORDER = ['trigger', 'logic', 'diagnosis', 'approval', 'action', 'verify', 'output']
const CATEGORY_TITLES = {
  trigger: 'Start when…',
  logic: 'Decide',
  diagnosis: 'Diagnose',
  approval: 'Ask a human',
  action: 'Fix (Airflow)',
  verify: 'Check',
  output: 'Record & notify',
}
const DIRTY_CHANGES = new Set(['position', 'remove', 'add', 'replace'])

function blankGraph(defs) {
  const type = 'trigger.incident'
  return {
    nodes: [{ id: 'trigger', type, config: defaultConfig(defs[type]?.config_schema), position: { x: 0, y: 0 } }],
    edges: [],
  }
}

function Palette({ catalog, hasTrigger, onAdd }) {
  const groups = CATEGORY_ORDER.map((category) => ({
    category,
    items: catalog.filter((d) => d.category === category),
  })).filter((g) => g.items.length)
  return (
    <aside className="editor-palette" aria-label="Blocks">
      <p className="muted small">Drag a block onto the canvas, or click to add it.</p>
      {groups.map(({ category, items }) => (
        <div key={category} className="palette-group">
          <div className="palette-title">{CATEGORY_TITLES[category] ?? category}</div>
          {items.map((def) => {
            const disabled = category === 'trigger' && hasTrigger
            return (
              <button
                key={def.type}
                type="button"
                className={`palette-item block-${category}`}
                draggable={!disabled}
                disabled={disabled}
                title={disabled ? 'A workflow has exactly one trigger' : def.description}
                onDragStart={(e) => {
                  e.dataTransfer.setData(DRAG_TYPE, def.type)
                  e.dataTransfer.effectAllowed = 'move'
                }}
                onClick={() => onAdd(def.type)}
              >
                <span className="block-icon" aria-hidden="true">
                  {nodeIcon(def.type)}
                </span>
                <span>{def.label}</span>
              </button>
            )
          })}
        </div>
      ))}
    </aside>
  )
}

function Problems({ problems, onSelect }) {
  if (!problems.length) return null
  return (
    <div className="editor-problems">
      <strong className="small">
        {problems.length} problem{problems.length === 1 ? '' : 's'}
      </strong>
      <ul>
        {problems.map((p, i) => (
          <li key={i}>
            {p.node ? (
              <button type="button" className="link-btn small" onClick={() => onSelect(p.node)}>
                {p.node}
              </button>
            ) : (
              p.edge && <span className="mono small">{p.edge}</span>
            )}{' '}
            <span className="small">{p.message}</span>
          </li>
        ))}
      </ul>
    </div>
  )
}

function Editor() {
  const { id } = useParams()
  const [params] = useSearchParams()
  const fromId = params.get('from')
  const isNew = id === 'new'
  const navigate = useNavigate()
  const toast = useToast()
  const { hasRole } = useAuth()
  const canEdit = hasRole('ADMIN')
  const { screenToFlowPosition, fitView } = useReactFlow()
  const wrapper = useRef(null)

  const [catalog, setCatalog] = useState(null)
  const [workflow, setWorkflow] = useState(null)
  const [meta, setMeta] = useState({ name: '', description: '' })
  const [nodes, setNodes, onNodesChange] = useNodesState([])
  const [edges, setEdges, onEdgesChange] = useEdgesState([])
  const [selection, setSelection] = useState({ node: null, edge: null })
  const [problems, setProblems] = useState([])
  const [dirty, setDirty] = useState(false)
  const [busy, setBusy] = useState(null)
  const [error, setError] = useState(null)

  const defs = useMemo(() => Object.fromEntries((catalog ?? []).map((d) => [d.type, d])), [catalog])

  // ------------------------------------------------------------------ load
  useEffect(() => {
    let cancelled = false
    async function load() {
      try {
        const types = await automationApi.nodeTypes()
        const byType = Object.fromEntries(types.map((d) => [d.type, d]))
        let wf = null
        let graph
        let name = ''
        let description = ''
        if (!isNew) {
          wf = await automationApi.getWorkflow(id)
          ;({ graph, name } = wf)
          description = wf.description ?? ''
        } else if (fromId) {
          const source = await automationApi.getWorkflow(fromId)
          graph = source.graph
          name = `Copy of ${source.name}`
          description = source.description ?? ''
        } else {
          graph = blankGraph(byType)
          name = 'New workflow'
        }
        if (cancelled) return
        const flow = toFlow(graph, (n) => ({ def: byType[n.type] }))
        setCatalog(types)
        setWorkflow(wf)
        setMeta({ name, description })
        setNodes(flow.nodes)
        setEdges(flow.edges)
        setProblems([])
        setDirty(isNew)
        setError(null)
        requestAnimationFrame(() => fitView({ padding: 0.2, maxZoom: 1 }))
      } catch (err) {
        if (!cancelled) setError(err.message)
      }
    }
    load()
    return () => {
      cancelled = true
    }
  }, [id, isNew, fromId, setNodes, setEdges, fitView])

  useEffect(() => {
    if (!dirty) return undefined
    const warn = (e) => {
      e.preventDefault()
      e.returnValue = ''
    }
    window.addEventListener('beforeunload', warn)
    return () => window.removeEventListener('beforeunload', warn)
  }, [dirty])

  // ------------------------------------------------------------------ editing
  const handleNodesChange = useCallback(
    (changes) => {
      onNodesChange(changes)
      if (changes.some((c) => DIRTY_CHANGES.has(c.type))) setDirty(true)
    },
    [onNodesChange],
  )

  const handleEdgesChange = useCallback(
    (changes) => {
      onEdgesChange(changes)
      if (changes.some((c) => DIRTY_CHANGES.has(c.type))) setDirty(true)
    },
    [onEdgesChange],
  )

  const onConnect = useCallback(
    (connection) => {
      const problem = connectionProblem(connection, nodes, edges)
      if (problem) return toast.error(problem)
      setEdges((current) => connect(connection, current))
      setDirty(true)
    },
    [nodes, edges, setEdges, toast],
  )

  const isValidConnection = useCallback(
    (connection) => !connectionProblem(connection, nodes, edges),
    [nodes, edges],
  )

  const hasTrigger = nodes.some((n) => isTrigger(n.data.nodeType))

  function addBlock(type, position) {
    if (isTrigger(type) && hasTrigger) return toast.error('A workflow has exactly one trigger')
    const def = defs[type]
    let at = position
    if (!at) {
      const box = wrapper.current?.getBoundingClientRect()
      const center = box ? { x: box.left + box.width / 2, y: box.top + box.height / 2 } : { x: 0, y: 0 }
      const point = screenToFlowPosition(center)
      at = { x: point.x - 110 + (nodes.length % 5) * 20, y: point.y - 40 + (nodes.length % 5) * 20 }
    }
    const nodeId = nextNodeId(type, nodes)
    setNodes((current) => [
      ...current.map((n) => ({ ...n, selected: false })),
      {
        id: nodeId,
        type: BLOCK,
        position: at,
        selected: true,
        data: { nodeType: type, name: '', config: defaultConfig(def?.config_schema), def },
      },
    ])
    setSelection({ node: nodeId, edge: null })
    setDirty(true)
  }

  function onDrop(event) {
    event.preventDefault()
    const type = event.dataTransfer.getData(DRAG_TYPE)
    if (!type || !canEdit) return
    addBlock(type, screenToFlowPosition({ x: event.clientX - 110, y: event.clientY - 30 }))
  }

  function updateNodeData(nodeId, patch) {
    setNodes((current) => current.map((n) => (n.id === nodeId ? { ...n, data: { ...n.data, ...patch } } : n)))
    setDirty(true)
  }

  function deleteNode(nodeId) {
    setNodes((current) => current.filter((n) => n.id !== nodeId))
    setEdges((current) => current.filter((e) => e.source !== nodeId && e.target !== nodeId))
    setSelection({ node: null, edge: null })
    setDirty(true)
  }

  function deleteEdge(edgeId) {
    setEdges((current) => current.filter((e) => e.id !== edgeId))
    setSelection({ node: null, edge: null })
    setDirty(true)
  }

  function selectNode(nodeId) {
    setNodes((current) => current.map((n) => ({ ...n, selected: n.id === nodeId })))
    setSelection({ node: nodeId, edge: null })
    const node = nodes.find((n) => n.id === nodeId)
    if (node) fitView({ nodes: [node], padding: 1.5, maxZoom: 1, duration: 300 })
  }

  function autoLayout() {
    const graph = fromFlow(nodes, edges)
    const positions = layeredLayout(graph.nodes, graph.edges)
    setNodes((current) => current.map((n) => ({ ...n, position: positions[n.id] ?? n.position })))
    setDirty(true)
    requestAnimationFrame(() => fitView({ padding: 0.2, maxZoom: 1, duration: 300 }))
  }

  // ------------------------------------------------------------------ validate & save
  function applyProblems(list) {
    const byNode = {}
    list.forEach((p) => {
      if (p.node) byNode[p.node] ??= p.message
    })
    setProblems(list)
    setNodes((current) => current.map((n) => ({ ...n, data: { ...n.data, problem: byNode[n.id] ?? null } })))
  }

  async function validate() {
    setBusy('validate')
    try {
      const result = await automationApi.validateGraph(fromFlow(nodes, edges))
      applyProblems(result.problems)
      if (result.valid) toast.success('Looks good: the workflow is valid')
      else toast.error(`${result.problems.length} problem(s) to fix`)
    } catch (err) {
      toast.error(err.message)
    } finally {
      setBusy(null)
    }
  }

  async function save() {
    if (!meta.name.trim()) return toast.error('Give the workflow a name')
    setBusy('save')
    const graph = fromFlow(nodes, edges)
    const body = { name: meta.name.trim(), description: meta.description.trim() || null, graph }
    try {
      if (isNew) {
        const created = await automationApi.createWorkflow(body)
        setDirty(false)
        toast.success('Workflow created (turned off). Turn it on from the Workflows list when ready.')
        navigate(`/automation/workflows/${created.id}`, { replace: true })
      } else {
        const updated = await automationApi.updateWorkflow(id, body)
        setWorkflow(updated)
        applyProblems([])
        setDirty(false)
        toast.success(updated.version !== workflow.version ? `Saved as version ${updated.version}` : 'Layout saved')
      }
    } catch (err) {
      if (err.code === 'invalid_graph') applyProblems(err.details?.problems ?? [])
      toast.error(err.message)
    } finally {
      setBusy(null)
    }
  }

  // ------------------------------------------------------------------ render
  if (error) {
    return (
      <div className="page">
        <Link to="/automation/workflows">← Workflows</Link>
        <Alert tone="danger">{error}</Alert>
      </div>
    )
  }
  if (!catalog) return <p className="muted">Loading…</p>

  const selectedNode = nodes.find((n) => n.id === selection.node)
  const selectedEdge = edges.find((e) => e.id === selection.edge)
  const selectedDef = selectedNode && defs[selectedNode.data.nodeType]

  return (
    <div className="editor-page">
      <div className="editor-toolbar">
        <Link to="/automation/workflows" className="small">
          ← Workflows
        </Link>
        <input
          className="editor-name"
          value={meta.name}
          disabled={!canEdit}
          aria-label="Workflow name"
          maxLength={200}
          onChange={(e) => {
            setMeta((m) => ({ ...m, name: e.target.value }))
            setDirty(true)
          }}
        />
        {workflow && (
          <>
            <Badge tone={workflow.enabled ? 'success' : 'neutral'}>{workflow.enabled ? 'on' : 'off'}</Badge>
            {workflow.mode === 'DRY_RUN' && <Badge tone="info">dry run</Badge>}
            <span className="muted small">v{workflow.version}</span>
          </>
        )}
        {!canEdit && <Badge>read-only</Badge>}
        {dirty && canEdit && <Badge tone="warning">unsaved changes</Badge>}
        <span className="spacer" />
        {canEdit && <Button onClick={autoLayout}>Tidy layout</Button>}
        <Button onClick={validate} loading={busy === 'validate'}>
          Validate
        </Button>
        {canEdit && (
          <Button variant="primary" onClick={save} loading={busy === 'save'} disabled={!dirty && !isNew}>
            {isNew ? 'Create workflow' : 'Save'}
          </Button>
        )}
      </div>

      <div className={`editor-shell ${canEdit ? '' : 'editor-readonly'}`}>
        {canEdit && <Palette catalog={catalog} hasTrigger={hasTrigger} onAdd={(type) => addBlock(type)} />}

        <div className="editor-canvas" ref={wrapper} onDragOver={(e) => e.preventDefault()} onDrop={onDrop}>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={NODE_TYPES}
            onNodesChange={handleNodesChange}
            onEdgesChange={handleEdgesChange}
            onConnect={onConnect}
            isValidConnection={isValidConnection}
            onSelectionChange={({ nodes: ns, edges: es }) =>
              setSelection({
                node: ns.length === 1 ? ns[0].id : null,
                edge: ns.length === 0 && es.length === 1 ? es[0].id : null,
              })
            }
            nodesDraggable={canEdit}
            nodesConnectable={canEdit}
            deleteKeyCode={canEdit ? ['Backspace', 'Delete'] : null}
            defaultEdgeOptions={EDGE_OPTIONS}
            colorMode="system"
            minZoom={0.2}
            fitView
            proOptions={{ hideAttribution: true }}
          >
            <Background gap={20} />
            <Controls showInteractive={false} />
            <MiniMap pannable zoomable className="editor-minimap" />
          </ReactFlow>
        </div>

        <aside className="editor-panel" aria-label="Settings">
          {selectedNode ? (
            <>
              <div className="panel-head">
                <span className="block-icon" aria-hidden="true">
                  {nodeIcon(selectedNode.data.nodeType)}
                </span>
                <div>
                  <strong>{selectedDef?.label ?? selectedNode.data.nodeType}</strong>
                  <div className="muted small mono">{selectedNode.id}</div>
                </div>
              </div>
              {selectedDef?.description && <p className="muted small">{selectedDef.description}</p>}
              {selectedNode.data.problem && <Alert tone="danger">{selectedNode.data.problem}</Alert>}
              <div className="field">
                <label htmlFor="block-name">Display name</label>
                <input
                  id="block-name"
                  value={selectedNode.data.name}
                  disabled={!canEdit}
                  placeholder={selectedDef?.label}
                  maxLength={200}
                  onChange={(e) => updateNodeData(selectedNode.id, { name: e.target.value })}
                />
              </div>
              <SchemaForm
                schema={selectedDef?.config_schema}
                value={selectedNode.data.config}
                disabled={!canEdit}
                onChange={(config) => updateNodeData(selectedNode.id, { config })}
              />
              {selectedDef?.ports?.length > 0 && (
                <p className="muted small">
                  Outputs: {selectedDef.ports.map((p) => selectedDef.port_labels?.[p] ?? p).join(' · ')}. Drag
                  from an output on the right of the block to the next block.
                </p>
              )}
              {canEdit && (
                <Button variant="danger" size="sm" onClick={() => deleteNode(selectedNode.id)}>
                  Delete block
                </Button>
              )}
            </>
          ) : selectedEdge ? (
            <>
              <strong>Connection</strong>
              <p className="small">
                <span className="mono">{selectedEdge.source}</span> —{selectedEdge.sourceHandle}→{' '}
                <span className="mono">{selectedEdge.target}</span>
              </p>
              {canEdit && (
                <Button variant="danger" size="sm" onClick={() => deleteEdge(selectedEdge.id)}>
                  Delete connection
                </Button>
              )}
            </>
          ) : (
            <>
              <strong>Workflow</strong>
              <div className="field">
                <label htmlFor="wf-description">Description</label>
                <textarea
                  id="wf-description"
                  rows={3}
                  maxLength={2000}
                  value={meta.description}
                  disabled={!canEdit}
                  onChange={(e) => {
                    setMeta((m) => ({ ...m, description: e.target.value }))
                    setDirty(true)
                  }}
                />
              </div>
              <p className="muted small">
                Select a block to edit its settings. Runs start at the trigger and follow the output of each block;
                an output with no connection ends the run. Every Airflow change on PROD still needs a human
                approval.
              </p>
              {!isNew && (
                <Link className="small" to={`/automation/runs?workflow_id=${workflow.id}`}>
                  View runs
                </Link>
              )}
            </>
          )}
          <Problems problems={problems} onSelect={selectNode} />
        </aside>
      </div>
    </div>
  )
}

export default function WorkflowEditor() {
  return (
    <ReactFlowProvider>
      <Editor />
    </ReactFlowProvider>
  )
}
