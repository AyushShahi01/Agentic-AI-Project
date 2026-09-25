import { Handle, Position } from '@xyflow/react'
import { memo } from 'react'
import { NODE_LABELS, describeConfig, nodeIcon } from './automationText'

const PORT_TONES = { failed: 'bad', rejected: 'bad', false: 'bad', paused: 'bad', busy: 'bad' }

/**
 * A workflow block on the canvas: one input on the left (except triggers) and one labelled
 * output handle per port on the right.
 *
 * data: { nodeType, name, config, def (catalog entry), problem?, run?: {status, port}, dimmed? }
 */
function BlockNode({ id, data, selected }) {
  const def = data.def
  const ports = def?.ports ?? []
  const category = def?.category ?? data.nodeType.split('.')[0]
  const title = data.name || def?.label || NODE_LABELS[data.nodeType] || data.nodeType
  const summary = describeConfig(data.nodeType, data.config)
  const classes = [
    'block',
    `block-${category}`,
    selected && 'block-selected',
    data.problem && 'block-problem',
    data.run && `block-run-${data.run.tone}`,
    data.dimmed && 'block-dimmed',
  ]
    .filter(Boolean)
    .join(' ')

  return (
    <div className={classes} title={data.problem ?? def?.description}>
      {category !== 'trigger' && <Handle type="target" position={Position.Left} className="block-handle-in" />}
      <div className="block-head">
        <span className="block-icon" aria-hidden="true">
          {nodeIcon(data.nodeType)}
        </span>
        <div className="block-titles">
          <div className="block-title">{title}</div>
          <div className="block-type">{data.name ? def?.label : id}</div>
        </div>
      </div>
      {summary && <div className="block-summary">{summary}</div>}
      {data.problem && <div className="block-error">{data.problem}</div>}
      {data.run?.label && <div className="block-run-label">{data.run.label}</div>}
      {ports.length > 0 && (
        <div className="block-ports">
          {ports.map((port) => (
            <div key={port} className={`block-port block-port-${PORT_TONES[port] ?? 'ok'}`}>
              <span className="block-port-label">{def.port_labels?.[port] ?? port}</span>
              <Handle type="source" position={Position.Right} id={port} className="block-handle-out" />
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

export default memo(BlockNode)
