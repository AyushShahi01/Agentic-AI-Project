import { Handle, Position } from '@xyflow/react'
import { memo } from 'react'
import { BAD_PORTS } from '../../components/flow/graph'
import { NODE_LABELS, describeConfig, nodeIcon, stageOf } from './automationText'

/**
 * A workflow block on the canvas: a coloured stage band, the title and a one-line summary.
 * One input on the left (except triggers); one output on the right, or one labelled output
 * per port when the block branches.
 *
 * data: { nodeType, name, config, def (catalog entry), problem?, run?: {tone, label}, dimmed? }
 */
function BlockNode({ data, selected }) {
  const def = data.def
  const ports = def?.ports ?? []
  const category = def?.category ?? data.nodeType.split('.')[0]
  const stage = stageOf(category)
  const kind = def?.label ?? NODE_LABELS[data.nodeType] ?? data.nodeType
  const title = data.name || kind
  const summary = describeConfig(data.nodeType, data.config)
  const classes = [
    'block',
    `stage-${stage.id}`,
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
      <div className="block-band">
        <span className="block-stage">{stage.label}</span>
        {data.name && <span className="block-kind">{kind}</span>}
      </div>
      <div className="block-head">
        <span className="block-icon" aria-hidden="true">
          {nodeIcon(data.nodeType)}
        </span>
        <div className="block-title">{title}</div>
      </div>
      {summary && <div className="block-summary">{summary}</div>}
      {data.problem && <div className="block-error">{data.problem}</div>}
      {data.run?.label && <div className="block-run-label">{data.run.label}</div>}
      {ports.length === 1 && (
        <Handle type="source" position={Position.Right} id={ports[0]} className="block-handle-out" />
      )}
      {ports.length > 1 && (
        <div className="block-ports">
          {ports.map((port) => (
            <div key={port} className={`block-port block-port-${BAD_PORTS.has(port) ? 'bad' : 'ok'}`}>
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
