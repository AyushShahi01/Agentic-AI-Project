import { useId } from 'react'
import { Toggle } from '../ui'

// Renders a settings form from a Pydantic JSON schema (the node-type catalog's config_schema).
// Supports the shapes our node configs use: booleans, integers, strings, enums ($ref or inline,
// optionally nullable), arrays of enums (checkbox groups) and arrays of strings (comma lists).

const LONG_TEXT = new Set(['message', 'note', 'description'])

function resolve(schema, root) {
  if (schema?.$ref) {
    const name = schema.$ref.split('/').at(-1)
    return { ...root.$defs?.[name], ...schema, $ref: undefined }
  }
  return schema ?? {}
}

/** Collapse `anyOf: [X, {type: null}]` into X with `nullable: true`. */
function normalize(schema, root) {
  const resolved = resolve(schema, root)
  if (resolved.type === 'array' && resolved.items) {
    return { ...resolved, items: resolve(resolved.items, root) }
  }
  if (resolved.anyOf) {
    const options = resolved.anyOf.map((s) => resolve(s, root))
    const nonNull = options.filter((s) => s.type !== 'null')
    if (nonNull.length === 1) {
      return normalize(
        { ...nonNull[0], nullable: options.length > 1, title: resolved.title, default: resolved.default },
        root,
      )
    }
  }
  return resolved
}

function humanize(key) {
  const text = key.replaceAll('_', ' ')
  return text.charAt(0).toUpperCase() + text.slice(1)
}

function EnumChecks({ options, value, onChange, disabled }) {
  const selected = new Set(value ?? [])
  return (
    <div className="check-group">
      {options.map((option) => (
        <label key={option} className="checkbox-inline">
          <input
            type="checkbox"
            checked={selected.has(option)}
            disabled={disabled}
            onChange={(e) => {
              const next = new Set(selected)
              if (e.target.checked) next.add(option)
              else next.delete(option)
              onChange(options.filter((o) => next.has(o))) // keep schema order
            }}
          />
          <span className="mono small">{option}</span>
        </label>
      ))}
    </div>
  )
}

function FieldControl({ name, schema, value, onChange, disabled, id }) {
  if (schema.type === 'boolean') {
    return <Toggle checked={Boolean(value)} disabled={disabled} onChange={onChange} label={humanize(name)} />
  }
  if (schema.enum) {
    return (
      <select id={id} value={value ?? ''} disabled={disabled} onChange={(e) => onChange(e.target.value || null)}>
        {schema.nullable && <option value="">— none —</option>}
        {!schema.nullable && value == null && (
          <option value="" disabled>
            — choose —
          </option>
        )}
        {schema.enum.map((option) => (
          <option key={option} value={option}>
            {option}
          </option>
        ))}
      </select>
    )
  }
  if (schema.type === 'array') {
    const items = schema.items ?? {}
    if (items.enum) {
      return <EnumChecks options={items.enum} value={value} onChange={onChange} disabled={disabled} />
    }
    return (
      <input
        id={id}
        type="text"
        value={(value ?? []).join(', ')}
        disabled={disabled}
        placeholder="comma, separated"
        onChange={(e) =>
          onChange(
            e.target.value
              .split(',')
              .map((v) => v.trim())
              .filter(Boolean),
          )
        }
      />
    )
  }
  if (schema.type === 'integer' || schema.type === 'number') {
    return (
      <input
        id={id}
        type="number"
        value={value ?? ''}
        min={schema.minimum}
        max={schema.maximum}
        disabled={disabled}
        placeholder={schema.nullable ? 'not set' : undefined}
        onChange={(e) => {
          if (e.target.value === '') return onChange(schema.nullable ? null : undefined)
          const parsed = Number(e.target.value)
          onChange(Number.isNaN(parsed) ? value : parsed)
        }}
      />
    )
  }
  if (LONG_TEXT.has(name)) {
    return (
      <textarea
        id={id}
        rows={3}
        value={value ?? ''}
        maxLength={schema.maxLength}
        disabled={disabled}
        onChange={(e) => onChange(e.target.value)}
      />
    )
  }
  return (
    <input
      id={id}
      type="text"
      value={value ?? ''}
      maxLength={schema.maxLength}
      disabled={disabled}
      onChange={(e) => onChange(schema.nullable && e.target.value === '' ? null : e.target.value)}
    />
  )
}

function SchemaField({ name, schema, value, onChange, disabled, required }) {
  const id = useId()
  const hint = [
    schema.minimum !== undefined && schema.maximum !== undefined && `${schema.minimum}–${schema.maximum}`,
    schema.type === 'array' && !schema.items?.enum && 'Empty = any',
    schema.type === 'array' && schema.items?.enum && 'None selected = any',
  ]
    .filter(Boolean)
    .join(' · ')
  return (
    <div className="field">
      <label htmlFor={id}>
        {humanize(name)}
        {required && ' *'}
      </label>
      <FieldControl id={id} name={name} schema={schema} value={value} onChange={onChange} disabled={disabled} />
      {hint && <small className="field-hint">{hint}</small>}
    </div>
  )
}

export default function SchemaForm({ schema, value, onChange, disabled }) {
  const properties = Object.entries(schema?.properties ?? {})
  if (!properties.length) return <p className="muted small">This block has no settings.</p>
  const required = new Set(schema.required ?? [])
  return (
    <div className="form-grid">
      {properties.map(([name, prop]) => (
        <SchemaField
          key={name}
          name={name}
          schema={normalize(prop, schema)}
          value={value?.[name]}
          required={required.has(name)}
          disabled={disabled}
          onChange={(next) => {
            const updated = { ...value }
            if (next === undefined) delete updated[name]
            else updated[name] = next
            onChange(updated)
          }}
        />
      ))}
    </div>
  )
}
