import { useState, useEffect, useRef, useCallback, useMemo } from 'react'
import './App.css'

const API_BASE = ''

/* ============================================================
   API helpers
   ============================================================ */

async function apiGet(key) {
  const res = await fetch(`${API_BASE}/kv/${encodeURIComponent(key)}`)
  return res.json()
}

async function apiSet(key, value) {
  const res = await fetch(`${API_BASE}/kv/${encodeURIComponent(key)}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ value }),
  })
  return res.json()
}

async function apiDelete(key) {
  const res = await fetch(`${API_BASE}/kv/${encodeURIComponent(key)}`, {
    method: 'DELETE',
  })
  return res.json()
}

async function fetchCluster() {
  const res = await fetch(`${API_BASE}/cluster`)
  return res.json()
}

async function killNode(id) {
  const res = await fetch(`${API_BASE}/nodes/${id}/kill`, { method: 'POST' })
  return res.json()
}

async function restartNode(id) {
  const res = await fetch(`${API_BASE}/nodes/${id}/restart`, { method: 'POST' })
  return res.json()
}

/* ============================================================
   Custom Hooks
   ============================================================ */

function useClusterStatus(interval = 800) {
  const [cluster, setCluster] = useState(null)

  useEffect(() => {
    let active = true
    const poll = async () => {
      try {
        const data = await fetchCluster()
        if (active) setCluster(data)
      } catch {
        // server might be down
      }
    }
    poll()
    const id = setInterval(poll, interval)
    return () => { active = false; clearInterval(id) }
  }, [interval])

  return cluster
}

function useEventStream(nodeFilter = 'all', categoryFilter = 'all') {
  const [events, setEvents] = useState([])

  useEffect(() => {
    setEvents([]) // Clear events on filter change
    const url = new URL(`${window.location.origin}${API_BASE}/events`)
    if (nodeFilter !== 'all') url.searchParams.set('node', nodeFilter)
    if (categoryFilter !== 'all') url.searchParams.set('category', categoryFilter)

    const es = new EventSource(url.toString())

    es.onmessage = (e) => {
      try {
        const event = JSON.parse(e.data)
        setEvents(prev => {
          const next = [...prev, event]
          return next.length > 1000 ? next.slice(-1000) : next
        })
      } catch {}
    }

    es.onerror = () => {
      // Will auto-reconnect
    }

    return () => es.close()
  }, [nodeFilter, categoryFilter])

  return events
}

/* ============================================================
   Components
   ============================================================ */

function Header({ cluster }) {
  const nodes = cluster?.nodes || []
  const leader = nodes.find(n => n.state === 'LEADER')
  const aliveCount = nodes.filter(n => n.alive).length
  const totalCount = nodes.length

  let clusterHealth = 'HEALTHY'
  let healthColor = 'var(--accent-green)'
  if (!leader) {
    clusterHealth = 'NO LEADER'
    healthColor = 'var(--accent-red)'
  } else if (aliveCount < totalCount) {
    clusterHealth = 'DEGRADED'
    healthColor = 'var(--accent-amber)'
  }

  return (
    <header className="header">
      <div className="header-left">
        <div className="header-logo">
          <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="var(--accent-cyan)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M12 2L2 7l10 5 10-5-10-5z"/>
            <path d="M2 17l10 5 10-5"/>
            <path d="M2 12l10 5 10-5"/>
          </svg>
          <h1>Distributed KV Store</h1>
        </div>
      </div>
      <div className="header-right">
        <div className="header-stat">
          <span className="header-stat-label">Leader</span>
          <span className="header-stat-value" style={{ color: leader ? 'var(--accent-green)' : 'var(--accent-red)' }}>
            {leader ? `Node ${leader.id}` : 'None'}
          </span>
        </div>
        <div className="header-divider" />
        <div className="header-stat">
          <span className="header-stat-label">Term</span>
          <span className="header-stat-value mono">
            {leader?.term ?? '—'}
          </span>
        </div>
        <div className="header-divider" />
        <div className="header-stat">
          <span className="header-stat-label">Cluster</span>
          <span className="header-stat-value" style={{ color: healthColor }}>
            {clusterHealth}
          </span>
        </div>
      </div>
    </header>
  )
}


function KVControls({ onOperationStarted }) {
  const [key, setKey] = useState('')
  const [value, setValue] = useState('')
  const [result, setResult] = useState(null)
  const [loading, setLoading] = useState(false)

  const doAction = async (action) => {
    if (!key.trim()) {
      setResult({ type: 'error', message: 'Key is required' })
      return
    }
    setLoading(true)
    try {
      let data
      if (action === 'GET') {
        data = await apiGet(key)
        if (data.error) {
          setResult({ type: 'error', message: data.error })
        } else {
          setResult({ type: 'success', message: `${data.key} = ${data.value}` })
        }
      } else if (action === 'SET') {
        if (!value.trim()) {
          setResult({ type: 'error', message: 'Value is required for SET' })
          setLoading(false)
          return
        }
        data = await apiSet(key, value)
        if (data.error) {
          setResult({ type: 'error', message: `${data.error} (leader: Node ${data.leader_id})` })
        } else {
          setResult({ type: 'success', message: `SET ${data.key} = ${data.value}` })
          if (data.log_index) onOperationStarted('SET', key, data.log_index)
        }
      } else if (action === 'DELETE') {
        data = await apiDelete(key)
        if (data.error) {
          setResult({ type: 'error', message: `${data.error} (leader: Node ${data.leader_id})` })
        } else {
          setResult({ type: 'success', message: `Deleted ${data.key}` })
          if (data.log_index) onOperationStarted('DELETE', key, data.log_index)
        }
      }
    } catch (err) {
      setResult({ type: 'error', message: 'Connection error: ' + err.message })
    }
    setLoading(false)
  }

  return (
    <div className="kv-controls card">
      <div className="card-header">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="var(--accent-blue)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <rect x="3" y="11" width="18" height="11" rx="2" ry="2"/>
          <path d="M7 11V7a5 5 0 0 1 10 0v4"/>
        </svg>
        <h2>Key-Value Controls</h2>
      </div>
      <div className="kv-inputs">
        <div className="input-group">
          <label htmlFor="kv-key">Key</label>
          <input
            id="kv-key"
            type="text"
            value={key}
            onChange={e => setKey(e.target.value)}
            placeholder="Enter key..."
            className="input"
          />
        </div>
        <div className="input-group">
          <label htmlFor="kv-value">Value</label>
          <input
            id="kv-value"
            type="text"
            value={value}
            onChange={e => setValue(e.target.value)}
            placeholder="Enter value..."
            className="input"
          />
        </div>
      </div>
      <div className="kv-actions">
        <button id="btn-set" className="btn btn-primary" onClick={() => doAction('SET')} disabled={loading}>
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><path d="M12 5v14M5 12h14"/></svg>
          SET
        </button>
        <button id="btn-get" className="btn btn-secondary" onClick={() => doAction('GET')} disabled={loading}>
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><circle cx="11" cy="11" r="8"/><path d="M21 21l-4.35-4.35"/></svg>
          GET
        </button>
        <button id="btn-delete" className="btn btn-danger" onClick={() => doAction('DELETE')} disabled={loading}>
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><path d="M3 6h18M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/></svg>
          DELETE
        </button>
      </div>
      {result && (
        <div className={`kv-result ${result.type}`}>
          <span className="kv-result-icon">{result.type === 'success' ? '✓' : '✗'}</span>
          <span className="mono">{result.message}</span>
        </div>
      )}
    </div>
  )
}


function OperationTimeline({ operation, events }) {
  if (!operation) return null

  // Analyze events to track operation progress
  const hasAppended = events.some(e => e.type === 'log_appended' && e.log_index === operation.index)
  const hasReplicated = events.some(e => e.type === 'log_replicated' && e.log_index >= operation.index)
  const hasCommitted = events.some(e => e.type === 'commit_advanced' && e.log_index >= operation.index)
  const hasApplied = events.some(e => e.type === 'command_applied' && e.log_index >= operation.index)

  return (
    <div className="timeline-section">
      <h3 style={{ fontSize: '0.9rem', color: 'var(--text-primary)', marginBottom: '8px' }}>
        Operation Timeline: <span className="mono">{operation.type} {operation.key}</span> (Index {operation.index})
      </h3>
      <div className="timeline-row">
        <div className="timeline-step done">
          <span>✓</span> Received
        </div>
        <span className="timeline-arrow">→</span>
        <div className={`timeline-step ${hasAppended ? 'done' : 'active'}`}>
          <span>{hasAppended ? '✓' : '⟳'}</span> Log Appended
        </div>
        <span className="timeline-arrow">→</span>
        <div className={`timeline-step ${hasReplicated ? 'done' : hasAppended ? 'active' : ''}`}>
          <span>{hasReplicated ? '✓' : hasAppended ? '⟳' : '○'}</span> Replicated
        </div>
        <span className="timeline-arrow">→</span>
        <div className={`timeline-step ${hasCommitted ? 'done' : hasReplicated ? 'active' : ''}`}>
          <span>{hasCommitted ? '✓' : hasReplicated ? '⟳' : '○'}</span> Committed
        </div>
        <span className="timeline-arrow">→</span>
        <div className={`timeline-step ${hasApplied ? 'done' : hasCommitted ? 'active' : ''}`}>
          <span>{hasApplied ? '✓' : hasCommitted ? '⟳' : '○'}</span> Applied
        </div>
      </div>
    </div>
  )
}


function ReplicationProgress({ cluster }) {
  if (!cluster || !cluster.nodes) return null
  const nodes = cluster.nodes
  const leader = nodes.find(n => n.state === 'LEADER')
  if (!leader) return null

  return (
    <div style={{ marginTop: '16px' }}>
      <h3 style={{ fontSize: '0.85rem', color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: '8px' }}>Replication Progress</h3>
      <table className="replication-grid">
        <thead>
          <tr>
            <th>Node</th>
            <th>Index</th>
            <th>Progress</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          {nodes.filter(n => n.id !== leader.id).map(node => {
            const isDead = !node.alive
            const progress = leader.last_log_index === 0 ? 100 : Math.min(100, Math.floor((node.last_log_index / leader.last_log_index) * 100))
            const inSync = node.last_log_index === leader.last_log_index
            
            return (
              <tr key={node.id}>
                <td>Node {node.id}</td>
                <td className="mono">{node.last_log_index} / {leader.last_log_index}</td>
                <td style={{ width: '40%' }}>
                  <div className="progress-bar-container">
                    <div className="progress-bar-fill" style={{ width: `${progress}%`, background: inSync ? 'var(--accent-green)' : 'var(--accent-amber)' }}></div>
                  </div>
                </td>
                <td>{isDead ? '💀' : inSync ? '✓' : '⟳'}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}


function NodeCard({ node, onKill, onRestart }) {
  const isLeader = node.state === 'LEADER'
  const isDead = !node.alive
  const isCandidate = node.state === 'CANDIDATE'

  let cardClass = 'node-card'
  if (isLeader) cardClass += ' leader'
  if (isDead) cardClass += ' dead'
  if (isCandidate) cardClass += ' candidate'

  return (
    <div className={cardClass}>
      <div className="node-header">
        <div className="node-id">
          <div className={`status-dot ${isDead ? 'dead' : isLeader ? 'leader' : 'follower'}`} />
          <span>Node {node.id}</span>
        </div>
        <span className={`state-badge ${node.state.toLowerCase()}`}>
          {isDead ? 'OFFLINE' : node.state}
        </span>
      </div>

      <div className="node-stats">
        <div className="stat-row">
          <span className="stat-label">Term</span>
          <span className="stat-value mono">{node.term}</span>
        </div>
        <div className="stat-row">
          <span className="stat-label">Leader</span>
          <span className="stat-value">{node.leader_id >= 0 ? `Node ${node.leader_id}` : '—'}</span>
        </div>
        <div className="stat-row">
          <span className="stat-label">Log Index</span>
          <span className="stat-value mono">{node.last_log_index}</span>
        </div>
        <div className="stat-row">
          <span className="stat-label">Commit Index</span>
          <span className="stat-value mono">{node.commit_index}</span>
        </div>
        <div className="stat-row">
          <span className="stat-label">Last Applied</span>
          <span className="stat-value mono">{node.last_applied}</span>
        </div>
        <div className="stat-row">
          <span className="stat-label">Status</span>
          <span className={`stat-value ${isDead ? 'text-red' : 'text-green'}`}>
            {isDead ? 'DEAD' : 'ALIVE'}
          </span>
        </div>
      </div>

      <button
        id={`kill-node-${node.id}`}
        className={`btn btn-kill ${isDead ? 'btn-restart' : ''}`}
        onClick={() => isDead ? onRestart(node.id) : onKill(node.id)}
      >
        {isDead ? 'RESTART NODE' : 'KILL NODE'}
      </button>
    </div>
  )
}


function ClusterOverview({ cluster, onKillNode, onRestartNode }) {
  const nodes = cluster?.nodes || []

  return (
    <div className="cluster-section">
      <div className="section-header">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="var(--accent-purple)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <rect x="2" y="2" width="20" height="8" rx="2" ry="2"/>
          <rect x="2" y="14" width="20" height="8" rx="2" ry="2"/>
          <line x1="6" y1="6" x2="6.01" y2="6"/>
          <line x1="6" y1="18" x2="6.01" y2="18"/>
        </svg>
        <h2>Cluster Overview</h2>
      </div>
      <div className="node-grid">
        {nodes.map(node => (
          <NodeCard key={node.id} node={node} onKill={onKillNode} onRestart={onRestartNode} />
        ))}
      </div>
      <ReplicationProgress cluster={cluster} />
    </div>
  )
}

function ObservabilityConsole({ events, nodeFilter, setNodeFilter, categoryFilter, setCategoryFilter, clusterNodes }) {
  const scrollRef = useRef(null)
  const [autoScroll, setAutoScroll] = useState(true)

  useEffect(() => {
    if (autoScroll && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight
    }
  }, [events, autoScroll])

  const handleScroll = useCallback(() => {
    if (!scrollRef.current) return
    const { scrollTop, scrollHeight, clientHeight } = scrollRef.current
    setAutoScroll(scrollHeight - scrollTop - clientHeight < 60)
  }, [])

  const formatTime = (ts) => {
    const d = new Date(ts)
    return d.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' })
  }

  const getEventColor = (type) => {
    if (type.includes('leader') || type === 'became_leader') return 'var(--accent-green)'
    if (type.includes('election') || type.includes('candidate') || type === 'became_candidate') return 'var(--accent-amber)'
    if (type.includes('vote')) return 'var(--accent-purple)'
    if (type.includes('heartbeat')) return 'var(--text-muted)'
    if (type.includes('commit') || type.includes('applied') || type.includes('appended')) return 'var(--accent-cyan)'
    if (type.includes('replicated')) return 'var(--accent-blue)'
    if (type.includes('stopped') || type.includes('failed')) return 'var(--accent-red)'
    if (type.includes('follower')) return 'var(--text-secondary)'
    return 'var(--text-secondary)'
  }

  // Aggregate Heartbeats for Heartbeats Category
  const renderHeartbeats = () => {
    if (events.length === 0) return <div className="event-empty">Waiting for events...</div>
    
    // Group heartbeats by node and time block (rough heuristic for "round")
    let hbNodes = {} // tracking leader -> followers stats
    
    // We want to just show an aggregated view. Since events array is growing, 
    // we can calculate the latest stats by reading backwards.
    events.forEach(e => {
      if (e.type === 'heartbeat_sent' && e.peer_id >= 0) {
        if (!hbNodes[e.node_id]) hbNodes[e.node_id] = {}
        if (!hbNodes[e.node_id][e.peer_id]) hbNodes[e.node_id][e.peer_id] = { count: 0, lastTime: e.timestamp }
        hbNodes[e.node_id][e.peer_id].count++
        hbNodes[e.node_id][e.peer_id].lastTime = e.timestamp
      }
    })

    return (
      <div style={{ padding: '8px' }}>
        {Object.keys(hbNodes).length === 0 ? <div className="event-empty">No heartbeats yet.</div> : null}
        {Object.entries(hbNodes).map(([leaderId, peers]) => (
          <div key={leaderId} className="heartbeat-group">
            <div className="heartbeat-header">
              <strong style={{ color: 'var(--accent-green)' }}>Leader: Node {leaderId}</strong>
            </div>
            <div className="heartbeat-peers">
              {Object.entries(peers).map(([peerId, stats]) => (
                <div key={peerId} className="peer-row">
                  <span style={{ minWidth: '80px' }}>→ Node {peerId}</span>
                  <span className="mono" style={{ color: 'var(--text-muted)' }}>{formatTime(stats.lastTime)}</span>
                  <span style={{ background: 'var(--surface-3)', padding: '2px 8px', borderRadius: '12px', fontSize: '0.8rem' }}>
                    {stats.count} pings
                  </span>
                  <span style={{ color: 'var(--accent-green)', marginLeft: 'auto' }}>✓ Healthy</span>
                </div>
              ))}
            </div>
          </div>
        ))}
        <div style={{ marginTop: '24px', borderTop: '1px solid var(--border-subtle)', paddingTop: '16px' }}>
          <h4 style={{ marginBottom: '8px', color: 'var(--text-muted)' }}>Detailed Replication Log</h4>
          {events.filter(e => e.type === 'log_replicated' || e.type === 'commit_advanced').slice(-50).map((e, i) => (
            <div key={i} className="event-line">
              <span className="event-time mono">{formatTime(e.timestamp)}</span>
              <span className="event-node" style={{ color: getEventColor(e.type) }}>[Node {e.node_id}]</span>
              <span className="event-msg">{e.message}</span>
              {e.peer_id >= 0 && <span style={{ color: 'var(--accent-cyan)' }}> → Node {e.peer_id}</span>}
              {e.log_index > 0 && <span className="log-index">idx:{e.log_index}</span>}
            </div>
          ))}
        </div>
      </div>
    )
  }

  const renderElections = () => {
    return events.map((event, i) => (
      <div key={i} className="event-line" style={{ padding: '8px 0', borderBottom: '1px solid var(--surface-2)' }}>
        <span className="event-time mono">{formatTime(event.timestamp)}</span>
        <span className="event-node" style={{ color: getEventColor(event.type), fontWeight: 'bold' }}>
          Node {event.node_id}
        </span>
        <span className="event-term mono" style={{ background: 'var(--surface-2)', padding: '2px 6px', borderRadius: '4px', margin: '0 8px' }}>
          Term {event.term}
        </span>
        <span className="event-msg" style={{ fontSize: '1.05rem', color: event.type.includes('leader') ? 'var(--accent-green)' : 'inherit' }}>
          {event.message.toUpperCase()}
        </span>
        {event.peer_id >= 0 && <span style={{ color: 'var(--accent-purple)' }}> (Node {event.peer_id})</span>}
      </div>
    ))
  }

  const renderAll = () => {
    // Filter out excessive heartbeat events in ALL view to keep it readable
    const filteredEvents = events.filter((e, i) => {
      if (e.type === 'heartbeat_sent' || e.type === 'heartbeat_received') return false
      return true
    })

    return filteredEvents.map((event, i) => (
      <div key={i} className="event-line">
        <span className="event-time mono">{formatTime(event.timestamp)}</span>
        <span className="event-node" style={{ color: getEventColor(event.type) }}>
          [Node {event.node_id}]
        </span>
        <span className="event-msg">{event.message}</span>
        {event.peer_id >= 0 && <span style={{ color: 'var(--accent-blue)', marginLeft: '4px' }}>→ Node {event.peer_id}</span>}
        {event.log_index > 0 && <span className="log-index">idx:{event.log_index}</span>}
        <span className="event-term mono">T{event.term}</span>
      </div>
    ))
  }

  return (
    <div className="event-log card" style={{ height: '500px', display: 'flex', flexDirection: 'column' }}>
      <div className="nav-filters" style={{ padding: '16px', borderBottom: '1px solid var(--border-subtle)', marginBottom: 0 }}>
        <div className="tab-group">
          <button className={`tab-btn ${categoryFilter === 'all' ? 'active' : ''}`} onClick={() => setCategoryFilter('all')}>ALL EVENTS</button>
          <button className={`tab-btn ${categoryFilter === 'heartbeats' ? 'active' : ''}`} onClick={() => setCategoryFilter('heartbeats')}>HEARTBEATS & REPLICATION</button>
          <button className={`tab-btn ${categoryFilter === 'elections' ? 'active' : ''}`} onClick={() => setCategoryFilter('elections')}>ELECTIONS</button>
        </div>
        
        <select className="node-filter" value={nodeFilter} onChange={e => setNodeFilter(e.target.value)}>
          <option value="all">All Nodes</option>
          {clusterNodes.map(n => (
            <option key={n.id} value={n.id}>Node {n.id}</option>
          ))}
        </select>
      </div>

      <div className="event-terminal" ref={scrollRef} onScroll={handleScroll} style={{ flex: 1, overflowY: 'auto' }}>
        {events.length === 0 ? (
          <div className="event-empty">Waiting for events...</div>
        ) : categoryFilter === 'heartbeats' ? (
          renderHeartbeats()
        ) : categoryFilter === 'elections' ? (
          renderElections()
        ) : (
          renderAll()
        )}
      </div>
    </div>
  )
}

/* ============================================================
   App
   ============================================================ */

function App() {
  const cluster = useClusterStatus(800)
  
  const [nodeFilter, setNodeFilter] = useState('all')
  const [categoryFilter, setCategoryFilter] = useState('all')
  const [operation, setOperation] = useState(null)
  
  // Custom hook that reconnects SSE on filter changes
  const events = useEventStream(nodeFilter, categoryFilter)

  const handleKillNode = async (id) => {
    try {
      await killNode(id)
    } catch (err) {
      console.error('Failed to kill node:', err)
    }
  }

  const handleRestartNode = async (id) => {
    try {
      await restartNode(id)
    } catch (err) {
      console.error('Failed to restart node:', err)
    }
  }

  const handleOperationStarted = (type, key, index) => {
    setOperation({ type, key, index })
  }

  return (
    <div className="app">
      <Header cluster={cluster} />

      <main className="main">
        <OperationTimeline operation={operation} events={events} />
        
        <div className="top-row">
          <KVControls onOperationStarted={handleOperationStarted} />
          <ClusterOverview cluster={cluster} onKillNode={handleKillNode} onRestartNode={handleRestartNode} />
        </div>
        
        <ObservabilityConsole 
          events={events} 
          nodeFilter={nodeFilter}
          setNodeFilter={setNodeFilter}
          categoryFilter={categoryFilter}
          setCategoryFilter={setCategoryFilter}
          clusterNodes={cluster?.nodes || []}
        />
      </main>
    </div>
  )
}

export default App
