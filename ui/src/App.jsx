import { useState, useEffect, useRef, useCallback } from 'react'
import { createChart } from 'lightweight-charts'
import axios from 'axios'
import './index.css'

function App() {
  const [connected, setConnected] = useState(false)
  const [currentPrice, setCurrentPrice] = useState(null)
  const [signals, setSignals] = useState([])
  const [indicators, setIndicators] = useState({})
  const [levels, setLevels] = useState([])
  const [footprint, setFootprint] = useState(null)
  const [showFootprint, setShowFootprint] = useState(false)
  const [profileMeta, setProfileMeta] = useState({
    histogram: [],
    poc: null,
    vah: null,
    val: null,
    window: 'rolling_86400'
  })
  
  const chartContainerRef = useRef(null)
  const chartRef = useRef(null)
  const wsRef = useRef(null)
  const levelLinesRef = useRef([])
  const avwapLinesRef = useRef([])
  const signalLinesRef = useRef([])
  const profileCanvasRef = useRef(null)
  const profileDataRef = useRef(profileMeta)
  const PROFILE_WINDOW = 'rolling_86400'

  const drawVolumeProfile = useCallback((histogram, poc) => {
    const canvas = profileCanvasRef.current
    const surface = chartContainerRef.current
    if (!canvas || !surface || !chartRef.current) {
      return
    }
    if (!histogram || histogram.length === 0) {
      const ctx = canvas.getContext('2d')
      if (ctx) {
        ctx.clearRect(0, 0, canvas.width, canvas.height)
      }
      return
    }
    const dpr = window.devicePixelRatio || 1
    const width = surface.clientWidth
    const height = surface.clientHeight
    if (canvas.width !== width * dpr || canvas.height !== height * dpr) {
      canvas.width = width * dpr
      canvas.height = height * dpr
      canvas.style.width = `${width}px`
      canvas.style.height = `${height}px`
    }
    const ctx = canvas.getContext('2d')
    if (!ctx) return
    ctx.setTransform(1, 0, 0, 1, 0, 0)
    ctx.clearRect(0, 0, canvas.width, canvas.height)
    ctx.scale(dpr, dpr)

    const series = chartRef.current.candlestickSeries
    const priceScale = series.priceScale()
    if (!priceScale) return

    const maxVol = Math.max(...histogram.map((bin) => bin.volume || 0))
    if (!maxVol || maxVol <= 0) return

    const priceStep = histogram.length > 1
      ? Math.abs(histogram[1].price - histogram[0].price)
      : Math.max(Math.abs(histogram[0].price) * 0.0005, 1e-2)
    const maxWidth = Math.min(width * 0.25, 120)

    histogram.forEach((bin) => {
      if (!bin || typeof bin.price !== 'number') return
      const coord = priceScale.priceToCoordinate(bin.price)
      if (coord === null || coord === undefined) return
      const upper = priceScale.priceToCoordinate(bin.price + priceStep / 2)
      const lower = priceScale.priceToCoordinate(bin.price - priceStep / 2)
      let thickness = Math.abs((upper ?? coord) - (lower ?? coord))
      if (!Number.isFinite(thickness) || thickness < 2) {
        thickness = 2
      }
      const widthPx = (bin.volume / maxVol) * maxWidth
      const x = width - widthPx - 12
      const y = coord - thickness / 2
      const isPoc = poc !== null && Math.abs(bin.price - poc) <= priceStep / 2
      ctx.fillStyle = isPoc ? 'rgba(249, 115, 22, 0.65)' : 'rgba(88, 166, 255, 0.35)'
      ctx.fillRect(x, y, widthPx, thickness)
    })
  }, [])

  useEffect(() => {
    if (chartContainerRef.current && !chartRef.current) {
      const surface = chartContainerRef.current
      const chart = createChart(surface, {
        layout: {
          background: { color: '#0d1117' },
          textColor: '#c9d1d9',
        },
        grid: {
          vertLines: { color: '#30363d' },
          horzLines: { color: '#30363d' },
        },
        width: surface.clientWidth,
        height: surface.clientHeight,
      })

      const candlestickSeries = chart.addCandlestickSeries({
        upColor: '#3fb950',
        downColor: '#f85149',
        borderVisible: false,
        wickUpColor: '#3fb950',
        wickDownColor: '#f85149',
      })

      chartRef.current = { chart, candlestickSeries }

      const handleResize = () => {
        chart.applyOptions({
          width: surface.clientWidth,
          height: surface.clientHeight,
        })
      }
      const overlayCanvas = document.createElement('canvas')
      overlayCanvas.className = 'profile-overlay'
      surface.appendChild(overlayCanvas)
      profileCanvasRef.current = overlayCanvas
      const resizeOverlay = () => {
        drawVolumeProfile(profileDataRef.current.histogram, profileDataRef.current.poc)
      }
      const resizeObserver = new ResizeObserver(resizeOverlay)
      resizeObserver.observe(surface)

      window.addEventListener('resize', handleResize)

      return () => {
        window.removeEventListener('resize', handleResize)
        resizeObserver.disconnect()
        if (profileCanvasRef.current && surface.contains(profileCanvasRef.current)) {
          surface.removeChild(profileCanvasRef.current)
          profileCanvasRef.current = null
        }
        chart.remove()
      }
    }
  }, [drawVolumeProfile])

  useEffect(() => {
    const ws = new WebSocket('ws://localhost:8080/ws')

    ws.onopen = () => {
      console.log('WebSocket connected')
      setConnected(true)
    }

    ws.onmessage = (event) => {
      const data = JSON.parse(event.data)
      
      if (data.type === 'update') {
        setCurrentPrice(data.price)
        setSignals(data.signals || [])
        setIndicators(data.indicators || {})
      }
    }

    ws.onclose = () => {
      console.log('WebSocket disconnected')
      setConnected(false)
    }

    ws.onerror = (error) => {
      console.error('WebSocket error:', error)
    }

    wsRef.current = ws

    return () => {
      ws.close()
    }
  }, [])

  useEffect(() => {
    const fetchLevels = async () => {
      try {
        const response = await axios.get('http://localhost:8080/api/levels')
        setLevels(response.data.levels || [])
      } catch (error) {
        console.error('Error fetching levels:', error)
      }
    }
    const fetchFootprint = async () => {
      try {
        const response = await axios.get('http://localhost:8080/api/footprint/current')
        setFootprint(response.data)
      } catch (error) {
        setFootprint(null)
      }
    }

    fetchLevels()
    fetchFootprint()
    const interval = setInterval(fetchLevels, 5000)
    const fpInt = setInterval(fetchFootprint, 2000)

    return () => {
      clearInterval(interval)
      clearInterval(fpInt)
    }
  }, [])

  useEffect(() => {
    const fetchProfile = async () => {
      try {
        const response = await axios.get(`http://localhost:8080/api/profile/${PROFILE_WINDOW}`)
        setProfileMeta({
          histogram: response.data.histogram || [],
          poc: response.data.poc,
          vah: response.data.vah,
          val: response.data.val,
          window: response.data.window || PROFILE_WINDOW
        })
      } catch (error) {
        // ignore fetch errors
      }
    }
    fetchProfile()
    const id = setInterval(fetchProfile, 10000)
    return () => clearInterval(id)
  }, [])

  useEffect(() => {
    profileDataRef.current = profileMeta
    drawVolumeProfile(profileMeta.histogram, profileMeta.poc)
  }, [profileMeta, drawVolumeProfile])

  useEffect(() => {
    if (!chartRef.current) return
    const { candlestickSeries } = chartRef.current

    levelLinesRef.current.forEach((l) => candlestickSeries.removePriceLine(l))
    levelLinesRef.current = []

    const colorFor = (lvl) => {
      if (lvl.type === 'LVN') return '#f59e0b'
      if (lvl.type === 'HVN') return '#a78bfa'
      if (lvl.type === 'POC') return '#58a6ff'
      if (lvl.type === 'VAH') return '#10b981'
      if (lvl.type === 'VAL') return '#ef4444'
      if (lvl.type === 'BREAKTHROUGH') return '#e11d48'
      return '#8b949e'
    }

    levels.slice(0, 20).forEach((lvl) => {
      if (!lvl.price) return
      const line = candlestickSeries.createPriceLine({
        price: lvl.price,
        color: colorFor(lvl),
        lineWidth: 1,
        lineStyle: 2,
        axisLabelVisible: true,
        title: `${lvl.type}${lvl.side ? ' ' + lvl.side.toUpperCase() : ''}`,
      })
      levelLinesRef.current.push(line)
    })
  }, [levels])

  useEffect(() => {
    if (!chartRef.current) return
    const { candlestickSeries } = chartRef.current

    // Clear previous signal lines
    signalLinesRef.current.forEach((l) => candlestickSeries.removePriceLine(l))
    signalLinesRef.current = []

    const colorForSignal = (sig) => {
      if (sig.state === 'filled') return sig.side === 'long' ? '#16a34a' : '#dc2626'
      if (sig.state === 'armed') return '#f59e0b'
      if (sig.state === 'monitored') return '#8b949e'
      return '#6b7280'
    }

    signals.forEach((sig) => {
      if (!sig.level_price) return
      const line = candlestickSeries.createPriceLine({
        price: sig.level_price,
        color: colorForSignal(sig),
        lineWidth: 2,
        lineStyle: 0,
        axisLabelVisible: true,
        title: `SIG ${sig.side.toUpperCase()} ${sig.state.toUpperCase()}`,
      })
      signalLinesRef.current.push(line)
    })
  }, [signals])

  useEffect(() => {
    if (!chartRef.current) return
    const { candlestickSeries } = chartRef.current

    const fetchAvwap = async () => {
      try {
        const res = await axios.get('http://localhost:8080/api/avwap')
        const avwaps = res.data.avwaps || {}
        avwapLinesRef.current.forEach((l) => candlestickSeries.removePriceLine(l))
        avwapLinesRef.current = []
        Object.values(avwaps).forEach((a) => {
          if (a.avwap) {
            avwapLinesRef.current.push(
              candlestickSeries.createPriceLine({ price: a.avwap, color: '#22d3ee', lineWidth: 1, title: 'AVWAP' })
            )
          }
          if (a.lower_band) {
            avwapLinesRef.current.push(
              candlestickSeries.createPriceLine({ price: a.lower_band, color: '#0ea5e9', lineWidth: 1, lineStyle: 3, title: 'AVWAP−σ' })
            )
          }
          if (a.upper_band) {
            avwapLinesRef.current.push(
              candlestickSeries.createPriceLine({ price: a.upper_band, color: '#0ea5e9', lineWidth: 1, lineStyle: 3, title: 'AVWAP+σ' })
            )
          }
        })
      } catch (e) {
        // ignore
      }
    }

    fetchAvwap()
    const id = setInterval(fetchAvwap, 7000)
    return () => clearInterval(id)
  }, [])

  return (
    <div className="app">
      <div className="header">
        <h1>Pivot Divergence System</h1>
        <div className="status">
          <div className={`status-indicator ${connected ? '' : 'disconnected'}`}></div>
          <span>{connected ? 'Connected' : 'Disconnected'}</span>
          {currentPrice && <span>Price: ${currentPrice.toFixed(2)}</span>}
        </div>
      </div>

      <div className="main-container">
        <div className="chart-container">
          <div className="chart-surface" ref={chartContainerRef}></div>
        </div>

        <div className="sidebar">
          <div className="section">
            <h2>Active Signals ({signals.length})</h2>
            {signals.length === 0 ? (
              <div className="card">
                <div className="card-details">No active signals</div>
              </div>
            ) : (
              signals.map((signal) => (
                <div className="card" key={signal.signal_id}>
                  <div className="card-header">
                    <span className="card-title">
                      {signal.level_price.toFixed(2)}
                    </span>
                    <div>
                      <span className={`badge ${signal.side}`}>{signal.side.toUpperCase()}</span>
                      <span className={`badge ${signal.state}`} style={{marginLeft: '4px'}}>
                        {signal.state.toUpperCase()}
                      </span>
                    </div>
                  </div>
                  <div className="card-details">
                    <div>
                      <span>Zone:</span>
                      <span>{signal.zone_low.toFixed(2)} - {signal.zone_high.toFixed(2)}</span>
                    </div>
                    <div>
                      <span>Score:</span>
                      <span>{signal.score.toFixed(2)}</span>
                    </div>
                    {signal.divergences && (
                      <div>
                        <span>Confirms:</span>
                        <span>
                          {signal.divergences.count || 0} 
                          {signal.divergences.rsi ? ' RSI' : ''}
                          {signal.divergences.cvd ? ' CVD' : ''}
                          {signal.divergences.obi ? ' OBI' : ''}
                        </span>
                      </div>
                    )}
                    {signal.entry_price && (
                      <div>
                        <span>Entry:</span>
                        <span>{signal.entry_price.toFixed(2)}</span>
                      </div>
                    )}
                  </div>
                </div>
              ))
            )}
          </div>

          <div className="section">
            <h2>Indicators</h2>
            <div className="indicator-grid">
              <div className="indicator-item">
                <div className="indicator-label">RSI</div>
                <div className="indicator-value">
                  {typeof indicators.rsi === 'number' ? indicators.rsi.toFixed(2) : '--'}
                </div>
              </div>
              <div className="indicator-item">
                <div className="indicator-label">CVD</div>
                <div className="indicator-value">
                  {typeof indicators.cvd === 'number' ? indicators.cvd.toFixed(0) : '--'}
                </div>
              </div>
              <div className="indicator-item">
                <div className="indicator-label">OBI Z</div>
                <div className="indicator-value">
                  {typeof indicators.obi_z === 'number' ? indicators.obi_z.toFixed(2) : '--'}
                </div>
              </div>
              <div className="indicator-item">
                <div className="indicator-label">MACD</div>
                <div className="indicator-value">
                  {typeof indicators.macd === 'number' ? indicators.macd.toFixed(2) : '--'}
                </div>
              </div>
              <div className="indicator-item">
                <div className="indicator-label">OBV</div>
                <div className="indicator-value">
                  {typeof indicators.obv === 'number' ? indicators.obv.toFixed(0) : '--'}
                </div>
              </div>
              <div className="indicator-item">
                <div className="indicator-label">Chaikin A/D</div>
                <div className="indicator-value">
                  {typeof indicators.ad_line === 'number' ? indicators.ad_line.toFixed(0) : '--'}
                </div>
              </div>
              <div className="indicator-item">
                <div className="indicator-label">Realized Vol</div>
                <div className="indicator-value">
                  {typeof indicators.realized_volatility === 'number'
                    ? `${(indicators.realized_volatility * 100).toFixed(2)}%`
                    : '--'}
                </div>
              </div>
            </div>
          </div>

          <div className="section">
            <h2>Volume Profile</h2>
            <div className="card">
              <div className="card-details">
                <div>
                  <span>Window:</span>
                  <span>{profileMeta.window}</span>
                </div>
                <div>
                  <span>POC:</span>
                  <span>{profileMeta.poc ? profileMeta.poc.toFixed(2) : '--'}</span>
                </div>
                <div>
                  <span>VAH:</span>
                  <span>{profileMeta.vah ? profileMeta.vah.toFixed(2) : '--'}</span>
                </div>
                <div>
                  <span>VAL:</span>
                  <span>{profileMeta.val ? profileMeta.val.toFixed(2) : '--'}</span>
                </div>
              </div>
            </div>
          </div>

          <div className="section">
            <h2>Key Levels ({levels.length})</h2>
            {levels.slice(0, 10).map((level, idx) => (
              <div className="card" key={idx}>
                <div className="card-header">
                  <span className="card-title">{level.price.toFixed(2)}</span>
                  <span className="badge">{level.type}</span>
                </div>
                <div className="card-details">
                  <div>
                    <span>Window:</span>
                    <span>{level.window}</span>
                  </div>
                </div>
              </div>
            ))}
          </div>

          <div className="section">
            <h2>Footprint 
              <button 
                onClick={() => setShowFootprint(!showFootprint)} 
                style={{fontSize: '12px', marginLeft: '8px', padding: '4px 8px', cursor: 'pointer'}}
              >
                {showFootprint ? 'Hide' : 'Show'}
              </button>
            </h2>
            {showFootprint && footprint && footprint.price_levels && (
              <div className="footprint-container">
                <div style={{fontSize: '11px', marginBottom: '8px', display: 'flex', gap: '12px'}}>
                  <div>Δ Total: {footprint.total_delta?.toFixed(0) || '0'}</div>
                  <div>Bid: {footprint.total_bid_vol?.toFixed(0) || '0'}</div>
                  <div>Ask: {footprint.total_ask_vol?.toFixed(0) || '0'}</div>
                </div>
                <div style={{maxHeight: '400px', overflowY: 'auto'}}>
                  <table style={{width: '100%', fontSize: '11px', borderCollapse: 'collapse'}}>
                    <thead>
                      <tr style={{borderBottom: '1px solid #30363d'}}>
                        <th style={{textAlign: 'right', padding: '4px'}}>Price</th>
                        <th style={{textAlign: 'right', padding: '4px'}}>Bid</th>
                        <th style={{textAlign: 'right', padding: '4px'}}>Ask</th>
                        <th style={{textAlign: 'right', padding: '4px'}}>Δ</th>
                      </tr>
                    </thead>
                    <tbody>
                      {footprint.price_levels.slice(0, 30).map((pl, i) => (
                        <tr key={i} style={{borderBottom: '1px solid #21262d'}}>
                          <td style={{textAlign: 'right', padding: '4px'}}>{pl.price.toFixed(2)}</td>
                          <td style={{textAlign: 'right', padding: '4px', color: '#3fb950'}}>{pl.bid_vol.toFixed(0)}</td>
                          <td style={{textAlign: 'right', padding: '4px', color: '#f85149'}}>{pl.ask_vol.toFixed(0)}</td>
                          <td style={{textAlign: 'right', padding: '4px', color: pl.delta > 0 ? '#3fb950' : '#f85149'}}>
                            {pl.delta > 0 ? '+' : ''}{pl.delta.toFixed(0)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

export default App
