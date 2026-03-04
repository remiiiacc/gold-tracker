// =============================================================================
// GOLD ANALYTICS — Market Structure Charts
// Included via <script src="/analytics.js"></script>
// Requires: Chart.js already loaded, quarterlyData already defined in index.html
// =============================================================================

// ---------------------------------------------------------------------------
// Global state
// ---------------------------------------------------------------------------
const analyticsState = {
  fred: null,   // { tips, gold, dxy, sofr, fetched_at }
  cot: null,    // { data: [...], fetched_at }
  yf: null,     // { gold_futures, gdx, gld, slv, silver_futures, fetched_at }
  status: null, // { fred, cot, yfinance }
  charts: {},
  // chart1r2 stored here as charts.chart1r2
  initialized: false,
  loading: false,
  chart1Mode: 'scatter',      // 'scatter' | 'timeseries'
  chart1Range: 0,
  chart2Range: 3,
  chart5Range: 5,
  chart6Range: 3,
  chart7Range: 5,
  chart7Unit: 'tonnes',       // 'tonnes' | 'usd'
};

// ---------------------------------------------------------------------------
// Main entry — called once when Market Structure tab is first activated
// ---------------------------------------------------------------------------
async function initMarketStructure() {
  if (analyticsState.initialized || analyticsState.loading) return;
  analyticsState.loading = true;

  try {
    await loadAnalyticsData();
    analyticsState.initialized = true;

    buildChart1();
    buildChart2();
    buildChart3();
    buildChart4();
    buildChart5();
    buildChart6();
    buildChart7();
    buildScorecard();

    // Wire up sub-nav scroll anchors
    document.querySelectorAll('.analytics-subnav-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        const target = document.getElementById(btn.dataset.target);
        if (target) target.scrollIntoView({ behavior: 'smooth', block: 'start' });
      });
    });

  } catch (err) {
    console.error('Analytics init error:', err);
    showAnalyticsError(err);
  } finally {
    analyticsState.loading = false;
  }
}

// ---------------------------------------------------------------------------
// Data loading
// ---------------------------------------------------------------------------
async function loadAnalyticsData() {
  setAllLoadingState(true);

  const [fredRes, cotRes, yfRes, statusRes] = await Promise.all([
    fetch('/api/fred').then(r => r.json()).catch(() => null),
    fetch('/api/cot').then(r => r.json()).catch(() => null),
    fetch('/api/yfinance').then(r => r.json()).catch(() => null),
    fetch('/api/status').then(r => r.json()).catch(() => null),
  ]);

  analyticsState.fred   = fredRes;
  analyticsState.cot    = cotRes;
  analyticsState.yf     = yfRes;
  analyticsState.status = statusRes;

  // Update last-fetched badges in HTML
  if (statusRes) {
    ['fred', 'cot', 'yfinance'].forEach(src => {
      const el = document.getElementById(`status-${src}`);
      if (!el) return;
      const s = statusRes[src];
      if (s) {
        const fresh = s.fresh ? 'Fresh' : `${Math.round(s.age_hours)}h old`;
        el.textContent = fresh;
        el.className = s.fresh ? 'data-freshness fresh' : 'data-freshness stale';
      }
    });
  }

  setAllLoadingState(false);
}

function setAllLoadingState(isLoading) {
  document.querySelectorAll('.chart-loading').forEach(el => {
    el.style.display = isLoading ? 'flex' : 'none';
  });
  document.querySelectorAll('.chart-canvas-wrap').forEach(el => {
    el.style.display = isLoading ? 'none' : 'block';
  });
}

function showAnalyticsError(err) {
  document.querySelectorAll('.chart-loading').forEach(el => {
    el.innerHTML = `<span style="color:var(--negative)">Error loading data. Check API server.</span>`;
    el.style.display = 'flex';
  });
}

// ---------------------------------------------------------------------------
// CHART 1 — Real Rate vs Gold
// ---------------------------------------------------------------------------
// CHART 1 — Real Rate vs Gold (Dual-Regime Regression)
// Primary view: scatter — two regime clusters + two OLS lines + pulse dot
// Secondary view: time series — dual-axis line + rolling 24M R² sub-chart
// ---------------------------------------------------------------------------

/** OLS regression returning Chart.js-ready line points and current-rate prediction.
 *  Wraps computeOLS(). xs = monthly TIPS yields, ys = monthly gold prices. */
function calculateRegression(xs, ys, currentX) {
  const ols = computeOLS(xs, ys);
  if (xs.length < 2) {
    return { slope: 0, intercept: 0, r2: 0, regressionLine: [], predictedAtCurrentRate: null };
  }
  const xMin = Math.min(...xs);
  const xMax = Math.max(...xs);
  return {
    slope:     ols.slope,
    intercept: ols.intercept,
    r2:        ols.r2,
    regressionLine: [
      { x: xMin, y: ols.slope * xMin + ols.intercept },
      { x: xMax, y: ols.slope * xMax + ols.intercept },
    ],
    predictedAtCurrentRate: currentX != null ? ols.slope * currentX + ols.intercept : null,
  };
}

/** Align monthly TIPS + monthly gold into sorted pair array. */
function getChart1Pairs() {
  const fred = analyticsState.fred;
  if (!fred || !fred.tips_monthly || !fred.gold_monthly) return null;
  const goldMap = buildDateMap(fred.gold_monthly);
  const pairs = [];
  (fred.tips_monthly || []).forEach(t => {
    const g = goldMap.get(t.date);
    if (g != null && t.value != null) {
      pairs.push({ date: t.date, tips: t.value, gold: g });
    }
  });
  pairs.sort((a, b) => a.date.localeCompare(b.date));
  return pairs.length >= 10 ? pairs : null;
}

function buildChart1() {
  if (analyticsState.chart1Mode === 'timeseries') {
    buildChart1Timeseries(analyticsState.chart1Range);
  } else {
    buildChart1Scatter();
  }
  updateChart1Stats();
}

function buildChart1Scatter() {
  const pairs = getChart1Pairs();
  if (!pairs) return;

  const regime1 = pairs.filter(p => p.date <= '2021-12-31');
  const regime2 = pairs.filter(p => p.date >= '2022-01-01');
  const latest  = pairs[pairs.length - 1];
  if (regime1.length < 2) return;

  const currentTIPS = latest ? latest.tips : null;
  const r1reg = calculateRegression(regime1.map(p => p.tips), regime1.map(p => p.gold), currentTIPS);
  const r2reg = regime2.length >= 5
    ? calculateRegression(regime2.map(p => p.tips), regime2.map(p => p.gold), currentTIPS)
    : null;

  // X axis extent
  const allTips = pairs.map(p => p.tips);
  const xMin = Math.min(...allTips) - 0.25;
  const xMax = Math.max(...allTips) + 0.25;

  // Regime 1 solid line (within its data range)
  const r1XMin = Math.min(...regime1.map(p => p.tips));
  const r1XMax = Math.max(...regime1.map(p => p.tips));
  const r1Solid = [
    { x: r1XMin, y: r1reg.slope * r1XMin + r1reg.intercept },
    { x: r1XMax, y: r1reg.slope * r1XMax + r1reg.intercept },
  ];
  // Regime 1 dashed extension — extrapolated into today's TIPS range
  const extEnd = currentTIPS != null ? Math.max(currentTIPS + 0.1, r1XMax) : xMax;
  const r1Ext = extEnd > r1XMax ? [
    { x: r1XMax, y: r1reg.slope * r1XMax + r1reg.intercept },
    { x: extEnd, y: r1reg.slope * extEnd  + r1reg.intercept },
  ] : [];

  // Regime 2 line
  const r2XMin = regime2.length ? Math.min(...regime2.map(p => p.tips)) : null;
  const r2XMax = regime2.length ? Math.max(...regime2.map(p => p.tips)) : null;
  const r2Solid = r2reg && r2XMin != null ? [
    { x: r2XMin, y: r2reg.slope * r2XMin + r2reg.intercept },
    { x: r2XMax, y: r2reg.slope * r2XMax + r2reg.intercept },
  ] : [];

  // Regime break premium
  const r1Pred  = r1reg.predictedAtCurrentRate;
  const premium = latest && r1Pred != null ? latest.gold - r1Pred : null;
  const premPct = premium != null && r1Pred ? (premium / r1Pred) * 100 : null;
  const yDataMax = Math.max(...pairs.map(p => p.gold));

  const isMobile = window.innerWidth < 768;

  // Annotations (desktop only — canvas annotations don't work well on small screens)
  const annotations = {};
  if (!isMobile) {
    if (latest) {
      annotations.currentLabel = {
        type: 'label',
        xValue: latest.tips,
        yValue: latest.gold,
        xAdjust: 14,
        yAdjust: -18,
        content: [`Now: $${Math.round(latest.gold).toLocaleString()}`],
        font: { size: 10, family: "'JetBrains Mono', monospace", weight: 'bold' },
        color: '#E74C3C',
        backgroundColor: 'transparent',
        textAlign: 'left',
      };
    }
    annotations.r1Label = {
      type: 'label',
      xValue: r1XMin,
      yValue: r1reg.slope * r1XMin + r1reg.intercept,
      xAdjust: 8,
      yAdjust: -30,
      content: [
        `2006\u20132021  R\u00B2=${r1reg.r2.toFixed(2)}`,
        `Predicted today: $${Math.round(r1Pred ?? 0).toLocaleString()}`,
      ],
      font: { size: 9, family: "'JetBrains Mono', monospace" },
      color: '#4A90D9',
      backgroundColor: 'rgba(15,20,25,0.88)',
      borderColor: 'rgba(74,144,217,0.45)',
      borderWidth: 1,
      borderRadius: 3,
      padding: 4,
      textAlign: 'left',
    };
    if (r2reg) {
      annotations.r2Label = {
        type: 'label',
        xValue: r2XMin,
        yValue: r2reg.slope * r2XMin + r2reg.intercept,
        xAdjust: 8,
        yAdjust: 28,
        content: [`2022\u2013Present  R\u00B2=${r2reg.r2.toFixed(2)}`],
        font: { size: 9, family: "'JetBrains Mono', monospace" },
        color: '#F5A623',
        backgroundColor: 'rgba(15,20,25,0.88)',
        borderColor: 'rgba(245,166,35,0.45)',
        borderWidth: 1,
        borderRadius: 3,
        padding: 4,
        textAlign: 'left',
      };
    }
    if (premium != null) {
      annotations.premiumBox = {
        type: 'label',
        xValue: xMin,
        yValue: yDataMax,
        xAdjust: 10,
        yAdjust: -8,
        content: [
          'REGIME BREAK PREMIUM',
          `$${Math.round(premium).toLocaleString()}`,
          `${Math.round(premPct ?? 0)}% above old model`,
          '\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500',
          'Portion unexplained by',
          'pre-2022 rate model',
        ],
        font: { size: 9, family: "'JetBrains Mono', monospace" },
        color: '#e6edf3',
        backgroundColor: 'rgba(21,27,35,0.94)',
        borderColor: '#d29922',
        borderWidth: 1,
        borderRadius: 4,
        padding: { top: 6, bottom: 6, left: 8, right: 8 },
        textAlign: 'left',
        position: 'start',
      };
    }
  }

  destroyChart('chart1');
  destroyChart('chart1r2');

  const scatterEl = document.getElementById('chart1-scatter');
  if (!scatterEl) return;
  scatterEl.style.display = 'block';
  const tsEl = document.getElementById('chart1-timeseries');
  if (tsEl) tsEl.style.display = 'none';
  const r2subWrap = document.getElementById('chart1-r2sub-wrap');
  if (r2subWrap) r2subWrap.style.display = 'none';
  const pulseEl = document.getElementById('chart1-pulse');
  if (pulseEl) pulseEl.style.display = 'none';

  analyticsState.charts.chart1 = new Chart(scatterEl, {
    type: 'scatter',
    data: {
      datasets: [
        {
          label: 'Regime 1: 2006\u20132021',
          data: regime1.map(p => ({ x: p.tips, y: p.gold, date: p.date })),
          backgroundColor: 'rgba(74,144,217,0.6)',
          borderColor: 'transparent',
          pointRadius: isMobile ? 3 : 4,
          pointHoverRadius: 6,
        },
        {
          label: 'Regime 2: 2022\u2013Present',
          data: regime2.map(p => ({ x: p.tips, y: p.gold, date: p.date })),
          backgroundColor: 'rgba(245,166,35,0.8)',
          borderColor: 'transparent',
          pointRadius: isMobile ? 4 : 5,
          pointHoverRadius: 7,
        },
        {
          label: latest ? `Current: $${Math.round(latest.gold).toLocaleString()}` : 'Current',
          data: latest ? [{ x: latest.tips, y: latest.gold, date: latest.date }] : [],
          backgroundColor: '#E74C3C',
          borderColor: '#ffffff',
          borderWidth: 2,
          pointRadius: isMobile ? 6 : 8,
          pointHoverRadius: 10,
        },
        {
          label: `Regime 1 Model (R\u00B2=${r1reg.r2.toFixed(2)})`,
          type: 'line',
          data: r1Solid,
          borderColor: '#4A90D9',
          backgroundColor: 'transparent',
          borderWidth: 2,
          pointRadius: 0,
          tension: 0,
        },
        ...(r1Ext.length ? [{
          label: '_r1ext',
          type: 'line',
          data: r1Ext,
          borderColor: '#4A90D9',
          backgroundColor: 'transparent',
          borderWidth: 1.5,
          borderDash: [5, 4],
          pointRadius: 0,
          tension: 0,
        }] : []),
        ...(r2Solid.length && r2reg ? [{
          label: `Regime 2 Model (R\u00B2=${r2reg.r2.toFixed(2)})`,
          type: 'line',
          data: r2Solid,
          borderColor: '#F5A623',
          backgroundColor: 'transparent',
          borderWidth: 2,
          pointRadius: 0,
          tension: 0,
        }] : []),
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: {
        onComplete: () => {
          if (latest) positionPulseDot(analyticsState.charts.chart1, latest.tips, latest.gold);
        },
      },
      plugins: {
        legend: {
          display: true,
          position: 'top',
          labels: {
            boxWidth: 10,
            padding: 16,
            filter: item => !item.text.startsWith('_'),
          },
        },
        tooltip: {
          callbacks: {
            label: ctx => {
              const p = ctx.raw;
              if (!p || p.date == null) {
                return `TIPS ${p?.x?.toFixed(2) ?? ''}%, Gold $${p?.y != null ? Math.round(p.y).toLocaleString() : '\u2014'}`;
              }
              const d = new Date(p.date + 'T00:00:00');
              const monthStr = d.toLocaleString('default', { month: 'short', year: 'numeric' });
              const regime = p.date >= '2022-01-01' ? '2 (2022\u2013Present)' : '1 (2006\u20132021)';
              const r1pred = r1reg ? Math.round(r1reg.slope * p.x + r1reg.intercept) : null;
              const prem   = r1pred != null ? Math.round(p.y - r1pred) : null;
              const lines  = [
                `Date: ${monthStr}`,
                `Gold: $${Math.round(p.y).toLocaleString()}`,
                `TIPS: ${p.x.toFixed(2)}%`,
                `Regime: ${regime}`,
              ];
              if (r1pred != null) {
                lines.push(`Old Model: $${r1pred.toLocaleString()}`);
                const pPct = r1pred ? Math.round(prem / r1pred * 100) : 0;
                lines.push(`Premium vs old model: $${prem.toLocaleString()} (${pPct}%)`);
              }
              return lines;
            },
          },
        },
        annotation: isMobile ? {} : { annotations },
      },
      scales: {
        x: {
          type: 'linear',
          min: xMin,
          max: xMax,
          grid: { color: '#2d3a4a' },
          title: { display: true, text: '10-Year Real Yield (TIPS) %', color: '#8b949e' },
          ticks: { callback: v => `${v.toFixed(1)}%`, stepSize: 0.5 },
        },
        y: {
          grid: { color: '#2d3a4a' },
          title: { display: !isMobile, text: 'Gold Price (USD/oz)', color: '#8b949e' },
          ticks: { callback: v => `$${v.toLocaleString()}` },
        },
      },
    },
  });
}

/** Overlay a CSS-animated pulse ring on the "current" data point. */
function positionPulseDot(chartInstance, xVal, yVal) {
  if (!chartInstance || !chartInstance.scales) return;
  try {
    const canvas = chartInstance.canvas;
    const wrap   = canvas.parentElement;
    if (!wrap) return;
    const px = chartInstance.scales.x.getPixelForValue(xVal);
    const py = chartInstance.scales.y.getPixelForValue(yVal);
    let pulse = document.getElementById('chart1-pulse');
    if (!pulse) {
      pulse = document.createElement('div');
      pulse.id = 'chart1-pulse';
      pulse.className = 'chart1-pulse-dot';
      wrap.appendChild(pulse);
    }
    pulse.style.left    = `${px}px`;
    pulse.style.top     = `${py}px`;
    pulse.style.display = 'block';
  } catch (_) {}
}

function buildChart1Timeseries(years) {
  let pairs = getChart1Pairs();
  if (!pairs) return;
  pairs = filterByRange(pairs, years);
  if (pairs.length < 4) return;

  const labels   = pairs.map(p => p.date);
  const goldVals = pairs.map(p => p.gold);
  const tipsVals = pairs.map(p => p.tips);

  destroyChart('chart1');
  const ctx = document.getElementById('chart1-timeseries');
  if (!ctx) return;
  ctx.style.display = 'block';
  const scatterEl = document.getElementById('chart1-scatter');
  if (scatterEl) scatterEl.style.display = 'none';
  const r2subWrap = document.getElementById('chart1-r2sub-wrap');
  if (r2subWrap) r2subWrap.style.display = 'block';
  const pulseEl = document.getElementById('chart1-pulse');
  if (pulseEl) pulseEl.style.display = 'none';

  const showBreak = labels.some(d => d < '2022-01-01') && labels.some(d => d >= '2022-01-01');

  analyticsState.charts.chart1 = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label: 'Gold ($/oz)',
          data: goldVals,
          borderColor: '#d29922',
          backgroundColor: 'rgba(210,153,34,0.08)',
          fill: true,
          tension: 0.3,
          pointRadius: 0,
          borderWidth: 2,
          yAxisID: 'y',
        },
        {
          label: 'TIPS Yield (%)',
          data: tipsVals,
          borderColor: '#4A90D9',
          backgroundColor: 'transparent',
          tension: 0.3,
          pointRadius: 0,
          borderWidth: 1.5,
          yAxisID: 'y1',
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: true, position: 'top', labels: { boxWidth: 10, padding: 16 } },
        tooltip: {
          callbacks: {
            label: ctx => ctx.datasetIndex === 0
              ? `Gold: $${Math.round(ctx.raw).toLocaleString()}`
              : `TIPS: ${ctx.raw.toFixed(2)}%`,
          },
        },
        annotation: {
          annotations: {
            ...(showBreak ? {
              regimeBreakLine: {
                type: 'line',
                xMin: '2022-01-01',
                xMax: '2022-01-01',
                borderColor: 'rgba(248,81,73,0.7)',
                borderWidth: 1.5,
                borderDash: [6, 4],
                label: {
                  display: true,
                  content: 'Regime Break',
                  position: 'start',
                  yAdjust: -6,
                  color: '#f85149',
                  backgroundColor: 'rgba(15,20,25,0.88)',
                  font: { size: 9, family: "'JetBrains Mono', monospace" },
                  padding: 3,
                },
              },
              regime2Shade: {
                type: 'box',
                xMin: '2022-01-01',
                backgroundColor: 'rgba(210,153,34,0.04)',
                borderWidth: 0,
                drawTime: 'beforeDatasetsDraw',
              },
            } : {}),
          },
        },
      },
      scales: {
        y: {
          type: 'linear',
          position: 'left',
          grid: { color: '#2d3a4a' },
          ticks: { callback: v => `$${v.toLocaleString()}` },
        },
        y1: {
          type: 'linear',
          position: 'right',
          reverse: true,
          grid: { display: false },
          ticks: { callback: v => `${v.toFixed(1)}%` },
        },
        x: {
          grid: { display: false },
          ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 8 },
        },
      },
    },
  });

  buildChart1R2Sub(pairs);
}

/** Rolling 24-month R² between TIPS and gold (shown only in time series mode). */
function buildChart1R2Sub(pairs) {
  destroyChart('chart1r2');
  const ctx = document.getElementById('chart1-r2sub');
  if (!ctx) return;

  const WINDOW = 24;
  const r2Vals = new Array(pairs.length).fill(null);
  for (let i = WINDOW - 1; i < pairs.length; i++) {
    const slice = pairs.slice(i - WINDOW + 1, i + 1);
    const ols = computeOLS(slice.map(p => p.tips), slice.map(p => p.gold));
    r2Vals[i] = Math.max(0, Math.min(1, ols.r2));
  }

  const showBreak = pairs.some(p => p.date < '2022-01-01') && pairs.some(p => p.date >= '2022-01-01');

  analyticsState.charts.chart1r2 = new Chart(ctx, {
    type: 'line',
    data: {
      labels: pairs.map(p => p.date),
      datasets: [{
        label: 'Rolling 24M R\u00B2',
        data: r2Vals,
        borderColor: '#58a6ff',
        backgroundColor: 'rgba(88,166,255,0.10)',
        fill: true,
        tension: 0.3,
        pointRadius: 0,
        borderWidth: 1.5,
        spanGaps: false,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: { label: ctx => `R\u00B2: ${ctx.raw != null ? ctx.raw.toFixed(3) : '\u2014'}` },
        },
        annotation: {
          annotations: {
            unreliableZone: {
              type: 'box',
              yMin: 0,
              yMax: 0.5,
              backgroundColor: 'rgba(210,153,34,0.07)',
              borderWidth: 0,
              drawTime: 'beforeDatasetsDraw',
            },
            threshold: {
              type: 'line',
              yMin: 0.5,
              yMax: 0.5,
              borderColor: '#d29922',
              borderWidth: 1,
              borderDash: [4, 3],
              label: {
                display: true,
                content: 'Relationship Threshold (0.5)',
                position: 'start',
                color: '#d29922',
                backgroundColor: 'rgba(15,20,25,0.82)',
                font: { size: 9, family: "'JetBrains Mono', monospace" },
                padding: 3,
              },
            },
            ...(showBreak ? {
              r2BreakLine: {
                type: 'line',
                xMin: '2022-01-01',
                xMax: '2022-01-01',
                borderColor: 'rgba(248,81,73,0.5)',
                borderWidth: 1,
                borderDash: [4, 3],
              },
            } : {}),
          },
        },
      },
      scales: {
        y: {
          min: 0,
          max: 1,
          grid: { color: '#2d3a4a' },
          title: { display: true, text: 'R\u00B2 (24-month rolling)', color: '#8b949e', font: { size: 10 } },
          ticks: { callback: v => v.toFixed(1), stepSize: 0.25 },
        },
        x: {
          grid: { display: false },
          ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 8 },
        },
      },
    },
  });
}

function updateChart1Range(years) {
  analyticsState.chart1Range = years;
  document.querySelectorAll('.c1-range-btn').forEach(b => {
    b.classList.toggle('active', parseInt(b.dataset.years) === years);
  });
  if (analyticsState.chart1Mode === 'timeseries') buildChart1Timeseries(years);
  updateChart1Stats();
}

function toggleChart1Mode() {
  analyticsState.chart1Mode = analyticsState.chart1Mode === 'timeseries' ? 'scatter' : 'timeseries';
  const btn = document.getElementById('chart1-toggle-btn');
  if (btn) btn.textContent = analyticsState.chart1Mode === 'timeseries' ? 'Scatter View' : 'Time Series';
  buildChart1();
}

function updateChart1Stats() {
  const pairs = getChart1Pairs();
  if (!pairs || pairs.length < 10) return;

  const regime1 = pairs.filter(p => p.date <= '2021-12-31');
  const regime2 = pairs.filter(p => p.date >= '2022-01-01');
  const latest  = pairs[pairs.length - 1];

  const r1ols = regime1.length >= 2 ? computeOLS(regime1.map(p => p.tips), regime1.map(p => p.gold)) : null;
  const r2ols = regime2.length >= 2 ? computeOLS(regime2.map(p => p.tips), regime2.map(p => p.gold)) : null;

  const currentTIPS = latest.tips;
  const currentGold = latest.gold;
  const r1pred  = r1ols ? r1ols.slope * currentTIPS + r1ols.intercept : null;
  const premium = r1pred != null ? currentGold - r1pred : null;
  const premPct = r1pred != null && r1pred !== 0 ? (premium / r1pred) * 100 : null;

  if (r1ols) setStatCard('c1-r1-r2', `R\u00B2 = ${r1ols.r2.toFixed(3)}`);
  if (r2ols) setStatCard('c1-r2-r2', `R\u00B2 = ${r2ols.r2.toFixed(3)}`);
  if (r1pred != null) {
    setStatCard('c1-model', `$${Math.round(r1pred).toLocaleString()}`);
    const modelSub = document.getElementById('c1-model-sub');
    if (modelSub) modelSub.textContent = `At TIPS ${currentTIPS.toFixed(2)}%`;
  }
  if (premium != null) {
    const cls = premPct != null && premPct > 50 ? 'negative' : 'positive';
    setStatCard('c1-premium', `$${Math.round(premium).toLocaleString()}`, cls);
    const premSub = document.getElementById('c1-premium-sub');
    if (premSub && premPct != null) premSub.textContent = `${Math.round(premPct)}% above old model`;
  }
  if (currentTIPS != null) {
    const tipsCls = currentTIPS > 1.5 ? 'negative' : currentTIPS < 0.5 ? 'positive' : 'neutral';
    setStatCard('c1-tips', `${currentTIPS.toFixed(2)}%`, tipsCls);
  }
}


// ---------------------------------------------------------------------------
// CHART 2 — DXY vs Gold (dual-axis + rolling 90-day correlation sub-chart)
// ---------------------------------------------------------------------------
function buildChart2() {
  const range = analyticsState.chart2Range;
  const fred = analyticsState.fred;
  if (!fred) return;

  const goldMap = buildDateMap(fred.gold);
  const dxyMap  = buildDateMap(fred.dxy);

  // Align on common dates
  let pairs = [];
  fred.gold.forEach(g => {
    const d = dxyMap.get(g.date);
    if (d != null && g.value != null) pairs.push({ date: g.date, gold: g.value, dxy: d });
  });
  pairs.sort((a, b) => a.date.localeCompare(b.date));
  pairs = filterByRange(pairs, range);

  const labels   = pairs.map(p => p.date);
  const goldVals = pairs.map(p => p.gold);
  const dxyVals  = pairs.map(p => p.dxy);

  // Rolling 90-day correlation
  const corrVals = rollingCorrelation(goldVals, dxyVals, 90);

  // Build decoupling background plugin (shading when |corr| < 0.1 or corr > 0)
  const decouplingSegments = [];
  for (let i = 1; i < corrVals.length; i++) {
    if (corrVals[i] !== null && corrVals[i] > -0.1) {
      decouplingSegments.push(i);
    }
  }

  destroyChart('chart2main');
  destroyChart('chart2corr');

  // Main chart
  const ctxMain = document.getElementById('chart2-main');
  if (ctxMain) {
    analyticsState.charts.chart2main = new Chart(ctxMain, {
      type: 'line',
      data: {
        labels,
        datasets: [
          {
            label: 'Gold ($/oz)',
            data: goldVals,
            borderColor: '#d29922',
            backgroundColor: 'rgba(210,153,34,0.08)',
            fill: true,
            tension: 0.3,
            pointRadius: 0,
            borderWidth: 2,
            yAxisID: 'y',
          },
          {
            label: 'DXY',
            data: dxyVals,
            borderColor: '#79c0ff',
            backgroundColor: 'transparent',
            tension: 0.3,
            pointRadius: 0,
            borderWidth: 1.5,
            yAxisID: 'y1',
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: { display: true, position: 'top', labels: { boxWidth: 10, padding: 16 } },
        },
        scales: {
          y:  { type: 'linear', position: 'left',  grid: { color: '#2d3a4a' }, ticks: { callback: v => `$${v.toLocaleString()}` } },
          y1: { type: 'linear', position: 'right', reverse: true, grid: { display: false }, ticks: { callback: v => v.toFixed(1) } },
          x:  { grid: { display: false }, ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 8 } },
        },
      },
    });
  }

  // Correlation sub-chart
  const ctxCorr = document.getElementById('chart2-corr');
  if (ctxCorr) {
    analyticsState.charts.chart2corr = new Chart(ctxCorr, {
      type: 'line',
      data: {
        labels,
        datasets: [{
          label: '90d Rolling Correlation (Gold vs DXY)',
          data: corrVals,
          borderColor: corrVals.map(v => v === null ? 'transparent' : v > 0 ? '#f85149' : '#58a6ff'),
          segment: {
            borderColor: ctx => {
              const v = ctx.p1.parsed.y;
              if (v === null) return 'transparent';
              return v > 0 ? '#f85149' : '#58a6ff';
            },
          },
          backgroundColor: 'transparent',
          tension: 0.3,
          pointRadius: 0,
          borderWidth: 1.5,
          fill: {
            target: { value: 0 },
            above: 'rgba(248,81,73,0.12)',
            below: 'rgba(88,166,255,0.10)',
          },
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: { label: ctx => `Corr: ${ctx.raw !== null ? ctx.raw.toFixed(2) : 'N/A'}` } },
          annotation: {
            annotations: {
              zeroLine: {
                type: 'line',
                yMin: 0, yMax: 0,
                borderColor: '#6e7681',
                borderWidth: 1,
                borderDash: [4, 3],
              },
            },
          },
        },
        scales: {
          y: {
            min: -1, max: 1,
            grid: { color: '#2d3a4a' },
            ticks: { callback: v => v.toFixed(1) },
          },
          x: { grid: { display: false }, ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 8 } },
        },
      },
    });
  }

  // Stat cards
  if (pairs.length >= 2) {
    const latestGold = goldVals[goldVals.length - 1];
    const priorGold  = goldVals[Math.max(0, goldVals.length - 91)];
    const latestDxy  = dxyVals[dxyVals.length - 1];
    const priorDxy   = dxyVals[Math.max(0, dxyVals.length - 91)];
    const goldChg    = ((latestGold - priorGold) / priorGold * 100);
    const dxyChg     = ((latestDxy  - priorDxy)  / priorDxy  * 100);
    const latestCorr = corrVals.filter(v => v !== null).slice(-1)[0] || 0;

    setStatCard('c2-gold-chg',  `${goldChg >= 0 ? '+' : ''}${goldChg.toFixed(1)}%`, goldChg >= 0 ? 'positive' : 'negative');
    setStatCard('c2-dxy-chg',   `${dxyChg  >= 0 ? '+' : ''}${dxyChg.toFixed(1)}%`,  dxyChg  >= 0 ? 'positive' : 'negative');
    setStatCard('c2-corr',      latestCorr.toFixed(2), latestCorr > 0.1 ? 'negative' : 'positive');
    setStatCard('c2-decouple',  latestCorr < 0.1 ? 'YES' : 'No', latestCorr < 0.1 ? 'positive' : 'neutral');
  }
}

function updateChart2Range(years) {
  analyticsState.chart2Range = years;
  document.querySelectorAll('.c2-range-btn').forEach(b => {
    b.classList.toggle('active', parseInt(b.dataset.years) === years);
  });
  buildChart2();
}

// ---------------------------------------------------------------------------
// CHART 3 — CFTC COT (net longs as bars, gold price as line, percentile lines)
// ---------------------------------------------------------------------------
function buildChart3() {
  const cot  = analyticsState.cot;
  const fred = analyticsState.fred;
  if (!cot || !cot.data) return;

  const data = [...cot.data].sort((a, b) => a.date.localeCompare(b.date));
  const netLongs = data.map(d => d.net_long);
  const dates    = data.map(d => d.date);

  // Build gold price series aligned by date
  const goldMap = fred ? buildDateMap(fred.gold) : new Map();
  const goldVals = dates.map(d => goldMap.get(d) || null);
  // Forward-fill gold price for alignment
  let lastG = null;
  const goldFilled = goldVals.map(v => { if (v !== null) lastG = v; return lastG; });

  // Percentile lines (80th and 20th over full history)
  const sorted = [...netLongs].filter(v => v != null).sort((a, b) => a - b);
  const p80 = sorted[Math.floor(sorted.length * 0.8)];
  const p20 = sorted[Math.floor(sorted.length * 0.2)];

  // Color bars by their own percentile level
  const barColors = netLongs.map(v => {
    const pct = computePercentile(sorted, v);
    if (pct >= 80) return 'rgba(248,81,73,0.75)';
    if (pct <= 20) return 'rgba(63,185,80,0.75)';
    return 'rgba(88,166,255,0.7)';
  });

  const currentNetLong = netLongs[netLongs.length - 1];
  const currentPct     = computePercentile(sorted, currentNetLong);
  const badge          = currentPct >= 80 ? 'crowded' : currentPct <= 20 ? 'washed' : 'neutral';

  // Update badge
  const badgeEl = document.getElementById('cot-badge');
  if (badgeEl) {
    if (badge === 'crowded') {
      badgeEl.textContent = 'Crowded Long';
      badgeEl.className = 'badge warning';
    } else if (badge === 'washed') {
      badgeEl.textContent = 'Washed Out';
      badgeEl.className = 'badge opportunity';
    } else {
      badgeEl.textContent = '';
      badgeEl.className = 'badge';
    }
  }

  destroyChart('chart3');
  const ctx = document.getElementById('chart3');
  if (!ctx) return;

  analyticsState.charts.chart3 = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: dates,
      datasets: [
        {
          type: 'bar',
          label: 'Net Long (contracts)',
          data: netLongs,
          backgroundColor: barColors,
          borderColor: barColors.map(c => c.replace('0.7', '1').replace('0.75', '1')),
          borderWidth: 1,
          borderRadius: 1,
          yAxisID: 'y',
        },
        {
          type: 'line',
          label: 'Gold Price ($/oz)',
          data: goldFilled,
          borderColor: '#d29922',
          backgroundColor: 'transparent',
          tension: 0.3,
          pointRadius: 0,
          borderWidth: 1.5,
          yAxisID: 'y1',
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: true, position: 'top', labels: { boxWidth: 10, padding: 16 } },
        annotation: {
          annotations: {
            p80Line: {
              type: 'line', yMin: p80, yMax: p80, yScaleID: 'y',
              borderColor: 'rgba(248,81,73,0.5)', borderWidth: 1, borderDash: [5, 4],
              label: { content: '80th pct', display: true, position: 'end', color: '#f85149', font: { size: 10 } },
            },
            p20Line: {
              type: 'line', yMin: p20, yMax: p20, yScaleID: 'y',
              borderColor: 'rgba(63,185,80,0.5)', borderWidth: 1, borderDash: [5, 4],
              label: { content: '20th pct', display: true, position: 'end', color: '#3fb950', font: { size: 10 } },
            },
          },
        },
      },
      scales: {
        y:  { type: 'linear', position: 'left',  grid: { color: '#2d3a4a' }, ticks: { callback: v => v.toLocaleString() } },
        y1: { type: 'linear', position: 'right', grid: { display: false },   ticks: { callback: v => `$${v.toLocaleString()}` } },
        x:  { grid: { display: false }, ticks: { maxRotation: 45, autoSkip: true, maxTicksLimit: 12 } },
      },
    },
  });

  setStatCard('c3-pct', `${Math.round(currentPct)}th %ile`);
  setStatCard('c3-net', currentNetLong != null ? currentNetLong.toLocaleString() : '—');
}

// ---------------------------------------------------------------------------
// CHART 4 — Gold Cobasis (spot - futures)
// ---------------------------------------------------------------------------
function buildChart4() {
  const yf = analyticsState.yf;
  if (!yf || !yf.gold_futures || !yf.gld) return;

  // Spot price = GLD ETF price / 0.09334 oz-per-share
  // Cobasis = Spot - Front-month Futures; positive = backwardation
  const GLD_OZ = 0.09334;
  const futMap = buildDateMap(yf.gold_futures, 'close');
  const gldMap = buildDateMap(yf.gld, 'close');

  let pairs = [];
  (yf.gld || []).forEach(g => {
    const fut = futMap.get(g.date);
    if (fut == null || g.close == null) return;
    const spot = g.close / GLD_OZ;
    pairs.push({ date: g.date, cobasis: spot - fut });
  });
  pairs.sort((a, b) => a.date.localeCompare(b.date));

  const labels   = pairs.map(p => p.date);
  const cobasis  = pairs.map(p => p.cobasis);
  const current  = cobasis[cobasis.length - 1];
  const state    = current > 0.5 ? 'Backwardation' : current < -2 ? 'Deep Contango' : current < 0 ? 'Contango' : 'Neutral';

  destroyChart('chart4');
  const ctx = document.getElementById('chart4');
  if (!ctx) return;

  analyticsState.charts.chart4 = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: 'Gold Cobasis (Spot − Futures, $/oz)',
        data: cobasis,
        borderColor: cobasis.map(v => v >= 0 ? '#d29922' : '#8b949e'),
        segment: {
          borderColor: ctx => ctx.p1.parsed.y >= 0 ? '#d29922' : '#8b949e',
        },
        backgroundColor: 'transparent',
        tension: 0.3,
        pointRadius: 0,
        borderWidth: 1.5,
        fill: false,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        annotation: {
          annotations: {
            zeroLine: {
              type: 'line', yMin: 0, yMax: 0,
              borderColor: '#6e7681', borderWidth: 1, borderDash: [4, 3],
            },
          },
        },
      },
      scales: {
        y: {
          grid: { color: '#2d3a4a' },
          ticks: { callback: v => `$${v.toFixed(1)}` },
        },
        x: { grid: { display: false }, ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 8 } },
      },
    },
  });

  setStatCard('c4-cobasis', current != null ? `$${current.toFixed(2)}` : '—');
  setStatCard('c4-state',   state, state === 'Backwardation' ? 'positive' : state.includes('Contango') ? 'negative' : 'neutral');
}

// ---------------------------------------------------------------------------
// CHART 5 — Gold/Silver Ratio with rolling mean ±1 SD bands
// ---------------------------------------------------------------------------
function buildChart5() {
  const range = analyticsState.chart5Range;
  const yf    = analyticsState.yf;
  if (!yf) return;

  // Use USD/oz futures prices: GLD/SLV are ETF share prices, not spot gold/silver prices
  const silvFutMap = buildDateMap(yf.silver_futures, 'close');
  let pairs = [];
  (yf.gold_futures || []).forEach(g => {
    const s = silvFutMap.get(g.date);
    if (s != null && g.close != null && s > 0) {
      pairs.push({ date: g.date, ratio: g.close / s });
    }
  });
  pairs.sort((a, b) => a.date.localeCompare(b.date));

  // Full history for percentile calc
  const allRatios = pairs.map(p => p.ratio);
  const currentRatio = allRatios[allRatios.length - 1];
  const currentPct   = computePercentile([...allRatios].sort((a, b) => a - b), currentRatio);

  pairs = filterByRange(pairs, range);
  const labels = pairs.map(p => p.date);
  const ratios = pairs.map(p => p.ratio);

  // Rolling mean and SD (60-day window)
  const win = 60;
  const means = [], upper = [], lower = [];
  for (let i = 0; i < ratios.length; i++) {
    const slice = ratios.slice(Math.max(0, i - win + 1), i + 1);
    const m   = slice.reduce((a, v) => a + v, 0) / slice.length;
    const sd  = Math.sqrt(slice.reduce((a, v) => a + (v - m) ** 2, 0) / slice.length);
    means.push(m);
    upper.push(m + sd);
    lower.push(m - sd);
  }

  destroyChart('chart5');
  const ctx = document.getElementById('chart5');
  if (!ctx) return;

  analyticsState.charts.chart5 = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label: 'Upper SD Band',
          data: upper,
          borderColor: 'transparent',
          backgroundColor: 'rgba(88,166,255,0.07)',
          fill: '+1',
          pointRadius: 0,
          tension: 0.3,
        },
        {
          label: 'Rolling Mean',
          data: means,
          borderColor: '#58a6ff',
          backgroundColor: 'transparent',
          borderWidth: 1,
          borderDash: [4, 3],
          pointRadius: 0,
          tension: 0.3,
        },
        {
          label: 'Lower SD Band',
          data: lower,
          borderColor: 'transparent',
          backgroundColor: 'rgba(88,166,255,0.07)',
          fill: false,
          pointRadius: 0,
          tension: 0.3,
        },
        {
          label: 'Gold/Silver Ratio',
          data: ratios,
          borderColor: '#d29922',
          backgroundColor: 'transparent',
          borderWidth: 2,
          pointRadius: 0,
          tension: 0.3,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: true, position: 'top', labels: { boxWidth: 10, padding: 16 } },
      },
      scales: {
        y: {
          grid: { color: '#2d3a4a' },
          ticks: { callback: v => v.toFixed(1) },
        },
        x: { grid: { display: false }, ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 8 } },
      },
    },
  });

  setStatCard('c5-ratio', currentRatio != null ? currentRatio.toFixed(1) : '—');
  setStatCard('c5-pct',   `${Math.round(currentPct)}th %ile`);

  // Late-cycle note card
  const noteEl = document.getElementById('c5-note');
  if (noteEl) {
    noteEl.style.display = currentRatio > 80 ? 'flex' : 'none';
  }
}

function updateChart5Range(years) {
  analyticsState.chart5Range = years;
  document.querySelectorAll('.c5-range-btn').forEach(b => {
    b.classList.toggle('active', parseInt(b.dataset.years) === years);
  });
  buildChart5();
}

// ---------------------------------------------------------------------------
// CHART 6 — GDX/GLD Ratio + rolling 90-day beta sub-chart
// ---------------------------------------------------------------------------
function buildChart6() {
  const range = analyticsState.chart6Range;
  const yf    = analyticsState.yf;
  if (!yf) return;

  const gldMap = buildDateMap(yf.gld, 'close');
  let pairs = [];
  yf.gdx.forEach(g => {
    const gld = gldMap.get(g.date);
    if (gld != null && g.close != null && gld > 0) {
      pairs.push({ date: g.date, ratio: g.close / gld, gdx: g.close, gld });
    }
  });
  pairs.sort((a, b) => a.date.localeCompare(b.date));
  pairs = filterByRange(pairs, range);

  const labels    = pairs.map(p => p.date);
  const ratios    = pairs.map(p => p.ratio);
  const gldVals   = pairs.map(p => p.gld);

  // 30-day changes for divergence detection
  const divergenceColors = labels.map((_, i) => {
    if (i < 30) return 'transparent';
    const dRatio = ratios[i] - ratios[i - 30];
    const dGold  = gldVals[i] - gldVals[i - 30];
    if (dRatio < 0 && dGold > 0) return 'rgba(248,81,73,0.08)';  // miner divergence
    if (dRatio > 0 && dGold > 0) return 'rgba(63,185,80,0.08)';  // confirmation
    return 'transparent';
  });

  // Rolling 90-day beta
  const gdxReturns = pairs.map((p, i) => i === 0 ? 0 : (p.gdx  - pairs[i-1].gdx)  / pairs[i-1].gdx);
  const gldReturns = pairs.map((p, i) => i === 0 ? 0 : (p.gld  - pairs[i-1].gld)  / pairs[i-1].gld);
  const betaVals   = [];
  for (let i = 0; i < gdxReturns.length; i++) {
    if (i < 90) { betaVals.push(null); continue; }
    const ols = computeOLS(gldReturns.slice(i - 90, i), gdxReturns.slice(i - 90, i));
    betaVals.push(ols.slope);
  }

  // Detect current divergence
  const lastIdx  = pairs.length - 1;
  const dRatio30 = lastIdx >= 30 ? ratios[lastIdx] - ratios[lastIdx - 30] : 0;
  const dGold30  = lastIdx >= 30 ? gldVals[lastIdx] - gldVals[lastIdx - 30] : 0;
  const divBadge = document.getElementById('chart6-divergence');
  if (divBadge) {
    if (dRatio30 < 0 && dGold30 > 0) {
      divBadge.textContent = 'Miner Divergence';
      divBadge.className = 'badge warning';
      divBadge.style.display = 'inline-flex';
    } else {
      divBadge.style.display = 'none';
    }
  }

  // 3-year average for stat
  const allPairs  = pairs; // already range-filtered — re-fetch full for 3Y avg
  const avg3y     = ratios.length >= 252 * 3
    ? ratios.slice(-756).reduce((a, v) => a + v, 0) / 756
    : ratios.reduce((a, v) => a + v, 0) / (ratios.length || 1);
  const currentRatio = ratios[ratios.length - 1];

  destroyChart('chart6main');
  destroyChart('chart6beta');

  const ctxMain = document.getElementById('chart6-main');
  if (ctxMain) {
    analyticsState.charts.chart6main = new Chart(ctxMain, {
      type: 'line',
      data: {
        labels,
        datasets: [
          {
            label: 'GDX/GLD Ratio',
            data: ratios,
            borderColor: '#79c0ff',
            backgroundColor: 'rgba(121,192,255,0.06)',
            fill: true,
            tension: 0.3,
            pointRadius: 0,
            borderWidth: 2,
            yAxisID: 'y',
          },
          {
            label: 'GLD Price',
            data: gldVals,
            borderColor: '#d29922',
            backgroundColor: 'transparent',
            tension: 0.3,
            pointRadius: 0,
            borderWidth: 1.5,
            yAxisID: 'y1',
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: { display: true, position: 'top', labels: { boxWidth: 10, padding: 16 } },
        },
        scales: {
          y:  { type: 'linear', position: 'left',  grid: { color: '#2d3a4a' }, ticks: { callback: v => v.toFixed(3) } },
          y1: { type: 'linear', position: 'right', grid: { display: false },   ticks: { callback: v => `$${v.toFixed(0)}` } },
          x:  { grid: { display: false }, ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 8 } },
        },
      },
    });
  }

  const ctxBeta = document.getElementById('chart6-beta');
  if (ctxBeta) {
    analyticsState.charts.chart6beta = new Chart(ctxBeta, {
      type: 'line',
      data: {
        labels,
        datasets: [{
          label: '90d Rolling Beta (GDX vs GLD)',
          data: betaVals,
          borderColor: '#a855f7',
          backgroundColor: 'rgba(168,85,247,0.08)',
          fill: true,
          tension: 0.3,
          pointRadius: 0,
          borderWidth: 1.5,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          annotation: {
            annotations: {
              betaOne: {
                type: 'line', yMin: 1, yMax: 1,
                borderColor: 'rgba(139,148,158,0.5)', borderWidth: 1, borderDash: [4, 3],
                label: { content: 'Beta = 1', display: true, position: 'end', color: '#8b949e', font: { size: 10 } },
              },
            },
          },
        },
        scales: {
          y: { grid: { color: '#2d3a4a' }, ticks: { callback: v => v.toFixed(2) } },
          x: { grid: { display: false }, ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 8 } },
        },
      },
    });
  }

  setStatCard('c6-ratio',   currentRatio != null ? currentRatio.toFixed(3) : '—');
  setStatCard('c6-3yavg',   avg3y.toFixed(3));
  const vsBench = currentRatio - avg3y;
  setStatCard('c6-vsbench', `${vsBench >= 0 ? '+' : ''}${vsBench.toFixed(3)}`, vsBench >= 0 ? 'positive' : 'negative');
}

function updateChart6Range(years) {
  analyticsState.chart6Range = years;
  document.querySelectorAll('.c6-range-btn').forEach(b => {
    b.classList.toggle('active', parseInt(b.dataset.years) === years);
  });
  buildChart6();
}

// ---------------------------------------------------------------------------
// CHART 7 — Enhanced Demand Decomposition (stacked bar + gold price line)
// ---------------------------------------------------------------------------
function buildChart7() {
  const unit  = analyticsState.chart7Unit;
  const range = analyticsState.chart7Range; // in years → convert to quarters
  const maxQ  = range * 4;

  // Merge Q4-2024 if not already present
  if (!quarterlyData.netPurchases['2024-Q4']) quarterlyData.netPurchases['2024-Q4'] = 333;
  if (!quarterlyData.jewelry['2024-Q4'])      quarterlyData.jewelry['2024-Q4']      = 527;
  if (!quarterlyData.barCoin['2024-Q4'])       quarterlyData.barCoin['2024-Q4']       = 344;
  if (!quarterlyData.etfFlows['2024-Q4'])      quarterlyData.etfFlows['2024-Q4']      = 19;
  if (!quarterlyData.goldPrice['2024-Q4'])     quarterlyData.goldPrice['2024-Q4']     = 2663;

  const allQ = Object.keys(quarterlyData.jewelry).sort();
  const quarters = allQ.slice(-maxQ);
  const prices = quarters.map(q => quarterlyData.goldPrice[q] || null);

  const scale = (q, raw) => {
    if (unit === 'usd') {
      const p = quarterlyData.goldPrice[q];
      return p ? (raw * p) / 1000 : raw; // billions USD
    }
    return raw;
  };

  const cbVals      = quarters.map(q => scale(q, quarterlyData.netPurchases[q] || 0));
  const barCoinVals = quarters.map(q => scale(q, quarterlyData.barCoin[q] || 0));
  const jewVals     = quarters.map(q => scale(q, quarterlyData.jewelry[q] || 0));
  const etfPos      = quarters.map(q => { const v = scale(q, quarterlyData.etfFlows[q] || 0); return Math.max(0, v); });
  const etfNeg      = quarters.map(q => { const v = scale(q, quarterlyData.etfFlows[q] || 0); return Math.min(0, v); });

  // 4-quarter rolling investment (ETF + BarCoin)
  const totalInv = quarters.map(q => (quarterlyData.barCoin[q] || 0) + (quarterlyData.etfFlows[q] || 0));
  const sum4 = (arr, i) => arr.slice(Math.max(0, i - 3), i + 1).reduce((a, v) => a + v, 0);
  const rollingInv = totalInv.map((_, i) => sum4(totalInv, i));
  const lastRoll   = rollingInv[rollingInv.length - 1];
  const prevRoll   = rollingInv[Math.max(0, rollingInv.length - 5)];
  const momentum   = lastRoll > prevRoll ? 'Accelerating' : lastRoll < prevRoll ? 'Decelerating' : 'Flat';

  const yLabel = unit === 'usd' ? 'Billion USD' : 'Tonnes';

  destroyChart('chart7');
  const ctx = document.getElementById('chart7');
  if (!ctx) return;

  analyticsState.charts.chart7 = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: quarters,
      datasets: [
        {
          label: `Central Banks (${yLabel})`,
          data: cbVals,
          backgroundColor: 'rgba(210,153,34,0.75)',
          borderColor: '#d29922',
          borderWidth: 1,
          stack: 'positive',
        },
        {
          label: `Bar & Coin (${yLabel})`,
          data: barCoinVals,
          backgroundColor: 'rgba(88,166,255,0.75)',
          borderColor: '#58a6ff',
          borderWidth: 1,
          stack: 'positive',
        },
        {
          label: `Jewelry (${yLabel})`,
          data: jewVals,
          backgroundColor: 'rgba(236,72,153,0.65)',
          borderColor: '#ec4899',
          borderWidth: 1,
          stack: 'positive',
        },
        {
          label: `ETF Inflow (${yLabel})`,
          data: etfPos,
          backgroundColor: 'rgba(63,185,80,0.75)',
          borderColor: '#3fb950',
          borderWidth: 1,
          stack: 'positive',
        },
        {
          label: `ETF Outflow (${yLabel})`,
          data: etfNeg,
          backgroundColor: 'rgba(248,81,73,0.65)',
          borderColor: '#f85149',
          borderWidth: 1,
          stack: 'negative',
        },
        {
          type: 'line',
          label: 'Gold Price ($/oz)',
          data: prices,
          borderColor: '#d29922',
          backgroundColor: 'transparent',
          borderWidth: 2,
          borderDash: [4, 3],
          pointRadius: 0,
          tension: 0.3,
          yAxisID: 'y1',
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: true, position: 'top', labels: { boxWidth: 10, padding: 12, font: { size: 11 } } },
      },
      scales: {
        y: {
          stacked: true,
          grid: { color: '#2d3a4a' },
          ticks: { callback: v => unit === 'usd' ? `$${v.toFixed(0)}B` : `${v}t` },
        },
        y1: {
          type: 'linear',
          position: 'right',
          stacked: false,
          grid: { display: false },
          ticks: { callback: v => `$${v.toLocaleString()}` },
        },
        x: { stacked: true, grid: { display: false }, ticks: { maxRotation: 45, autoSkip: true, maxTicksLimit: 12 } },
      },
    },
  });

  const arrowEl = document.getElementById('c7-momentum-arrow');
  if (arrowEl) {
    arrowEl.textContent = momentum === 'Accelerating' ? '↑' : momentum === 'Decelerating' ? '↓' : '→';
    arrowEl.className   = momentum === 'Accelerating' ? 'positive' : momentum === 'Decelerating' ? 'negative' : '';
  }
  setStatCard('c7-momentum', momentum, momentum === 'Accelerating' ? 'positive' : momentum === 'Decelerating' ? 'negative' : 'neutral');
  setStatCard('c7-4qinv', `${lastRoll.toFixed(0)}t`);
}

function updateChart7Range(years) {
  analyticsState.chart7Range = years;
  document.querySelectorAll('.c7-range-btn').forEach(b => {
    b.classList.toggle('active', parseInt(b.dataset.years) === years);
  });
  buildChart7();
}

function toggleChart7Unit() {
  analyticsState.chart7Unit = analyticsState.chart7Unit === 'tonnes' ? 'usd' : 'tonnes';
  const btn = document.getElementById('chart7-unit-btn');
  if (btn) btn.textContent = analyticsState.chart7Unit === 'tonnes' ? 'Switch to USD' : 'Switch to Tonnes';
  buildChart7();
}

// ---------------------------------------------------------------------------
// CHART 8 — Bull Market Scorecard (pure HTML, no canvas)
// ---------------------------------------------------------------------------
function buildScorecard() {
  const fred = analyticsState.fred;
  const cot  = analyticsState.cot;
  const yf   = analyticsState.yf;

  const signals = [];

  // 1. Real Rate Trend — TIPS 3-month change
  let tipsSignal = 'neutral';
  let tipsVal    = '—';
  if (fred && fred.tips && fred.tips.length >= 2) {
    const tipsSorted = [...fred.tips].sort((a, b) => a.date.localeCompare(b.date));
    const latestTips = tipsSorted[tipsSorted.length - 1];
    const prior90    = tipsSorted[Math.max(0, tipsSorted.length - 65)]; // ~3 months trading days
    const chg = latestTips.value - prior90.value;
    tipsVal   = `${chg >= 0 ? '+' : ''}${chg.toFixed(2)}%`;
    tipsSignal = chg < -0.1 ? 'bullish' : chg > 0.1 ? 'cautionary' : 'neutral';
  }
  signals.push({ name: 'Real Rate Trend', value: tipsVal, status: tipsSignal, hint: '3-month TIPS change' });

  // 2. Rate Model Premium
  let premiumSignal = 'neutral';
  let premiumVal    = '—';
  if (fred && fred.tips && fred.gold) {
    const goldMap = buildDateMap(fred.gold);
    let pairs = [];
    fred.tips.forEach(t => {
      const g = goldMap.get(t.date);
      if (g != null && t.value != null) pairs.push({ tips: t.value, gold: g });
    });
    if (pairs.length >= 2) {
      const ols     = computeOLS(pairs.map(p => p.tips), pairs.map(p => p.gold));
      const curr    = pairs[pairs.length - 1];
      const model   = ols.slope * curr.tips + ols.intercept;
      const premium = ((curr.gold - model) / model) * 100;
      premiumVal    = `${premium >= 0 ? '+' : ''}${premium.toFixed(1)}%`;
      premiumSignal = premium < 10 ? 'bullish' : premium > 20 ? 'cautionary' : 'neutral';
    }
  }
  signals.push({ name: 'Rate Model Premium', value: premiumVal, status: premiumSignal, hint: 'Speculative vs model-implied' });

  // 3. DXY Trend — 90-day change
  let dxySignal = 'neutral';
  let dxyVal    = '—';
  if (fred && fred.dxy && fred.dxy.length >= 2) {
    const dxySorted = [...fred.dxy].sort((a, b) => a.date.localeCompare(b.date));
    const latestDxy = dxySorted[dxySorted.length - 1];
    const prior90   = dxySorted[Math.max(0, dxySorted.length - 90)];
    const chg = ((latestDxy.value - prior90.value) / prior90.value) * 100;
    dxyVal    = `${chg >= 0 ? '+' : ''}${chg.toFixed(1)}%`;
    dxySignal = chg < -1 ? 'bullish' : chg > 1 ? 'cautionary' : 'neutral';
  }
  signals.push({ name: 'DXY Trend', value: dxyVal, status: dxySignal, hint: '90-day DXY change' });

  // 4. COT Percentile
  let cotSignal = 'neutral';
  let cotVal    = '—';
  if (cot && cot.data && cot.data.length > 0) {
    const sorted    = cot.data.map(d => d.net_long).filter(v => v != null).sort((a, b) => a - b);
    const latest    = cot.data[cot.data.length - 1];
    const pct       = computePercentile(sorted, latest.net_long);
    cotVal    = `${Math.round(pct)}th %ile`;
    cotSignal = pct > 80 ? 'cautionary' : pct < 20 ? 'bullish' : 'neutral';
  }
  signals.push({ name: 'COT Positioning', value: cotVal, status: cotSignal, hint: 'CFTC managed money net long' });

  // 5. Gold Cobasis
  let cobasisSignal = 'neutral';
  let cobasisVal    = '—';
  if (fred && yf && fred.gold && yf.gold_futures) {
    const futMap = buildDateMap(yf.gold_futures, 'close');
    let cb = null;
    [...fred.gold].sort((a, b) => a.date.localeCompare(b.date)).forEach(g => {
      const fut = futMap.get(g.date);
      if (fut != null && g.value != null) cb = g.value - fut;
    });
    if (cb !== null) {
      cobasisVal    = `$${cb.toFixed(2)}`;
      cobasisSignal = cb > 0 ? 'bullish' : cb < -2 ? 'cautionary' : 'neutral';
    }
  }
  signals.push({ name: 'Gold Cobasis', value: cobasisVal, status: cobasisSignal, hint: 'Spot − Futures (backwardation = bullish)' });

  // 6. Miner Confirmation — 30-day GDX/GLD change
  let minerSignal = 'neutral';
  let minerVal    = '—';
  if (yf && yf.gdx && yf.gld) {
    const gldMap = buildDateMap(yf.gld, 'close');
    let pairs = [];
    yf.gdx.forEach(g => {
      const gld = gldMap.get(g.date);
      if (gld != null && g.close != null && gld > 0) pairs.push({ date: g.date, ratio: g.close / gld });
    });
    pairs.sort((a, b) => a.date.localeCompare(b.date));
    if (pairs.length >= 30) {
      const chg  = pairs[pairs.length - 1].ratio - pairs[pairs.length - 31].ratio;
      minerVal   = `${chg >= 0 ? '+' : ''}${chg.toFixed(3)}`;
      minerSignal = chg > 0 ? 'bullish' : chg < 0 ? 'cautionary' : 'neutral';
    }
  }
  signals.push({ name: 'Miner Confirmation', value: minerVal, status: minerSignal, hint: '30-day GDX/GLD ratio change' });

  // 7. Gold/Silver Ratio
  let gsrSignal = 'neutral';
  let gsrVal    = '—';
  if (yf && yf.gld && yf.slv) {
    const silvMap = buildDateMap(yf.slv, 'close');
    let lastRatio = null;
    [...yf.gld].sort((a, b) => a.date.localeCompare(b.date)).forEach(g => {
      const s = silvMap.get(g.date);
      if (s != null && g.close != null && s > 0) lastRatio = g.close / s;
    });
    if (lastRatio !== null) {
      gsrVal    = lastRatio.toFixed(1);
      gsrSignal = lastRatio < 70 ? 'bullish' : lastRatio > 80 ? 'cautionary' : 'neutral';
    }
  }
  signals.push({ name: 'Gold/Silver Ratio', value: gsrVal, status: gsrSignal, hint: 'GSR < 70 = bullish; > 80 = cautionary' });

  // 8. CB Demand Trend
  const cbKeys = Object.keys(quarterlyData.netPurchases).sort();
  const cbLast = quarterlyData.netPurchases[cbKeys[cbKeys.length - 1]];
  const cbPrev = quarterlyData.netPurchases[cbKeys[cbKeys.length - 2]];
  const cbSignal = cbLast > cbPrev ? 'bullish' : cbLast < cbPrev ? 'cautionary' : 'neutral';
  signals.push({
    name: 'CB Demand Trend', value: `${cbLast}t`,
    status: cbSignal, hint: 'Latest vs prior quarter',
  });

  // 9. ETF Flow Trend
  const etfKeys = Object.keys(quarterlyData.etfFlows).sort();
  const etfLast = quarterlyData.etfFlows[etfKeys[etfKeys.length - 1]];
  const etfPrev = quarterlyData.etfFlows[etfKeys[etfKeys.length - 2]];
  const etfSignal = etfLast > 0 ? 'bullish' : etfLast < 0 ? 'cautionary' : 'neutral';
  signals.push({
    name: 'ETF Flow Trend', value: `${etfLast >= 0 ? '+' : ''}${etfLast}t`,
    status: etfSignal, hint: 'Latest quarter ETF flow',
  });

  // Composite score
  const bullishCount = signals.filter(s => s.status === 'bullish').length;
  const scoreFraction = `${bullishCount}/9`;

  // Render into DOM
  const scoreEl = document.getElementById('scorecard-composite');
  if (scoreEl) {
    const scoreColor = bullishCount >= 7 ? '#3fb950' : bullishCount >= 4 ? '#d29922' : '#f85149';
    scoreEl.innerHTML = `
      <span class="composite-score-num" style="color:${scoreColor}">${scoreFraction}</span>
      <span class="composite-score-label">Bullish Signals</span>
    `;
  }

  const gridEl = document.getElementById('scorecard-grid');
  if (!gridEl) return;
  gridEl.innerHTML = signals.map(sig => `
    <div class="signal-card ${sig.status}">
      <div class="signal-name">${sig.name}</div>
      <div class="signal-value">${sig.value}</div>
      <div class="signal-footer">
        <span class="signal-dot signal-dot--${sig.status}"></span>
        <span class="signal-status">${capitalize(sig.status)}</span>
      </div>
      <div class="signal-hint">${sig.hint}</div>
    </div>
  `).join('');
}

// ---------------------------------------------------------------------------
// UTILITIES
// ---------------------------------------------------------------------------

/** Filter array of {date, ...} objects to last N years */
function filterByRange(data, years) {
  if (!years || years <= 0) return data;
  const cutoff = new Date();
  cutoff.setFullYear(cutoff.getFullYear() - years);
  const cutoffStr = cutoff.toISOString().slice(0, 10);
  return data.filter(d => (d.date || '') >= cutoffStr);
}

/** Build a Map of date → value from array of objects.
 *  valueKey: key to use for value (default 'value', use 'close' for yfinance data) */
function buildDateMap(arr, valueKey = 'value') {
  const map = new Map();
  if (!arr) return map;
  arr.forEach(item => {
    if (item.date != null && item[valueKey] != null) {
      map.set(item.date, item[valueKey]);
    }
  });
  return map;
}

/** Rolling Pearson correlation over a sliding window.
 *  Returns array of same length; first (window-1) entries are null. */
function rollingCorrelation(arr1, arr2, window) {
  const result = new Array(arr1.length).fill(null);
  for (let i = window - 1; i < arr1.length; i++) {
    const a = arr1.slice(i - window + 1, i + 1);
    const b = arr2.slice(i - window + 1, i + 1);
    const n = window;
    let sumA = 0, sumB = 0, sumAB = 0, sumA2 = 0, sumB2 = 0;
    for (let j = 0; j < n; j++) {
      if (a[j] == null || b[j] == null) { sumA = null; break; }
      sumA  += a[j];
      sumB  += b[j];
      sumAB += a[j] * b[j];
      sumA2 += a[j] * a[j];
      sumB2 += b[j] * b[j];
    }
    if (sumA === null) continue;
    const num = n * sumAB - sumA * sumB;
    const den = Math.sqrt((n * sumA2 - sumA * sumA) * (n * sumB2 - sumB * sumB));
    result[i] = den === 0 ? 0 : num / den;
  }
  return result;
}

/** Simple OLS linear regression: y = slope*x + intercept.
 *  Returns {slope, intercept, r2}. Skips null/NaN pairs. */
function computeOLS(xs, ys) {
  let n = 0, sx = 0, sy = 0, sxy = 0, sx2 = 0, sy2 = 0;
  for (let i = 0; i < xs.length; i++) {
    const x = xs[i], y = ys[i];
    if (x == null || y == null || isNaN(x) || isNaN(y)) continue;
    n++; sx += x; sy += y; sxy += x * y; sx2 += x * x; sy2 += y * y;
  }
  if (n < 2) return { slope: 0, intercept: 0, r2: 0 };
  const denom = n * sx2 - sx * sx;
  if (denom === 0) return { slope: 0, intercept: sy / n, r2: 0 };
  const slope     = (n * sxy - sx * sy) / denom;
  const intercept = (sy - slope * sx) / n;
  // R²
  const yMean = sy / n;
  let ssTot = 0, ssRes = 0;
  for (let i = 0; i < xs.length; i++) {
    const x = xs[i], y = ys[i];
    if (x == null || y == null || isNaN(x) || isNaN(y)) continue;
    const yHat = slope * x + intercept;
    ssRes += (y - yHat) ** 2;
    ssTot += (y - yMean) ** 2;
  }
  const r2 = ssTot === 0 ? 0 : 1 - ssRes / ssTot;
  return { slope, intercept, r2 };
}

/** Given a SORTED array and a value, return its percentile rank (0–100). */
function computePercentile(sortedArr, value) {
  if (!sortedArr || sortedArr.length === 0) return 50;
  let below = 0;
  for (const v of sortedArr) {
    if (v < value) below++;
    else break;
  }
  return (below / sortedArr.length) * 100;
}

/** Destroy a chart by key in analyticsState.charts */
function destroyChart(key) {
  if (analyticsState.charts[key]) {
    try { analyticsState.charts[key].destroy(); } catch (_) {}
    delete analyticsState.charts[key];
  }
}

/** Set the value (and optionally class) on a stat card element */
function setStatCard(id, value, cssClass) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = value;
  if (cssClass) {
    el.className = el.className.replace(/\b(positive|negative|neutral)\b/g, '').trim();
    el.classList.add(cssClass);
  }
}

function capitalize(str) {
  return str.charAt(0).toUpperCase() + str.slice(1);
}


// ── AI Analysis Panels ──────────────────────────────────────────────────────

/**
 * Request AI analysis for a chart panel.
 * States: dormant → loading → loaded | error
 */
function requestAnalysis(chartId) {
  setAiPanelState(chartId, 'loading');

  fetch('/api/analysis', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ chart: chartId }),
  })
    .then(r => r.json())
    .then(data => {
      if (data.error) {
        setAiPanelState(chartId, 'error', data.error);
      } else {
        setAiPanelState(chartId, 'loaded', data);
      }
    })
    .catch(err => {
      setAiPanelState(chartId, 'error', err.message || 'Network error');
    });
}

/**
 * Update an AI panel's visible state.
 * @param {string}  chartId  - e.g. 'chart1'
 * @param {string}  state    - 'dormant' | 'loading' | 'loaded' | 'error'
 * @param {*}       data     - for 'loaded': API response object; for 'error': string
 */
function setAiPanelState(chartId, state, data) {
  const panel = document.getElementById('ai-panel-' + chartId);
  if (!panel) return;

  const dormant = panel.querySelector('.ai-panel-dormant');
  const loading = panel.querySelector('.ai-panel-loading');
  const loaded  = panel.querySelector('.ai-panel-loaded');
  const error   = panel.querySelector('.ai-panel-error');

  // Hide all
  [dormant, loading, loaded, error].forEach(el => {
    if (el) el.style.display = 'none';
  });

  if (state === 'loading') {
    if (loading) loading.style.display = 'flex';

  } else if (state === 'loaded') {
    if (!loaded) return;
    loaded.style.display = 'block';

    const textEl = document.getElementById('ai-text-' + chartId);
    const metaEl = document.getElementById('ai-meta-' + chartId);

    if (textEl) textEl.textContent = data.analysis || '';

    if (metaEl) {
      const ts     = (data.generatedAt || '').replace('T', ' ').replace('Z', ' UTC');
      const cachedSuffix = data.cached
        ? ' · cached ' + (data.cacheAge > 60 ? Math.round(data.cacheAge / 60) + 'm ago' : 'just now')
        : ' · just generated';
      metaEl.textContent = ts + cachedSuffix;
    }

    // Mobile collapse/expand
    if (textEl) {
      const isMobile = window.innerWidth <= 768;
      if (isMobile) {
        textEl.classList.remove('ai-expanded');
        // Only add toggle once
        if (!loaded.querySelector('.ai-collapse-toggle')) {
          const toggle = document.createElement('button');
          toggle.className = 'ai-collapse-toggle';
          toggle.textContent = 'Show analysis ▼';
          toggle.onclick = function () {
            const expanded = textEl.classList.toggle('ai-expanded');
            toggle.textContent = expanded ? 'Hide analysis ▲' : 'Show analysis ▼';
          };
          loaded.querySelector('.ai-panel-header').insertAdjacentElement('afterend', toggle);
        }
      } else {
        textEl.classList.add('ai-expanded');
      }
    }

  } else if (state === 'error') {
    if (error) error.style.display = 'flex';
    const errEl = document.getElementById('ai-error-' + chartId);
    if (errEl) errEl.textContent = typeof data === 'string' ? data : 'Analysis unavailable';

  } else {
    // dormant (default)
    if (dormant) dormant.style.display = 'flex';
  }
}
