const chartCanvas = document.getElementById('maxChart');
const debugInfoEl = document.getElementById('debugInfo');
const integritySelectEl = document.getElementById('integritySelect');
const integritySummaryEl = document.getElementById('integritySummary');
const integrityEventsEl = document.getElementById('integrityEvents');
const integrityTogglesEl = document.getElementById('integrityToggles');
const debugMode = window.location.hash.includes('debug');

function abortBootstrap(message) {
  console.error(message);
  if (debugInfoEl) {
    debugInfoEl.textContent = message;
  }
  throw new Error(message);
}

if (!chartCanvas) {
  abortBootstrap('未找到图表画布元素，无法初始化仪表盘。');
}

const chartCtx = chartCanvas.getContext('2d');
if (!chartCtx) {
  abortBootstrap('无法获取画布上下文，图表初始化失败。');
}

if (!window.Chart) {
  abortBootstrap('Chart.js 未加载，无法绘制带宽图表。');
}

const ChartJS = window.Chart;

const hoverGuideLinePlugin = {
  id: 'hoverGuideLine',
  afterDatasetsDraw(chart) {
    const { ctx, tooltip, chartArea } = chart;
    if (!tooltip || !tooltip.getActiveElements || tooltip.getActiveElements().length === 0) {
      return;
    }
    const { element } = tooltip.getActiveElements()[0];
    if (!element) {
      return;
    }
    const x = element.x;
    ctx.save();
    ctx.beginPath();
    ctx.moveTo(x, chartArea.top);
    ctx.lineTo(x, chartArea.bottom);
    ctx.lineWidth = 1;
    ctx.strokeStyle = 'rgba(255, 255, 255, 0.25)';
    ctx.stroke();
    ctx.restore();
  },
};

ChartJS.register(hoverGuideLinePlugin);

if (debugMode) {
  console.info('[XDP] 调试模式已开启，fetch /api/buckets?debug=1');
}
const INTEGRITY_TYPE_LABELS = {
  trade: 'Trade',
  inc_seq: 'INC',
  rest_summary: 'REST',
};
const INTEGRITY_FAIL_COLOR = '#f87171';
const INTEGRITY_POINT_STYLE = {
  trade: 'circle',
  inc_seq: 'triangle',
  rest_summary: 'rectRounded',
};
const TRADE_TOOLTIP_LIMIT = 6;
const INTEGRITY_STREAM_COLOR_POOL = [
  '#0ea5e9',
  '#a855f7',
  '#f97316',
  '#14b8a6',
  '#f59e0b',
  '#ec4899',
  '#6366f1',
  '#10b981',
  '#22d3ee',
  '#fbbf24',
  '#fb7185',
  '#2dd4bf',
];
const integrityStreamColorCache = new Map();
let integrityStreamColorIndex = 0;
const integrityStreamLabels = new Map();
const TRADE_CATEGORY_PREFIX = 'trade/';
const DEFAULT_TRADE_CATEGORY = `${TRADE_CATEGORY_PREFIX}default`;
const APP_VERSION = '20251101-8';

console.info(`[Integrity] bundle ${APP_VERSION}`);

let refreshIntervalMs = 5000;
let maxChart;
let alertThresholdBps = 0;
let windowSeconds = 0;
const UNIT_SCALE = 1_000_000; // 将 bps 转换为 Mbps 以便图表展示
let bucketRanges = [];
const integrityStreamMeta = new Map();
const integrityStreamVisibility = new Map();
const integrityToggleInputs = new Map();
let isRefreshing = false;
let integritySnapshotByBucket = [];
let latestIntegrityEvents = [];
let latestBuckets = [];
let latestBucketMeta = null;

function formatBps(bps) {
  const units = ['bps', 'Kbps', 'Mbps', 'Gbps', 'Tbps'];
  let value = bps;
  for (const unit of units) {
    if (Math.abs(value) < 1000) {
      return `${value.toFixed(2)} ${unit}`;
    }
    value /= 1000;
  }
  return `${value.toFixed(2)} Pbps`;
}

function formatThresholdText(bps) {
  if (!bps) {
    return '阈值: 未设置';
  }
  return `阈值: ${formatBps(bps)}`;
}

function toLocal(ts) {
  return new Date(ts * 1000).toLocaleString();
}

function formatTimeOfDay(ts) {
  const date = new Date(ts * 1000);
  return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function getIntegrityTypeLabel(type) {
  if (!type) {
    return '未知';
  }
  return INTEGRITY_TYPE_LABELS[type] || type.toUpperCase();
}

function getIntegrityPointStyle(type) {
  return INTEGRITY_POINT_STYLE[type] || 'rectRot';
}

function getIntegrityStreamColor(streamKey) {
  if (!streamKey) {
    return '#6b7280';
  }
  if (!integrityStreamColorCache.has(streamKey)) {
    const color = INTEGRITY_STREAM_COLOR_POOL[integrityStreamColorIndex % INTEGRITY_STREAM_COLOR_POOL.length];
    integrityStreamColorIndex += 1;
    integrityStreamColorCache.set(streamKey, color);
  }
  return integrityStreamColorCache.get(streamKey);
}

const INTEGRITY_CATEGORY_LEVELS = {
  [`${TRADE_CATEGORY_PREFIX}binance-futures`]: 0.9,
  [`${TRADE_CATEGORY_PREFIX}binance`]: 0.78,
  [DEFAULT_TRADE_CATEGORY]: 0.86,
  inc_seq: 0.6,
  'rest/1m': 0.32,
  'rest/5m': 0.24,
  rest_summary: 0.28,
  default: 0.18,
};

function computeStreamJitter(streamKey, spread = 0.12) {
  if (!streamKey) {
    return 0;
  }
  let hash = 0;
  for (let idx = 0; idx < streamKey.length; idx += 1) {
    hash = (hash * 31 + streamKey.charCodeAt(idx)) | 0;
  }
  const normalized = (hash >>> 0) / 0xffffffff;
  return (normalized - 0.5) * spread;
}

function clamp(value, min, max) {
  if (value < min) {
    return min;
  }
  if (value > max) {
    return max;
  }
  return value;
}

function getIntegrityYPosition(category, isOk, streamKey) {
  const normalized = typeof category === 'string' ? category : '';
  const base =
    INTEGRITY_CATEGORY_LEVELS[normalized]
    ?? (normalized.startsWith(TRADE_CATEGORY_PREFIX)
      ? INTEGRITY_CATEGORY_LEVELS[DEFAULT_TRADE_CATEGORY]
      : INTEGRITY_CATEGORY_LEVELS.default);
  let offset = isOk ? 0.07 : -0.07;
  if (isTradeCategory(normalized)) {
    offset += computeStreamJitter(streamKey, 0.16);
  }
  return clamp(base + offset, -0.1, 1.1);
}

function resolveIntegrityStream(event) {
  if (!event) {
    return null;
  }
  const typeRaw = event.type ?? event.stream_category;
  const type = String(typeRaw || '').toLowerCase();
  const exchange = String(event.exchange || '').trim();
  const symbol = String(event.symbol || '').trim().toUpperCase();
  const stageValue = event.stage ?? event.stream_stage;
  const stage = String(stageValue || '').trim();
  const hostname = String(event.hostname || '').trim();
  const iface = String(event.interface || '').trim();

  let key = event.stream_key || event.key || '';
  if (!key) {
    const parts = [];
    if (hostname) {
      parts.push(hostname);
    }
    if (iface) {
      parts.push(iface);
    }
    if (exchange) {
      parts.push(exchange.toLowerCase());
    }
    if (stage) {
      parts.push(stage.toLowerCase());
    }
    if (type) {
      parts.push(type);
    }
    if (symbol) {
      parts.push(symbol);
    }
    key = parts.length ? parts.join('::') : 'integrity';
  }

  const typeLabel = getIntegrityTypeLabel(type) || '完整性';
  const labelParts = [];
  if (exchange) {
    labelParts.push(exchange);
  }
  if (stage) {
    labelParts.push(stage);
  }
  if (symbol) {
    labelParts.push(symbol);
  }
  labelParts.push(typeLabel);
  const label = labelParts.join(' · ');

  let category;
  if (type === 'trade') {
    const exchangeSlug = (exchange || 'unknown').toLowerCase().replace(/[^a-z0-9]+/g, '-');
    category = `${TRADE_CATEGORY_PREFIX}${exchangeSlug || 'default'}`;
  } else if (type === 'rest_summary') {
    const stageSlug = stage ? stage.toLowerCase() : 'summary';
    category = `rest/${stageSlug}`;
  } else if (type) {
    category = type;
  } else {
    category = 'integrity';
  }

  event.stream_key = key;
  event.stream_label = label;
  event.stream_category = category;
  if (stage) {
    event.stage = stage;
    event.stream_stage = stage;
  }

  integrityStreamLabels.set(key, label);
  return { key, label, category, stage };
}

function describeStreamLabel(key) {
  return integrityStreamLabels.get(key) || key;
}

function isTradeCategory(category) {
  return typeof category === 'string' && category.startsWith(TRADE_CATEGORY_PREFIX);
}

function ensureStreamVisibility() {
  integrityStreamMeta.forEach((meta, streamKey) => {
    if (integrityStreamVisibility.has(streamKey)) {
      return;
    }
    const defaultVisible = String(meta.type || '').toLowerCase() === 'trade';
    integrityStreamVisibility.set(streamKey, defaultVisible);
  });
}

function formatIntegrityStatusLabel(event, stream, { includeSymbol = false } = {}) {
  const baseLabel = stream?.label || '';
  const type = String(event?.type || '').toLowerCase();
  const exchange = String(event?.exchange || '').trim();
  const stage = String(event?.stage || '').trim();
  const symbol = String(event?.symbol || '').trim().toUpperCase();

  if (baseLabel) {
    if (includeSymbol || !symbol) {
      return baseLabel;
    }
    const fragments = baseLabel.split(' · ').filter((fragment) => fragment !== symbol);
    return fragments.length ? fragments.join(' · ') : baseLabel;
  }

  const parts = [];
  if (exchange) {
    parts.push(exchange);
  }
  if (stage) {
    parts.push(stage);
  }
  if (includeSymbol && symbol) {
    parts.push(symbol);
  }
  const typeLabel = getIntegrityTypeLabel(type) || (type ? type.toUpperCase() : '完整性');
  parts.push(typeLabel);
  return parts.join(' · ') || typeLabel;
}

function getFailedRequests(event) {
  if (!event) {
    return [];
  }
  const preset = Array.isArray(event.failed_requests) ? event.failed_requests : [];
  const normalizedPreset = preset
    .map((item) => String(item || '').trim())
    .filter((item) => item.length > 0);
  if (normalizedPreset.length) {
    return Array.from(new Set(normalizedPreset));
  }
  const fallback = [];
  const requests = Array.isArray(event.requests) ? event.requests : [];
  for (const req of requests) {
    if (!req || typeof req !== 'object') {
      continue;
    }
    const status = String(req.status || '').toLowerCase();
    if (status === 'ok') {
      continue;
    }
    const name = String(req.name || '').trim();
    if (name) {
      fallback.push(name);
      continue;
    }
    const detail = String(req.detail || '').trim();
    if (detail) {
      fallback.push(detail);
    }
  }
  const results = Array.isArray(event.results) ? event.results : [];
  for (const result of results) {
    if (!result || typeof result !== 'object') {
      continue;
    }
    const symbol = String(result.symbol || '').trim().toUpperCase();
    const nested = Array.isArray(result.requests) ? result.requests : [];
    for (const req of nested) {
      if (!req || typeof req !== 'object') {
        continue;
      }
      const status = String(req.status || '').toLowerCase();
      if (status === 'ok') {
        continue;
      }
      const baseName = String(req.name || '').trim() || String(req.detail || '').trim();
      if (!baseName) {
        continue;
      }
      const label = symbol ? `${symbol}:${baseName}` : baseName;
      fallback.push(label);
    }
  }
  return Array.from(new Set(fallback.filter((item) => item.length > 0)));
}

function buildIntegrityDetailSegments(event) {
  const segments = [];
  const failedRequests = getFailedRequests(event);
  if (failedRequests.length) {
    segments.push(`失败请求: ${failedRequests.join(', ')}`);
  }
  const failedCount = Number(event?.failed_request_count) || failedRequests.length;
  const totalCount =
    Number(event?.request_count)
    || (Array.isArray(event?.requests) ? event.requests.length : 0)
    || (Array.isArray(event?.results)
      ? event.results.reduce(
          (acc, item) => acc + (Array.isArray(item?.requests) ? item.requests.length : 0),
          0,
        )
      : 0);
  if (totalCount > 0 && failedCount > 0) {
    segments.push(`请求失败 ${failedCount}/${totalCount}`);
  }
  const failedSymbols = Array.isArray(event?.failed_symbols)
    ? event.failed_symbols.map((item) => String(item || '').trim()).filter((item) => item.length > 0)
    : [];
  if (!failedSymbols.length && Array.isArray(event?.results)) {
    event.results.forEach((result) => {
      if (!result || typeof result !== 'object') {
        return;
      }
      const status = String(result.status || '').toLowerCase();
      if (status === 'ok') {
        return;
      }
      const symbol = String(result.symbol || '').trim().toUpperCase();
      if (symbol) {
        failedSymbols.push(symbol);
      }
    });
  }
  if (failedSymbols.length) {
    segments.push(`失败合约: ${failedSymbols.join(', ')}`);
  }
  const detail = event?.detail;
  if (detail) {
    segments.push(String(detail));
  }
  return segments;
}

function normalizeIntegrityEvent(raw, fallbackHost = '', fallbackInterface = '') {
  if (!raw || typeof raw !== 'object') {
    return null;
  }

  const timestamp = Number(raw.timestamp) || 0;
  const status = String(raw.status || '').toLowerCase();
  const isOk = raw.is_ok ?? status === 'ok';
  const type = String(raw.type || '').toLowerCase();
  const stage = String(raw.stage || raw.stream_stage || '').trim();
  const exchange = String(raw.exchange || '').trim();
  const symbol = String(raw.symbol || '').trim().toUpperCase();
  const hostname = String(raw.hostname || fallbackHost || '').trim();
  const iface = String(raw.interface || fallbackInterface || '').trim();
  const streamName = String(raw.stream || raw.source || '').trim();

  const normalizedResults = Array.isArray(raw.results)
    ? raw.results
        .map((item) => {
          if (!item || typeof item !== 'object') {
            return null;
          }
          const itemSymbol = String(item.symbol || '').trim().toUpperCase();
          const itemStatus = String(item.status || '').toLowerCase();
          const normalized = {
            symbol: itemSymbol,
            status: itemStatus,
          };
          if (item.detail !== undefined && item.detail !== null) {
            normalized.detail = String(item.detail);
          }
          const requests = Array.isArray(item.requests)
            ? item.requests
                .map((req) => {
                  if (!req || typeof req !== 'object') {
                    return null;
                  }
                  const reqName = String(req.request ?? req.name ?? '').trim();
                  const reqStatus = String(req.status || '').toLowerCase();
                  const normalizedReq = {
                    name: reqName,
                    status: reqStatus,
                  };
                  if (req.detail !== undefined && req.detail !== null) {
                    normalizedReq.detail = String(req.detail);
                  }
                  return normalizedReq;
                })
                .filter(Boolean)
            : [];
          if (requests.length) {
            normalized.requests = requests;
          }
          return normalized;
        })
        .filter(Boolean)
    : [];

  const failedSymbols = Array.isArray(raw.failed_symbols)
    ? raw.failed_symbols.map((item) => String(item || '').trim().toUpperCase()).filter((item) => item.length > 0)
    : [];
  const failedRequests = Array.isArray(raw.failed_requests)
    ? raw.failed_requests.map((item) => String(item || '').trim()).filter((item) => item.length > 0)
    : [];

  const event = {
    key: raw.key || raw.stream_key || '',
    stream: streamName,
    timestamp,
    timestamp_iso: raw.timestamp_iso,
    status,
    is_ok: isOk,
    exchange,
    symbol,
    stage,
    period: Number(raw.period) || undefined,
    hostname,
    interface: iface,
    type,
    detail: raw.detail,
    results: normalizedResults,
    results_count: Number(raw.results_count) || normalizedResults.length,
    failed_symbols: failedSymbols,
    failed_count: Number(raw.failed_count) || failedSymbols.length,
    failed_requests: failedRequests,
    failed_request_count:
      Number(raw.failed_request_count) || failedRequests.length,
    mode: raw.mode,
  };

  resolveIntegrityStream(event);
  return event;
}

function initChart() {
  if (maxChart) {
    maxChart.destroy();
  }
  maxChart = new ChartJS(chartCtx, {
    type: 'line',
    data: {
      labels: [],
      datasets: [
        {
          label: '最大带宽',
          data: [],
          borderColor: '#4f9cf6',
          backgroundColor: 'rgba(79, 156, 246, 0.25)',
          borderWidth: 2,
          tension: 0.2,
          pointRadius: 0,
          pointHoverRadius: 0,
          fill: false,
        },
        {
          label: '平均带宽',
          data: [],
          borderColor: '#22c55e',
          backgroundColor: 'rgba(34, 197, 94, 0.18)',
          borderWidth: 2,
          borderDash: [4, 4],
          tension: 0.2,
          pointRadius: 0,
          pointHoverRadius: 0,
          fill: false,
        },
        {
          label: '阈值',
          data: [],
          borderColor: '#f87171',
          borderDash: [6, 6],
          pointRadius: 0,
          pointHoverRadius: 0,
          fill: false,
          hidden: true,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: true },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const datasetLabel = ctx.dataset.label || '';
              if (ctx.dataset.xdpIntegrity) {
                const raw = ctx.raw || {};
                const status = String(raw.status || '').toLowerCase();
                const isOk = raw.is_ok ?? raw.isOk ?? status === 'ok';
                const stream = resolveIntegrityStream(raw);
                const statusLabel = formatIntegrityStatusLabel(raw, stream, { includeSymbol: true });
                const host = raw.hostname || '';
                const iface = raw.interface || '';
                const time = raw.timestamp ? formatTimeOfDay(raw.timestamp) : '';
                const parts = [`[${statusLabel}]`];
                if (host) {
                  parts.push(host);
                }
                if (iface) {
                  parts.push(iface);
                }
                if (time) {
                  parts.push(time);
                }
                parts.push(isOk ? 'OK' : '异常');
                let message = `${datasetLabel}: ${parts.join(' ')}`;
                const extraSegments = buildIntegrityDetailSegments(raw);
                if (extraSegments.length) {
                  message += ` · ${extraSegments.join(' · ')}`;
                }
                return message;
              }
              if (datasetLabel === '阈值') {
                return `阈值: ${formatBps(alertThresholdBps)}`;
              }
              const rawBps = ctx.parsed.y * UNIT_SCALE;
              return `${datasetLabel}: ${formatBps(rawBps)}`;
            },
            footer: (items) => {
              if (!items || !items.length) {
                return '';
              }
              const first = items[0];
              const rawIndex = Math.round(first.parsed?.x ?? first.dataIndex ?? 0);
              if (!Number.isFinite(rawIndex) || integritySnapshotByBucket.length === 0) {
                return '';
              }
              const bucketIndex = Math.min(
                Math.max(rawIndex, 0),
                integritySnapshotByBucket.length - 1,
              );
              const lines = getIntegrityTooltipFooterLines(bucketIndex);
              return lines;
            },
          },
        },
      },
      scales: {
        x: {
          type: 'linear',
          display: true,
          title: {
            display: true,
            text: '窗口',
          },
          min: 0,
          max: 1,
          ticks: {
            stepSize: 1,
            callback: (value) => {
              if (!bucketRanges.length) {
                return '';
              }
              if (value === 0) {
                const first = bucketRanges[0];
                return ['开始', first.startTime];
              }
              if (value === bucketRanges.length - 1) {
                const last = bucketRanges[bucketRanges.length - 1];
                return ['结束', last.endTime];
              }
              return '';
            },
          },
        },
        y: {
          display: true,
          title: {
            display: true,
            text: '带宽 (Mbps)',
          },
          min: 0,
          max: 300,
          ticks: {
            callback: (value) => `${Number(value).toFixed(0)} Mbps`,
          },
        },
        integrity: {
          type: 'linear',
          position: 'right',
          min: -0.1,
          max: 1.1,
          display: false,
          grid: { display: false },
        },
      },
      interaction: {
        mode: 'index',
        intersect: false,
      },
    },
  });
  maxChart.options.plugins.tooltip.callbacks.title = (items) => {
    const item = items && items.length ? items[0] : null;
    if (!item) {
      return '';
    }
    const idx = Math.round(item.parsed?.x ?? item.dataIndex ?? 0);
    const range = bucketRanges[idx];
    if (!range) {
      return '';
    }
    return `${range.startLocal} → ${range.endLocal}`;
  };
}

async function loadStatus() {
  const res = await fetch('/api/status');
  const data = await res.json();
  const cfg = data.config;
  document.getElementById('iface').textContent = `接口: ${cfg.interface}`;
  document.getElementById('window').textContent = `窗口: ${cfg.window_seconds}s`;
  document.getElementById('tick').textContent = `Tick: ${cfg.tick_ms}ms`;
  document.getElementById('mode').textContent = `模式: ${cfg.mode}`;
  if (data.last_error) {
    document.getElementById('mode').textContent += ` (错误: ${data.last_error})`;
  }
  windowSeconds = cfg.window_seconds;
  alertThresholdBps = Number(cfg.alert_threshold_bps) || 0;
  const thresholdEl = document.getElementById('threshold');
  if (thresholdEl) {
    thresholdEl.textContent = formatThresholdText(alertThresholdBps);
  }
  const sourceEl = document.getElementById('sourceInfo');
  if (sourceEl) {
    const tickLabel = cfg.tick_ms ? `${Number(cfg.tick_ms).toFixed(0)}ms` : '未知';
    const windowLabel = windowSeconds ? `${Number(windowSeconds).toFixed(2)}s` : '未知';
    sourceEl.textContent = `数据来源：XDP ${tickLabel} Tick → ${windowLabel} 窗口 max`;
  }
  if (cfg && cfg.refresh_interval_ms) {
    const newInterval = Number(cfg.refresh_interval_ms);
    if (Number.isFinite(newInterval) && newInterval > 0) {
      const changed = newInterval !== refreshIntervalMs;
      refreshIntervalMs = newInterval;
      if (changed && refreshTimerId) {
        startAutoRefresh();
      }
    }
  }
}

async function fetchBucketsPayload() {
  const endpoint = debugMode ? '/api/buckets?debug=1' : '/api/buckets';
  const res = await fetch(endpoint);
  if (!res.ok) {
    console.error('获取数据失败', await res.text());
    return { data: [], meta: undefined };
  }
  return res.json();
}

function populateIntegritySelect() {
  if (!integritySelectEl) {
    return;
  }
  integritySelectEl.innerHTML = '';
  const option = document.createElement('option');
  option.value = '';
  option.textContent = '全部流';
  integritySelectEl.appendChild(option);
  integritySelectEl.disabled = true;
}

function renderIntegrityStreamToggles() {
  if (!integrityTogglesEl) {
    return;
  }
  integrityTogglesEl.innerHTML = '';
  integrityToggleInputs.clear();

  ensureStreamVisibility();

  const entries = Array.from(integrityStreamMeta.entries()).map(([key, meta]) => ({
    key,
    meta,
  }));
  entries.sort((a, b) => {
    const aType = String(a.meta.type || '');
    const bType = String(b.meta.type || '');
    if (aType === 'trade' && bType !== 'trade') {
      return -1;
    }
    if (aType !== 'trade' && bType === 'trade') {
      return 1;
    }
    return (a.meta.label || a.key).localeCompare(b.meta.label || b.key, 'zh-CN');
  });

  if (!entries.length) {
    return;
  }

  for (const { key, meta } of entries) {
    const label = document.createElement('label');
    label.className = 'integrity-toggle';
    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.value = key;
    const shouldCheck = integrityStreamVisibility.get(key) ?? false;
    checkbox.checked = shouldCheck;
    checkbox.addEventListener('change', () => {
      integrityStreamVisibility.set(key, checkbox.checked);
      if (!latestBuckets.length) {
        return;
      }
      renderBuckets(latestBuckets, latestBucketMeta, latestIntegrityEvents);
      renderIntegrityEvents(latestIntegrityEvents);
    });
    integrityToggleInputs.set(key, checkbox);

    const span = document.createElement('span');
    const labelParts = [];
    if (meta.label) {
      labelParts.push(meta.label);
    } else {
      labelParts.push(describeStreamLabel(key));
    }
    if (meta.type && meta.type !== 'trade') {
      labelParts.push(getIntegrityTypeLabel(meta.type));
    }
    span.textContent = labelParts.join(' · ');

    label.appendChild(checkbox);
    label.appendChild(span);
    integrityTogglesEl.appendChild(label);
  }
}

async function fetchIntegrityData(limit = 200) {
  const params = new URLSearchParams({ limit: String(limit), meta: '1' });
  const res = await fetch(`/api/integrity?${params.toString()}`);
  if (!res.ok) {
    throw new Error(await res.text());
  }
  return res.json();
}

function pickNumber(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : 0;
}

function computeIntegritySummaryByBucket(buckets, events) {
  if (!Array.isArray(buckets) || !buckets.length) {
    return [];
  }
  const sortedEvents = Array.isArray(events)
    ? [...events].sort((a, b) => (Number(a.timestamp) || 0) - (Number(b.timestamp) || 0))
    : [];
  const latestByStream = new Map();
  const snapshots = buckets.map(() => ({}));
  let cursor = 0;
  for (let idx = 0; idx < buckets.length; idx += 1) {
    const bucket = buckets[idx];
    const bucketEnd = Number(bucket.end_ts || bucket.endTs || 0);
    while (cursor < sortedEvents.length) {
      const event = sortedEvents[cursor];
      const ts = Number(event.timestamp) || 0;
      if (bucketEnd && ts > bucketEnd) {
        break;
      }
      const stream = resolveIntegrityStream(event);
      if (stream) {
        latestByStream.set(stream.key, event);
      }
      cursor += 1;
    }
    const snapshot = {};
    latestByStream.forEach((value, key) => {
      snapshot[key] = value;
    });
    snapshots[idx] = snapshot;
  }
  return snapshots;
}

function getIntegrityTooltipFooterLines(bucketIndex) {
  const snapshot = integritySnapshotByBucket[bucketIndex];
  if (!snapshot) {
    return [];
  }
  const entries = Object.entries(snapshot);
  if (!entries.length) {
    return [];
  }
  const defaultLines = [];
  const restLines = [];
  const tradeLines = [];
  for (const [streamKey, event] of entries) {
    const stream = resolveIntegrityStream(event);
    if (!stream) {
      continue;
    }
    const ok = event.is_ok ?? event.isOk ?? String(event.status || '').toLowerCase() === 'ok';
    const timeLabel = event.timestamp ? formatTimeOfDay(event.timestamp) : '未知时间';
    const label = formatIntegrityStatusLabel(event, stream, { includeSymbol: true });
    const statusLabel = ok ? '正常' : '异常';
    let line = `${label}: ${timeLabel} ${statusLabel}`;
    const extraSegments = buildIntegrityDetailSegments(event);
    if (!ok && extraSegments.length) {
      line += ` · ${extraSegments.join(' · ')}`;
    } else if (!ok && !extraSegments.length && event.detail) {
      line += ` · ${event.detail}`;
    }

    if (isTradeCategory(stream.category)) {
      tradeLines.push(line);
    } else if (typeof stream.category === 'string' && stream.category.startsWith('rest/')) {
      restLines.push(line);
    } else {
      defaultLines.push(line);
    }
  }
  defaultLines.sort((a, b) => a.localeCompare(b, 'zh-CN'));
  restLines.sort((a, b) => a.localeCompare(b, 'zh-CN'));
  tradeLines.sort((a, b) => a.localeCompare(b, 'zh-CN'));
  const lines = [...defaultLines, ...restLines];
  if (tradeLines.length > TRADE_TOOLTIP_LIMIT) {
    lines.push(...tradeLines.slice(0, TRADE_TOOLTIP_LIMIT));
    lines.push(`… 其余 ${tradeLines.length - TRADE_TOOLTIP_LIMIT} 个交易流`);
  } else {
    lines.push(...tradeLines);
  }
  return lines.length ? lines : ['无完整性数据'];
}

function renderBuckets(buckets, meta = undefined, integrityEvents = []) {
  ensureStreamVisibility();
  bucketRanges = buckets.map((bucket) => ({
    startTs: bucket.start_ts,
    endTs: bucket.end_ts,
    startLocal: toLocal(bucket.start_ts),
    endLocal: toLocal(bucket.end_ts),
    startTime: formatTimeOfDay(bucket.start_ts),
    endTime: formatTimeOfDay(bucket.end_ts),
  }));
  const pointCount = buckets.length;
  const rawMaxValues = buckets.map((b) => pickNumber(b.max_bps ?? b.bps_max ?? b.max));
  const rawAvgValues = buckets.map((b) => pickNumber(b.avg_bps ?? b.bps_avg ?? b.avg));
  const values = rawMaxValues.map((value) => value / UNIT_SCALE);
  const avgValues = rawAvgValues.map((value) => value / UNIT_SCALE);
  const markFlags = rawMaxValues.map((value) => alertThresholdBps > 0 && value >= alertThresholdBps);
  const singlePoint = pointCount <= 1;

  function locateBucketIndex(timestamp) {
    if (!bucketRanges.length || timestamp === undefined || timestamp === null) {
      return null;
    }
    let low = 0;
    let high = bucketRanges.length - 1;
    while (low <= high) {
      const mid = Math.floor((low + high) / 2);
      const range = bucketRanges[mid];
      if (timestamp < range.startTs) {
        high = mid - 1;
      } else if (timestamp > range.endTs) {
        low = mid + 1;
      } else {
        return mid;
      }
    }
    if (high >= 0) {
      return Math.min(Math.max(high, 0), bucketRanges.length - 1);
    }
    return null;
  }

  if (debugMode) {
    const latestRange = bucketRanges[bucketRanges.length - 1];
    const debugSnapshot = {
      totalPoints: pointCount,
      endpoint: singlePoint ? 'single-point' : 'multi-point',
      latest: pointCount
        ? {
            start: latestRange?.startLocal,
            end: latestRange?.endLocal,
            max_bps: rawMaxValues[rawMaxValues.length - 1],
            max_bps_mbps: values[values.length - 1],
            avg_bps: rawAvgValues[rawAvgValues.length - 1],
            avg_bps_mbps: avgValues[avgValues.length - 1],
            avg_source: buckets[buckets.length - 1].avg_source ?? null,
            max_source: buckets[buckets.length - 1].max_source ?? null,
            keys: Object.keys(buckets[buckets.length - 1]).sort(),
          }
        : null,
      metaSummary: meta
        ? {
            rawSeriesKeys: Object.keys(meta.raw_series || {}),
            seriesLength: Object.values(meta.raw_series || {}).map((series) => series.length),
          }
        : null,
      integrityPoints: integrityEvents.length,
      integrityStreamCount: integrityStreamMeta.size,
      integrityVisibleStreams: Array.from(integrityStreamVisibility.entries()),
    };
    console.debug('[XDP] renderBuckets 调试', debugSnapshot);
    if (debugInfoEl) {
      debugInfoEl.textContent = JSON.stringify(debugSnapshot, null, 2);
    }
  } else if (debugInfoEl) {
    if (pointCount === 0) {
      debugInfoEl.textContent = '尚无数据';
    } else {
      const firstRange = bucketRanges[0];
      const latestRange = bucketRanges[bucketRanges.length - 1];
      debugInfoEl.textContent = [
        `点数: ${pointCount}`,
        `时间范围: ${firstRange.startTime} → ${latestRange.endTime}`,
        `最后窗口: max=${values[values.length - 1].toFixed(2)} Mbps, avg=${avgValues[avgValues.length - 1].toFixed(2)} Mbps`,
      ].join('\n');
    }
  }

  maxChart.data.labels = [];
  const datasets = maxChart.data.datasets || [];
  const fallbackDatasets = datasets.slice(0, 3);
  const maxDataset =
    datasets.find((item) => item?.label === '最大带宽') || fallbackDatasets[0];
  const avgDataset =
    datasets.find((item) => item?.label === '平均带宽') || fallbackDatasets[1];
  const thresholdDataset =
    datasets.find((item) => item?.label === '阈值') || fallbackDatasets[2];
  const baseDatasets = [maxDataset, avgDataset, thresholdDataset].filter(Boolean);
  const existingIntegrityDatasets = new Map();
  for (const dataset of datasets) {
    if (dataset?.xdpIntegrity?.streamKey) {
      existingIntegrityDatasets.set(dataset.xdpIntegrity.streamKey, dataset);
    }
  }

  if (maxDataset) {
    maxDataset.data = values.map((y, idx) => ({ x: idx, y }));
    maxDataset.pointRadius = values.map((_, idx) => {
      if (singlePoint) {
        return 3;
      }
      return markFlags[idx] ? 5 : 0;
    });
    maxDataset.pointHoverRadius = values.map((_, idx) => {
      if (singlePoint) {
        return 5;
      }
      return markFlags[idx] ? 7 : 0;
    });
    maxDataset.pointHitRadius = values.map((_, idx) => {
      if (singlePoint) {
        return 6;
      }
      return markFlags[idx] ? 9 : 1;
    });
    maxDataset.pointBackgroundColor = markFlags.map((flag) => (flag ? '#f87171' : '#4f9cf6'));
    maxDataset.pointBorderColor = maxDataset.pointBackgroundColor;
  }

  if (avgDataset) {
    avgDataset.data = avgValues.map((y, idx) => ({ x: idx, y }));
    avgDataset.pointRadius = avgValues.map(() => (pointCount <= 1 ? 4 : 0));
    avgDataset.pointHoverRadius = avgValues.map(() => (pointCount <= 1 ? 6 : 0));
    avgDataset.pointHitRadius = avgValues.map(() => (pointCount <= 1 ? 6 : 1));
  }

  const overlayByStream = new Map();
  for (const event of integrityEvents) {
    const idx = locateBucketIndex(event.timestamp);
    if (idx === null) {
      continue;
    }
    const stream = resolveIntegrityStream(event);
    if (!stream) {
      continue;
    }
    const isOk = event.is_ok ?? event.isOk ?? String(event.status || '').toLowerCase() === 'ok';
    const eventType = event.type || '';
    const yValue = getIntegrityYPosition(stream.category, isOk, stream.key);
    const streamKey = stream.key;

    let entry = overlayByStream.get(streamKey);
    if (!entry) {
      const baseColor = getIntegrityStreamColor(streamKey);
      entry = {
        stream,
        baseColor,
        points: [],
        colors: [],
        borderColors: [],
        radii: [],
        hoverRadii: [],
        hitRadii: [],
        borderWidths: [],
        styles: [],
      };
      overlayByStream.set(streamKey, entry);
    }
    const streamVisible = integrityStreamVisibility.get(streamKey) ?? false;
    if (!streamVisible && isOk) {
      continue;
    }
    const pointColor = isOk ? entry.baseColor : INTEGRITY_FAIL_COLOR;
    entry.points.push({
      x: idx,
      y: yValue,
      timestamp: event.timestamp,
      status: event.status,
      detail: event.detail,
      is_ok: isOk,
      exchange: event.exchange,
      symbol: event.symbol,
      minute: event.minute,
      hostname: event.hostname,
      interface: event.interface,
      type: eventType,
      stage: event.stage,
      stream_key: streamKey,
      stream_label: stream.label,
      stream_category: stream.category,
      results: event.results,
      results_count: event.results_count,
      failed_symbols: event.failed_symbols,
      failed_requests: event.failed_requests,
      failed_request_count: event.failed_request_count,
    });
    entry.colors.push(pointColor);
    entry.borderColors.push(pointColor);
    entry.radii.push(isOk ? 5 : 7);
    entry.hoverRadii.push(isOk ? 7 : 9);
    entry.hitRadii.push(isOk ? 9 : 11);
    entry.borderWidths.push(isOk ? 1 : 2);
    entry.styles.push(isOk ? getIntegrityPointStyle(eventType) : 'triangle');
  }

  integritySnapshotByBucket = computeIntegritySummaryByBucket(buckets, integrityEvents);

  const integrityDatasets = [];
  overlayByStream.forEach((entry) => {
    const sortedPoints = entry.points
      .slice()
      .sort((a, b) => (a.x === b.x ? (a.timestamp || 0) - (b.timestamp || 0) : a.x - b.x));
    const dataset =
      existingIntegrityDatasets.get(entry.stream.key) || {
        type: 'scatter',
        showLine: false,
        yAxisID: 'integrity',
      };
    dataset.type = 'scatter';
    dataset.showLine = false;
    dataset.yAxisID = 'integrity';
    dataset.label = entry.stream.label;
    dataset.backgroundColor = entry.baseColor;
    dataset.borderColor = entry.baseColor;
    dataset.data = sortedPoints;
    dataset.hidden = sortedPoints.length === 0;
    dataset.order = String(entry.stream.category || '').startsWith(TRADE_CATEGORY_PREFIX) ? 20 : 25;
    dataset.xdpIntegrity = {
      streamKey: entry.stream.key,
      streamLabel: entry.stream.label,
      category: entry.stream.category,
      radii: entry.radii.slice(),
      hoverRadii: entry.hoverRadii.slice(),
      hitRadii: entry.hitRadii.slice(),
      borderWidths: entry.borderWidths.slice(),
      styles: entry.styles.slice(),
      colors: entry.colors.slice(),
      borderColors: entry.borderColors.slice(),
      baseColor: entry.baseColor,
      visible: integrityStreamVisibility.get(entry.stream.key) ?? false,
    };
    dataset.pointRadius = (ctx) => {
      const arr = ctx.dataset.xdpIntegrity?.radii;
      if (Array.isArray(arr) && ctx.dataIndex < arr.length) {
        return arr[ctx.dataIndex];
      }
      return ctx.raw?.is_ok ? 5 : 7;
    };
    dataset.pointHoverRadius = (ctx) => {
      const arr = ctx.dataset.xdpIntegrity?.hoverRadii;
      if (Array.isArray(arr) && ctx.dataIndex < arr.length) {
        return arr[ctx.dataIndex];
      }
      return ctx.raw?.is_ok ? 7 : 9;
    };
    dataset.pointHitRadius = (ctx) => {
      const arr = ctx.dataset.xdpIntegrity?.hitRadii;
      if (Array.isArray(arr) && ctx.dataIndex < arr.length) {
        return arr[ctx.dataIndex];
      }
      return ctx.raw?.is_ok ? 9 : 11;
    };
    dataset.pointBorderWidth = (ctx) => {
      const arr = ctx.dataset.xdpIntegrity?.borderWidths;
      if (Array.isArray(arr) && ctx.dataIndex < arr.length) {
        return arr[ctx.dataIndex];
      }
      return ctx.raw?.is_ok ? 1 : 2;
    };
    dataset.pointStyle = (ctx) => {
      const arr = ctx.dataset.xdpIntegrity?.styles;
      if (Array.isArray(arr) && ctx.dataIndex < arr.length) {
        return arr[ctx.dataIndex];
      }
      return getIntegrityPointStyle(ctx.raw?.type);
    };
    dataset.pointBackgroundColor = (ctx) => {
      const arr = ctx.dataset.xdpIntegrity?.colors;
      if (Array.isArray(arr) && ctx.dataIndex < arr.length) {
        return arr[ctx.dataIndex];
      }
      return ctx.dataset.xdpIntegrity?.baseColor || '#22c55e';
    };
    dataset.pointBorderColor = (ctx) => {
      const arr = ctx.dataset.xdpIntegrity?.borderColors;
      if (Array.isArray(arr) && ctx.dataIndex < arr.length) {
        return arr[ctx.dataIndex];
      }
      return ctx.dataset.xdpIntegrity?.baseColor || '#22c55e';
    };
    integrityDatasets.push(dataset);
  });
  integrityDatasets.sort((a, b) => a.label.localeCompare(b.label, 'zh-CN'));

  if (thresholdDataset) {
    if (alertThresholdBps > 0 && pointCount > 0) {
      const thresholdMbps = alertThresholdBps / UNIT_SCALE;
      thresholdDataset.data = rawMaxValues.map((_, idx) => ({ x: idx, y: thresholdMbps }));
      thresholdDataset.hidden = false;
    } else {
      thresholdDataset.data = [];
      thresholdDataset.hidden = true;
    }
  }

  maxChart.data.datasets = [...baseDatasets, ...integrityDatasets];

  const scaleX = maxChart.options.scales.x;
  scaleX.min = 0;
  scaleX.max = pointCount > 0 ? Math.max(pointCount - 1, 1) : 1;
  scaleX.ticks.stepSize = pointCount > 1 ? Math.max(pointCount - 1, 1) : 1;
  scaleX.ticks.callback = (value) => {
    if (!bucketRanges.length) {
      return '';
    }
    if (value === 0) {
      const first = bucketRanges[0];
      return ['开始', first.startTime];
    }
    if (value === bucketRanges.length - 1) {
      const last = bucketRanges[bucketRanges.length - 1];
      return ['结束', last.endTime];
    }
    return '';
  };

  maxChart.update();

  const reversed = [...buckets].reverse();

  const alertList = document.getElementById('alertList');
  if (alertList) {
    alertList.innerHTML = '';
    if (alertThresholdBps <= 0) {
      const li = document.createElement('li');
      li.classList.add('empty');
      li.textContent = '未设置阈值，无法标记窗口。';
      alertList.appendChild(li);
    } else {
      const flagged = reversed.filter((bucket) => bucket.max_bps >= alertThresholdBps);
      if (flagged.length === 0) {
        const li = document.createElement('li');
        li.classList.add('empty');
        li.textContent = '最近窗口中没有超过阈值的流量。';
        alertList.appendChild(li);
      } else {
        for (const bucket of flagged) {
          const li = document.createElement('li');
          li.innerHTML = `<strong>${toLocal(bucket.start_ts)}</strong> → ${toLocal(bucket.end_ts)} · ${windowSeconds || 0}s 窗口 · 最大 ${formatBps(bucket.max_bps)}`;
          alertList.appendChild(li);
        }
      }
    }
  }
}

function renderIntegrityEvents(events) {
  const streams = new Map();

  if (Array.isArray(events)) {
    for (const event of events) {
      const stream = resolveIntegrityStream(event);
      if (!stream) {
        continue;
      }
      let entry = streams.get(stream.key);
      if (!entry) {
        entry = { stream, events: [] };
        streams.set(stream.key, entry);
      }
      entry.events.push(event);
    }
  }

  streams.forEach((entry) => {
    entry.events.sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));
    entry.latest = entry.events.length ? entry.events[entry.events.length - 1] : null;
  });

  if (integritySummaryEl) {
    if (!streams.size) {
      integritySummaryEl.textContent = '未接收完整性数据';
      integritySummaryEl.classList.remove('ok');
      integritySummaryEl.classList.remove('bad');
    } else {
      const sortedStreams = Array.from(streams.values()).sort((a, b) => {
        const aCat = isTradeCategory(a.stream.category) ? 1 : 0;
        const bCat = isTradeCategory(b.stream.category) ? 1 : 0;
        if (aCat !== bCat) {
          return aCat - bCat;
        }
        return a.stream.label.localeCompare(b.stream.label, 'zh-CN');
      });
      const aggregated = new Map();
      for (const entry of sortedStreams) {
        const latest = entry.latest;
        if (!latest) {
          continue;
        }
        const ok = latest.is_ok ?? String(latest.status || '').toLowerCase() === 'ok';
        const category = String(entry.stream.category || latest.type || 'integrity').toLowerCase();
        const isTrade = isTradeCategory(category) || String(latest.type || '').toLowerCase() === 'trade';
        const exchangeLabel = (latest.exchange || '').trim() || '未知来源';
        const summaryKey = `${exchangeLabel.toLowerCase()}::${isTrade ? 'trade' : category}`;
        const summaryLabel = formatIntegrityStatusLabel(latest, entry.stream, { includeSymbol: false });
        const existing = aggregated.get(summaryKey);
        if (!existing) {
          aggregated.set(summaryKey, {
            label: summaryLabel,
            ok,
            order: isTrade ? 1 : 0,
          });
        } else {
          existing.ok = existing.ok && ok;
        }
      }
      const summaryItems = Array.from(aggregated.values()).sort(
        (a, b) => a.order - b.order || a.label.localeCompare(b.label, 'zh-CN'),
      );
      const summaryParts = summaryItems.map((item) => `${item.label}: ${item.ok ? '正常' : '异常'}`);
      const overallOk = summaryItems.every((item) => item.ok);
      if (!summaryParts.length) {
        integritySummaryEl.textContent = '未接收完整性数据';
        integritySummaryEl.classList.remove('ok');
        integritySummaryEl.classList.remove('bad');
      } else {
        integritySummaryEl.textContent = summaryParts.join(' · ');
        integritySummaryEl.classList.toggle('ok', overallOk);
        integritySummaryEl.classList.toggle('bad', !overallOk);
      }
    }
  }

  if (!integrityEventsEl) {
    return;
  }
  integrityEventsEl.innerHTML = '';

  const combined = [];
  streams.forEach((entry) => {
    for (const event of entry.events) {
      combined.push({ event, stream: entry.stream });
    }
  });

  if (!combined.length) {
    const li = document.createElement('li');
    li.classList.add('empty');
    li.textContent = '尚无完整性检查信息。';
    integrityEventsEl.appendChild(li);
    return;
  }

  combined.sort((a, b) => (b.event.timestamp || 0) - (a.event.timestamp || 0));

  const visibleItems = [];
  for (const item of combined) {
    const event = item.event;
    const stream = item.stream;
    const ok = event.is_ok ?? String(event.status || '').toLowerCase() === 'ok';
    if (ok) {
      continue;
    }
    visibleItems.push({ event, stream });
    if (visibleItems.length >= 12) {
      break;
    }
  }

  if (!visibleItems.length) {
    const li = document.createElement('li');
    li.classList.add('empty');
    li.textContent = '暂无异常事件。';
    integrityEventsEl.appendChild(li);
    return;
  }

  for (const { event, stream } of visibleItems) {
    const li = document.createElement('li');
    const ok = event.is_ok ?? String(event.status || '').toLowerCase() === 'ok';
    li.classList.add('integrity-item');
    li.classList.add(ok ? 'ok' : 'bad');
    if (event.type) {
      li.classList.add(`type-${event.type}`);
    }

    const tag = document.createElement('span');
    tag.className = `integrity-tag ${event.type || 'undefined'}`;
    tag.textContent = getIntegrityTypeLabel(event.type);
    li.appendChild(tag);

    const time = event.timestamp ? formatTimeOfDay(event.timestamp) : '未知时间';
    const timeStrong = document.createElement('strong');
    timeStrong.textContent = time;
    li.appendChild(timeStrong);

    const fragments = [];
    const streamLabel = stream.label || event.stream_label || '';
    if (streamLabel) {
      fragments.push(streamLabel);
    }
    if (event.exchange && !streamLabel.includes(event.exchange)) {
      fragments.push(event.exchange);
    }
    if (event.stage) {
      fragments.push(event.stage);
    }
    fragments.push(ok ? 'OK' : '异常');
    const extraSegments = !ok ? buildIntegrityDetailSegments(event) : [];
    if (!ok && !extraSegments.length && event.detail) {
      extraSegments.push(event.detail);
    }
    const detailText = extraSegments.length ? ` · ${extraSegments.join(' · ')}` : '';
    li.appendChild(document.createTextNode(` ${fragments.join(' · ')}${detailText}`));
    integrityEventsEl.appendChild(li);
  }
}

async function refreshData() {
  if (isRefreshing) {
    return;
  }
  isRefreshing = true;
  try {
    const [bucketPayload, integrityPayload] = await Promise.all([fetchBucketsPayload(), fetchIntegrityData()]);

    const buckets = Array.isArray(bucketPayload?.data) ? bucketPayload.data : [];
    const bucketMeta = bucketPayload?.meta;
    latestBuckets = buckets;
    latestBucketMeta = bucketMeta;

    const rawIntegrityData = Array.isArray(integrityPayload?.data) ? integrityPayload.data : [];
    const normalizedEvents = rawIntegrityData
      .map((item) => normalizeIntegrityEvent(item))
      .filter(Boolean)
      .sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));
    latestIntegrityEvents = normalizedEvents;

    const nextStreamMeta = new Map();
    normalizedEvents.forEach((event) => {
      const key = event.stream_key || event.key;
      if (!key) {
        return;
      }
      if (!nextStreamMeta.has(key)) {
        nextStreamMeta.set(key, {
          key,
          label: event.stream_label || describeStreamLabel(key),
          type: String(event.type || '').toLowerCase(),
          stage: event.stage || '',
          stream: event.stream || '',
          exchange: event.exchange || '',
        });
      }
    });
    const metaKeys = Array.isArray(integrityPayload?.meta?.keys) ? integrityPayload.meta.keys : [];
    metaKeys.forEach((item) => {
      if (!item || typeof item !== 'object') {
        return;
      }
      const key = item.key || '';
      if (!key || nextStreamMeta.has(key)) {
        return;
      }
      const typeList = Array.isArray(item.types) ? item.types.filter((value) => value) : [];
      const stageList = Array.isArray(item.stages) ? item.stages.filter((value) => value) : [];
      const type = typeList.length ? String(typeList[0]).toLowerCase() : '';
      const stage = stageList.length ? String(stageList[0]) : '';
      const exchange = String(item.exchange || '').trim();
      const labelParts = [];
      if (exchange) {
        labelParts.push(exchange);
      }
      if (stage) {
        labelParts.push(stage);
      }
      if (type) {
        labelParts.push(getIntegrityTypeLabel(type));
      }
      const fallbackLabel = labelParts.length ? labelParts.join(' · ') : key;
      nextStreamMeta.set(key, {
        key,
        label: fallbackLabel,
        type,
        stage,
        stream: item.stream || item.hostname || '',
        exchange,
      });
    });
    integrityStreamMeta.clear();
    nextStreamMeta.forEach((value, key) => {
      integrityStreamMeta.set(key, value);
    });
    const activeStreamKeys = new Set(nextStreamMeta.keys());
    integrityStreamVisibility.forEach((_, key) => {
      if (!activeStreamKeys.has(key)) {
        integrityStreamVisibility.delete(key);
      }
    });
    renderIntegrityStreamToggles();

    renderBuckets(buckets, bucketMeta, normalizedEvents);
    renderIntegrityEvents(normalizedEvents);
  } catch (error) {
    console.error('刷新仪表盘失败', error);
  } finally {
    isRefreshing = false;
  }
}

let refreshTimerId = null;
function startAutoRefresh() {
  if (refreshTimerId) {
    clearInterval(refreshTimerId);
  }
  if (refreshIntervalMs <= 0) {
    return;
  }
  refreshTimerId = setInterval(() => {
    refreshData().catch((error) => console.error('周期刷新失败', error));
  }, refreshIntervalMs);
}

async function bootstrap() {
  initChart();
  await loadStatus();
  populateIntegritySelect();
  await refreshData();
  startAutoRefresh();
}

bootstrap().catch((err) => {
  console.error('仪表盘初始化失败', err);
  if (debugInfoEl) {
    const message = err && err.message ? err.message : String(err);
    debugInfoEl.textContent = `初始化失败: ${message}`;
  }
});
