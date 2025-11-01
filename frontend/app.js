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
};
const INTEGRITY_FAIL_COLOR = '#f87171';
const INTEGRITY_POINT_STYLE = {
  trade: 'circle',
  inc_seq: 'triangle',
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
const INTEGRITY_TOGGLE_VISIBLE_TYPES = new Set(['trade']);
const APP_VERSION = '20251101-6';

console.info(`[Integrity] bundle ${APP_VERSION}`);

let refreshIntervalMs = 5000;
let maxChart;
let alertThresholdBps = 0;
let windowSeconds = 0;
const UNIT_SCALE = 1_000_000; // 将 bps 转换为 Mbps 以便图表展示
let bucketRanges = [];
const integrityKeyMeta = new Map();
const integrityToggleInputs = new Map();
let integritySelectedKey = null;
let integrityKeys = [];
const integrityActiveTypes = new Set();
let isRefreshing = false;
let integritySnapshotByBucket = [];

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
  inc_seq: 0.52,
  default: 0.32,
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
  const typeRaw = event.stream_category || event.type;
  const type = String(typeRaw || '').toLowerCase();
  const exchange = String(event.exchange || '').trim();
  const symbol = String(event.symbol || '').trim();

  let key;
  let label;
  let category;
  if (type === 'trade') {
    const exchangePart = exchange || '未知交易所';
    const normalizedExchange = (exchange || 'unknown').trim();
    const exchangeKey = normalizedExchange.toLowerCase();
    const isBatch = Boolean(event.trade_batch) || symbol === '__batch__';
    if (isBatch) {
      key = `${exchangeKey}::trade::__batch__`;
      label = `${exchangePart} TRADE 汇总`;
    } else {
      const symbolPart = symbol || '未指定合约';
      const normalizedSymbol = symbol || '*';
      const normalizedSymbolKey = normalizedSymbol.toUpperCase();
      key = `${exchangeKey}::trade::${normalizedSymbolKey}`;
      label = `${symbolPart}-${exchangePart}-trade（合约）`;
    }
    const exchangeSlug = exchangeKey.replace(/[^a-z0-9]+/g, '-');
    category = `${TRADE_CATEGORY_PREFIX}${exchangeSlug || 'unknown'}`;
    event.stream_is_batch = isBatch;
  } else {
    const exchangePart = exchange || '未知来源';
    const typeLabel = type ? getIntegrityTypeLabel(type) : '完整性';
    const normalizedType = type || 'generic';
    const exchangeKey = (exchange || 'unknown').toLowerCase();
    key = `${exchangeKey}::${normalizedType}`;
    label = exchange ? `${exchangePart} ${typeLabel}` : typeLabel;
    category = normalizedType;
  }

  if (event.stream_key !== key) {
    event.stream_key = key;
  }
  if (event.stream_label !== label) {
    event.stream_label = label;
  }
  if (event.stream_category !== category) {
    event.stream_category = category;
  }

  integrityStreamLabels.set(key, label);
  return { key, label, category };
}

function describeStreamLabel(key) {
  return integrityStreamLabels.get(key) || key;
}

function isTradeCategory(category) {
  return typeof category === 'string' && category.startsWith(TRADE_CATEGORY_PREFIX);
}

function ensureActiveTypes() {
  if (integrityActiveTypes.size) {
    return;
  }
  const meta = integritySelectedKey ? integrityKeyMeta.get(integritySelectedKey) : null;
  const types = meta && Array.isArray(meta.types) ? meta.types : [];
  for (const type of types) {
    integrityActiveTypes.add(type);
    const input = integrityToggleInputs.get(type);
    if (input) {
      input.checked = true;
    }
  }
}

function formatIntegrityStatusLabel(event, stream, { includeSymbol = false } = {}) {
  const exchangeRaw = (event?.exchange || '').trim();
  const fallbackLabel = stream?.label || '';
  const fallbackExchange = fallbackLabel.includes(' ')
    ? fallbackLabel.split(' ')[0]
    : fallbackLabel.split('·')[0];
  const exchangeLabel = exchangeRaw || fallbackExchange || '未知来源';
  const categoryRaw = String(stream?.category || event?.type || '').toLowerCase();
  const isTrade = isTradeCategory(categoryRaw) || String(event?.type || '').toLowerCase() === 'trade';
  const typeLabel = isTrade ? 'TRADE' : getIntegrityTypeLabel(categoryRaw || event?.type);
  const parts = [];
  const isBatch = Boolean(event?.trade_batch);
  if (includeSymbol && event?.symbol && !(isBatch && event.symbol === '__batch__')) {
    parts.push(event.symbol);
  }
  parts.push(exchangeLabel);
  if (typeLabel) {
    parts.push(typeLabel);
  }
  if (includeSymbol && isBatch && (!event?.symbol || event.symbol === '__batch__')) {
    parts.push('汇总');
  }
  return parts.join(' ').trim();
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
                if (raw.detail) {
                  message += ` · ${raw.detail}`;
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

function formatIntegrityOption(meta) {
  const hostLabel = meta.hostname || '未知主机';
  const ifaceLabel = meta.interface ? ` · ${meta.interface}` : '';
  const typeLabel = meta.types && meta.types.length ? ` (${meta.types.map(getIntegrityTypeLabel).join(', ')})` : '';
  return `${hostLabel}${ifaceLabel}${typeLabel}`;
}

function populateIntegritySelect(keys) {
  if (!integritySelectEl) {
    return;
  }
  integritySelectEl.innerHTML = '';
  integrityKeyMeta.clear();
  integrityToggleInputs.clear();
  integrityKeys = keys || [];

  if (!integrityKeys.length) {
    const option = document.createElement('option');
    option.value = '';
    option.textContent = '无可用数据';
    integritySelectEl.appendChild(option);
    integritySelectEl.disabled = true;
    integritySelectedKey = null;
    integrityActiveTypes.clear();
    rebuildIntegrityToggles();
    return;
  }

  for (const item of integrityKeys) {
    const meta = {
      key: item.key,
      hostname: item.hostname || '',
      interface: item.interface || '',
      types: Array.isArray(item.types) ? item.types.filter((t) => t) : [],
    };
    integrityKeyMeta.set(meta.key, meta);
    const option = document.createElement('option');
    option.value = meta.key;
    option.textContent = formatIntegrityOption(meta);
    integritySelectEl.appendChild(option);
  }

  const hasSelected = integritySelectedKey && integrityKeyMeta.has(integritySelectedKey);
  if (!hasSelected) {
    integritySelectedKey = integrityKeys[0].key;
  }

  integritySelectEl.value = integritySelectedKey || '';
  integritySelectEl.disabled = false;
  rebuildIntegrityToggles();
}

function rebuildIntegrityToggles() {
  if (!integrityTogglesEl) {
    return;
  }
  integrityTogglesEl.innerHTML = '';
  integrityToggleInputs.clear();
  const meta = integritySelectedKey ? integrityKeyMeta.get(integritySelectedKey) : null;
  const types = meta && Array.isArray(meta.types) && meta.types.length ? [...meta.types] : [];

  if (!types.length) {
    integrityActiveTypes.clear();
    return;
  }

  const visibleTypes = [];
  const hiddenTypes = [];
  for (const type of types) {
    if (INTEGRITY_TOGGLE_VISIBLE_TYPES.has(type)) {
      visibleTypes.push(type);
    } else {
      hiddenTypes.push(type);
    }
  }
  visibleTypes.sort((a, b) => a.localeCompare(b));
  const previousSelection = new Set(integrityActiveTypes);
  integrityActiveTypes.clear();

  hiddenTypes.forEach((type) => integrityActiveTypes.add(type));

  let initialTypes = visibleTypes.filter((type) => previousSelection.has(type));
  if (!initialTypes.length && visibleTypes.length) {
    initialTypes = [...visibleTypes];
  }
  initialTypes.forEach((type) => integrityActiveTypes.add(type));

  if (!visibleTypes.length) {
    integrityToggleInputs.clear();
    return;
  }

  for (const type of visibleTypes) {
    const label = document.createElement('label');
    label.className = 'integrity-toggle';
    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.value = type;
    const shouldCheck = initialTypes.includes(type);
    checkbox.checked = shouldCheck;
    if (shouldCheck) {
      integrityActiveTypes.add(type);
    }
    checkbox.addEventListener('change', () => {
      if (checkbox.checked) {
        integrityActiveTypes.add(type);
      } else {
        if (integrityActiveTypes.size <= 1) {
          checkbox.checked = true;
          return;
        }
        integrityActiveTypes.delete(type);
      }
      refreshData().catch((error) => console.error('刷新完整性数据失败', error));
    });
    integrityToggleInputs.set(type, checkbox);

    const span = document.createElement('span');
    span.textContent = getIntegrityTypeLabel(type);

    label.appendChild(checkbox);
    label.appendChild(span);
    integrityTogglesEl.appendChild(label);
  }
}

async function loadIntegrityMeta() {
  try {
    const params = new URLSearchParams({ limit: '1', meta: '1' });
    const res = await fetch(`/api/integrity?${params.toString()}`);
    if (!res.ok) {
      console.error('获取完整性元数据失败', await res.text());
      return;
    }
    const payload = await res.json();
    integrityKeys = payload.meta?.keys || [];
    populateIntegritySelect(integrityKeys);
  } catch (error) {
    console.error('loadIntegrityMeta error', error);
  }
}

async function fetchIntegritySeriesForType(type) {
  if (!integritySelectedKey) {
    return [];
  }
  try {
    const targetMeta = integrityKeyMeta.get(integritySelectedKey);
    if (!targetMeta) {
      return [];
    }
    const params = new URLSearchParams({ limit: '180', type });

    if (targetMeta?.hostname) {
      params.set('hostname', targetMeta.hostname);
    }
    if (targetMeta?.interface) {
      params.set('interface', targetMeta.interface);
    }

    const res = await fetch(`/api/integrity?${params.toString()}`);
    if (!res.ok) {
      console.error('获取完整性数据失败', await res.text());
      return [];
    }
    const payload = await res.json();
    return (payload.data || []).map((item) => {
      const rawBatchItems = Array.isArray(item.trade_batch_items) ? item.trade_batch_items : [];
      const tradeBatchItems = rawBatchItems
        .map((child) => {
          if (!child) {
            return null;
          }
          const timestampValue = Number(child.timestamp);
          const normalizedTimestamp = Number.isFinite(timestampValue) && timestampValue > 0 ? timestampValue : 0;
          return {
            symbol: child.symbol || '',
            status: String(child.status || '').toLowerCase(),
            detail: child.detail,
            minute: Number(child.minute) || 0,
            timestamp: normalizedTimestamp,
            timestamp_iso:
              child.timestamp_iso
              || (normalizedTimestamp ? new Date(normalizedTimestamp * 1000).toISOString() : null),
          };
        })
        .filter(Boolean);
      const record = {
        timestamp: Number(item.timestamp) || 0,
        timestamp_iso: item.timestamp_iso,
        status: String(item.status || '').toLowerCase(),
        detail: item.detail,
        is_ok: item.is_ok ?? String(item.status || '').toLowerCase() === 'ok',
        exchange: item.exchange,
        symbol: item.symbol,
        minute: item.minute,
        hostname: item.hostname || targetMeta?.hostname || '',
        interface: item.interface || targetMeta?.interface || '',
        type: item.type || type,
        trade_batch: Boolean(item.trade_batch),
        trade_batch_size: Number(item.trade_batch_size) || tradeBatchItems.length,
        trade_batch_failures:
          Number(item.trade_batch_failures)
          || tradeBatchItems.reduce((acc, child) => (child.status === 'ok' ? acc : acc + 1), 0),
        trade_batch_items: tradeBatchItems,
      };
      resolveIntegrityStream(record);
      return record;
    });
  } catch (error) {
    console.error('fetchIntegritySeries error', error);
    return [];
  }
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
  const incLines = [];
  const tradeLines = [];
  for (const [streamKey, event] of entries) {
    const stream = resolveIntegrityStream(event);
    if (!stream) {
      continue;
    }
    const ok = event.is_ok ?? event.isOk ?? String(event.status || '').toLowerCase() === 'ok';
    const timeLabel = event.timestamp ? formatTimeOfDay(event.timestamp) : '未知时间';
    if (isTradeCategory(stream.category) && event.trade_batch) {
      const baseLabel = formatIntegrityStatusLabel(event, stream, { includeSymbol: false });
      const items = Array.isArray(event.trade_batch_items) ? event.trade_batch_items : [];
      const failingItems = items.filter(
        (item) => String(item?.status || '').toLowerCase() !== 'ok',
      );
      if (!failingItems.length) {
        const detailText = event.detail ? ` · ${event.detail}` : '';
        tradeLines.push(`${baseLabel}: ${timeLabel} 正常${detailText}`);
      } else {
        failingItems.forEach((item) => {
          const symbolLabel = item?.symbol || '未指定合约';
          const itemTime = Number(item?.timestamp) ? formatTimeOfDay(Number(item.timestamp)) : timeLabel;
          const detailSuffix = item?.detail ? ` · ${item.detail}` : '';
          tradeLines.push(`${baseLabel} ${symbolLabel}: ${itemTime} 异常${detailSuffix}`);
        });
      }
      continue;
    }
    const label = formatIntegrityStatusLabel(event, stream, { includeSymbol: true });
    const statusLabel = ok ? '正常' : '异常';
    let line = `${label}: ${timeLabel} ${statusLabel}`;
    if (!ok && event.detail) {
      line += ` · ${event.detail}`;
    }
    if (isTradeCategory(stream.category)) {
      tradeLines.push(line);
    } else {
      incLines.push(line);
    }
  }
  incLines.sort((a, b) => a.localeCompare(b, 'zh-CN'));
  tradeLines.sort((a, b) => a.localeCompare(b, 'zh-CN'));
  const lines = [...incLines];
  if (tradeLines.length > TRADE_TOOLTIP_LIMIT) {
    lines.push(...tradeLines.slice(0, TRADE_TOOLTIP_LIMIT));
    lines.push(`… 其余 ${tradeLines.length - TRADE_TOOLTIP_LIMIT} 个交易流`);
  } else {
    lines.push(...tradeLines);
  }
  return lines.length ? lines : ['无完整性数据'];
}

function renderBuckets(buckets, meta = undefined, integrityEvents = []) {
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
  const selectedMeta = integritySelectedKey ? integrityKeyMeta.get(integritySelectedKey) : null;

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
      integrityKey: integritySelectedKey,
      integrityPoints: integrityEvents.length,
      integrityStreams: (() => {
        const unique = new Set();
        for (const event of integrityEvents) {
          const streamKey = event.stream_key || resolveIntegrityStream(event)?.key;
          if (streamKey) {
            unique.add(streamKey);
          }
        }
        return unique.size;
      })(),
      integrityTypes: Array.from(integrityActiveTypes),
      integrityHost: selectedMeta ? {
        hostname: selectedMeta.hostname,
        interface: selectedMeta.interface,
      } : null,
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
    const isTrade = isTradeCategory(stream.category);
    const isOk = event.is_ok ?? event.isOk ?? String(event.status || '').toLowerCase() === 'ok';
    const eventType = event.type || '';
    const yValue = isTrade ? getIntegrityYPosition(stream.category, isOk, stream.key) : 1;

    let entry = overlayByStream.get(stream.key);
    if (!entry && isTrade) {
      const baseColor = getIntegrityStreamColor(stream.key);
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
      overlayByStream.set(stream.key, entry);
    }
    if (!entry) {
      // 非 trade 类型只用于 tooltip 统计，不绘制散点
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
      stream_key: stream.key,
      stream_label: stream.label,
      stream_category: stream.category,
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
    dataset.order = 20;
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

function renderIntegrityEvents(eventsByType) {
  const streams = new Map();
  const selectedMeta = integritySelectedKey ? integrityKeyMeta.get(integritySelectedKey) : null;

  eventsByType.forEach((events) => {
    if (!Array.isArray(events)) {
      return;
    }
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
  });

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
      const prefix = selectedMeta
        ? `${selectedMeta.hostname || '未知主机'}${selectedMeta.interface ? ' · ' + selectedMeta.interface : ''}`
        : '';
      if (!summaryParts.length) {
        integritySummaryEl.textContent = prefix ? `${prefix} · 未接收完整性数据` : '未接收完整性数据';
        integritySummaryEl.classList.remove('ok');
        integritySummaryEl.classList.remove('bad');
      } else {
        integritySummaryEl.textContent = prefix
          ? `${prefix} · ${summaryParts.join(' · ')}`
          : summaryParts.join(' · ');
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
    if (event.hostname) {
      fragments.push(event.hostname);
    }
    if (event.interface) {
      fragments.push(event.interface);
    }
    fragments.push(ok ? 'OK' : '异常');
    const detailText = event.detail ? ` · ${event.detail}` : '';
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
    ensureActiveTypes();
    if (!integrityActiveTypes.size && integrityToggleInputs.size) {
      integrityToggleInputs.forEach((input, type) => {
        if (input) {
          input.checked = true;
          integrityActiveTypes.add(type);
        }
      });
    }

    const activeTypes = Array.from(integrityActiveTypes);
    const fetchTasks = [fetchBucketsPayload()];
    for (const type of activeTypes) {
      fetchTasks.push(fetchIntegritySeriesForType(type));
    }
    const results = await Promise.all(fetchTasks);
    const bucketPayload = results[0];
    const buckets = bucketPayload?.data || [];
    const meta = bucketPayload?.meta;
    const eventsByType = new Map();
    activeTypes.forEach((type, idx) => {
      const events = results[idx + 1] || [];
      eventsByType.set(type, events);
    });
    const combinedEvents = [];
    eventsByType.forEach((events, type) => {
      for (const event of events) {
        combinedEvents.push({ ...event, type });
      }
    });
    combinedEvents.sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));
    renderBuckets(buckets, meta, combinedEvents);
    renderIntegrityEvents(eventsByType);
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

if (integritySelectEl) {
  integritySelectEl.addEventListener('change', () => {
    const value = integritySelectEl.value;
    integritySelectedKey = value || null;
    rebuildIntegrityToggles();
    refreshData().catch((error) => console.error('切换完整性数据源失败', error));
  });
}

async function bootstrap() {
  initChart();
  await loadStatus();
  await loadIntegrityMeta();
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
