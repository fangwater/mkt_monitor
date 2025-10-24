const chartCtx = document.getElementById('maxChart').getContext('2d');
let refreshIntervalMs = 5000;
let maxChart;
let alertThresholdBps = 0;
let windowSeconds = 0;

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

function initChart() {
  if (maxChart) {
    maxChart.destroy();
  }
  maxChart = new Chart(chartCtx, {
    type: 'line',
    data: {
      labels: [],
      datasets: [
        {
          label: '窗口最大带宽',
          data: [],
          borderColor: '#4f9cf6',
          backgroundColor: 'rgba(79, 156, 246, 0.25)',
          tension: 0.2,
          pointRadius: 0,
          pointHoverRadius: 0,
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
            label: (ctx) => `最大值: ${formatBps(ctx.parsed.y)}`,
          },
        },
      },
      scales: {
        x: {
          display: true,
          title: {
            display: true,
            text: '时间',
          },
        },
        y: {
          display: true,
          title: {
            display: true,
            text: '带宽',
          },
          ticks: {
            callback: (value) => formatBps(value),
          },
        },
      },
    },
  });
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
  if (cfg && cfg.refresh_interval_ms) {
    refreshIntervalMs = cfg.refresh_interval_ms;
  }
}

async function loadBuckets() {
  const res = await fetch('/api/buckets');
  if (!res.ok) {
    console.error('获取数据失败', await res.text());
    return;
  }
  const { data } = await res.json();
  renderBuckets(data);
}

function renderBuckets(buckets) {
  const labels = buckets.map((b) => toLocal(b.start_ts));
  const values = buckets.map((b) => b.max_bps);
  const mainDataset = maxChart.data.datasets[0];
  const thresholdDataset = maxChart.data.datasets[1];
  const markFlags = values.map((value) => alertThresholdBps > 0 && value >= alertThresholdBps);

  maxChart.data.labels = labels;
  mainDataset.data = values;
  mainDataset.pointRadius = markFlags.map((flag) => (flag ? 5 : 0));
  mainDataset.pointHoverRadius = markFlags.map((flag) => (flag ? 7 : 0));
  mainDataset.pointHitRadius = markFlags.map((flag) => (flag ? 9 : 1));
  mainDataset.pointBackgroundColor = markFlags.map((flag) => (flag ? '#f87171' : '#4f9cf6'));
  mainDataset.pointBorderColor = mainDataset.pointBackgroundColor;

  if (alertThresholdBps > 0) {
    thresholdDataset.data = labels.map(() => alertThresholdBps);
    thresholdDataset.hidden = false;
  } else {
    thresholdDataset.data = [];
    thresholdDataset.hidden = true;
  }

  maxChart.update();

  const tbody = document.getElementById('bucketTable');
  tbody.innerHTML = '';
  const reversed = [...buckets].reverse();
  for (const bucket of reversed) {
    const tr = document.createElement('tr');
    const shouldMark = alertThresholdBps > 0 && bucket.max_bps >= alertThresholdBps;
    if (shouldMark) {
      tr.classList.add('threshold-hit');
    }
    const markCell = shouldMark ? '<span class="mark-tag">⚠️ Mark</span>' : '';
    tr.innerHTML = `
      <td>${markCell}</td>
      <td>${toLocal(bucket.start_ts)}</td>
      <td>${toLocal(bucket.end_ts)}</td>
      <td>${formatBps(bucket.max_bps)}</td>
      <td>${bucket.sample_count}</td>
    `;
    tbody.appendChild(tr);
  }

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

async function bootstrap() {
  initChart();
  await loadStatus();
  await loadBuckets();
  setInterval(loadBuckets, refreshIntervalMs);
}

bootstrap().catch((err) => console.error(err));
