// Floodgate dashboard — talks to the Go API functions.

const $ = (id) => document.getElementById(id);
const ALGO_LABEL = {
  token_bucket: 'token bucket',
  sliding_window: 'sliding window',
  gcra: 'GCRA',
};

let keys = [];

async function loadKeys() {
  try {
    const res = await fetch('/api/keys');
    keys = res.ok ? await res.json() : [];
  } catch {
    keys = [];
  }
  renderKeys();
  renderKeyOptions();
}

function renderKeys() {
  const tb = $('keys');
  if (!keys.length) {
    tb.innerHTML =
      '<tr><td colspan="5" class="muted">No keys — click “Reset demo keys”.</td></tr>';
    return;
  }
  tb.innerHTML = keys
    .map(
      (k) => `
      <tr>
        <td>${escapeHtml(k.name)}</td>
        <td><span class="algo">${ALGO_LABEL[k.algorithm] || k.algorithm}</span></td>
        <td class="mono">${k.rate_limit}/${k.window_sec}s · burst ${k.burst}</td>
        <td><code class="mono">${k.secret}</code></td>
        <td><button class="btn ghost" data-del="${k.id}" style="height:28px;padding:0 10px">Delete</button></td>
      </tr>`,
    )
    .join('');
  tb.querySelectorAll('[data-del]').forEach((b) => {
    b.onclick = async () => {
      await fetch('/api/keys?id=' + b.dataset.del, { method: 'DELETE' });
      await loadKeys();
    };
  });
}

function renderKeyOptions() {
  $('lt-key').innerHTML = keys
    .map((k) => `<option value="${k.secret}">${escapeHtml(k.name)}</option>`)
    .join('');
}

async function seed() {
  const btn = $('seed');
  btn.disabled = true;
  btn.textContent = 'Resetting…';
  try {
    await fetch('/api/seed', { method: 'POST' });
    await loadKeys();
    await loadUsage();
  } finally {
    btn.disabled = false;
    btn.textContent = 'Reset demo keys';
  }
}

async function runBurst() {
  const secret = $('lt-key').value;
  if (!secret) return;
  const count = parseInt($('lt-count').value, 10);
  const grid = $('lt-grid');
  const summary = $('lt-summary');
  grid.innerHTML = '';
  const cells = [];
  for (let i = 0; i < count; i++) {
    const c = document.createElement('div');
    c.className = 'cell';
    grid.appendChild(c);
    cells.push(c);
  }
  $('run').disabled = true;
  summary.textContent = 'running…';

  let allowed = 0;
  let denied = 0;
  // Fire all requests; the limiter serialises them server-side.
  await Promise.all(
    cells.map(async (cell, i) => {
      try {
        const res = await fetch('/api/gateway?key=' + encodeURIComponent(secret));
        if (res.status === 200) {
          cell.className = 'cell ok';
          allowed++;
        } else {
          cell.className = 'cell no';
          denied++;
        }
      } catch {
        cell.className = 'cell no';
        denied++;
      }
      summary.textContent = `${allowed} allowed · ${denied} limited`;
      void i;
    }),
  );
  summary.textContent = `${allowed} allowed · ${denied} limited`;
  $('run').disabled = false;
  await loadUsage();
}

async function loadUsage() {
  let totals = [];
  try {
    const res = await fetch('/api/usage');
    totals = res.ok ? await res.json() : [];
  } catch {
    totals = [];
  }
  const box = $('usage');
  if (!totals.length) {
    box.innerHTML = '<span class="muted">No traffic yet — run a burst above.</span>';
    return;
  }
  const byId = Object.fromEntries(keys.map((k) => [k.id, k]));
  box.innerHTML = totals
    .map((u) => {
      const k = byId[u.key_id];
      const total = u.allowed + u.denied || 1;
      const okPct = (u.allowed / total) * 100;
      return `
        <div style="margin-bottom:14px">
          <div class="row" style="justify-content:space-between;margin-bottom:5px">
            <span style="font-size:13px">${k ? escapeHtml(k.name) : u.key_id}</span>
            <span class="mono">${u.allowed} ok · ${u.denied} limited</span>
          </div>
          <div class="bar">
            <div style="width:${okPct}%;background:#34d399"></div>
            <div style="width:${100 - okPct}%;background:#fb7185"></div>
          </div>
        </div>`;
    })
    .join('');
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"]/g, (c) =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c],
  );
}

$('seed').onclick = seed;
$('run').onclick = runBurst;
loadKeys().then(loadUsage);
