/* Lowes Scraper UI — vanilla JS, no build step. */

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

// ── Tabs ──────────────────────────────────────────────
$$(".tab").forEach((t) =>
  t.addEventListener("click", () => {
    if (t.disabled) return;
    $$(".tab").forEach((x) => x.classList.remove("active"));
    $$(".tab-panel").forEach((x) => x.classList.remove("active"));
    t.classList.add("active");
    const id = "tab-" + t.dataset.tab;
    document.getElementById(id).classList.add("active");
    if (t.dataset.tab === "queue") refreshJobs();
    if (t.dataset.tab === "dashboard") refreshDashboard();
    if (t.dataset.tab === "settings") refreshWorkers();
  })
);

// ── API helpers ──────────────────────────────────────────────
async function api(path, opts = {}) {
  let r;
  try {
    r = await fetch(path, {
      headers: { "Content-Type": "application/json" },
      ...opts,
    });
  } catch (e) {
    // Network-level failure: server crashed, blocked event loop, or port closed.
    throw new Error(
      `Network error (server unreachable). ` +
        `Check the terminal where you ran 'python app.py'. ` +
        `Original: ${e.message}`
    );
  }
  if (!r.ok) {
    let txt = "";
    try {
      txt = await r.text();
    } catch (_) {}
    throw new Error(`HTTP ${r.status}: ${txt || r.statusText}`);
  }
  return r.json();
}

// ── Dashboard ──────────────────────────────────────────────
async function refreshDashboard() {
  try {
    const stats = await api("/api/stats");
    $("#stat-total").textContent = stats.total;
    $("#stat-pending").textContent = stats.pending;
    $("#stat-claimed").textContent = stats.claimed;
    $("#stat-done").textContent = stats.done;
    $("#stat-failed").textContent = stats.failed;
    $("#stat-refill").textContent = stats.needs_refill;

    const { categories } = await api("/api/categories");
    const tbody = $("#tbl-categories tbody");
    tbody.innerHTML = "";
    for (const c of categories) {
      const pct = c.total ? Math.round((100 * (c.done || 0)) / c.total) : 0;
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${escapeHtml(c.category)}</td>
        <td>${c.total}</td>
        <td>${c.pending || 0}</td>
        <td>${c.claimed || 0}</td>
        <td>${c.done || 0}</td>
        <td>${c.failed || 0}</td>
        <td>${c.needs_refill || 0}</td>
        <td>
          <div class="progress-cell"><div style="width:${pct}%"></div></div>
          <span class="muted small">${pct}%</span>
        </td>
        <td>
          <button class="btn ghost xs" data-act="reconcile" data-cat="${escapeAttr(c.category)}">Reconcile</button>
          <button class="btn ghost xs" data-act="requeue" data-cat="${escapeAttr(c.category)}">Refresh all</button>
          <button class="btn ghost xs" data-act="retry-failed" data-cat="${escapeAttr(c.category)}"
                  ${(c.failed || 0) === 0 ? 'disabled title="no failed jobs"' : ''}>Retry Failed (${c.failed || 0})</button>
        </td>`;
      tbody.appendChild(tr);
    }
    // Populate queue category filter
    const sel = $("#q-category");
    const current = sel.value;
    sel.innerHTML = '<option value="">all</option>';
    for (const c of categories) {
      const opt = document.createElement("option");
      opt.value = c.category;
      opt.textContent = c.category;
      sel.appendChild(opt);
    }
    sel.value = current;
  } catch (e) {
    console.error("Dashboard refresh failed", e);
  }
}

$("#btn-refresh-cats").addEventListener("click", refreshDashboard);

// Delegate action buttons in category rows
$("#tbl-categories").addEventListener("click", async (ev) => {
  const btn = ev.target.closest("button[data-act]");
  if (!btn) return;
  const cat = btn.dataset.cat;
  const act = btn.dataset.act;
  try {
    if (act === "reconcile") {
      const r = await api("/api/state/reconcile", {
        method: "POST",
        body: JSON.stringify({ category: cat }),
      });
      alert(
        `Reconciled '${cat}':\n` +
        `  enqueued new: ${r.enqueued_new}\n` +
        `  marked done: ${r.marked_done}\n` +
        `  needs refill: ${r.marked_needs_refill}\n` +
        `  no detail file: ${r.no_detail_file}`
      );
    } else if (act === "requeue") {
      if (!confirm(`Move all 'done' jobs in '${cat}' back to 'needs_refill'?`)) return;
      const r = await api("/api/state/requeue-refill", {
        method: "POST",
        body: JSON.stringify({ category: cat }),
      });
      alert(`Requeued ${r.requeued} jobs as needs_refill.`);
    } else if (act === "retry-failed") {
      if (!confirm(`Retry all 'failed' jobs in '${cat}'? Their attempts will reset to 0.`)) return;
      const r = await api("/api/state/retry-failed", {
        method: "POST",
        body: JSON.stringify({ category: cat }),
      });
      alert(`Re-armed ${r.retried} failed jobs → pending.`);
    }
    refreshDashboard();
  } catch (e) {
    alert(`Action failed: ${e.message}`);
  }
});

// ── Runner ──────────────────────────────────────────────
const logEl = $("#log");
const autoScroll = $("#autoscroll");
let ws = null;

function appendLog(entry) {
  const div = document.createElement("div");
  div.className = "log-line " + (entry.level || "info");
  div.innerHTML = `<span class="t">${escapeHtml(entry.t)}</span>${escapeHtml(entry.msg)}`;
  logEl.appendChild(div);
  if (autoScroll.checked) logEl.scrollTop = logEl.scrollHeight;
  // Cap to last 2000 lines to keep DOM light
  while (logEl.childElementCount > 2000) logEl.removeChild(logEl.firstChild);
}

function connectWS() {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  ws = new WebSocket(`${proto}//${location.host}/ws/logs`);
  ws.onmessage = (ev) => {
    try {
      appendLog(JSON.parse(ev.data));
    } catch (_) {}
  };
  ws.onclose = () => setTimeout(connectWS, 1500);
  ws.onerror = () => ws.close();
}
connectWS();

$("#btn-clear-log").addEventListener("click", () => (logEl.innerHTML = ""));

// Update the workers hint with the current pool size so user knows the cap.
async function refreshWorkersHint() {
  const hint = $("#workers-hint");
  if (!hint) return;
  try {
    const r = await api("/api/config/workers?reload=0");
    hint.textContent = `pool size = ${r.worker_count} (max). Empty -> uses pool default.`;
  } catch (_) {
    hint.textContent = "";
  }
}
refreshWorkersHint();

// ── Category picker (for detail action) ─────────────────────────────────
// Loaded from /api/categories. A category is "queueable" when it has any
// pending / needs_refill / claimed (stale) jobs. The Runner sends ticked
// names as `category_names` so the worker processes them sequentially.
const catPickerLabel = $("#cat-picker-label");
const catPickerList = $("#cat-picker-list");
const catPickerSummary = $("#cat-picker-summary");

function updateCatPickerVisibility() {
  const isDetail = $("#action").value === "detail";
  catPickerLabel.style.display = isDetail ? "" : "none";
}

async function refreshCatPicker() {
  catPickerSummary.textContent = "loading…";
  try {
    const r = await api("/api/categories");
    const all = r.categories || [];
    // Only show categories with queueable work (pending + needs_refill + claimed).
    const queueable = all.filter((c) =>
      (c.pending || 0) + (c.needs_refill || 0) + (c.claimed || 0) > 0
    );
    if (queueable.length === 0) {
      catPickerList.innerHTML =
        '<div class="muted small">No categories with pending jobs. Run <code>list</code> first to enqueue products.</div>';
      catPickerSummary.textContent = "0 queueable";
      return;
    }
    // Sort by total queueable items, descending — biggest backlogs first.
    queueable.sort(
      (a, b) =>
        (b.pending + b.needs_refill + b.claimed) -
        (a.pending + a.needs_refill + a.claimed)
    );
    catPickerList.innerHTML = queueable
      .map((c) => {
        const pend = c.pending || 0;
        const refill = c.needs_refill || 0;
        const claim = c.claimed || 0;
        const done = c.done || 0;
        const fail = c.failed || 0;
        const parts = [`${pend} pending`];
        if (refill) parts.push(`${refill} refill`);
        if (claim) parts.push(`${claim} claimed`);
        const counts = parts.join(", ");
        const aside = `${done} done · ${fail} failed`;
        return `<label class="cat-picker-row">
          <input type="checkbox" class="cat-cb" value="${escapeHtml(c.category)}" />
          <span class="cat-name">${escapeHtml(c.category)}</span>
          <span class="cat-counts">${counts} <span class="muted">(${aside})</span></span>
        </label>`;
      })
      .join("");
    catPickerSummary.textContent = `${queueable.length} queueable categor${queueable.length === 1 ? "y" : "ies"}`;
  } catch (e) {
    catPickerList.innerHTML = `<div class="muted small">Failed to load: ${escapeHtml(String(e.message))}</div>`;
    catPickerSummary.textContent = "load failed";
  }
}

$("#action").addEventListener("change", updateCatPickerVisibility);
$("#btn-cat-all").addEventListener("click", (e) => {
  e.preventDefault();
  catPickerList.querySelectorAll(".cat-cb").forEach((cb) => (cb.checked = true));
});
$("#btn-cat-none").addEventListener("click", (e) => {
  e.preventDefault();
  catPickerList.querySelectorAll(".cat-cb").forEach((cb) => (cb.checked = false));
});
$("#btn-cat-refresh").addEventListener("click", (e) => {
  e.preventDefault();
  refreshCatPicker();
});
updateCatPickerVisibility();
refreshCatPicker();

$("#btn-start").addEventListener("click", async () => {
  const action = $("#action").value;
  // Multi-URL batch: textarea splits by newline. Each non-empty line is one
  // category URL the runner will process sequentially. Single line → behaves
  // exactly like before (sent as `url`); multiple lines → sent as `urls`.
  const urlsRaw = $("#url").value;
  const urls = urlsRaw
    .split(/\r?\n/)
    .map((s) => s.trim())
    .filter(Boolean);
  const pages = $("#pages").value.trim();
  const workers = $("#workers").value.trim();
  const body = { action };
  // Collect ticked categories from the picker — only meaningful for detail.
  const pickedCats = Array.from(
    catPickerList.querySelectorAll(".cat-cb:checked")
  ).map((cb) => cb.value);

  if (["list", "detail", "full"].includes(action)) {
    // URL is REQUIRED for list/full (the scraper needs a starting point)
    // but OPTIONAL for detail (queue already knows what to scrape — leaving
    // the field empty consumes pending jobs across ALL categories, OR pick
    // specific ones from the category dropdown).
    if (urls.length === 0 && action !== "detail") {
      alert("Category URL is required for list/full");
      return;
    }
    if (urls.length > 0) {
      const bad = urls.find((u) => !/^https?:\/\//i.test(u));
      if (bad) {
        alert("Each line must start with http:// or https://\nOffending: " + bad);
        return;
      }
      if (urls.length === 1) body.url = urls[0];
      else body.urls = urls;
    }
    if (action === "detail" && pickedCats.length > 0) {
      body.category_names = pickedCats;
    }
  }
  if (["list", "full"].includes(action) && pages) body.pages = pages;
  if (["detail", "full"].includes(action) && workers) {
    const n = parseInt(workers, 10);
    if (!Number.isFinite(n) || n < 1) {
      alert("Workers must be a positive integer (or empty for default)");
      return;
    }
    body.workers = n;
  }
  try {
    await api("/api/runner/start", { method: "POST", body: JSON.stringify(body) });
  } catch (e) {
    alert("Start failed: " + e.message);
  }
});

$("#btn-stop").addEventListener("click", async () => {
  try {
    await api("/api/runner/stop", { method: "POST" });
  } catch (e) {
    alert("Stop failed: " + e.message);
  }
});

async function pollRunnerStatus() {
  try {
    const s = await api("/api/runner/status");
    const pill = $("#runner-pill");
    const elapsed = $("#runner-elapsed");
    const startBtn = $("#btn-start");
    const stopBtn = $("#btn-stop");
    if (s.running) {
      pill.className = "pill running";
      pill.textContent = s.action || "running";
      elapsed.textContent = fmtElapsed(s.elapsed);
      startBtn.disabled = true;
      stopBtn.disabled = false;
    } else {
      pill.className = "pill idle";
      pill.textContent = "idle";
      elapsed.textContent = "";
      startBtn.disabled = false;
      stopBtn.disabled = true;
    }
  } catch (_) {
    /* server may be restarting */
  }
}

setInterval(pollRunnerStatus, 1000);
setInterval(refreshDashboard, 2500);

// ── Queue ──────────────────────────────────────────────
async function refreshJobs() {
  try {
    const params = new URLSearchParams();
    const st = $("#q-status").value;
    const cat = $("#q-category").value;
    const q = $("#q-search").value.trim();
    if (st) params.set("status", st);
    if (cat) params.set("category", cat);
    if (q) params.set("q", q);
    params.set("limit", "200");
    const res = await api(`/api/jobs?${params}`);
    const tbody = $("#tbl-jobs tbody");
    tbody.innerHTML = "";
    for (const j of res.jobs) {
      const tr = document.createElement("tr");
      const upd = j.updated_at ? new Date(j.updated_at * 1000).toLocaleString() : "";
      const info = j.last_error || j.missing_fields || "";
      tr.innerHTML = `
        <td><a href="#" data-pid="${escapeAttr(j.product_id)}">${escapeHtml(j.product_id)}</a></td>
        <td><span class="status-badge status-${j.status}">${j.status}</span></td>
        <td>${escapeHtml(j.category)}</td>
        <td>${j.attempts}</td>
        <td>${escapeHtml(j.worker_id || "")}</td>
        <td>${upd}</td>
        <td title="${escapeAttr(info)}">${escapeHtml((info || "").slice(0, 60))}</td>`;
      tbody.appendChild(tr);
    }
    $("#q-total").textContent = `${res.jobs.length} of ${res.total} jobs`;
  } catch (e) {
    console.error("Jobs refresh failed", e);
  }
}

$("#btn-q-refresh").addEventListener("click", refreshJobs);
["q-status", "q-category"].forEach((id) =>
  $("#" + id).addEventListener("change", refreshJobs)
);
$("#q-search").addEventListener("keydown", (e) => {
  if (e.key === "Enter") refreshJobs();
});

// ── Utils ──────────────────────────────────────────────
function escapeHtml(s) {
  return String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}
function escapeAttr(s) {
  return escapeHtml(s).replace(/"/g, "&quot;");
}
function fmtElapsed(sec) {
  if (!sec) return "";
  sec = Math.floor(sec);
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = sec % 60;
  if (h) return `${h}h ${m}m ${s}s`;
  if (m) return `${m}m ${s}s`;
  return `${s}s`;
}

// ── Settings ──────────────────────────────────────────────
async function refreshWorkers(opts = {}) {
  const info = $("#workers-reload-info");
  if (info) info.textContent = "reloading...";
  try {
    // Explicit reload first (POST) so the server re-reads config.py fresh.
    // Falls through to the GET even if reload errors.
    if (opts.explicit !== false) {
      try {
        await api("/api/config/reload", { method: "POST" });
      } catch (e) {
        console.warn("Config reload endpoint failed (will still GET workers):", e);
      }
    }
    const r = await api("/api/config/workers");
    const tbody = $("#tbl-workers tbody");
    tbody.innerHTML = "";
    for (const w of r.workers) {
      const tr = document.createElement("tr");
      const profileCell = w.profile || '<span class="muted">(empty — auto-create)</span>';
      const proxyCell = w.proxy_masked || '<span class="muted">(none — uses local IP)</span>';
      tr.innerHTML = `
        <td>${w.worker}</td>
        <td>${w.active ? '<span class="status-badge status-done">yes</span>' : '<span class="muted">no</span>'}</td>
        <td>${profileCell}</td>
        <td>${proxyCell}</td>
        <td class="status-cell"><span class="muted">unchecked</span></td>
        <td>
          <button class="btn ghost xs" data-act="test-worker" data-w="${w.worker}">Test IP</button>
          <button class="btn ghost xs" data-act="setup-worker" data-w="${w.worker}">Setup</button>
        </td>`;
      tbody.appendChild(tr);
    }
    if (info) {
      const ts = new Date().toLocaleTimeString();
      info.textContent = `WORKER_COUNT=${r.worker_count} · reloaded @ ${ts}`;
    }
  } catch (e) {
    console.error("Workers refresh failed", e);
    if (info) info.textContent = `error: ${e.message}`;
  }
}

$("#btn-workers-refresh").addEventListener("click", () => refreshWorkers({ explicit: true }));

$("#tbl-workers").addEventListener("click", async (ev) => {
  const btn = ev.target.closest("button[data-act]");
  if (!btn) return;
  const w = btn.dataset.w;
  const row = btn.closest("tr");
  const statusCell = row.querySelector(".status-cell");
  const act = btn.dataset.act;
  statusCell.innerHTML = '<span class="muted">working...</span>';
  try {
    if (act === "test-worker") {
      // Just test the proxy string from this row
      const proxyRaw = (await api("/api/config/workers")).workers[w].proxy;
      if (!proxyRaw) {
        statusCell.innerHTML = '<span class="muted">no proxy configured</span>';
        return;
      }
      const r = await api("/api/proxy/test", {
        method: "POST",
        body: JSON.stringify({ proxy: proxyRaw }),
      });
      if (r.ok) {
        statusCell.innerHTML =
          `<span class="status-badge status-done">${r.ip}</span> ` +
          `<span class="muted small">${r.rtt_ms}ms</span>`;
      } else {
        statusCell.innerHTML =
          `<span class="status-badge status-failed">FAIL</span> ` +
          `<span class="muted small" title="${escapeAttr(r.error || '')}">${escapeHtml((r.error || '').slice(0, 50))}</span>`;
      }
    } else if (act === "setup-worker") {
      const r = await api("/api/profile/setup", {
        method: "POST",
        body: JSON.stringify({ worker: Number(w), verify: true }),
      });
      const info = r.worker;
      statusCell.innerHTML =
        `<span class="status-badge status-done">profile=${escapeHtml(info.profile_id)}</span> ` +
        `<span class="muted small">ip=${info.ip || "?"}` +
        `${info.created ? " ✨created" : ""}` +
        `${info.proxy_updated ? " 🔧updated" : ""}</span>`;
    }
  } catch (e) {
    statusCell.innerHTML = `<span class="status-badge status-failed" title="${escapeAttr(e.message)}">error</span>`;
  }
});

$("#btn-proxy-test").addEventListener("click", async () => {
  const raw = $("#proxy-test-input").value.trim();
  const out = $("#proxy-test-result");
  if (!raw) {
    out.textContent = "Enter a proxy string first.";
    return;
  }
  out.textContent = "Testing...";
  try {
    const r = await api("/api/proxy/test", {
      method: "POST",
      body: JSON.stringify({ proxy: raw }),
    });
    if (r.ok) {
      out.innerHTML =
        `<span class="status-badge status-done">OK</span> ` +
        `IP <code>${escapeHtml(r.ip)}</code> · ` +
        `${r.rtt_ms}ms · ` +
        `via ${escapeHtml(r.endpoint)}<br>` +
        `<span class="muted">parsed: ${escapeHtml(r.proxy?.scheme || "?")}://${escapeHtml(r.proxy?.host || "?")}:${r.proxy?.port}${r.proxy?.user ? ` (user=${escapeHtml(r.proxy.user)})` : ""}</span>`;
    } else {
      out.innerHTML =
        `<span class="status-badge status-failed">FAIL</span> ${escapeHtml(r.error)}`;
    }
  } catch (e) {
    out.textContent = "Error: " + e.message;
  }
});

// ── Phase 5.1: CF block banner polling ─────────────────────────────────
async function refreshCfBanner() {
  const banner = $("#cf-banner");
  const rows = $("#cf-banner-rows");
  if (!banner || !rows) return;
  try {
    const r = await api("/api/workers/live");
    const orch = r.orchestrator;
    if (!orch || !orch.workers) {
      banner.hidden = true;
      return;
    }
    const paused = orch.workers.filter((w) => w.status === "blocked_cf");
    if (paused.length === 0) {
      banner.hidden = true;
      rows.innerHTML = "";
      return;
    }
    banner.hidden = false;
    rows.innerHTML = paused
      .map(
        (w) => `
        <div class="cf-banner-row" data-worker="${escapeAttr(w.worker_id)}">
          <span class="worker-id">${escapeHtml(w.worker_id)}</span>
          <span class="profile-id">profile=${escapeHtml(w.profile_id || "?")}</span>
          <span class="pid">pid=${escapeHtml(w.cf_pending_pid || "?")}</span>
          <span class="detail" title="${escapeAttr(w.cf_pending_detail || "")}">
            ${escapeHtml((w.cf_pending_detail || "").slice(0, 60))}
          </span>
          <button class="btn primary xs" data-act="mark-solved"
                  data-w="${escapeAttr(w.worker_id)}">Mark Solved</button>
        </div>`
      )
      .join("");
  } catch (e) {
    // /api/workers/live errors are non-fatal — banner just stays/hidden.
    console.warn("CF banner poll failed:", e);
  }
}

// Delegate the "Mark Solved" clicks for all current/future banner rows.
document.addEventListener("click", async (ev) => {
  const btn = ev.target.closest('button[data-act="mark-solved"]');
  if (!btn) return;
  const worker = btn.dataset.w;
  btn.disabled = true;
  btn.textContent = "...";
  try {
    await api(`/api/workers/${encodeURIComponent(worker)}/mark-solved`, {
      method: "POST",
    });
    btn.textContent = "✓ solved";
    // Banner will disappear on next poll once worker moves out of blocked_cf.
    setTimeout(refreshCfBanner, 500);
  } catch (e) {
    btn.disabled = false;
    btn.textContent = "Mark Solved";
    alert("Mark solved failed: " + e.message);
  }
});

setInterval(refreshCfBanner, 3000);

// ── Boot ──────────────────────────────────────────────
refreshDashboard();
refreshCfBanner();
