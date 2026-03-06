const output = document.getElementById("output");
const uploadMsg = document.getElementById("uploadMsg");
const apiInput = document.getElementById("apiBase");
const deployWarning = document.getElementById("deployWarning");
const opsAuthBadge = document.getElementById("opsAuthBadge");

if (
  apiInput.value === "/api" &&
  ["127.0.0.1", "localhost"].includes(window.location.hostname) &&
  window.location.port === "3000"
) {
  apiInput.value = "http://127.0.0.1:8000";
}

if (window.location.hostname.endsWith(".vercel.app")) {
  deployWarning.style.display = "block";
}

const PAGE_SIZE = 15;
let driftAll = [];
let driftPage = 1;
let lineageGraphAll = { nodes: [], edges: [] };
let lineagePage = 1;
let pipelineLastRun = null;

function apiBase() {
  return document.getElementById("apiBase").value.replace(/\/$/, "");
}

async function callApi(path, options = {}) {
  const url = `${apiBase()}${path}`;
  const headers = { ...(options.headers || {}) };
  const opsApiKey = document.getElementById("opsApiKey")?.value?.trim();
  if (path.startsWith("/ops/") && opsApiKey) {
    headers["x-api-key"] = opsApiKey;
  }
  const res = await fetch(url, { ...options, headers });
  const text = await res.text();
  let payload;
  try {
    payload = text ? JSON.parse(text) : {};
  } catch {
    payload = { raw: text };
  }
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText}: ${JSON.stringify(payload)}`);
  }
  return payload;
}

function setOpsAuthBadge(label, color = "#334155", bg = "#eef3fb") {
  if (!opsAuthBadge) return;
  opsAuthBadge.textContent = `Ops auth: ${label}`;
  opsAuthBadge.style.color = color;
  opsAuthBadge.style.background = bg;
}

async function refreshOpsAuthStatus() {
  try {
    const url = `${apiBase()}/ops/runtime`;
    const anonymous = await fetch(url);
    if (anonymous.status === 401) {
      setOpsAuthBadge("enabled", "#92400e", "#fff7ed");
      return;
    }
    if (anonymous.ok) {
      setOpsAuthBadge("disabled", "#166534", "#ecfdf3");
      return;
    }
    setOpsAuthBadge(`unknown (${anonymous.status})`);
  } catch {
    setOpsAuthBadge("unreachable", "#991b1b", "#fef2f2");
  }
}

function show(obj) {
  output.textContent = JSON.stringify(obj, null, 2);
}

function fillSelect(selectEl, options, getLabel, getValue) {
  selectEl.innerHTML = "";
  for (const item of options) {
    const opt = document.createElement("option");
    opt.textContent = getLabel(item);
    opt.value = getValue(item);
    selectEl.appendChild(opt);
  }
}

function paginate(items, page) {
  const totalPages = Math.max(1, Math.ceil(items.length / PAGE_SIZE));
  const safePage = Math.min(Math.max(1, page), totalPages);
  const start = (safePage - 1) * PAGE_SIZE;
  return {
    pageItems: items.slice(start, start + PAGE_SIZE),
    page: safePage,
    totalPages,
    totalItems: items.length,
  };
}

function filteredDriftEvents() {
  const sev = document.getElementById("driftSeverity").value;
  if (sev === "ALL") return driftAll;
  return driftAll.filter((e) => e.severity === sev);
}

function renderDriftEvents() {
  const events = filteredDriftEvents();
  const paging = paginate(events, driftPage);
  driftPage = paging.page;

  const tbody = document.querySelector("#driftTable tbody");
  tbody.innerHTML = "";
  for (const event of paging.pageItems) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${event.dataset_name ?? ""}</td>
      <td>${event.change_type ?? ""}</td>
      <td>${event.column_name ?? ""}</td>
      <td>${event.severity ?? ""}</td>
      <td>${event.old_value ?? ""}</td>
      <td>${event.new_value ?? ""}</td>
    `;
    tbody.appendChild(tr);
  }

  document.getElementById("driftPageInfo").textContent =
    `Page ${paging.page}/${paging.totalPages} (${paging.totalItems} rows)`;
}

function edgeTypeFiltered(graph) {
  const edgeType = document.getElementById("lineageEdgeType").value;
  if (edgeType === "ALL") return graph.edges || [];
  return (graph.edges || []).filter((e) => e.edge_type === edgeType);
}

function refreshLineageEdgeTypeOptions(graph) {
  const select = document.getElementById("lineageEdgeType");
  const current = select.value;
  const types = Array.from(new Set((graph.edges || []).map((e) => e.edge_type))).sort();
  select.innerHTML = '<option value="ALL">ALL</option>';
  for (const t of types) {
    const opt = document.createElement("option");
    opt.value = t;
    opt.textContent = t;
    select.appendChild(opt);
  }
  if (types.includes(current)) select.value = current;
}

function renderLineageEdges(graph = lineageGraphAll) {
  const nodes = Object.fromEntries((graph.nodes || []).map((n) => [n.node_id, n]));
  const filteredEdges = edgeTypeFiltered(graph);
  const paging = paginate(filteredEdges, lineagePage);
  lineagePage = paging.page;

  const tbody = document.querySelector("#lineageEdgeTable tbody");
  tbody.innerHTML = "";
  for (const edge of paging.pageItems) {
    const from = nodes[edge.from_node_id]?.display_name || edge.from_node_id;
    const to = nodes[edge.to_node_id]?.display_name || edge.to_node_id;
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${edge.edge_type ?? ""}</td>
      <td>${from}</td>
      <td>${to}</td>
    `;
    tbody.appendChild(tr);
  }

  document.getElementById("lineagePageInfo").textContent =
    `Page ${paging.page}/${paging.totalPages} (${paging.totalItems} edges)`;
}

function renderPipelineObservability(payload = pipelineLastRun) {
  const tbody = document.querySelector("#pipelineStageTable tbody");
  tbody.innerHTML = "";
  if (!payload || !Array.isArray(payload.stage_metrics)) {
    document.getElementById("pipelineSummary").textContent = "No pipeline run yet.";
    return;
  }

  for (const metric of payload.stage_metrics) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${metric.stage ?? ""}</td>
      <td>${metric.duration_ms ?? 0}</td>
      <td><code>${JSON.stringify(metric.details ?? {})}</code></td>
    `;
    tbody.appendChild(tr);
  }

  document.getElementById("pipelineSummary").textContent =
    `Correlation ${payload.correlation_id}, total ${payload.total_duration_ms} ms, stages=${payload.stage_metrics.length}`;
  const corrInput = document.getElementById("bundleCorrelationId");
  if (corrInput) corrInput.value = payload.correlation_id || "";
}

async function copyCorrelationId() {
  if (!pipelineLastRun || !pipelineLastRun.correlation_id) {
    show({ error: "No pipeline correlation ID available." });
    return;
  }
  try {
    await navigator.clipboard.writeText(pipelineLastRun.correlation_id);
    show({ copied_correlation_id: pipelineLastRun.correlation_id });
  } catch (e) {
    show({ error: `Clipboard copy failed: ${e.message}` });
  }
}

function downloadText(filename, text) {
  const blob = new Blob([text], { type: "text/plain;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

async function downloadFromApi(path, filename) {
  const url = `${apiBase()}${path}`;
  const headers = {};
  const opsApiKey = document.getElementById("opsApiKey")?.value?.trim();
  if (path.startsWith("/ops/") && opsApiKey) {
    headers["x-api-key"] = opsApiKey;
  }
  const res = await fetch(url, { headers });
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${txt}`);
  }
  const text = await res.text();
  downloadText(filename, text);
}

document.getElementById("btnHealth").onclick = async () => {
  try { show(await callApi("/health")); } catch (e) { show({ error: e.message }); }
};

document.getElementById("btnDatasets").onclick = async () => {
  try { show(await callApi("/datasets")); } catch (e) { show({ error: e.message }); }
};

document.getElementById("btnRuntimeInfo").onclick = async () => {
  try {
    const info = await callApi("/ops/runtime");
    document.getElementById("runtimeSummary").textContent =
      `mode=${info.runtime_mode}, vercel=${info.is_vercel}, db=${info.db_path}, exists=${info.db_exists}`;
    await refreshOpsAuthStatus();
    show(info);
  } catch (e) {
    show({ error: e.message });
  }
};

document.getElementById("btnPipelineRun").onclick = async () => {
  try {
    pipelineLastRun = await callApi("/ops/pipeline/run?auto_accept_inference=true", { method: "POST" });
    renderPipelineObservability(pipelineLastRun);
    show(pipelineLastRun);
  } catch (e) {
    show({ error: e.message });
  }
};

document.getElementById("btnCopyCorrelation").onclick = copyCorrelationId;
document.getElementById("btnDownloadBundle").onclick = async () => {
  const corrId =
    document.getElementById("bundleCorrelationId").value.trim() ||
    pipelineLastRun?.correlation_id;
  if (!corrId) {
    show({ error: "Correlation ID is required." });
    return;
  }
  try {
    const url = `${apiBase()}/exports/run/${encodeURIComponent(corrId)}.zip`;
    const res = await fetch(url);
    if (!res.ok) {
      const txt = await res.text();
      throw new Error(`${res.status} ${res.statusText}: ${txt}`);
    }
    const blob = await res.blob();
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = `dataforge_bundle_${corrId}.zip`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(link.href);
    show({ downloaded_bundle_for: corrId });
  } catch (e) {
    show({ error: e.message });
  }
};

document.getElementById("btnInference").onclick = async () => {
  try { show(await callApi("/inference/run", { method: "POST" })); } catch (e) { show({ error: e.message }); }
};

document.getElementById("btnDriftRun").onclick = async () => {
  try { show(await callApi("/drift/run", { method: "POST" })); } catch (e) { show({ error: e.message }); }
};

document.getElementById("btnDriftLatest").onclick = async () => {
  try { show(await callApi("/drift/latest")); } catch (e) { show({ error: e.message }); }
};

document.getElementById("btnValidation").onclick = async () => {
  try { show(await callApi("/validation/run", { method: "POST" })); } catch (e) { show({ error: e.message }); }
};

document.getElementById("btnLineageBuild").onclick = async () => {
  try { show(await callApi("/lineage/build", { method: "POST" })); } catch (e) { show({ error: e.message }); }
};

document.getElementById("btnLineageGraph").onclick = async () => {
  try { show(await callApi("/lineage/graph")); } catch (e) { show({ error: e.message }); }
};

document.getElementById("btnSeedKpi").onclick = async () => {
  try { show(await callApi("/kpi/seed", { method: "POST" })); } catch (e) { show({ error: e.message }); }
};

document.getElementById("btnRunKpi").onclick = async () => {
  try { show(await callApi("/kpi/run", { method: "POST" })); } catch (e) { show({ error: e.message }); }
};

document.getElementById("btnDashboard").onclick = async () => {
  try { show(await callApi("/dashboard/executive")); } catch (e) { show({ error: e.message }); }
};

document.getElementById("btnDriftRuns").onclick = async () => {
  try {
    const runs = await callApi("/drift/runs");
    const select = document.getElementById("driftRunSelect");
    fillSelect(
      select,
      runs,
      (r) => `${r.dataset_name} v${r.from_version ?? "-"}→v${r.to_version ?? "-"} (${r.status})`,
      (r) => r.dataset_name
    );
    if (runs.length) {
      document.getElementById("driftRunSummary").textContent =
        `Latest: ${runs[0].dataset_name}, events=${runs[0].event_count}, high=${runs[0].high_count}, medium=${runs[0].medium_count}, low=${runs[0].low_count}`;
      driftAll = await callApi(`/drift/events/${encodeURIComponent(runs[0].dataset_name)}`);
      driftPage = 1;
      renderDriftEvents();
    } else {
      document.getElementById("driftRunSummary").textContent = "No drift runs found.";
      driftAll = [];
      renderDriftEvents();
    }
    show(runs);
  } catch (e) {
    show({ error: e.message });
  }
};

document.getElementById("driftRunSelect").onchange = async (ev) => {
  const dataset = ev.target.value;
  if (!dataset) return;
  try {
    driftAll = await callApi(`/drift/events/${encodeURIComponent(dataset)}`);
    driftPage = 1;
    renderDriftEvents();
  } catch (e) {
    show({ error: e.message });
  }
};

document.getElementById("driftSeverity").onchange = () => {
  driftPage = 1;
  renderDriftEvents();
};

document.getElementById("btnDriftPrev").onclick = () => {
  driftPage = Math.max(1, driftPage - 1);
  renderDriftEvents();
};

document.getElementById("btnDriftNext").onclick = () => {
  driftPage += 1;
  renderDriftEvents();
};

document.getElementById("btnDriftExport").onclick = async () => {
  const dataset = document.getElementById("driftDataset").value.trim() || "orders";
  try {
    await downloadFromApi(`/exports/drift/${encodeURIComponent(dataset)}.csv`, `drift_${dataset}.csv`);
  } catch (e) {
    show({ error: e.message });
  }
};

document.getElementById("btnLineageRuns").onclick = async () => {
  try {
    const runs = await callApi("/lineage/runs");
    const select = document.getElementById("lineageRunSelect");
    fillSelect(
      select,
      runs,
      (r) => `${r.lineage_run_id.slice(0, 8)} (${r.status})`,
      (r) => r.lineage_run_id
    );
    if (runs.length) {
      lineageGraphAll = await callApi(`/lineage/graph?lineage_run_id=${encodeURIComponent(runs[0].lineage_run_id)}`);
      refreshLineageEdgeTypeOptions(lineageGraphAll);
      lineagePage = 1;
      document.getElementById("lineageSummary").textContent =
        `Run ${runs[0].lineage_run_id.slice(0, 8)}: nodes=${lineageGraphAll.nodes.length}, edges=${lineageGraphAll.edges.length}`;
      renderLineageEdges(lineageGraphAll);
    } else {
      document.getElementById("lineageSummary").textContent = "No lineage runs found.";
      lineageGraphAll = { nodes: [], edges: [] };
      renderLineageEdges(lineageGraphAll);
    }
    show(runs);
  } catch (e) {
    show({ error: e.message });
  }
};

document.getElementById("lineageRunSelect").onchange = async (ev) => {
  const runId = ev.target.value;
  if (!runId) return;
  try {
    lineageGraphAll = await callApi(`/lineage/graph?lineage_run_id=${encodeURIComponent(runId)}`);
    refreshLineageEdgeTypeOptions(lineageGraphAll);
    lineagePage = 1;
    document.getElementById("lineageSummary").textContent =
      `Run ${runId.slice(0, 8)}: nodes=${lineageGraphAll.nodes.length}, edges=${lineageGraphAll.edges.length}`;
    renderLineageEdges(lineageGraphAll);
  } catch (e) {
    show({ error: e.message });
  }
};

document.getElementById("lineageEdgeType").onchange = () => {
  lineagePage = 1;
  renderLineageEdges(lineageGraphAll);
};

document.getElementById("btnLineagePrev").onclick = () => {
  lineagePage = Math.max(1, lineagePage - 1);
  renderLineageEdges(lineageGraphAll);
};

document.getElementById("btnLineageNext").onclick = () => {
  lineagePage += 1;
  renderLineageEdges(lineageGraphAll);
};

document.getElementById("btnLineageExport").onclick = async () => {
  const runId = document.getElementById("lineageRunSelect").value;
  if (!runId) {
    show({ error: "Select a lineage run first." });
    return;
  }
  try {
    const data = await callApi(`/exports/lineage/${encodeURIComponent(runId)}.json`);
    downloadText(`lineage_${runId}.json`, JSON.stringify(data, null, 2));
  } catch (e) {
    show({ error: e.message });
  }
};

document.getElementById("btnValidationExport").onclick = async () => {
  const runId = document.getElementById("validationRunId").value.trim();
  if (!runId) {
    show({ error: "Validation run ID is required." });
    return;
  }
  try {
    await downloadFromApi(`/exports/validation/${encodeURIComponent(runId)}.csv`, `validation_${runId}.csv`);
  } catch (e) {
    show({ error: e.message });
  }
};

document.getElementById("btnDriftEvents").onclick = async () => {
  const dataset = document.getElementById("driftDataset").value.trim();
  if (!dataset) {
    show({ error: "Drift dataset is required." });
    return;
  }
  try {
    driftAll = await callApi(`/drift/events/${encodeURIComponent(dataset)}`);
    driftPage = 1;
    renderDriftEvents();
    show(driftAll);
  } catch (e) { show({ error: e.message }); }
};

document.getElementById("btnLineageByKpi").onclick = async () => {
  const kpiCode = document.getElementById("kpiCode").value.trim();
  if (!kpiCode) {
    show({ error: "KPI code is required." });
    return;
  }
  try {
    lineageGraphAll = await callApi(`/lineage/kpi/${encodeURIComponent(kpiCode)}`);
    refreshLineageEdgeTypeOptions(lineageGraphAll);
    lineagePage = 1;
    renderLineageEdges(lineageGraphAll);
    show(lineageGraphAll);
  } catch (e) { show({ error: e.message }); }
};

document.getElementById("btnLineageByDataset").onclick = async () => {
  const dataset = document.getElementById("datasetName").value.trim();
  if (!dataset) {
    show({ error: "Dataset name is required." });
    return;
  }
  try {
    lineageGraphAll = await callApi(`/lineage/dataset/${encodeURIComponent(dataset)}`);
    refreshLineageEdgeTypeOptions(lineageGraphAll);
    lineagePage = 1;
    renderLineageEdges(lineageGraphAll);
    document.getElementById("lineageSummary").textContent =
      `Dataset ${dataset}: nodes=${lineageGraphAll.nodes.length}, edges=${lineageGraphAll.edges.length}`;
    show(lineageGraphAll);
  } catch (e) { show({ error: e.message }); }
};

document.getElementById("btnUpload").onclick = async () => {
  const fileInput = document.getElementById("uploadFile");
  const file = fileInput.files?.[0];
  if (!file) {
    uploadMsg.textContent = "Select a file first.";
    uploadMsg.className = "status warn";
    return;
  }

  try {
    const formData = new FormData();
    formData.append("file", file);
    const payload = await callApi("/upload", { method: "POST", body: formData });
    uploadMsg.textContent = `Upload complete: ${payload.dataset_name} (${payload.row_count} rows)`;
    uploadMsg.className = "status ok";
    show(payload);
  } catch (e) {
    uploadMsg.textContent = e.message;
    uploadMsg.className = "status bad";
    show({ error: e.message });
  }
};

refreshOpsAuthStatus();
