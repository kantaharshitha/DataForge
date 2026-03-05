const output = document.getElementById("output");
const uploadMsg = document.getElementById("uploadMsg");
const apiInput = document.getElementById("apiBase");

if (
  apiInput.value === "/api" &&
  ["127.0.0.1", "localhost"].includes(window.location.hostname) &&
  window.location.port === "3000"
) {
  apiInput.value = "http://127.0.0.1:8000";
}

function apiBase() {
  return document.getElementById("apiBase").value.replace(/\/$/, "");
}

async function callApi(path, options = {}) {
  const url = `${apiBase()}${path}`;
  const res = await fetch(url, options);
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

function renderDriftEvents(events) {
  const tbody = document.querySelector("#driftTable tbody");
  tbody.innerHTML = "";
  for (const event of events) {
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
}

function renderLineageEdges(graph) {
  const nodes = Object.fromEntries((graph.nodes || []).map((n) => [n.node_id, n]));
  const tbody = document.querySelector("#lineageEdgeTable tbody");
  tbody.innerHTML = "";
  for (const edge of graph.edges || []) {
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
}

document.getElementById("btnHealth").onclick = async () => {
  try { show(await callApi("/health")); } catch (e) { show({ error: e.message }); }
};

document.getElementById("btnDatasets").onclick = async () => {
  try { show(await callApi("/datasets")); } catch (e) { show({ error: e.message }); }
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
      const events = await callApi(`/drift/events/${encodeURIComponent(runs[0].dataset_name)}`);
      renderDriftEvents(events.slice(0, 50));
    } else {
      document.getElementById("driftRunSummary").textContent = "No drift runs found.";
      renderDriftEvents([]);
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
    const events = await callApi(`/drift/events/${encodeURIComponent(dataset)}`);
    renderDriftEvents(events.slice(0, 50));
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
      const graph = await callApi(`/lineage/graph?lineage_run_id=${encodeURIComponent(runs[0].lineage_run_id)}`);
      document.getElementById("lineageSummary").textContent =
        `Run ${runs[0].lineage_run_id.slice(0, 8)}: nodes=${graph.nodes.length}, edges=${graph.edges.length}`;
      renderLineageEdges(graph);
    } else {
      document.getElementById("lineageSummary").textContent = "No lineage runs found.";
      renderLineageEdges({ nodes: [], edges: [] });
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
    const graph = await callApi(`/lineage/graph?lineage_run_id=${encodeURIComponent(runId)}`);
    document.getElementById("lineageSummary").textContent =
      `Run ${runId.slice(0, 8)}: nodes=${graph.nodes.length}, edges=${graph.edges.length}`;
    renderLineageEdges(graph);
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
    const events = await callApi(`/drift/events/${encodeURIComponent(dataset)}`);
    renderDriftEvents(events.slice(0, 50));
    show(events);
  } catch (e) { show({ error: e.message }); }
};

document.getElementById("btnLineageByKpi").onclick = async () => {
  const kpiCode = document.getElementById("kpiCode").value.trim();
  if (!kpiCode) {
    show({ error: "KPI code is required." });
    return;
  }
  try { show(await callApi(`/lineage/kpi/${encodeURIComponent(kpiCode)}`)); } catch (e) { show({ error: e.message }); }
};

document.getElementById("btnLineageByDataset").onclick = async () => {
  const dataset = document.getElementById("datasetName").value.trim();
  if (!dataset) {
    show({ error: "Dataset name is required." });
    return;
  }
  try {
    const graph = await callApi(`/lineage/dataset/${encodeURIComponent(dataset)}`);
    renderLineageEdges(graph);
    document.getElementById("lineageSummary").textContent =
      `Dataset ${dataset}: nodes=${graph.nodes.length}, edges=${graph.edges.length}`;
    show(graph);
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
