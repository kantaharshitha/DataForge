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

document.getElementById("btnDriftEvents").onclick = async () => {
  const dataset = document.getElementById("driftDataset").value.trim();
  if (!dataset) {
    show({ error: "Drift dataset is required." });
    return;
  }
  try { show(await callApi(`/drift/events/${encodeURIComponent(dataset)}`)); } catch (e) { show({ error: e.message }); }
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
  try { show(await callApi(`/lineage/dataset/${encodeURIComponent(dataset)}`)); } catch (e) { show({ error: e.message }); }
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
