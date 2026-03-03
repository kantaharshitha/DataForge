import pandas as pd
import requests
import streamlit as st

API_BASE = st.sidebar.text_input("API Base URL", "http://localhost:8000")

st.title("DataForge - Console")
page = st.sidebar.radio(
    "Page",
    [
        "Upload",
        "Dataset Inventory",
        "Profiling Summary",
        "Relationship Inference",
        "Validation & Trust",
        "KPI Registry",
        "Executive Dashboard",
    ],
)

if page == "Upload":
    st.header("Upload Dataset (CSV/XLSX)")
    uploaded = st.file_uploader("Choose a file", type=["csv", "xlsx", "xls"])
    if uploaded and st.button("Ingest + Profile"):
        files = {"file": (uploaded.name, uploaded.getvalue())}
        response = requests.post(f"{API_BASE}/upload", files=files, timeout=120)
        if response.ok:
            st.success("Upload complete")
            st.json(response.json())
        else:
            st.error(response.text)

elif page == "Dataset Inventory":
    st.header("Dataset Registry")
    response = requests.get(f"{API_BASE}/datasets", timeout=30)
    if response.ok:
        rows = response.json()
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True)
        else:
            st.info("No datasets registered yet.")
    else:
        st.error(response.text)

elif page == "Profiling Summary":
    st.header("Latest Profiling Run")
    datasets_resp = requests.get(f"{API_BASE}/datasets", timeout=30)
    if not datasets_resp.ok:
        st.error("Could not load datasets")
    else:
        datasets = datasets_resp.json()
        if not datasets:
            st.info("Upload a dataset first.")
        else:
            options = {
                f"{d['dataset_name']} ({d['dataset_id'][:8]})": d["dataset_id"]
                for d in datasets
            }
            selected = st.selectbox("Dataset", list(options.keys()))
            if st.button("Load Profile"):
                profile_resp = requests.get(f"{API_BASE}/profiles/{options[selected]}", timeout=30)
                if profile_resp.ok:
                    profile = profile_resp.json()
                    st.subheader("Run Summary")
                    st.json(
                        {
                            "run_id": profile["run_id"],
                            "dataset_name": profile["dataset_name"],
                            "row_count": profile["row_count"],
                            "duplicate_rows": profile["duplicate_rows"],
                        }
                    )
                    st.subheader("Column Metrics")
                    st.dataframe(pd.DataFrame(profile["columns"]), use_container_width=True)
                else:
                    st.error(profile_resp.text)

elif page == "Relationship Inference":
    st.header("Relationship Inference")

    if st.button("Run Inference"):
        run_resp = requests.post(f"{API_BASE}/inference/run", timeout=90)
        if run_resp.ok:
            st.success("Inference run completed")
            st.json(run_resp.json())
        else:
            st.error(run_resp.text)

    candidates_resp = requests.get(f"{API_BASE}/inference/candidates", timeout=30)
    if not candidates_resp.ok:
        st.info("No inference results yet. Run inference first.")
    else:
        candidates = candidates_resp.json()
        if not candidates:
            st.info("No candidates found yet. Upload more related datasets and run inference.")
        else:
            frame = pd.DataFrame(candidates)
            st.subheader("Candidates")
            st.dataframe(
                frame[
                    [
                        "candidate_id",
                        "child_dataset_name",
                        "child_column",
                        "parent_dataset_name",
                        "parent_column",
                        "confidence_score",
                        "cardinality_hint",
                        "status",
                    ]
                ],
                use_container_width=True,
            )

            id_map = {
                (
                    f"{row['child_dataset_name']}.{row['child_column']} -> "
                    f"{row['parent_dataset_name']}.{row['parent_column']} "
                    f"({row['confidence_score']})"
                ): row["candidate_id"]
                for row in candidates
            }
            selected = st.selectbox("Select candidate", list(id_map.keys()))
            decision = st.radio("Decision", ["ACCEPTED", "REJECTED"], horizontal=True)
            notes = st.text_input("Reviewer notes (optional)")

            if st.button("Save Decision"):
                decision_resp = requests.post(
                    f"{API_BASE}/inference/decide",
                    json={
                        "candidate_id": id_map[selected],
                        "decision": decision,
                        "reviewer_notes": notes or None,
                    },
                    timeout=30,
                )
                if decision_resp.ok:
                    st.success("Decision saved")
                    st.json(decision_resp.json())
                else:
                    st.error(decision_resp.text)

elif page == "Validation & Trust":
    st.header("Validation & Trust Score")

    if st.button("Run Validation"):
        run_resp = requests.post(f"{API_BASE}/validation/run", timeout=120)
        if run_resp.ok:
            st.success("Validation run completed")
            st.json(run_resp.json())
        else:
            st.error(run_resp.text)

    latest_resp = requests.get(f"{API_BASE}/trust/latest", timeout=30)
    if latest_resp.ok:
        latest = latest_resp.json()
        st.subheader("Latest Trust Score")
        st.metric("Trust Score", latest["trust_score"])
        st.json(
            {
                "validation_run_id": latest["validation_run_id"],
                "status": latest["status"],
                "dimension_scores": latest["dimension_scores"],
            }
        )
    else:
        st.info("No validation runs yet.")

    runs_resp = requests.get(f"{API_BASE}/validation/runs", timeout=30)
    if not runs_resp.ok:
        st.error("Could not load validation runs")
    else:
        runs = runs_resp.json()
        if not runs:
            st.info("No validation history yet.")
        else:
            st.subheader("Validation Run History")
            st.dataframe(pd.DataFrame(runs), use_container_width=True)

            options = {
                f"{r['validation_run_id'][:8]} | {r['status']} | score={r['trust_score']}": r["validation_run_id"]
                for r in runs
            }
            selected_run = st.selectbox("Select validation run", list(options.keys()))

            details_resp = requests.get(
                f"{API_BASE}/validation/results/{options[selected_run]}",
                timeout=30,
            )
            if details_resp.ok:
                details = details_resp.json()
                st.subheader("Rule Results")
                st.dataframe(pd.DataFrame(details["results"]), use_container_width=True)

                st.subheader("Exceptions")
                exceptions = details.get("exceptions", [])
                if exceptions:
                    st.dataframe(pd.DataFrame(exceptions), use_container_width=True)
                else:
                    st.info("No exceptions for this run.")
            else:
                st.error(details_resp.text)

elif page == "KPI Registry":
    st.header("KPI Registry")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Seed Default KPIs"):
            seed_resp = requests.post(f"{API_BASE}/kpi/seed", timeout=30)
            if seed_resp.ok:
                st.success(f"Seeded {seed_resp.json()['inserted']} KPI(s)")
            else:
                st.error(seed_resp.text)

    with col2:
        if st.button("Run KPI Calculation"):
            run_resp = requests.post(f"{API_BASE}/kpi/run", timeout=60)
            if run_resp.ok:
                st.success("KPI run completed")
                st.json(run_resp.json())
            else:
                st.error(run_resp.text)

    registry_resp = requests.get(f"{API_BASE}/kpi/registry", timeout=30)
    if registry_resp.ok:
        registry = registry_resp.json()
        if registry:
            st.dataframe(pd.DataFrame(registry), use_container_width=True)
        else:
            st.info("KPI registry is empty. Seed default KPIs.")
    else:
        st.error(registry_resp.text)

elif page == "Executive Dashboard":
    st.header("Executive Dashboard")

    latest_kpi = requests.get(f"{API_BASE}/kpi/latest", timeout=30)
    if not latest_kpi.ok:
        st.info("No KPI runs available. Seed KPIs and run KPI calculation first.")
    else:
        payload = latest_kpi.json()
        st.caption(f"KPI Run: {payload['kpi_run_id']} | Generated: {payload['generated_at']}")
        cards = payload.get("kpi_values", {})
        if cards:
            cols = st.columns(3)
            idx = 0
            for key, value in cards.items():
                cols[idx % 3].metric(key, value)
                idx += 1

        dashboard_resp = requests.get(f"{API_BASE}/dashboard/executive", timeout=30)
        if dashboard_resp.ok:
            dash = dashboard_resp.json()
            st.subheader("Trust Context")
            trust = dash.get("trust_context")
            if trust:
                st.metric("Data Trust Score", trust["trust_score"])
                st.json(trust)
            else:
                st.info("No trust context yet. Run validation first.")
        else:
            st.error(dashboard_resp.text)
