import os
import requests
import pytest


@pytest.mark.integration
def test_databricks_job_exists_and_can_run():
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    token = os.environ["DATABRICKS_TOKEN"]
    job_name = os.environ["DATABRICKS_JOB_NAME"]

    headers = {"Authorization": f"Bearer {token}"}

    # Find job by name
    r = requests.get(f"{host}/api/2.1/jobs/list", headers=headers, timeout=60)
    r.raise_for_status()
    jobs = r.json().get("jobs", [])
    matches = [j for j in jobs if j.get("settings", {}).get("name") == job_name]
    assert matches, f"Job not found: {job_name}"

    job_id = matches[0]["job_id"]

    # Trigger run-now (non-blocking)
    r2 = requests.post(
        f"{host}/api/2.1/jobs/run-now",
        headers=headers,
        json={"job_id": job_id},
        timeout=60
    )
    r2.raise_for_status()
    assert "run_id" in r2.json()
