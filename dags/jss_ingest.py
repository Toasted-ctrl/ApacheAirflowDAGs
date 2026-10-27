import time
import uuid
from datetime import datetime
from logging import getLogger

import requests
from airflow.sdk import DAG, task
from airflow.sdk.exceptions import AirflowSkipException
from schemas.jss.ingest_jss import IngestDataJSS, IngestJobsJSS
from shared.database.engine import get_engine_url
from shared.database.session import get_db_session
from shared.database.table_exists import ensure_table_exists

log = getLogger(__name__)


@task.python
def ensure_schemas():
    engine_url = get_engine_url(db_database="prod_jss")
    for schema in [IngestJobsJSS, IngestDataJSS]:
        ensure_table_exists(engine_url=engine_url, schema=schema)


@task.python
def get_jobs() -> list[dict]:
    engine_url = get_engine_url(db_database="prod_jss")
    with get_db_session(engine_url=engine_url) as session:
        jobs = session.query(IngestJobsJSS).all()
        if not jobs:
            raise AirflowSkipException("Found no jobs.")
        j = []
        for job in jobs:
            log.info(str(job))
            j.append(
                {
                    "id": str(job.id),
                    "filters": job.filters,
                    "api_key": job.api_key,
                    "url": job.url,
                }
            )
        return j


@task.python
def process_job(job: dict) -> None:
    log.info(f"Processing job {job['id']} from {job['url']}")
    params = {"token": job.get("api_key")}
    response = requests.post(url=job["url"], params=params, json=job["filters"])
    if not response.ok:
        raise AirflowSkipException(
            f"API call failed for job {job.get('id')}: HTTP {response.status_code}"
        )

    response_data = response.json()
    log.info(f"API response: {response_data}")

    # Extract run ID and dataset info
    # Use 'id' field (run ID) from response data, not 'actId'
    run_id = response_data.get("data", {}).get("id")
    dataset_url = response_data.get("data", {}).get("output", {}).get("jobs")

    if not run_id:
        raise AirflowSkipException(
            f"No run ID found in response for job {job.get('id')}"
        )

    if not dataset_url:
        raise AirflowSkipException(
            f"No dataset URL found in response for job {job.get('id')}"
        )

    # Poll the run status until it's SUCCEEDED
    # Apify API endpoint: GET /v2/actor-runs/{RUN_ID}
    api_key = job.get("api_key")
    run_check_url = f"https://api.apify.com/v2/actor-runs/{run_id}"

    max_attempts = 12
    poll_interval = 30  # seconds

    for attempt in range(max_attempts):
        run_response = requests.get(run_check_url, params={"token": api_key})
        if not run_response.ok:
            raise AirflowSkipException(
                f"Failed to check run status for job {job.get('id')}: HTTP {run_response.status_code}"
            )

        run_status = run_response.json().get("data", {}).get("status")
        log.info(f"Run status (attempt {attempt + 1}/{max_attempts}): {run_status}")

        if run_status == "SUCCEEDED":
            break
        elif run_status in ("FAILED", "ABORTED", "TIMED-OUT", "TIMING-OUT", "ABORTING"):
            raise AirflowSkipException(
                f"Run {run_status.lower()} for job {job.get('id')}"
            )
        elif run_status not in (
            "SUCCEEDED",
            "FAILED",
            "ABORTED",
            "TIMED-OUT",
            "TIMING-OUT",
            "ABORTING",
            "RUNNING",
            "READY",
            "CREATING",
        ):
            raise AirflowSkipException(
                f"Unexpected run status '{run_status}' for job {job.get('id')}"
            )

        if attempt < max_attempts - 1:
            time.sleep(poll_interval)
    else:
        raise AirflowSkipException(
            f"Run did not succeed within {max_attempts * poll_interval} seconds for job {job.get('id')}"
        )

    data_response = requests.get(dataset_url, params={"token": api_key})
    if not data_response.ok:
        raise AirflowSkipException(
            f"Failed to fetch dataset for job {job.get('id')}: HTTP {data_response.status_code}"
        )

    dataset_items = data_response.json()
    log.info(f"Fetched {len(dataset_items)} items from dataset")

    engine_url = get_engine_url(db_database="prod_jss")
    with get_db_session(engine_url=engine_url) as session:
        for item in dataset_items:
            new_record = IngestDataJSS(
                job_id=uuid.UUID(job["id"]), data=item, inserted_by="airflow"
            )
            session.add(new_record)
        session.commit()
        log.info(f"Inserted {len(dataset_items)} records for job {job['id']}")


with DAG(
    dag_id="jss.ingest",
    dag_display_name="JSS: Ingest",
    schedule="0 15 * * 4",
    start_date=datetime(2026, 10, 25),
    catchup=False,
) as jss_ingest:
    schemas = ensure_schemas()
    jobs = get_jobs()
    process_all = process_job.expand(job=jobs)

    (schemas >> jobs >> process_all)
