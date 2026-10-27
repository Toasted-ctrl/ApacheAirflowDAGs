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
    response = requests.post(url=job['url'], params=params, json=job['filters'])
    if not response.ok:
        raise AirflowSkipException(
            f"API call failed for job {job.get('id')}: HTTP {response.status_code}"
        )
    log.info(response.json())


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
