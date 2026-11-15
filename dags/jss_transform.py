from datetime import datetime, timedelta
from logging import getLogger

import requests
from airflow.sdk import DAG, task
from airflow.sdk.exceptions import AirflowSkipException
from schemas.jss.ingest_jss import IngestJobsJSS
from schemas.jss.stage_jss import StageDataJSS
from schemas.jss.transform_jss import TransformDataJSS
from shared.database.engine import get_engine_url
from shared.database.session import get_db_session
from shared.database.table_exists import ensure_table_exists
from shared.inference.resources import get_resources

log = getLogger(__name__)


@task.python
def ensure_requirements() -> None:
    engine_url = get_engine_url(db_database="prod_jss")
    for schema in [StageDataJSS, TransformDataJSS, IngestJobsJSS]:
        ensure_table_exists(engine_url=engine_url, schema=schema)


@task.python
def process_jobs() -> None:
    engine_url = get_engine_url(db_database="prod_jss")
    with get_db_session(engine_url=engine_url) as session:
        items = (
            session.query(StageDataJSS)
            .where(StageDataJSS.inserted_date > (datetime.now() - timedelta(days=9)))
            .all()
        )
        if not items:
            raise AirflowSkipException("No new jobs to process.")
        inference = get_resources()
        transform = []
        count = 0
        for item in items:
            user_requirements = session.query(IngestJobsJSS.requirements).where(
                IngestJobsJSS.id == item.job_id
            )
            actual_description = item.j_description
            json = {
                "agent_id": inference.get("INFERENCE_JSS_AGENT_ID"),
                "state": {
                    "user_requirements": user_requirements,
                    "actual_description": actual_description,
                },
            }
            headers: dict = {"X-API-Key": inference.get("INFERENCE_API_KEY")}
            response = requests.post(
                url=f"{inference.get('INFERENCE_API_URL')}{inference.get('INFERENCE_ENDPOINT_DECISION')}",
                headers=headers,
                json=json
            )
            log.info(f"Response code {response.status_code}")
            log.info(response.json())
            count += 1


with DAG(
    dag_id="jss.transform",
    dag_display_name="JSS: Transform",
    schedule="* 16 * * 4",
    start_date=datetime(2026, 11, 13),
    catchup=False,
) as jss_transform:
    requirements = ensure_requirements()
    jobs = process_jobs()
    requirements >> jobs
