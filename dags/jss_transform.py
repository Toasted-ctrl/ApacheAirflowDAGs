import random
import time
from datetime import datetime, timedelta, timezone
from logging import getLogger

import requests
from airflow.sdk import DAG, task
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException
from requests import exceptions as requests_exceptions
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


RATING_THRESHOLD = 0.80


@task.python
def process_jobs() -> None:
    engine_url = get_engine_url(db_database="prod_jss")
    with get_db_session(engine_url=engine_url) as session:
        items = (
            session.query(StageDataJSS)
            .where(
                StageDataJSS.inserted_date
                > (datetime.now(timezone.utc) - timedelta(days=9))
            )
            .all()
        )
        if not items:
            raise AirflowSkipException("No new jobs to process.")
        total = len(items)
        inference = get_resources()
        transform = []
        current_job = 0
        completed = 0
        failed_melious_calls = 0
        for item in items:
            current_job += 1
            if failed_melious_calls > 5:
                raise AirflowFailException("Exceeded max Melious failures.")
            time.sleep(random.uniform(5, 10))
            user_requirements = (
                session.query(IngestJobsJSS.requirements)
                .where(IngestJobsJSS.id == item.job_id)
                .scalar()
            )
            actual_description = item.j_description
            payload = {
                "agent_id": inference.get("INFERENCE_JSS_AGENT_ID"),
                "state": {
                    "user_requirements": user_requirements,
                    "actual_description": actual_description,
                },
            }
            headers: dict = {"X-API-Key": inference.get("INFERENCE_API_KEY")}
            try:
                response = requests.post(
                    url=f"{inference.get('INFERENCE_API_URL')}{inference.get('INFERENCE_ENDPOINT_DECISION')}",
                    headers=headers,
                    json=payload,
                    timeout=30,
                )
                log.info(
                    f"Job {current_job}/{total}, Stage ID: {item.id}. Response code {response.status_code}."
                )
            except requests_exceptions.Timeout:
                log.exception(
                    f"Job {current_job}/{total}, Stage ID: {item.id}. Request timed out"
                )
                failed_melious_calls += 1
                continue
            except requests_exceptions.ConnectionError:
                log.exception(
                    f"Job {current_job}/{total}, Stage ID: {item.id}. Connection error"
                )
                failed_melious_calls += 1
                continue
            except requests_exceptions.RequestException:
                log.exception(
                    f"Job {current_job}/{total}, Stage ID: {item.id}. Request failed"
                )
                failed_melious_calls += 1
                continue
            if response.status_code != 200:
                failed_melious_calls += 1
                continue
            rating: float = response.json()["answers"]["is_match"]["noul"]
            if rating >= RATING_THRESHOLD:
                transform.append(
                    TransformDataJSS(
                        ingest_id=item.ingest_id,
                        stage_id=item.id,
                        job_id=item.job_id,
                        j_data=item.j_data,
                        j_score=rating,
                        processed_by_user=False,
                        has_applied=False
                    )
                )
                log.info(f"Rating threshold ({RATING_THRESHOLD}) met, adding ...")
            completed += 1
        session.add_all(transform)
        session.commit()
        log.info(f"Completed {completed}/{total} jobs.")
        log.info(f"Failed {failed_melious_calls} Melious API calls.")
        return


with DAG(
    dag_id="jss.transform",
    dag_display_name="JSS: Transform",
    schedule="0 16 * * 0",
    start_date=datetime(2026, 11, 13),
    catchup=False,
    max_active_runs=1,
) as jss_transform:
    requirements = ensure_requirements()
    jobs = process_jobs()
    requirements >> jobs
