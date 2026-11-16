from datetime import datetime, timedelta, timezone
from logging import getLogger

from airflow.sdk import DAG, task
from airflow.sdk.exceptions import AirflowSkipException
from schemas.jss.ref import ReferenceDataJSS
from schemas.jss.transform_jss import TransformDataJSS
from shared.database.engine import get_engine_url
from shared.database.session import get_db_session
from shared.database.table_exists import ensure_table_exists

log = getLogger(__name__)


@task.python
def ensure_requirements() -> None:
    engine_url = get_engine_url(db_database="prod_jss")
    for schema in [TransformDataJSS, ReferenceDataJSS]:
        ensure_table_exists(engine_url=engine_url, schema=schema)


@task.python
def process_jobs() -> None:
    engine_url = get_engine_url(db_database="prod_jss")
    with get_db_session(engine_url=engine_url) as session:
        new = (
            session.query(TransformDataJSS)
            .where(
                TransformDataJSS.inserted_date
                > (datetime.now(timezone.utc)) - timedelta(days=2)
            )
            .all()
        )
        if not new:
            raise AirflowSkipException("No new jobs found")
        log.info(f"Processing {len(new)} jobs ...")
        ref = []
        for n in new:
            ref.append(
                ReferenceDataJSS(
                    transform_id=n.id,
                    job_id=n.job_id,
                    ingest_id=n.ingest_id,
                    stage_id=n.stage_id,
                    j_data=n.j_data,
                    j_score=n.j_score,
                    processed_by_user=n.processed_by_user,
                )
            )
        session.add_all(ref)
        session.commit()


with DAG(
    dag_id="jss.reference",
    dag_display_name="JSS: Reference",
    schedule="0 16 * * 1",
    start_date=datetime(2026, 11, 13),
    catchup=False,
    max_active_runs=1,
) as jss_transform:
    requirements = ensure_requirements()
    jobs = process_jobs()
    requirements >> jobs
