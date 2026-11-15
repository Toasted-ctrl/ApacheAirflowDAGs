from datetime import datetime, timedelta
from logging import getLogger

from airflow.sdk import DAG, task
from airflow.sdk.exceptions import AirflowSkipException
from schemas.jss.ingest_jss import IngestDataJSS
from schemas.jss.stage_jss import StageDataJSS
from shared.database.engine import get_engine_url
from shared.database.session import get_db_session
from shared.database.table_exists import ensure_table_exists

log = getLogger(__name__)


@task.python
def ensure_schemas():
    engine_url = get_engine_url(db_database="prod_jss")
    for schema in [IngestDataJSS, StageDataJSS]:
        ensure_table_exists(engine_url=engine_url, schema=schema)


@task.python
def process_jobs() -> int:
    engine_url = get_engine_url(db_database="prod_jss")
    with get_db_session(engine_url=engine_url) as session:
        items = (
            session.query(IngestDataJSS)
            .where(IngestDataJSS.inserted_date > (datetime.now() - timedelta(days=9)))
            .all()
        )
        if not items:
            raise AirflowSkipException("Found no jobs")
        staged = []
        count = 0
        for item in items:
            if session.query(
                session.query(StageDataJSS)
                .where(StageDataJSS.j_id == item.data.get("id"))
                .exists()
            ).scalar():
                continue
            staged.append(
                StageDataJSS(
                    ingest_id=item.id,
                    job_id=item.job_id,
                    j_id=item.data.get("id"),
                    j_description=item.data.get("description"),
                    j_data=item.data,
                )
            )
            count += 1
        session.add_all(staged)
        session.commit()
        log.info(f"Added {count} items to staging.")
        return count


with DAG(
    dag_id="jss.stage",
    dag_display_name="JSS: Stage",
    schedule="30 15 * * 0",
    start_date=datetime(2026, 11, 13),
    catchup=False,
) as jss_staging:
    schemas = ensure_schemas()
    jobs = process_jobs()

    schemas >> jobs
