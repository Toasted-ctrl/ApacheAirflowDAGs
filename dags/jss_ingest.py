from datetime import datetime
from logging import getLogger

from airflow.sdk import task, DAG
from schemas.jss.ingest_jss import IngestDataJSS, IngestJobsJSS
from shared.database.engine import get_engine_url
from shared.database.table_exists import ensure_table_exists

log = getLogger(__name__)


@task.python
def ensure_schemas():
    engine_url = get_engine_url(db_database="prod_jss")
    for schema in [IngestJobsJSS, IngestDataJSS]:
        ensure_table_exists(engine_url=engine_url, schema=schema)


with DAG(
    dag_id="jss.ingest",
    dag_display_name="JSS: Ingest",
    schedule="0 15 * * 4",
    start_date=datetime(2026, 10, 25),
    catchup=False,
) as jss_ingest:
    schemas = ensure_schemas()

    (
        schemas
    )