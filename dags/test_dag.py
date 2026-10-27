from datetime import datetime
from logging import getLogger

from airflow import DAG
from airflow.sdk import task

# -------------------------------------------------------------------------
# This is a test dag you can run to check if AirFlow is working as expected
# -------------------------------------------------------------------------

log = getLogger(__name__)


@task.python
def write_test_log() -> str:

    log.info("AirFlow is working properly.")
    log.info("Returning test value")

    return "test_value"


with DAG(
    dag_id="Test.AirFlow.Operational",
    dag_display_name="Test: AirFlow Operational",
    start_date=datetime(2026, 8, 14),
    schedule="@daily",
    catchup=False,
) as dag_daily:
    write_test_log()
