from datetime import datetime
from logging import getLogger

from airflow.sdk import DAG, task
from shared.inference.resources import get_resources

log = getLogger(__name__)


@task.python
def get_inference_resources() -> dict[str, str]:
    ir = get_resources()
    log.info(f"Inference API URL: {ir.get('INFERENCE_API_URL')}")
    return ir


with DAG(
    dag_id="jss.transform",
    dag_display_name="JSS: Transform",
    schedule="* 16 * * 4",
    start_date=datetime(2026, 11, 13),
    catchup=False,
) as jss_staging:
    resources = get_resources()
