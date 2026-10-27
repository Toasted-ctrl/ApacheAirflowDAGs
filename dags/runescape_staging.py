from dataclasses import asdict
from datetime import datetime
from logging import getLogger

from airflow import DAG
from airflow.sdk import task
from custom.runescape.unpack_hiscore import unpack_hiscore_entry
from schemas.runescape.ingest_hiscore import IngestHiscoresT
from schemas.runescape.staging_hiscore import StagingHiscoresT
from shared.database.engine import get_engine_url
from shared.database.extremes import get_max_value_from_schema_by_column
from shared.database.session import get_db_session
from shared.database.table_exists import ensure_table_exists

log = getLogger(__name__)


@task.python
def ensure_schemas() -> None:
    engine_url = get_engine_url(db_database="prod_runescape")
    for schema in [IngestHiscoresT, StagingHiscoresT]:
        ensure_table_exists(engine_url=engine_url, schema=schema)


@task.python
def get_and_transform_unprocessed_hiscore_records() -> None:
    engine_url = get_engine_url(db_database="prod_runescape")
    with get_db_session(engine_url=engine_url) as session:
        max_ingested_date = get_max_value_from_schema_by_column(
            session=session, schema=StagingHiscoresT, column_name="ingested_date"
        )

        if not max_ingested_date:
            log.info(
                f"No records in '{StagingHiscoresT.__tablename__}' ... "
                f"Fetching all records from '{IngestHiscoresT.__tablename__}' ..."
            )
            ud = (
                session.query(IngestHiscoresT)
                .filter(IngestHiscoresT.status_code == 200)
                .all()
            )

        else:
            log.info(
                f"Last processed record ingested on {max_ingested_date} ... "
                f"Retrieving all new records from '{IngestHiscoresT.__tablename__}' ..."
            )
            ud = (
                session.query(IngestHiscoresT)
                .filter(
                    IngestHiscoresT.inserted_date > max_ingested_date,
                    IngestHiscoresT.status_code == 200,
                )
                .all()
            )

        log.info(f"Retrieved {len(ud)} new record(s) for processing. Continuing ...")

        records = 0
        lines = 0

        for r in ud:
            m = unpack_hiscore_entry(r.data)
            for item in m:
                ni = StagingHiscoresT(
                    user_id=r.user_id, ingested_date=r.inserted_date, **asdict(item)
                )

                session.add(ni)
                lines += 1
            records += 1

        session.commit()
        log.info(
            f"Done! Processed {records} records ... "
            f"Added {lines} line(s) to '{StagingHiscoresT.__tablename__}' ..."
        )


with DAG(
    dag_id="runescape.staging",
    dag_display_name="Runescape: Staging",
    start_date=datetime(2026, 8, 15),
    catchup=False,
    schedule="30 4 * * *",
) as runescape_staging:
    schemas = ensure_schemas()

    schemas >> [get_and_transform_unprocessed_hiscore_records()]
