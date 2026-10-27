from datetime import datetime
from logging import getLogger

import requests
from airflow import DAG
from airflow.sdk import task
from airflow.sdk.exceptions import AirflowSkipException
from custom.runescape.models import TrackedUser
from schemas.runescape.ingest_hiscore import IngestHiscoresT
from schemas.runescape.ingest_runemetrics import IngestRuneMetricsT
from schemas.runescape.tracked_users import TrackedUsersT
from shared.database.engine import get_engine_url
from shared.database.session import get_db_session
from shared.database.table_exists import ensure_table_exists

log = getLogger(__name__)


@task.python
def ensure_schemas():
    engine_url = get_engine_url(db_database="prod_runescape")
    for schema in [TrackedUsersT, IngestRuneMetricsT, IngestHiscoresT]:
        ensure_table_exists(engine_url=engine_url, schema=schema)


@task.python
def get_tracked_users() -> list[dict]:
    """Get tracked users. First determine if the table exists."""
    engine_url = get_engine_url(db_database="prod_runescape")
    with get_db_session(engine_url=engine_url) as session:
        log.info("Fetching usernames ...")
        users = (
            session.query(TrackedUsersT.player_name, TrackedUsersT.id)
            .filter(TrackedUsersT.is_tracked.is_(True))
            .all()
        )
        if not users:
            raise AirflowSkipException(
                f"No users found in '{TrackedUsersT.__tablename__}'. Shutting down ... "
            )
        log.info(f"Found {len(users)} user(s), returning all ...")
        return [
            TrackedUser(name=user.player_name, id=user.id).to_dict() for user in users
        ]


@task.python
def get_hiscores(user: dict) -> None:
    """Fetching and storing the RuneScape Hiscore data for the specified player."""
    tracked_user = TrackedUser.from_dict(user)
    engine_url = get_engine_url(db_database="prod_runescape")
    log.info(
        f"Fetching RuneScape Hiscore data for user '{tracked_user.name}': '{tracked_user.id}' ..."
    )

    url = "https://secure.runescape.com/m=hiscore/index_lite.ws"
    params = {"player": tracked_user.name}

    try:
        response = requests.get(url=url, params=params, timeout=30)
        if response.status_code != 200:
            log.warning(
                f"Non-200 response ({response.status_code}) for user '{tracked_user.name}': '{tracked_user.id}' ..."
            )
        else:
            log.info(
                f"Successfully retrieved RuneScape Hiscore data for user '{tracked_user.name}': '{tracked_user.id}' ..."
            )
        with get_db_session(engine_url=engine_url) as session:
            nhe = IngestHiscoresT(
                user_id=tracked_user.id,
                status_code=response.status_code,
                data=response.text if response.status_code == 200 else None,
            )

            session.add(nhe)
            session.commit()

            log.info(
                f"Inserted new Hiscore records for user '{tracked_user.name}': '{tracked_user.id}' ..."
            )

    except requests.exceptions.RequestException as e:
        raise AirflowSkipException(
            f"Error while fetching RuneScape Hiscore data for user '{tracked_user.name}': '{tracked_user.id}': {e} ..."
        )


@task.python
def get_runemetrics(user: dict) -> None:
    """Fetching and storing the RuneMetrics data for the specified player."""
    tracked_user = TrackedUser.from_dict(user)
    engine_url = get_engine_url(db_database="prod_runescape")
    log.info(
        f"Fetching RuneMetrics data for user '{tracked_user.name}': '{tracked_user.id}' ..."
    )

    url = "https://apps.runescape.com/runemetrics/profile/profile"
    params: dict[str, str | int] = {"user": tracked_user.name, "activities": 15}

    try:
        response = requests.get(url=url, params=params, timeout=30)
        if response.status_code != 200:
            log.warning(
                f"Non-200 response ({response.status_code}) for user '{tracked_user.name}': '{tracked_user.id}' ..."
            )
        else:
            log.info(
                f"Successfully retrieved RuneMetrics data for user '{tracked_user.name}': '{tracked_user.id}' ..."
            )
        with get_db_session(engine_url=engine_url) as session:
            nre = IngestRuneMetricsT(
                user_id=tracked_user.id,
                status_code=response.status_code,
                data=response.json() if response.status_code == 200 else None,
            )

            session.add(nre)
            session.commit()

            log.info(
                f"Inserted new RuneMetrics records for user '{tracked_user.name}': '{tracked_user.id}' ..."
            )

    except requests.exceptions.RequestException as e:
        raise AirflowSkipException(
            f"Error while fetching RuneMetrics data for '{tracked_user.name}': '{tracked_user.id}': {e} ..."
        )


with DAG(
    dag_id="runescape.ingest",
    dag_display_name="Runescape: Ingest",
    schedule="@hourly",
    start_date=datetime(2026, 8, 14),
    catchup=False,
) as runescape_ingest:
    schemas = ensure_schemas()
    users = get_tracked_users()

    (
        schemas
        >> users
        >> [get_hiscores.expand(user=users), get_runemetrics.expand(user=users)]
    )
