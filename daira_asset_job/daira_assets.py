import pandas as pd
import psycopg2
from dagster import Config, AssetMaterialization, Output, asset, DagsterInstance, RunsFilter, DagsterRunStatus, AssetKey

from datetime import datetime


class DBConfig(Config):
    host: str
    username: str
    password: str
    dbname: str
    port: int
    table: str


@asset
def daira_assets(context, config: DBConfig) -> pd.DataFrame:
    last_run_timestamp = get_last_successful_asset_op_timestamp("daira_assets")
    context.log.info(f"Last run timestamp: {last_run_timestamp}")
    host = config.host
    username = config.username
    password = config.password
    dbname = config.dbname
    port = config.port
    table = config.table
    context.log.info(f"dbname: {dbname}")

    conn_string = (f"dbname={dbname} "
                   f"user={username} "
                   f"password={password} "
                   f"host={host} "
                   f"port={port}")

    try:
        with psycopg2.connect(conn_string) as conn:
            query = f"SELECT * FROM {table} WHERE created_at >= '{last_run_timestamp}'"
            df = pd.read_sql_query(query, conn)
            rows = df.to_dict(orient="records")
        context.log.info(f"Extracted columns: {', '.join(df.columns)}")
        yield AssetMaterialization(
            asset_key="asset",
            description="asset data extracted from the database",
            metadata={
                "row_count": len(rows),
                "db_name": dbname,
                "table": table
            }
        )
        yield Output(df)
    except Exception as e:
        context.log.error(f"Error extracting assets: {e}")
        return Output(None)


def get_last_successful_asset_op_timestamp(asset_name: str) -> datetime:
    instance = DagsterInstance.get()
    asset_key = AssetKey(asset_name)
    latest_materialization_event = instance.get_latest_materialization_event(asset_key)

    if latest_materialization_event:
        return datetime.fromtimestamp(latest_materialization_event.timestamp)
    else:
        return datetime.now()


def get_last_successful_job_timestamp(job_name: str) -> datetime:
    instance = DagsterInstance.get()
    run_records = instance.get_run_records(
        filters=RunsFilter(
            job_name=job_name,
            statuses=[DagsterRunStatus.SUCCESS]
        ),
        limit=1,
        order_by="update_timestamp",
        ascending=False
    )

    if run_records:
        last_successful_run = run_records[0]
        return datetime.fromtimestamp(last_successful_run.end_time)
    else:
        return datetime.now()