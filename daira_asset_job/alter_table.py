import trino
from dagster import asset, Field, resource, Output
from pystarburst import Session


@resource(
    config_schema={
        "host": Field(str),
        "port": Field(int),
        "user": Field(str),
        "password": Field(str)
    }
)
def starburst_session(context):
    session_properties = {
        "host": context.resource_config["host"],
        "port": context.resource_config["port"],
        "http_scheme": "https",
        "auth": trino.auth.BasicAuthentication(context.resource_config["user"], context.resource_config["password"])
    }
    return Session.builder.configs(session_properties).create()


@asset(
    required_resource_keys={"starburst_session"}
)
def alter_table(context, trusted_table: dict[str, str]):
    session = context.resources.starburst_session

    landing_schema = session.sql(f"DESCRIBE {trusted_table['catalog']}.{trusted_table['raw_schema']}.{trusted_table['raw_table']}").collect()
    context.log.info(f"Landing schema: {landing_schema}")
    trusted_schema = session.sql(f"DESCRIBE {trusted_table['catalog']}.{trusted_table['trusted_schema']}.{trusted_table['trusted_table']}").collect()
    context.log.info(f"Trusted schema: {trusted_schema}")
    landing_columns = {row['Column'] for row in landing_schema}
    trusted_columns = {row['Column'] for row in trusted_schema}
    additional_columns = landing_columns - trusted_columns
    table_sql = f"{trusted_table['catalog']}.{trusted_table['trusted_schema']}.{trusted_table['trusted_table']}"
    try:
        if additional_columns:
            for column in additional_columns:
                col_details = [row for row in landing_schema if row['Column'] == column][0]
                alter_sql = f"ALTER TABLE {table_sql} ADD COLUMN {col_details['Column']} {col_details['Type']}"
                session.sql(alter_sql).collect()
                trusted_columns.add(col_details['Column'])
                context.log.info(f"Added column {col_details['Column']} to {table_sql} table.")
        return Output(
            {
                "trusted_table": trusted_table,
                "unique_id": "edgelab_id",
                "trusted_columns": list(trusted_columns)
             }
        )
    except Exception as e:
        context.log.error(f"Error: {e}")
        Output(
            {
                "trusted_table": "",
                "unique_id": "",
                "trusted_columns": []
            }
        )
