import trino
from dagster import op, Field, asset, Output
from pystarburst import Session


@asset(
    required_resource_keys={"starburst_session"},
    config_schema={
        "catalog": Field(str),
        "trusted_schema": Field(str),
        "raw_schema": Field(str),
        "raw_table": Field(str)
    }
)
def trusted_schema(context, schema_discovery: list[str]):
    context.log.info(f"sql statements: {schema_discovery}")

    try:
        session = context.resources.starburst_session
        sql = f"""
            CREATE SCHEMA IF NOT EXISTS "{context.op_config['catalog']}"."{context.op_config['trusted_schema']}"
        """
        result = session.sql(sql).collect()
        context.log.info(result)
        context.log.info("Schema creation SQL executed successfully.")
        yield Output(
            {
                "trusted_schema": context.op_config['trusted_schema'],
                "catalog": context.op_config['catalog'],
                "raw_schema": context.op_config['raw_schema'],
                "raw_table": context.op_config['raw_table'],
            }
        )
    except Exception as e:
        context.log.error(f"Error: {e}")
        yield Output(
            {
                "trusted_schema": "",
                "catalog": "",
                "raw_schema": "",
                "raw_table": "",
            }
        )
