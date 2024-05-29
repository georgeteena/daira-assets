import trino
from dagster import asset, Field, Output
from pystarburst import Session


@asset(
    config_schema={
        "trusted_table": Field(str),
    },
    required_resource_keys={"starburst_session"}
)
def trusted_table(context, trusted_schema: dict[str, str]):
    try:
        session = context.resources.starburst_session
        catalog = trusted_schema["catalog"]
        t_schema = trusted_schema["trusted_schema"]
        t_table = context.op_config["trusted_table"]
        r_schema = trusted_schema["raw_schema"]
        r_table = trusted_schema["raw_table"]
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{t_schema}.{t_table}
        WITH (
                format = 'Parquet',
                type = 'Iceberg'
            ) AS SELECT * FROM {catalog}.{r_schema}.{r_table}
        """
        context.log.info(f"Creating table with SQL: {create_table_sql}")
        result = session.sql(create_table_sql).collect()
        context.log.info(result)
        context.log.info("Table creation SQL executed successfully.")
        trusted_schema_table = trusted_schema
        trusted_schema_table["trusted_table"] = t_table
        context.log.info(f"Trusted schema table: {trusted_schema_table}")
        yield Output(trusted_schema_table)
    except Exception as e:
        context.log.error(f"Error: {e}")
        yield Output(
            {
                "trusted_schema": "",
                "catalog": "",
                "raw_schema": "",
                "raw_table": "",
                "trusted_table": "",
            }
        )

