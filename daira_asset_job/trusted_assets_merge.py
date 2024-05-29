from dagster import asset


@asset(
    required_resource_keys={"starburst_session"}
    )
def trusted_assets_merge(context, alter_table: dict):
    updated_columns = alter_table['trusted_columns']
    unique_id = alter_table['unique_id']
    session = context.resources.starburst_session
    set_clause = ", ".join([f"{col} = s.{col}" for col in updated_columns if col != {unique_id}])
    values_clause = ", ".join([f"s.{col}" for col in updated_columns])
    columns_clause = ", ".join(updated_columns)
    context.log.info(f"Trusted table content: alter_table['trusted_table']")
    catalog = alter_table["trusted_table"]["catalog"]
    t_schema = alter_table["trusted_table"]["trusted_schema"]
    t_table = alter_table["trusted_table"]["trusted_table"]
    r_schema = alter_table["trusted_table"]["raw_schema"]
    r_table = alter_table["trusted_table"]["raw_table"]
    merge_sql = f"""
    MERGE INTO {catalog}.{t_schema}.{t_table} AS t
    USING {catalog}.{r_schema}.{r_table} AS s
    ON t.{unique_id} = s.{unique_id}
    WHEN MATCHED THEN UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN INSERT ({columns_clause})
    VALUES ({values_clause})
    """

    context.log.info(f"Executing merge SQL: {merge_sql}")
    try:
        session.sql(merge_sql).collect()
        context.log.info(f"Synchronization of {r_schema} to {t_schema} tables completed.")
    except Exception as e:
        context.log.error(f"Error: {e}")
        raise e
