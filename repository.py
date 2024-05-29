from dagster import define_asset_job, Definitions

from daira_asset_job.alter_table import starburst_session, alter_table
from daira_asset_job.daira_assets import daira_assets
from daira_asset_job.schema_discovery import starburst_resource, schema_discovery
from daira_asset_job.trusted_assets_merge import trusted_assets_merge
from daira_asset_job.trusted_schema import trusted_schema
from daira_asset_job.trusted_table import trusted_table
from daira_asset_job.upload_s3 import upload_s3, s3_resource

daira_assets_job = define_asset_job(
    name="daira_assets_job",
    selection=
    [
        "daira_assets",
        "upload_s3",
        "schema_discovery",
        "trusted_schema",
        "trusted_table",
        "alter_table",
        "trusted_assets_merge"
    ]
)

defs = Definitions(
    jobs=[daira_assets_job],
    assets=[daira_assets, upload_s3, schema_discovery, trusted_schema, trusted_table, alter_table, trusted_assets_merge],
    resources={"s3": s3_resource, "starburst": starburst_resource, "starburst_session": starburst_session},
)
