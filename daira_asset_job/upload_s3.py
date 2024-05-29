import os

import boto3
import pandas as pd
from dagster import Config, Field, asset, resource, StringSource, Output


class S3Config(Config):
    bucket_name: str
    key_prefix: str


@resource(
    config_schema={
        'profile': Field(StringSource, is_required=False),
        'region': Field(StringSource, is_required=False),
    }
)
def s3_resource(context):
    profile = context.resource_config.get('profile')
    region = context.resource_config.get('region')
    context.log.info(f"Creating S3 resource with profile: {profile} and region: {region}")
    session = boto3.Session(profile_name=profile, region_name=region)
    return session.resource('s3')


@asset(
    required_resource_keys={"s3"},
    config_schema={
        'bucket_name': Field(str),
        'key_prefix': Field(str),
    }
)
def upload_s3(context, daira_assets: pd.DataFrame):
    s3 = context.resources.s3
    config = context.op_config
    bucket_name = config['bucket_name']
    key_prefix = f"{config['key_prefix']}asset"
    daira_assets['created_at'] = pd.to_datetime(daira_assets['created_at'])
    daira_assets['year_month'] = daira_assets['created_at'].dt.to_period('M').astype(str)

    context.log.info(f"Uploading data to S3 bucket: {bucket_name}")

    try:
        partitioned_folder = './asset/partitioned_parquet'

        daira_assets.to_parquet(partitioned_folder, partition_cols=['year_month'], index=False, engine='pyarrow')
        context.log.info(f"Data partitioned and saved to {partitioned_folder}")

        for root, dirs, files in os.walk(partitioned_folder):
            for file in files:
                file_path = os.path.join(root, file)
                s3_key = os.path.join(key_prefix, os.path.relpath(file_path, partitioned_folder))
                s3.Bucket(bucket_name).upload_file(file_path, s3_key)
                context.log.info(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
                os.remove(file_path)

        yield Output(f"s3://{bucket_name}/{key_prefix}/")
    except Exception as e:
        context.log.error(f"Error uploading data to S3: {e}")
        yield Output(None)
