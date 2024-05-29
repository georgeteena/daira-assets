import base64

import requests
from dagster import asset, Field, Output, resource


@resource(config_schema={
    "client_id": Field(str),
    "secret_key": Field(str),
    "auth_url": Field(str),
    "discovery_url": Field(str),
})
def starburst_resource(context):
    return {
        "client_id": context.resource_config['client_id'],
        "secret_key": context.resource_config['secret_key'],
        "auth_url": context.resource_config['auth_url'],
        "discovery_url": context.resource_config['discovery_url']
    }


@asset(
    required_resource_keys={"starburst"},
    config_schema={
        "get_sql_url": Field(str)
    }
)
def schema_discovery(context, upload_s3: str):
    context.log.info(f"URI: {upload_s3}")
    access_token = get_starburst_auth_token(context)
    context.log.info(f"S3 URI: {upload_s3}")
    discovery_body = {
        "uri": upload_s3,
        "defaultSchemaName": "discovered_schema",
        "skipApplying": False,
        "forceFullDiscovery": False,
        "options": {
            "maxSampleFilesPerTable": "10",
            "maxSampleLines": "10"
        }
    }

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    discovery_url = context.resources.starburst['discovery_url']
    context.log.info(f"Triggering schema discovery for {upload_s3} and discovery_url {discovery_url}")
    try:
        response = requests.request("POST", discovery_url, headers=headers, json=discovery_body)
        if response.ok:
            response_data = response.json()
            context.log.info(f"Schema discovery response {response_data}")
            sql_statements = get_sql_statements(context, response_data)
            yield Output(sql_statements)
        else:
            context.log.error("Schema discovery request failed.")
            raise Exception(f"{response.json()}")
    except Exception as e:
        context.log.error(f"Error triggering schema discovery: {e}")
        raise e


def get_sql_statements(context, response_data):
    schema_discovery_id = response_data.get("schemaDiscoveryId")
    access_token = get_starburst_auth_token(context)
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }
    sql_statements_resp = requests.get(url=f"{context.op_config['get_sql_url']}/{schema_discovery_id}", headers=headers)
    if sql_statements_resp.ok:
        sql_statements = sql_statements_resp.json().get("sqlStatements", [])
        context.log.info(f"SQL statements: {sql_statements}")
        context.log.info(f"Table Counts - Created: {sql_statements_resp.json().get('createdTablesCount')},  "
                         f"Updated: {sql_statements_resp.json().get('updatedTablesCount')}, "
                         f"Deleted: {sql_statements_resp.json().get('deletedTablesCount')}")
        return sql_statements
    else:
        context.log.error(f"Error getting SQL statements: {sql_statements_resp.text}")
        return []


def get_starburst_auth_token(context):
    context.log.info("Authenticating with Starburst")
    starburst = context.resources.starburst
    client_id = starburst['client_id']
    secret_key = starburst['secret_key']
    url = starburst['auth_url']

    credentials = f"{client_id}:{secret_key}".encode('utf-8')
    encoded_credentials = base64.b64encode(credentials).decode('utf-8')

    headers = {
        'Authorization': f'Basic {encoded_credentials}',
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    data = {
        'grant_type': 'client_credentials',
    }

    try:
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            context.log.info("Authentication successful")
            return response.json()['access_token']
        else:
            context.log.error(f"Failed to authenticate with Starburst: {response.status_code} - {response.text}")
            raise Exception('Failed to authenticate with Starburst.')
    except Exception as e:
        context.log.error(f"Error authenticating with Starburst: {e}")
        raise e
