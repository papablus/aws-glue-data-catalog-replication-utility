import json
import os
from typing import Dict, List

import boto3
from botocore.config import Config

from util.gdc_util import GDCUtil
from util.table_with_partitions import TableWithPartitions

def print_env_variables(target_glue_catalog_id, skip_table_archive, ddb_tbl_name_for_db_status_tracking,
                        ddb_tbl_name_for_table_status_tracking, sqs_queue_url, region):
    print(f"Target Catalog Id: {target_glue_catalog_id}")
    print(f"Skip Table Archive: {skip_table_archive}")
    print(f"DynamoDB Table for DB Import Auditing: {ddb_tbl_name_for_db_status_tracking}")
    print(f"DynamoDB Table for Table Import Auditing: {ddb_tbl_name_for_table_status_tracking}")
    print(f"Dead Letter Queue URL: {sqs_queue_url}")
    print(f"Region: {region}")

def lambda_handler(event, context):
    region = os.environ.get("region", "us-east-1")
    target_glue_catalog_id = os.environ.get("target_glue_catalog_id", "1234567890")
    skip_table_archive = os.environ.get("skip_archive", "true").lower() == "true"
    ddb_tbl_name_for_db_status_tracking = os.environ.get("ddb_name_db_import_status", "ddb_name_db_import_status")
    ddb_tbl_name_for_table_status_tracking = os.environ.get("ddb_name_table_import_status", "ddb_name_table_import_status")
    sqs_queue_url = os.environ.get("dlq_url_sqs", "")

    print_env_variables(target_glue_catalog_id, skip_table_archive, ddb_tbl_name_for_db_status_tracking,
                        ddb_tbl_name_for_table_status_tracking, sqs_queue_url, region)

    config = Config(retries={"max_attempts": 10})
    glue = boto3.client("glue", region_name=region, config=config)
    sqs = boto3.client("sqs", region_name=region, config=config)

    print(f"Number of messages in SQS Event: {len(event['Records'])}")

    for record in event["Records"]:
        ddl = record["body"]
        export_batch_id = ""
        source_glue_catalog_id = ""
        schema_type = ""
        is_table = False

        for key, value in record["messageAttributes"].items():
            if key.lower() == "exportbatchid":
                export_batch_id = value["stringValue"]
                print(f"Export Batch Id: {export_batch_id}")
            elif key.lower() == "sourcegluedatacatalogid":
                source_glue_catalog_id = value["stringValue"]
                print(f"Source Glue Data Catalog Id: {source_glue_catalog_id}")
            elif key.lower() == "schematype":
                schema_type = value["stringValue"]
                print(f"Message Schema Type {schema_type}")

        print(f"Schema: {ddl}")

        if schema_type.lower() == "table":
            is_table = True

        process_record(context, glue, sqs, sqs_queue_url, target_glue_catalog_id, ddb_tbl_name_for_db_status_tracking,
                       ddb_tbl_name_for_table_status_tracking, ddl, skip_table_archive, export_batch_id,
                       source_glue_catalog_id, is_table)

    return "Success"

def process_record(context, glue, sqs, sqs_queue_url, target_glue_catalog_id, ddb_tbl_name_for_db_status_tracking,
                   ddb_tbl_name_for_table_status_tracking, message, skip_table_archive, export_batch_id,
                   source_glue_catalog_id, is_table):
    is_database_type = False
    is_table_type = False

    db = None
    table = None

    if is_table:
        context.log("The input message is of type Glue Table.")
        try:
            table = TableWithPartitions(json.loads(message))
            is_table_type = True
        except json.JSONDecodeError as e:
            print("Cannot parse SNS message to Glue Table Type.")
            print(e)
    else:
        context.log("The input message is of type Glue Database.")
        try:
            db = json.loads(message)
            is_database_type = True
        except json.JSONDecodeError as e:
            print("Cannot parse SNS message to Glue Database Type.")
            print(e)

    gdc_util = GDCUtil()
    if is_database_type:
        gdc_util.process_database_schema(glue, sqs, target_glue_catalog_id, db, message, sqs_queue_url,
                                         source_glue_catalog_id, export_batch_id, ddb_tbl_name_for_db_status_tracking)
    elif is_table_type:
        gdc_util.process_table_schema(glue, sqs, target_glue_catalog_id, source_glue_catalog_id, table, message,
                                      ddb_tbl_name_for_table_status_tracking, sqs_queue_url, export_batch_id,
                                      skip_table_archive)
