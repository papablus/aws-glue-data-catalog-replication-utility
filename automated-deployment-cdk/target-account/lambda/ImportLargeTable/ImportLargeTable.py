import json
import os
import time
from typing import Dict, List

import boto3
from botocore.config import Config

from util.ddb_util import DDBUtil
from util.glue_util import GlueUtil
from util.large_table import LargeTable
from util.s3_util import S3Util
from util.table_replication_status import TableReplicationStatus

def print_env_variables(target_glue_catalog_id, skip_table_archive, ddb_tbl_name_for_table_status_tracking, region):
    print(f"Target Catalog Id: {target_glue_catalog_id}")
    print(f"Skip Table Archive: {skip_table_archive}")
    print(f"DynamoDB Table for Table Import Auditing: {ddb_tbl_name_for_table_status_tracking}")
    print(f"Region: {region}")

def lambda_handler(event, context):
    region = os.environ.get("region", "us-east-1")
    target_glue_catalog_id = os.environ.get("target_glue_catalog_id", "1234567890")
    skip_table_archive = os.environ.get("skip_archive", "true").lower() == "true"
    ddb_tbl_name_for_table_status_tracking = os.environ.get("ddb_name_table_import_status", "ddb_name_table_import_status")

    print_env_variables(target_glue_catalog_id, skip_table_archive, ddb_tbl_name_for_table_status_tracking, region)

    config = Config(retries={"max_attempts": 10})
    glue = boto3.client("glue", region_name=region, config=config)
    sqs = boto3.client("sqs", region_name=region, config=config)
    
    print("EVENT INCIAL")
    print(event)

    print(f"Number of messages in SQS Event: {len(event['Records'])}")

    for record in event["Records"]:
        ddl = record["body"]
        export_batch_id = ""
        schema_type = ""
        source_glue_catalog_id = ""

        for key, value in record["messageAttributes"].items():
            if key.lower() == "exportbatchid":
                export_batch_id = value["stringValue"]
                print(f"Export Batch Id: {export_batch_id}")
            elif key.lower() == "sourcegluedatacatalogid":
                source_glue_catalog_id = value["stringValue"]
                print(f"Source Glue Data Catalog Id: {source_glue_catalog_id}")
            elif key.lower() == "schematype":
                schema_type = value["stringValue"]
                print(f"Message Schema Type: {schema_type}")

        if schema_type.lower() == "largetable":
            record_processed = process_record(context, glue, sqs, target_glue_catalog_id, ddb_tbl_name_for_table_status_tracking,
                                              ddl, skip_table_archive, export_batch_id, source_glue_catalog_id, region)

        if not record_processed:
            print(f"Input message '{ddl}' could not be processed. This is an exception. It will be reprocessed again.")
            raise RuntimeError()

    return "Success"

def process_record(context, glue, sqs, target_glue_catalog_id, ddb_tbl_name_for_table_status_tracking,
                   message, skip_table_archive, export_batch_id, source_glue_catalog_id, region):
    record_processed = False
    s3_util = S3Util()
    ddb_util = DDBUtil()
    glue_util = GlueUtil()

    large_table = None
    table_status = None
    import_run_id = int(time.time() * 1000)

    try:
        msg = json.loads(message)
        large_table = LargeTable()
        large_table.catalog_id = msg.get("catalog_id", "")
        large_table.large_table = msg.get("large_table", False)
        large_table.number_of_partitions = msg.get("number_of_partitions", 0)
        large_table.table = msg.get("table", "")
        large_table.s3_object_key = msg.get("s3_object_key", "")
        large_table.s3_bucket_name = msg.get("s3_bucket_name", "")
    except json.JSONDecodeError as e:
        print("Cannot parse SNS message to Glue Table Type.")
        print(e)

    if large_table:
        table_status = glue_util.create_or_update_table(glue, large_table.table, target_glue_catalog_id, skip_table_archive)
        table_status.table_schema = message

    if not table_status.error:
        partition_list_from_export = s3_util.get_partitions_from_s3(region, large_table.s3_bucket_name, large_table.s3_object_key)
        partitions_b4_replication = glue_util.get_partitions(glue, target_glue_catalog_id, large_table.table["DatabaseName"], large_table.table["Name"])
        print(f"Number of partitions before replication: {len(partitions_b4_replication)}")

        if table_status.replicated and len(partition_list_from_export) > 0:
            table_status.export_has_partitions = True
            if len(partitions_b4_replication) == 0:
                print("Adding partitions based on the export.")
                partitions_added = glue_util.add_partitions(glue, partition_list_from_export, target_glue_catalog_id,
                                                            large_table.table["DatabaseName"], large_table.table["Name"])
                if partitions_added:
                    table_status.partitions_replicated = True
                    record_processed = True
            else:
                print("Target table has partitions. They will be deleted first before adding partitions based on Export.")
                partitions_deleted = glue_util.delete_partitions(glue, target_glue_catalog_id, large_table.table["DatabaseName"],
                                                                 large_table.table["Name"], partitions_b4_replication)
                partitions_added = glue_util.add_partitions(glue, partition_list_from_export, target_glue_catalog_id,
                                                            large_table.table["DatabaseName"], large_table.table["Name"])

                if partitions_deleted and partitions_added:
                    table_status.partitions_replicated = True
                    record_processed = True

        elif table_status.replicated and len(partition_list_from_export) == 0:
            table_status.export_has_partitions = False
            if len(partitions_b4_replication) > 0:
                partitions_deleted = glue_util.delete_partitions(glue, target_glue_catalog_id, large_table.table["DatabaseName"],
                                                                 large_table.table["Name"], partitions_b4_replication)
                if partitions_deleted:
                    table_status.partitions_replicated = True
                    record_processed = True
    else:
        print("Table replicated but partitions were not replicated. Message will be reprocessed again.")

    ddb_util.track_table_import_status(table_status, source_glue_catalog_id, target_glue_catalog_id, import_run_id,
                                       export_batch_id, ddb_tbl_name_for_table_status_tracking)
    print(f"Processing of Table schema completed. Result: Table replicated: {table_status.replicated}, "
          f"Export has partitions: {table_status.export_has_partitions}, Partitions replicated: {table_status.partitions_replicated}, "
          f"Error: {table_status.error}")

    return record_processed
