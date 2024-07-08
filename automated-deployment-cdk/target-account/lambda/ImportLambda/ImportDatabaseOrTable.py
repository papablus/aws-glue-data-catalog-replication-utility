import boto3
import json
import os

from botocore.config import Config
from botocore.exceptions import ClientError
from typing import List, Dict

from util.gdc_util import GDCUtil
from util.large_table import LargeTable
from util.sqs_util import SQSUtil
from util.table_with_partitions import TableWithPartitions


region = os.environ.get("region", "us-east-1")
target_glue_catalog_id = os.environ.get("target_glue_catalog_id", "1234567890")
skip_table_archive = bool(os.environ.get("skip_archive", "true"))
ddb_tbl_name_for_db_status_tracking = os.environ.get("ddb_name_db_import_status", "ddb_name_db_import_status")
ddb_tbl_name_for_table_status_tracking = os.environ.get("ddb_name_table_import_status", "ddb_name_table_import_status")
sqs_queue_url = os.environ.get("dlq_url_sqs", "")
sqs_queue_url_large_table = os.environ.get("sqs_queue_url_large_tables", "")

config = Config(retries={"max_attempts": 10})
glue = boto3.client("glue", region_name=region, config = config)
sqs = boto3.client("sqs", region_name=region, config = config)

def print_env_variables():
    print(f"Target Catalog Id: {target_glue_catalog_id}")
    print(f"Skip Table Archive: {skip_table_archive}")
    print(f"DynamoDB Table for DB Import Auditing: {ddb_tbl_name_for_db_status_tracking}")
    print(f"DynamoDB Table for Table Import Auditing: {ddb_tbl_name_for_table_status_tracking}")
    print(f"Dead Letter Queue URL: {sqs_queue_url}")
    print(f"Region: {region}")
    print(f"SQS Queue URL for Large Tables: {sqs_queue_url_large_table}")

def process_sns_event(sns_records: List[Dict]):
    sqs_util = SQSUtil()

    for sns_record in sns_records:
        is_database_type = False
        is_table_type = False
        is_large_table = False
        large_table = None
        db = None
        table = None

        message = sns_record["Sns"]["Message"]
        print(f"SNS Message Payload: {message}")

        msg_attribute_map = sns_record["Sns"]["MessageAttributes"]
        msg_type_attr = msg_attribute_map["message_type"]["Value"]
        source_catalog_id_attr = msg_attribute_map["source_catalog_id"]["Value"]
        export_batch_id_attr = msg_attribute_map["export_batch_id"]["Value"]
        source_glue_catalog_id = source_catalog_id_attr
        export_batch_id = export_batch_id_attr
        print(f"Message Type: {msg_type_attr}")
        print(f"Source Catalog Id: {source_glue_catalog_id}")

        try:
            if msg_type_attr.lower() == "database":
                db = json.loads(message)
                is_database_type = True
            elif msg_type_attr.lower() == "table":
                msg = json.loads(message)
                table = TableWithPartitions(msg)
                is_table_type = True
            elif msg_type_attr.lower() == "largetable":
                msg = json.loads(message)
                large_table = LargeTable()
                large_table.catalog_id = msg.get("catalog_id", "")
                large_table.large_table = msg.get("large_table", False)
                large_table.number_of_partitions = msg.get("number_of_partitions", 0)
                large_table.table = msg.get("table", "")
                large_table.s3_object_key = msg.get("s3_object_key", "")
                large_table.s3_bucket_name = msg.get("s3_bucket_name", "")
                is_large_table = True
        except json.JSONDecodeError as e:
            print("Cannot parse SNS message to Glue Database Type.")
            print(e)

        gdc_util = GDCUtil()
        
        if is_database_type:
            gdc_util.process_database_schema(glue, sqs, target_glue_catalog_id, db, message,
                                                sqs_queue_url, source_glue_catalog_id, export_batch_id,
                                                ddb_tbl_name_for_db_status_tracking)
        elif is_table_type:
            gdc_util.process_table_schema(glue, sqs, target_glue_catalog_id, source_glue_catalog_id,
                                            table, message, ddb_tbl_name_for_table_status_tracking,
                                            sqs_queue_url, export_batch_id, skip_table_archive)
        elif is_large_table:
            sqs_util.send_large_table_schema_to_sqs(sqs, sqs_queue_url_large_table, export_batch_id,
                                                    source_glue_catalog_id, message, large_table)

def lambda_handler(event, context):
    print_env_variables()
    sns_records = event["Records"]
    process_sns_event(sns_records)
    return "Success"
