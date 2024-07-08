import json
import os
import time
from datetime import datetime
from typing import Dict, List

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from util.ddb_util import DDBUtil
from util.glue_util import GlueUtil
from util.large_table import LargeTable
from util.s3_util import S3Util
from util.sns_util import SNSUtil

def lambda_handler(event, context):
    region = os.environ.get("region", "us-east-1")
    topic_arn = os.environ.get("sns_topic_arn_export_dbs_tables", "arn:aws:sns:us-east-1:1234567890:GlueExportSNSTopic")
    bucket_name = os.environ.get("s3_bucket_name", "")
    ddb_tbl_name_for_table_status_tracking = os.environ.get("ddb_name_table_export_status", "ddb_name_table_export_status")

    config = Config(retries={"max_attempts": 10})
    glue = boto3.client("glue", region_name=region, config=config)
    sns = boto3.client("sns", region_name=region)

    ddb_util = DDBUtil()
    glue_util = GlueUtil()
    s3_util = S3Util()
    sns_util = SNSUtil()

    object_key = ""
    large_table = None
    record_processed = False
    object_created = False

    print(f"Number of messages in SQS Event: {len(event['Records'])}")
    print(event["Records"])

    for record in event["Records"]:
        payload = json.loads(record["body"])
        export_batch_id = ""
        source_glue_catalog_id = ""
        message_type = ""

        export_run_id = int(time.time() * 1000)

        for key, value in record["messageAttributes"].items():
            if key.lower() == "exportbatchid":
                export_batch_id = value["stringValue"]
                print(f"Export Batch Id: {export_batch_id}")
            elif key.lower() == "sourcegluedatacatalogid":
                source_glue_catalog_id = value["stringValue"]
                print(f"Source Glue Data Catalog Id: {source_glue_catalog_id}")
            elif key.lower() == "schematype":
                message_type = value["stringValue"] 
                print(f"Message Type: {message_type}")

        if message_type.lower() == "largetable":
            large_table = LargeTable()
            large_table.catalog_id = payload.get("CatalogId")
            large_table.large_table = payload.get("LargeTable", False)
            large_table.number_of_partitions = payload.get("NumberOfPartitions", 0)
            large_table.table = payload.get("Table")
            large_table.s3_object_key = payload.get("s3ObjectKey", "")
            large_table.s3_bucket_name = payload.get("s3BucketName", bucket_name)

            if large_table.large_table:
                date_str = datetime.now().strftime("%Y-%m-%d")
                object_key = f"{date_str}_{int(time.time() * 1000)}_{source_glue_catalog_id}_{large_table.table['DatabaseName']}_{large_table.table['Name']}.txt"

                content = get_partitions_and_create_object_content(context, glue, glue_util, source_glue_catalog_id, large_table, export_batch_id)
                object_created = s3_util.create_s3_object(region, bucket_name, object_key, content)

            publish_response = None
            large_table_json = ""

            if object_created and object_key:
                large_table.s3_object_key = object_key
                large_table.s3_bucket_name = bucket_name
                large_table_json = json.dumps(large_table.__dict__)
                print(f"Large Table JSON: {large_table_json}")
                publish_response = sns_util.publish_large_table_schema_to_sns(
                    sns, topic_arn, region, bucket_name, large_table_json,
                    source_glue_catalog_id, export_batch_id, message_type
                )
                if publish_response:
                    print(f"Large Table Schema Published to SNS Topic. Message Id: {publish_response['MessageId']}")
                    record_processed = True

            if publish_response:
                ddb_util.track_table_export_status(
                    ddb_tbl_name_for_table_status_tracking,
                    large_table.table["DatabaseName"], large_table.table["Name"], large_table_json,
                    publish_response["MessageId"], source_glue_catalog_id, export_run_id, export_batch_id,
                    True, True, bucket_name, object_key
                )
            else:
                ddb_util.track_table_export_status(
                    ddb_tbl_name_for_table_status_tracking,
                    large_table.table["DatabaseName"], large_table.table["Name"], large_table_json,
                    "", source_glue_catalog_id, export_run_id, export_batch_id,
                    False, True, None, None
                )

    if not record_processed:
        print(f"Schema for table '{large_table.table['Name']}' of database '{large_table.table['DatabaseName']}' could not be exported. This is an exception. It will be retried again.")
        raise RuntimeError()

    return "Success"

def get_partitions_and_create_object_content(context, glue, glue_util, source_glue_catalog_id, large_table, export_batch_id):
    content = []
    table = glue_util.get_table(glue, source_glue_catalog_id, large_table.table["DatabaseName"], large_table.table["Name"])
    if table:
        partition_list = glue_util.get_partitions(glue, source_glue_catalog_id, large_table.table["DatabaseName"], large_table.table["Name"])
        for i, partition in enumerate(partition_list, start=1):
            partition_ddl = json.dumps(partition)
            content.append(partition_ddl)
            print(f"Partition #: {i}, schema: {partition_ddl}.")

    return "\n".join(content)
