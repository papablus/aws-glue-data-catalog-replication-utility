import json
from typing import Dict, Any
 
import boto3

class SQSUtil:

    def send_table_schema_to_sqs_queue(self, sqs: boto3.client, queue_url: str, large_table: Dict[str, Any],
                                       export_batch_id: str, source_glue_catalog_id: str) -> bool:

        table_info = json.dumps(large_table)

        status_code = 400
        message_sent_to_sqs = False
        message_attributes = {
            "ExportBatchId": {
                "DataType": "String.ExportBatchId",
                "StringValue": export_batch_id
            },
            "SourceGlueDataCatalogId": {
                "DataType": "String.SourceGlueDataCatalogId",
                "StringValue": source_glue_catalog_id
            },
            "SchemaType": {
                "DataType": "String.SchemaType",
                "StringValue": "largeTable"
            }
        }

        req = {
            "QueueUrl": queue_url,
            "MessageBody": table_info,
            "MessageAttributes": message_attributes
        }

        try:
            send_msg_res = sqs.send_message(**req)
            status_code = send_msg_res["ResponseMetadata"]["HTTPStatusCode"]
        except Exception as e:
            print(f"Exception thrown while writing message to SQS. {e}")

        if status_code == 200:
            message_sent_to_sqs = True
            print(f"Table details for table '{large_table['Table']['Name']}' of database '{large_table['Table']['DatabaseName']}' sent to SQS.")

        return message_sent_to_sqs

    def send_large_table_schema_to_sqs(self, sqs: boto3.client, queue_url: str, export_batch_id: str,
                                       source_glue_catalog_id: str, message: str, large_table: Dict[str, Any]) -> None:

        status_code = 400
        message_attributes = {
            "ExportBatchId": {
                "DataType": "String.ExportBatchId",
                "StringValue": export_batch_id
            },
            "SourceGlueDataCatalogId": {
                "DataType": "String.SourceGlueDataCatalogId",
                "StringValue": source_glue_catalog_id
            },
            "SchemaType": {
                "DataType": "String.SchemaType",
                "StringValue": "largeTable"
            }
        }

        req = {
            "QueueUrl": queue_url,
            "MessageBody": message,
            "MessageAttributes": message_attributes
        }

        try:
            send_msg_res = sqs.send_message(**req)
            status_code = send_msg_res["ResponseMetadata"]["HTTPStatusCode"]
        except Exception as e:
            print(f"Exception thrown while writing message to SQS. {e}")

        if status_code == 200:
            try:
                print(f"Large Table schema for table '{large_table['Table']['Name']}' of database '{large_table['Table']['DatabaseName']}' sent to SQS.")
            except Exception as e:
                print(f"Large Table schema for table '{large_table.table['Name']}' of database '{large_table.table['DatabaseName']}' sent to SQS.")

    def send_table_schema_to_dead_letter_queue(self, sqs: boto3.client, queue_url: str, table_status,
                                               export_batch_id: str, source_glue_catalog_id: str) -> None:

        status_code = 400
        message_attributes = {
            "ExportBatchId": {
                "DataType": "String.ExportBatchId",
                "StringValue": export_batch_id
            },
            "SourceGlueDataCatalogId": {
                "DataType": "String.SourceGlueDataCatalogId",
                "StringValue": source_glue_catalog_id
            },
            "SchemaType": {
                "DataType": "String.SchemaType",
                "StringValue": "Table"
            }
        }

        req = {
            "QueueUrl": queue_url,
            "MessageBody": table_status.table_schema,
            "MessageAttributes": message_attributes
        }

        try:
            send_msg_res = sqs.send_message(**req)
            status_code = send_msg_res["ResponseMetadata"]["HTTPStatusCode"]
        except Exception as e:
            print(f"Exception thrown while writing message to SQS. {e}")

        if status_code == 200:
            print(f"Table schema for table '{table_status.table_name}' of database '{table_status.db_name}' sent to SQS.")

    def send_database_schema_to_dead_letter_queue(self, sqs: boto3.client, queue_url: str, database_ddl: str,
                                                  database_name: str, export_batch_id: str,
                                                  source_glue_catalog_id: str) -> None:

        status_code = 400
        message_attributes = {
            "ExportBatchId": {
                "DataType": "String.ExportBatchId",
                "StringValue": export_batch_id
            },
            "SourceGlueDataCatalogId": {
                "DataType": "String.SourceGlueDataCatalogId",
                "StringValue": source_glue_catalog_id
            },
            "SchemaType": {
                "DataType": "String.SchemaType",
                "StringValue": "Database"
            }
        }

        req = {
            "QueueUrl": queue_url,
            "MessageBody": database_ddl,
            "MessageAttributes": message_attributes
        }

        try:
            send_msg_res = sqs.send_message(**req)
            status_code = send_msg_res["ResponseMetadata"]["HTTPStatusCode"]
        except Exception as e:
            print(f"Exception thrown while writing message to SQS. {e}")

        if status_code == 200:
            print(f"Database schema for database '{database_name}' sent to SQS.")
