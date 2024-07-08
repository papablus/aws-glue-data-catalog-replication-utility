import boto3

from botocore.exceptions import ClientError
from typing import List, Optional

class DDBUtil:

    def __init__(self, region_name: str = "us-east-1"):
        self.dynamodb = boto3.resource("dynamodb", region_name=region_name)

    def track_table_import_status(self, table_status, source_glue_catalog_id, target_glue_catalog_id,
                                  import_run_id, export_batch_id, ddb_tbl_name):
        table = self.dynamodb.Table(ddb_tbl_name)
        item = {
            "table_id": f"{table_status.table_name}|{table_status.db_name}",
            "import_run_id": import_run_id,
            "export_batch_id": export_batch_id,
            "table_name": table_status.table_name,
            "database_name": table_status.db_name,
            "table_schema": table_status.table_schema,
            "target_glue_catalog_id": target_glue_catalog_id,
            "source_glue_catalog_id": source_glue_catalog_id,
            "table_created": table_status.created,
            "table_updated": table_status.updated,
            "export_has_partitions": table_status.export_has_partitions,
            "partitions_updated": table_status.partitions_replicated
        }

        try:
            table.put_item(Item=item)
            print(f"Table item inserted to DynamoDB table. Table name: {table_status.table_name}")
            return True
        except ClientError as e:
            print(f"Could not insert a Table import status to DynamoDB table: {ddb_tbl_name}")
            print(e)
            return False

    def track_database_import_status(self, source_glue_catalog_id, target_glue_catalog_id, ddb_tbl_name,
                                     database_name, import_run_id, export_batch_id, is_created):
        table = self.dynamodb.Table(ddb_tbl_name)
        item = {
            "db_id": database_name,
            "import_run_id": import_run_id,
            "export_batch_id": export_batch_id,
            "target_glue_catalog_id": target_glue_catalog_id,
            "source_glue_catalog_id": source_glue_catalog_id,
            "is_created": is_created
        }

        try:
            table.put_item(Item=item)
            print(f"Database item inserted to DynamoDB table. Database name: {database_name}")
            return True
        except ClientError as e:
            print(f"Could not insert a Database import status to DynamoDB table: {ddb_tbl_name}")
            print(e)
            return False

    def track_table_export_status(self, ddb_tbl_name, glue_db_name, glue_table_name, glue_table_schema,
                                  sns_msg_id, glue_catalog_id, export_run_id, export_batch_id, is_exported,
                                  is_large_table, bucket_name=None, object_key=None):
        table = self.dynamodb.Table(ddb_tbl_name)
        item = {
            "table_id": f"{glue_table_name}|{glue_db_name}",
            "export_run_id": export_run_id,
            "export_batch_id": export_batch_id,
            "source_glue_catalog_id": glue_catalog_id,
            "table_schema": glue_table_schema,
            "sns_msg_id": sns_msg_id,
            "is_exported": is_exported,
            "is_large_table": is_large_table
        }

        if bucket_name and object_key:
            item["s3_bucket_name"] = bucket_name
            item["object_key"] = object_key

        try:
            table.put_item(Item=item)
            print(f"Table item inserted to DynamoDB table. Table name: {glue_table_name}")
            return True
        except ClientError as e:
            print(f"Could not insert a Table export status to DynamoDB table: {ddb_tbl_name}")
            print(e)
            return False

    def track_database_export_status(self, ddb_tbl_name, glue_db_name, glue_db_schema, sns_msg_id,
                                     glue_catalog_id, export_run_id, export_batch_id, is_exported):
        table = self.dynamodb.Table(ddb_tbl_name)
        item = {
            "db_id": glue_db_name,
            "export_run_id": export_run_id,
            "export_batch_id": export_batch_id,
            "source_glue_catalog_id": glue_catalog_id,
            "database_schema": glue_db_schema,
            "sns_msg_id": sns_msg_id,
            "is_exported": is_exported
        }

        try:
            table.put_item(Item=item)
            print(f"Status inserted to DynamoDB table for Glue Database: {glue_db_name}")
            return True
        except ClientError as e:
            print(f"Could not insert a Database export status to DynamoDB table: {ddb_tbl_name}")
            print(e)
            return False

    def insert_into_dynamodb(self, item_list: List[dict], dynamodb_tbl_name: str):
        print(f"Inserting {len(item_list)} items to DynamoDB using Batch API call.")
        dynamodb = boto3.client("dynamodb")
        batch_size = 25
        for i in range(0, len(item_list), batch_size):
            mini_batch = item_list[i:i + batch_size]
            request_items = {dynamodb_tbl_name: [item for item in mini_batch]}
            try:
                response = dynamodb.batch_write_item(RequestItems=request_items)
                unprocessed_items = response.get("UnprocessedItems", {})
                while unprocessed_items:
                    response = dynamodb.batch_write_item(RequestItems=unprocessed_items)
                    unprocessed_items = response.get("UnprocessedItems", {})
            except ClientError as e:
                print(f"Error inserting items to DynamoDB table: {dynamodb_tbl_name}")
                print(e)
