import json
import datetime
import time
import sys
import os
import uuid
from typing import List, Dict
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from util.ddb_util import DDBUtil
from util.glue_util import GlueUtil
from util.sns_util import SNSUtil
from util.sqs_util import SQSUtil
from util.s3_util import S3Util

region = os.environ.get("region", "us-east-1")
source_glue_catalog_id = os.environ.get("source_glue_catalog_id", "1234567890")
topic_arn = os.environ.get("sns_topic_arn_export_dbs_tables", "arn:aws:sns:us-east-1:1234567890:GlueExportSNSTopic")
topic_table_list_arn = os.environ.get("sns_topic_arn_table_list", "arn:aws:sns:us-east-1:905418170506:ReplicationPlannerSNSTopic")
ddb_tbl_name_for_db_status_tracking = os.environ.get("ddb_name_db_export_status", "ddb_name_db_export_status")
ddb_tbl_name_for_table_status_tracking = os.environ.get("ddb_name_table_export_status", "ddb_name_table_export_status")
sqs_queue_4_large_tables = os.environ.get("sqs_queue_url_large_tables", "")
s3_large_table_schema = os.environ.get("s3_large_table_schema", "")
partition_threshold = 10
table_partitions_threshold = 245000

config = Config(retries={"max_attempts": 10})
glue = boto3.client("glue", region_name=region)
sns = boto3.client("sns", region_name=region)
sqs = boto3.client("sqs", region_name=region, config=config)

def process_sns_event(sns_records: List[Dict], ddb_util: DDBUtil, sns_util: SNSUtil, glue_util: GlueUtil, sqs_util: SQSUtil):
    export_run_id = int(time.time() * 1000)

    for sns_record in sns_records:
        is_database_type = False

        database_ddl = sns_record["Sns"]["Message"]
        print(f"SNS Message Payload: {database_ddl}")
        msg_attribute_map = sns_record["Sns"]["MessageAttributes"]
        msg_attr_message_type = msg_attribute_map["message_type"]["Value"]
        msg_attr_export_batch_id = msg_attribute_map["export_batch_id"]["Value"]

        print(f"Message Attribute value: {msg_attr_message_type}")

        try:
            if msg_attr_message_type.lower() == "database":
                db = json.loads(database_ddl)
                is_database_type = True
        except json.JSONDecodeError as e:
            print("Cannot parse SNS message to Glue Database Type.")
            print(e)

        if is_database_type:
            database = glue_util.get_database_if_exist(glue, source_glue_catalog_id, db)
            if database:
                publish_db_response = sns_util.publish_database_schema_to_sns(sns, topic_arn, database_ddl,
                                                                                source_glue_catalog_id, msg_attr_export_batch_id)
                if publish_db_response["MessageId"]:
                    print(f"Database schema published to SNS Topic. Message_Id: {publish_db_response['MessageId']}")
                    ddb_util.track_database_export_status(ddb_tbl_name_for_db_status_tracking, db["Name"], database_ddl,
                                                            publish_db_response["MessageId"], source_glue_catalog_id,
                                                            export_run_id, msg_attr_export_batch_id, True)
                else:
                    ddb_util.track_database_export_status(ddb_tbl_name_for_db_status_tracking, db["Name"], database_ddl,
                                                            "", source_glue_catalog_id, export_run_id, msg_attr_export_batch_id, False)

                #Hoy en día esa función retorna una lista con la totalidad de tablas para empezar a recorrer y obtener las particiones
                glue_util.get_tables(glue, source_glue_catalog_id, database["Name"], sns_util, sns, export_run_id, msg_attr_export_batch_id, topic_table_list_arn)

            else:
                print(f"There is no Database with name '{db['Name']}' exist in Glue Data Catalog. Tables cannot be retrieved.")
        else:
            print("Message received from SNS Topic seems to be invalid. It could not be converted to Glue Database Type.")

#Funcion encargada de recibir un listado de N tablas, obtener las particiones y hacer que el proceso siga común y corriente
def process_sns_table_event(db_table_list: List[Dict], ddb_util: DDBUtil, sns_util: SNSUtil, glue_util: GlueUtil, sqs_util: SQSUtil, export_run_id, msg_attr_export_batch_id, s3_util):

    number_of_tables_exported = 0
    item_list = []
    table_lt = json.loads(db_table_list)
    db_name = table_lt[0]['DatabaseName']

    for table in table_lt:
        partition_list = glue_util.get_partitions(glue, source_glue_catalog_id, table["DatabaseName"], table["Name"])

        print(f"Database: {table['DatabaseName']}, Table: {table['Name']}, num_partitions: {len(partition_list)}")

        table_with_parts = {
            "PartitionList": partition_list,
            "Table": table
        }

        size = get_json_size(table_with_parts) / 1024
        print(f"Table size {table['Name']}: {size} KB")

        if len(partition_list) <= partition_threshold and get_json_size(table_with_parts) < table_partitions_threshold:
            print(f"Table {table['Name']} Case 1. Num Partitions <= Threshold and size < {size}kb")

            table_ddl = json.dumps(table_with_parts)
            publish_table_response = sns_util.publish_table_schema_to_sns(sns, topic_arn, table, table_ddl,
                                                                            source_glue_catalog_id, msg_attr_export_batch_id)

            item = {
                "table_id": {"S" : f"{table['Name']}|{table['DatabaseName']}"},
                "export_run_id": {"N" : str(export_run_id)},
                "export_batch_id": {"S" : msg_attr_export_batch_id},
                "source_glue_catalog_id": {"S" : source_glue_catalog_id},
                "table_schema": {"S" : table_ddl},
                "is_large_table": {"S" : "false"}
            }

            if publish_table_response["MessageId"]:
                item["sns_msg_id"] = {"S" : publish_table_response["MessageId"]}
                item["is_exported"] = {"S" : "true"}
                number_of_tables_exported += 1
            else:
                item["sns_msg_id"] = {"S" : ""}
                item["is_exported"] = {"S" : "false"}

            item_list.append({"PutRequest": {"Item": item}})
        elif len(partition_list) > partition_threshold and get_json_size(table) < table_partitions_threshold:
            print(f"Table {table['Name']} Case 2. Num Partitions > Threshold and size < {size}kb")

            large_table = {
                "Table": table,
                "LargeTable": True,
                "NumberOfPartitions": len(partition_list),
                "CatalogId": source_glue_catalog_id
            }

            print(f"Database: {table['DatabaseName']}, Table: {table['Name']}, num_partitions: {len(partition_list)}")
            print("This will be sent to SQS Queue for further processing.")

            sqs_util.send_table_schema_to_sqs_queue(sqs, sqs_queue_4_large_tables, large_table,
                                                    msg_attr_export_batch_id, source_glue_catalog_id)

        elif get_json_size(table_with_parts) >= table_partitions_threshold:
            print(f"Table {table['Name']} Case 3. (Table + Partitions) size >= {size}kb")

            date_str = datetime.datetime.now().strftime("%Y-%m-%d")
            object_key = f"{date_str}_{int(time.time() * 1000)}_{source_glue_catalog_id}_{table['DatabaseName']}_{table['Name']}.txt"

            object_created = s3_util.create_s3_object(region, s3_large_table_schema, object_key, json.dumps(table_with_parts))

            msg = {"bucket_name":s3_large_table_schema, "object_key":object_key}

            publish_response = sns_util.publish_large_table_schema_to_sns(
                sns, topic_arn, region, s3_large_table_schema, str(msg),
                source_glue_catalog_id, msg_attr_export_batch_id, "table")

            if publish_response:
                ddb_util.track_table_export_status(
                    ddb_tbl_name_for_table_status_tracking,
                    table["DatabaseName"], table["Name"], table_with_parts,
                    publish_response["MessageId"], source_glue_catalog_id, int(export_run_id), msg_attr_export_batch_id,
                    True, True, s3_large_table_schema, object_key
                )
            else:
                ddb_util.track_table_export_status(
                    ddb_tbl_name_for_table_status_tracking,
                    table["DatabaseName"], table["Name"], table_with_parts,
                    "", source_glue_catalog_id, int(export_run_id), msg_attr_export_batch_id,
                    False, True, None, None
                )


    print(f"Inserting Table statistics to DynamoDB for database: {db_name}")
    ddb_util.insert_into_dynamodb(item_list, ddb_tbl_name_for_table_status_tracking)
    print(f"Table export statistics: number of tables exported to SNS in this event = {len(table_lt)}")

def get_json_size(json_dict):
    json_string = json.dumps(json_dict)
    json_size = len(json_string.encode('utf-8'))

    return json_size

def lambda_handler(event, context):

    print(F"event: {event}")
    print(f"Source Catalog Id: {source_glue_catalog_id}")
    print(f"SNS Topic Arn: {topic_arn}")
    print(f"DynamoDB Table for DB Export Auditing: {ddb_tbl_name_for_db_status_tracking}")
    print(f"DynamoDB Table for Table Export Auditing: {ddb_tbl_name_for_table_status_tracking}")
    print(f"SQS queue for large tables: {sqs_queue_4_large_tables}")

    sns_records = event["Records"]

    print(f"Number of messages in SNS Event: {len(sns_records)}")

    ddb_util = DDBUtil()
    sns_util = SNSUtil()
    glue_util = GlueUtil()
    sqs_util = SQSUtil()
    s3_util = S3Util()

    if sns_records[0]['Sns']['MessageAttributes']['message_type']['Value'] == 'table_list':

        export_run_id = sns_records[0]['Sns']['MessageAttributes']['export_run_id']['Value']
        msg_attr_export_batch_id = sns_records[0]['Sns']['MessageAttributes']['msg_attr_export_batch_id']['Value']

        #Funcion nueva que se encargará de procesar cada uno de los chunks
        process_sns_table_event(sns_records[0]['Sns']['Message'], ddb_util, sns_util, glue_util, sqs_util, export_run_id, msg_attr_export_batch_id, s3_util)
    else:
        #Funcion original que se encargará de obtener el listado de tablas y enviar los SNS por chunks
        process_sns_event(sns_records, ddb_util, sns_util, glue_util, sqs_util)

    return "Message from SNS Topic was processed successfully!"
