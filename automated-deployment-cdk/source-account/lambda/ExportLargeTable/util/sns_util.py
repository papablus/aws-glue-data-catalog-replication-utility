import json
import time

from typing import List
from boto3 import client
from util.ddb_util import DDBUtil

class SNSUtil:

    def publish_large_table_schema_to_sns(self, sns_client, topic_arn, region, bucket_name, message,
                                          source_glue_catalog_id, export_batch_id, message_type):
        message_attributes = {
            "source_catalog_id": {
                "DataType": "String",
                "StringValue": source_glue_catalog_id
            },
            "message_type": {
                "DataType": "String",
                "StringValue": message_type
            },
            "export_batch_id": {
                "DataType": "String",
                "StringValue": export_batch_id
            },
            "bucket_name": {
                "DataType": "String",
                "StringValue": bucket_name
            },
            "region_name": {
                "DataType": "String",
                "StringValue": region
            }
        }

        try:
            publish_response = sns_client.publish(
                TopicArn=topic_arn,
                Message=message,
                MessageAttributes=message_attributes
            )
            return publish_response
        except Exception as e:
            print(f"Large Table message could not be published to SNS Topic. Topic ARN: {topic_arn}")
            print(f"Message to be published: {message}")
            print(e)

    def publish_database_schema_to_sns(self, sns_client, topic_arn, database_ddl,
                                       source_glue_catalog_id, export_batch_id):
        message_attributes = {
            "source_catalog_id": {
                "DataType": "String",
                "StringValue": source_glue_catalog_id
            },
            "message_type": {
                "DataType": "String",
                "StringValue": "database"
            },
            "export_batch_id": {
                "DataType": "String",
                "StringValue": export_batch_id
            }
        }

        try:
            
            print("database_ddldatabase_ddldatabase_ddl")
            print(database_ddl)
             
            publish_response = sns_client.publish(
                TopicArn=topic_arn,
                Message=database_ddl,
                MessageAttributes=message_attributes
            )
            return publish_response
        except Exception as e:
            print("Database schema could not be published to SNS Topic.")
            print(e)

    def publish_database_schemas_to_sns(self, sns_client, master_db_list: List[dict], sns_topic_arn: str,
                                        ddb_util: DDBUtil, ddb_tbl_name: str, source_glue_catalog_id: str) -> int:
        export_run_id = str(int(time.time() * 1000))  # Convert to milliseconds
        export_batch_id = export_run_id

        source_catalog_id_ma = {"DataType": "String", "StringValue": source_glue_catalog_id}
        msg_type_ma = {"DataType": "String", "StringValue": "database"}
        export_batch_id_ma = {"DataType": "String", "StringValue": export_batch_id}

        number_of_databases_exported = 0

        for db in master_db_list:
            database_ddl = json.dumps(db, default=lambda obj: obj.__dict__)
            
            message_attributes = {
                "source_catalog_id": source_catalog_id_ma,
                "message_type": msg_type_ma,
                "export_batch_id": export_batch_id_ma
            }

            try:
                publish_response = sns_client.publish(
                    TopicArn=sns_topic_arn,
                    Message=database_ddl,
                    MessageAttributes=message_attributes
                )
                number_of_databases_exported += 1
                print(f"Schema for Database '{db['Name']}' published to SNS Topic. Message_Id: {publish_response['MessageId']}")
                ddb_util.track_database_export_status(ddb_tbl_name, db['Name'], database_ddl, publish_response['MessageId'],
                                                      source_glue_catalog_id, int(export_run_id), export_batch_id, True)
            except Exception as e:
                print(f"Schema for Database '{db['Name']}' could not be published to SNS Topic. It will be audited in DynamoDB table.")
                print(e)
                ddb_util.track_database_export_status(ddb_tbl_name, db['Name'], database_ddl, "", source_glue_catalog_id,
                                                      int(export_run_id), export_batch_id, False)

        print(f"Number of databases exported to SNS: {number_of_databases_exported}")
        return number_of_databases_exported

    def publish_table_schema_to_sns(self, sns_client, topic_arn, table, table_ddl,
                                    source_glue_catalog_id, export_batch_id):
        message_attributes = {
            "source_catalog_id": {
                "DataType": "String",
                "StringValue": source_glue_catalog_id
            },
            "message_type": {
                "DataType": "String",
                "StringValue": "table"
            },
            "export_batch_id": {
                "DataType": "String",
                "StringValue": export_batch_id
            }
        }

        try:
            publish_response = sns_client.publish(
                TopicArn=topic_arn,
                Message=table_ddl,
                MessageAttributes=message_attributes
            )
            print(f"Table schema for Table '{table['Name']}' of database '{table['DatabaseName']}' published to SNS Topic. Message_Id: {publish_response['MessageId']}")
            return publish_response
        except Exception as e:
            print(f"Table schema for Table '{table['Name']}' of database '{table['DatabaseName']}' could not be published to SNS Topic. This will be tracked in DynamoDB table.")
            print(e)

    def publish_table_list_to_sns(self, sns_client, topic_arn, table_list, export_run_id, source_glue_catalog_id, export_batch_id):
        message_attributes = {
            "source_catalog_id": {
                "DataType": "String",
                "StringValue": source_glue_catalog_id
            },
            "message_type": {
                "DataType": "String",
                "StringValue": "table_list"
            },
            "msg_attr_export_batch_id": {
                "DataType": "String",
                "StringValue": export_batch_id
            },
            "export_run_id": {
                "DataType": "String",
                "StringValue": export_run_id
            }
        }

        try:
            publish_response = sns_client.publish(
                TopicArn=topic_arn,
                Message=table_list,
                MessageAttributes=message_attributes
            )
            print(f"Table list published to SNS Topic. Message_Id: {publish_response['MessageId']}")
            return publish_response
        except Exception as e:
            print(f"Table list could not be published to SNS Topic. This will be tracked in DynamoDB table.")
            print(e)
