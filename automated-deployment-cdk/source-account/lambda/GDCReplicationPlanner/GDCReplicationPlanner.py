import os
import logging
from typing import Optional, List
from boto3 import client
from boto3.session import Session

from util.ddb_util import DDBUtil
from util.glue_util import GlueUtil
from util.sns_util import SNSUtil


def lambda_handler(event, context):
    print(f"event: {event}")

    region = os.environ.get("region", "us-east-1")
    source_glue_catalog_id = os.environ.get("source_glue_catalog_id", "1234567890")
    database_prefix_list = os.environ.get("database_prefix_list", "")
    separator = os.environ.get("separator", "|")
    topic_arn = os.environ.get("sns_topic_arn_gdc_replication_planner",
                               "arn:aws:sns:us-east-1:1234567890:GlueExportSNSTopic")
    ddb_tbl_name_for_db_status_tracking = os.environ.get("ddb_name_gdc_replication_planner",
                                                         "ddb_name_gdc_replication_planner")

    # Print environment variables
    print_env_variables(source_glue_catalog_id, topic_arn, ddb_tbl_name_for_db_status_tracking,
                        database_prefix_list, separator)

    # Create clients for Glue and SNS
    glue = client("glue", region_name=region)
    sns = client("sns", region_name=region)

    # Create instances of utility classes
    ddb_util = DDBUtil()
    sns_util = SNSUtil()
    glue_util = GlueUtil()

    # Get databases from Glue
    db_list = glue_util.get_databases(glue, source_glue_catalog_id)

    # When database prefix string is empty or not provided, import all databases
    # Otherwise, import only the databases with the specified prefixes
    if not database_prefix_list:
        num_databases_exported = sns_util.publish_database_schemas_to_sns(
            sns, db_list, topic_arn, ddb_util, ddb_tbl_name_for_db_status_tracking, source_glue_catalog_id
        )
    else:
        # Tokenize the database prefix string into a list of database prefixes
        db_prefix_list = tokenize_database_prefix_string(database_prefix_list, separator)
        # Identify required databases to export
        dbs_to_export = get_required_databases(db_list, db_prefix_list)
        # Publish schemas for databases to SNS Topic
        num_databases_exported = sns_util.publish_database_schemas_to_sns(
            sns, dbs_to_export, topic_arn, ddb_util, ddb_tbl_name_for_db_status_tracking, source_glue_catalog_id
        )

    print(f"Database export statistics: number of databases exist = {len(db_list)}, "
          f"number of databases exported to SNS = {num_databases_exported}.")

    return "Lambda function to get a list of Databases completed successfully!"

def print_env_variables(source_glue_catalog_id, topic_arn, ddb_tbl_name_for_db_status_tracking,
                        database_prefix_list, separator):
    print(f"source_glue_catalog_id: {source_glue_catalog_id}")
    print(f"topic_arn: {topic_arn}")
    print(f"ddb_tbl_name_for_db_status_tracking: {ddb_tbl_name_for_db_status_tracking}")
    print(f"database_prefix_list: {database_prefix_list}")
    print(f"separator: {separator}")

def tokenize_database_prefix_string(database_prefix_string: str, separator: str) -> List[str]:
    return [prefix.strip() for prefix in database_prefix_string.split(separator) if prefix.strip()]

def get_required_databases(db_list: List[dict], db_prefix_list: List[str]) -> List[dict]:
    return [db for db in db_list if any(db["Name"].startswith(prefix) for prefix in db_prefix_list)]
