import json
import time

from util.ddb_util import DDBUtil
from util.sqs_util import SQSUtil
from util.glue_util import GlueUtil

class GDCUtil:
    
    def process_table_schema(self, glue, sqs, target_glue_catalog_id, source_glue_catalog_id,
                             table_with_partitions, message, ddb_tbl_name_for_table_status_tracking,
                             sqs_queue_url, export_batch_id, skip_table_archive):
        ddb_util = DDBUtil()
        sqs_util = SQSUtil()
        glue_util = GlueUtil()
        import_run_id = int(time.time() * 1000)

        table = table_with_partitions.table
        partition_list_from_export = table_with_partitions.partition_list

        table_status = glue_util.create_or_update_table(glue, table, target_glue_catalog_id, skip_table_archive)

        if table_status.db_not_found_error:
            print(f"Creating Database with name: '{table['DatabaseName']}'.")
            db_status = glue_util.create_glue_databases(glue, target_glue_catalog_id, table["DatabaseName"],
                                                       f"Database Imported from Glue Data Catalog of AWS Account Id: {source_glue_catalog_id}")
            if db_status.created:
                table_status = glue_util.create_or_update_table(glue, table_with_partitions.table, target_glue_catalog_id, skip_table_archive)

        table_status.get_schema(message)

        if not table_status.error:
            partitions_b4_replication = glue_util.get_partitions(glue, target_glue_catalog_id, table["DatabaseName"], table["Name"])
            print(f"Number of partitions before replication: {len(partitions_b4_replication)}")

            if len(partition_list_from_export) > 0:
                table_status.export_has_partitions = True
                if len(partitions_b4_replication) == 0:
                    print("Adding partitions based on the export.")
                    partitions_added = glue_util.add_partitions(glue, partition_list_from_export, target_glue_catalog_id,
                                                                table["DatabaseName"], table["Name"])
                    if partitions_added:
                        table_status.partitions_replicated = True
                else:
                    print("Table has partitions. They will be deleted first before adding partitions based on Export.")
                    partitions_deleted = glue_util.delete_partitions(glue, target_glue_catalog_id, table["DatabaseName"],
                                                                     table["Name"], partitions_b4_replication)
                    partitions_added = glue_util.add_partitions(glue, partition_list_from_export, target_glue_catalog_id,
                                                                table["DatabaseName"], table["Name"])

                    if partitions_deleted and partitions_added:
                        table_status.partitions_replicated = True
            elif len(partition_list_from_export) == 0:
                table_status.export_has_partitions = False
                if len(partitions_b4_replication) > 0:
                    partitions_deleted = glue_util.delete_partitions(glue, target_glue_catalog_id, table["DatabaseName"],
                                                                     table["Name"], partitions_b4_replication)
                    if partitions_deleted:
                        table_status.partitions_replicated = True
        else:
            print("Error in creating/updating table in the Glue Data Catalog. It will be sent to DLQ.")
            sqs_util.send_table_schema_to_dead_letter_queue(sqs, sqs_queue_url, table_status, export_batch_id, source_glue_catalog_id)

        ddb_util.track_table_import_status(table_status, source_glue_catalog_id, target_glue_catalog_id, import_run_id,
                                           export_batch_id, ddb_tbl_name_for_table_status_tracking)
        print(f"Processing of Table schema completed. Result: Table replicated: {table_status.replicated}, "
              f"Export has partitions: {table_status.export_has_partitions}, "
              f"Partitions replicated: {table_status.partitions_replicated}, Error: {table_status.error}")

    def process_database_schema(self, glue, sqs, target_glue_catalog_id, db,
                                message, sqs_queue_url, source_glue_catalog_id, export_batch_id,
                                ddb_tbl_name_for_db_status_tracking):
        ddb_util = DDBUtil()
        glue_util = GlueUtil()
        sqs_util = SQSUtil()

        is_db_created = False
        import_run_id = int(time.time() * 1000)
        database = glue_util.get_database_if_exist(glue, target_glue_catalog_id, db)
        db_exist = bool(database)

        if not db_exist:
            db_status = glue_util.create_glue_database(glue, target_glue_catalog_id, db)
            if db_status.error:
                print("Error in creating database in the Glue Data Catalog. It will be sent to DLQ.")
                sqs_util.send_database_schema_to_dead_letter_queue(sqs, sqs_queue_url, message, db["Name"], export_batch_id,
                                                                   source_glue_catalog_id)
            else:
                is_db_created = True
        else:
            print(f"Database with name '{database['Name']}' already exists in target Glue Data Catalog. No action will be taken.")

        ddb_util.track_database_import_status(source_glue_catalog_id, target_glue_catalog_id, ddb_tbl_name_for_db_status_tracking,
                                              db["Name"], import_run_id, export_batch_id, is_db_created)
        print(f"Processing of Database schema completed. Result: DB already exists: {db_exist}, DB created: {is_db_created}.")









