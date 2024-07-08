import boto3
import time
import json

from datetime import datetime
from util.db_replication_status import DBReplicationStatus
from util.table_replication_status import TableReplicationStatus

class GlueUtil:

    def get_database_if_exist(self, glue, target_catalog_id, db):
        database = None
        try:
            database = glue.get_database(CatalogId=target_catalog_id, Name=db['Name'])['Database']
        except glue.exceptions.EntityNotFoundException:
            print(f"Database '{db['Name']}' not found.")
        return database

    def get_databases(self, glue, source_glue_catalog_id):
        paginator = glue.get_paginator('get_databases')
        page_iterator = paginator.paginate(CatalogId=source_glue_catalog_id)
    
        master_db_list = []
        for page in page_iterator:
            for db in page['DatabaseList']:
                if 'CreateTime' in db:
                    db['CreateTime'] = str(db['CreateTime'])
                if 'UpdateTime' in db:
                    db['UpdateTime'] = str(db['UpdateTime'])
                if 'LastAccessTime' in db:
                    db['LastAccessTime'] = str(db['LastAccessTime'])
                master_db_list.append(db)
    
        print(f"Total number of databases fetched: {len(master_db_list)}")
        return master_db_list

    def create_glue_databases(self, glue, target_glue_catalog_id, db_name, db_description):
        db_status = DBReplicationStatus()
        try:
            glue.create_database(
                CatalogId=target_glue_catalog_id,
                DatabaseInput={
                    'Name': db_name,
                    'Description': db_description
                }
            )
            print(f"Database created successfully. Database name: '{db_name}'.")
            db_status.created = True
            db_status.error = False
        except Exception as e:
            print(f"Exception thrown while creating Glue Database: {e}")
            db_status.db_name = db_name
            db_status.error = True
        return db_status

    def create_glue_database(self, glue, target_glue_catalog_id, db):
        db_status = DBReplicationStatus()
        try:
            glue.create_database(
                CatalogId=target_glue_catalog_id,
                DatabaseInput={
                    'Name': db['Name'],
                    'Description': db.get('Description', "N/A"),
                    'LocationUri': db.get('LocationUri', "N/A"),
                    'Parameters': db.get('Parameters', {})
                }
            )
            print(f"Database created successfully. Database name: '{db['Name']}'.")
            db_status.created = True
            db_status.error = False
        except Exception as e:
            print(f"Exception in creating Database with name: '{db['Name']}'. {e}")
            db_status.db_name = db['Name']
            db_status.error = True
        return db_status

    def create_table_input(self, table):

        description =  table['Description'] if 'Description' in table else ""
        LastAccessTime = datetime.strptime(table.get('LastAccessTime'), '%Y-%m-%d %H:%M:%S%z') if 'LastAccessTime' in table else datetime.strptime("1900-01-01 00:00:00+00:00", '%Y-%m-%d %H:%M:%S%z')

        Owner = table['Owner'] if 'Owner' in table else "N/A"
        Name = table['Name'] if 'Name' in table else ""
        PartitionKeys = table['PartitionKeys'] if 'PartitionKeys' in table else []
        TableType = table['TableType'] if 'TableType' in table else ""
        ViewExpandedText = table.get('ViewExpandedText') if 'ViewExpandedText' in table else ""
        ViewOriginalText = table.get('ViewOriginalText') if 'ViewOriginalText' in table else ""
        Parameters = table.get('Parameters') if 'Parameters' in table else {}

        table_input = {
            'Description': description,
            'LastAccessTime': LastAccessTime,
            'Owner': Owner,
            'Name': Name,
            'PartitionKeys': PartitionKeys,
            'TableType': TableType,
            'ViewExpandedText': ViewExpandedText,
            'ViewOriginalText': ViewOriginalText,
            'Parameters': Parameters
        }
        storage_descriptor = table.get('StorageDescriptor')
        if storage_descriptor:
            table_input['StorageDescriptor'] = storage_descriptor
            if 'Parameters' in storage_descriptor:
                table_input['Parameters'] = storage_descriptor['Parameters']
        return table_input

    def get_tables(self, glue, glue_catalog_id, database_name, sns_util, sns, export_run_id, msg_attr_export_batch_id, topic_table_list_arn):
        print(f"Start - Fetching table list for Database {database_name}")

        message_number = 0
        max_group_tables = 50
        paginator = glue.get_paginator('get_tables')
        page_iterator = paginator.paginate(CatalogId=glue_catalog_id, DatabaseName=database_name)

        master_table_list = []
        for page in page_iterator:
            for db in page['TableList']:
                if 'CreateTime' in db:
                    db['CreateTime'] = str(db['CreateTime'])
                if 'UpdateTime' in db:
                    db['UpdateTime'] = str(db['UpdateTime'])
                if 'LastAccessTime' in db:
                    db['LastAccessTime'] = str(db['LastAccessTime'])
                master_table_list.append(db)

        print(f"Database '{database_name}' has {len(master_table_list)} tables.")
        print(f"End - Fetching table list for Database {database_name}")

        #Loops through AWS Glue catalog table list
        chunks = [master_table_list[i : i + max_group_tables] for i in range(0, len(master_table_list), max_group_tables)]

        for i, chunk in enumerate(chunks, start=1):
            message_number += 1
            first_pos = (i - 1) * max_group_tables + 1
            last_pos = first_pos + len(chunk) - 1

            #Sending SNS message with list of tables
            print(f"Sending to SNS message number {message_number} with tables from {first_pos} to {last_pos}")
            sns_util.publish_table_list_to_sns(sns, topic_table_list_arn, json.dumps(chunk), str(export_run_id), glue_catalog_id, msg_attr_export_batch_id) #Validar estos parametrios!
            print('Message send to SNS')

        print(f"End - Sending all {message_number} SNS messages for Database {database_name}")

    def get_table(self, glue, glue_catalog_id, database_name, table_name):
        try:
            table = glue.get_table(CatalogId=glue_catalog_id, DatabaseName=database_name, Name=table_name)['Table']
        except glue.exceptions.EntityNotFoundException:
            print(f"Table '{table_name}' not found.")
            table = None
        return table

    def create_or_update_table(self, glue, source_table, target_glue_catalog_id, skip_table_archive):
        table_status = TableReplicationStatus()
        table_status.table_name = source_table['Name']
        table_status.db_name = source_table['DatabaseName']
        table_status.replication_time = int(time.time() * 1000)

        try:
            target_table = glue.get_table(
                CatalogId=target_glue_catalog_id,
                DatabaseName=source_table['DatabaseName'],
                Name=source_table['Name']
            )['Table']
        except glue.exceptions.EntityNotFoundException:
            print(f"Table '{source_table['Name']}' not found. It will be created.")
            target_table = None
        except Exception as e:
            print(f"Exception in getting getTable: {e}")
            target_table = None

        table_input = self.create_table_input(source_table)

        if target_table:
            print("Table exist. It will be updated")
            try:
                glue.update_table(
                    DatabaseName=source_table['DatabaseName'],
                    TableInput=table_input,
                    SkipArchive=skip_table_archive
                )
                table_status.updated = True
                table_status.replicated = True
                table_status.error = False
                print(f"Table '{source_table['Name']}' updated successfully.")
            except glue.exceptions.EntityNotFoundException as e:
                print(f"Exception thrown while updating table '{source_table['Name']}'. Reason: '{source_table['DatabaseName']}' does not exist already. {e}")
                table_status.replicated = False
                table_status.db_not_found_error = True
                table_status.error = True
            except Exception as e:
                print(f"Exception thrown while updating table '{source_table['Name']}'. {e}")
                table_status.replicated = False
                table_status.error = True
        else:
            try:
                glue.create_table(
                    CatalogId=target_glue_catalog_id,
                    DatabaseName=source_table['DatabaseName'],
                    TableInput=table_input
                )
                table_status.created = True
                table_status.replicated = True
                table_status.error = False
                print(f"Table '{source_table['Name']}' created successfully.")
            except glue.exceptions.EntityNotFoundException as e:
                print(f"Exception thrown while creating table '{source_table['Name']}'. Reason: '{source_table['DatabaseName']}' does not exist already. {e}")
                table_status.replicated = False
                table_status.db_not_found_error = True
            except Exception as e:
                print(f"Exception thrown while creating table '{source_table['Name']}' {e}")
                table_status.replicated = False
                table_status.error = True
        return table_status

    def get_partitions(self, glue, catalog_id, database_name, table_name):
        master_partition_list = []
        paginator = glue.get_paginator('get_partitions')
        page_iterator = paginator.paginate(DatabaseName=database_name, CatalogId=catalog_id, TableName=table_name)
        for page in page_iterator:
            for partition in page["Partitions"]:
                if "CreationTime" in partition:
                    partition["CreationTime"] = str(partition["CreationTime"])
                if "UpdateTime" in partition:
                    partition["UpdateTime"] = str(partition["UpdateTime"])
                master_partition_list.append(partition)
                if "LastAccessTime" in partition:
                    partition["LastAccessTime"] = str(partition["LastAccessTime"])
        return master_partition_list
        
    def add_partitions(self, glue, partitions_to_add, catalog_id, database_name, table_name):
            num_partitions_added = 0
            partitions_added = False
            batch_create_partition_request = {
                'CatalogId': catalog_id,
                'DatabaseName': database_name,
                'TableName': table_name
            }
    
            partition_input_list = []
            for partition in partitions_to_add:
                partition_input = {
                    'StorageDescriptor': partition.get('StorageDescriptor'),
                    'Values': partition['Values']
                }
                partition_input_list.append(partition_input)
    
            print(f"Partition Input List Size: {len(partition_input_list)}")
            if len(partition_input_list) > 100:
                print("The input has more than 100 partitions, it will be sliced into smaller lists with 100 partitions each.")
    
            smaller_lists = [partition_input_list[i:i+100] for i in range(0, len(partition_input_list), 100)]
            for part_input_list in smaller_lists:
                batch_create_partition_request['PartitionInputList'] = part_input_list
                try:
                    result = glue.batch_create_partition(**batch_create_partition_request)
                    status_code = result['ResponseMetadata']['HTTPStatusCode']
                    part_errors = result.get('Errors', [])
                    if status_code == 200 and not part_errors:
                        print(f"{len(part_input_list)} partitions were added to table '{table_name}' of database '{database_name}'.")
                        partitions_added = True
                        num_partitions_added += len(part_input_list)
                        print(f"{num_partitions_added} of {len(partition_input_list)} partitions added so far.")
                    else:
                        print(f"Not all partitions were added. Status Code: {status_code}, Number of partition errors: {len(part_errors)}")
                        for part_error in part_errors:
                            print(f"Partition Error Message: {part_error['ErrorDetail']['ErrorMessage']}")
                            for value in part_error['PartitionValue']:
                                print(f"Partition error value: {value}")
                except ClientError as e:
                    print(f"Exception in adding partitions: {e}")
                    print(f"{num_partitions_added} of {len(partition_input_list)} partitions added so far.")
                    # TODO - what to do when there are exceptions here?
    
            print(f"Total partitions added: {num_partitions_added}")
            return partitions_added
    
    def delete_partition(self, glue, catalog_id, database_name, table_name, partition):
        partition_deleted = False
        delete_partition_request = {
            'CatalogId': catalog_id,
            'DatabaseName': database_name,
            'TableName': table_name,
            'PartitionValues': partition['Values']
        }

        try:
            result = glue.delete_partition(**delete_partition_request)
            status_code = result['ResponseMetadata']['HTTPStatusCode']
            if status_code == 200:
                print(f"Partition deleted from table '{table_name}' of database '{database_name}'")
                partition_deleted = True
        except ClientError as e:
            print(f"Exception in deleting partition: {e}")

        return partition_deleted

    def delete_partitions(self, glue, catalog_id, database_name, table_name, partitions_to_delete):
        partitions_deleted = False

        batch_delete_partition_request = {
            'CatalogId': catalog_id,
            'DatabaseName': database_name,
            'TableName': table_name
        }

        partition_value_list = [{'Values': partition['Values']} for partition in partitions_to_delete]
        print(f"Size of List of PartitionValueList: {len(partition_value_list)}")

        smaller_lists = [partition_value_list[i:i+25] for i in range(0, len(partition_value_list), 25)]
        for smaller_list in smaller_lists:
            batch_delete_partition_request['PartitionsToDelete'] = smaller_list
            try:
                result = glue.batch_delete_partition(**batch_delete_partition_request)
                status_code = result['ResponseMetadata']['HTTPStatusCode']
                part_errors = result.get('Errors', [])
                if status_code == 200 and not part_errors:
                    print(f"{len(smaller_list)} partitions from table '{table_name}' of database '{database_name}' were deleted.")
                    partitions_deleted = True
                else:
                    print(f"Not all partitions were deleted. Status Code: {status_code}, Number of partition errors: {len(part_errors)}")
                    for part_error in part_errors:
                        print(f"Partition Error Message: {part_error['ErrorDetail']['ErrorMessage']}")
                        for value in part_error['PartitionValue']:
                            print(f"Partition value: {value}")
            except ClientError as e:
                print(f"Exception in deleting partitions: {e}")

        return partitions_deleted
