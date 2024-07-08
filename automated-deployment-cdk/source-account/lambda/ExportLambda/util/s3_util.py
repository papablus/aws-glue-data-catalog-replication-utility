import boto3
import json
from botocore.exceptions import ClientError
from io import BytesIO

class S3Util:
    def create_s3_object(self, region, bucket, object_key, content):
        object_created = False
        s3 = boto3.client('s3', region_name=region)

        content_bytes = content.encode('utf-8')
        input_stream = BytesIO(content_bytes)

        metadata = {
            'ContentLength': str(len(content_bytes))
        }

        put_object_request = {
            'Bucket': bucket,
            'Key': object_key,
            'Body': input_stream,
            'Metadata': metadata
        }

        try:
            s3.put_object(**put_object_request)
            object_created = True
            print(f"Partition Object uploaded to S3. Object key: {object_key}")
        except ClientError as e:
            print(f"Error: {e}")
        except Exception as e:
            print(f"Exception: {e}")
        finally:
            input_stream.close()
        return object_created

    def upload_object(self, region, bucket_name, obj_key_name, local_file_path):
        print("Uploading file to S3.")
        object_uploaded = False
        s3_client = boto3.client('s3', region_name=region)

        try:
            # Upload a text string as a new object.
            s3_client.put_object(Bucket=bucket_name, Key=obj_key_name, Body="Uploaded String Object")
            
            # Upload a file as a new object with ContentType and title specified.
            with open(local_file_path, 'rb') as file:
                request = {
                    'Bucket': bucket_name,
                    'Key': obj_key_name,
                    'Body': file,
                    'ContentType': 'plain/text',
                    'Metadata': {
                        'x-amz-meta-title': 'PartitionFile'
                    }
                }
                s3_client.put_object(**request)
                object_uploaded = True
        except ClientError as e:
            print(f"ClientError: {e}")
        except Exception as e:
            print(f"Exception: {e}")

        return object_uploaded

    def create_object(self, region, bucket_name, table_ddl, string_obj_key_name):
        object_created = False

        try:
            s3_client = boto3.client('s3', region_name=region)
            s3_client.put_object(Bucket=bucket_name, Key=string_obj_key_name, Body=table_ddl)
            object_created = True
        except ClientError as e:
            print(f"ClientError: {e}")
        except Exception as e:
            print(f"Exception: {e}")

        return object_created

    def get_object(self, region, bucket_name, key):
        s3_client = boto3.client('s3', region_name=region)

        try:
            # Get an object and print its contents.
            print("Downloading an object")
            response = s3_client.get_object(Bucket=bucket_name, Key=key)
            print(f"Content-Type: {response['ContentType']}")
            print("Content:")
            self.display_text_input_stream(response['Body'])

            # Get a range of bytes from an object and print the bytes.
            range_object_request = {
                'Bucket': bucket_name,
                'Key': key,
                'Range': 'bytes=0-9'
            }
            response = s3_client.get_object(**range_object_request)
            print("Printing bytes retrieved.")
            self.display_text_input_stream(response['Body'])

            # Get an entire object, overriding the specified response headers, and print the object's content.
            header_overrides = {
                'ResponseCacheControl': 'No-cache',
                'ResponseContentDisposition': 'attachment; filename=example.txt'
            }
            get_object_request_header_override = {
                'Bucket': bucket_name,
                'Key': key,
                'ResponseHeaders': header_overrides
            }
            response = s3_client.get_object(**get_object_request_header_override)
            self.display_text_input_stream(response['Body'])
        except ClientError as e:
            print(f"ClientError: {e}")
        except Exception as e:
            print(f"Exception: {e}")

    @staticmethod
    def display_text_input_stream(input_stream):
        # Read the text input stream one line at a time and display each line.
        for line in input_stream.iter_lines():
            print(line.decode('utf-8'))
        print()

    def get_partitions_from_s3(self, region, bucket, key):
        s3 = boto3.client('s3', region_name=region)
        print(f"Bucket Name: {bucket}, Object Key: {key}")

        try:
            response = s3.get_object(Bucket=bucket, Key=key)
        except Exception as e:
            print(f"Exception thrown while reading object from S3: {e}")
            return []

        content_type = response['ContentType']
        print(f"CONTENT TYPE: {content_type}")

        # Read the text input stream one line at a time and display each line.
        partition_list = []

        for line in response['Body'].iter_lines():
            try:
                partition = json.loads(line.decode('utf-8'))
                partition_list.append(partition)
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                print(f"Exception occurred while reading partition information from S3 object: {e}")

        print(f"Number of partitions read from S3: {len(partition_list)}")
        return partition_list