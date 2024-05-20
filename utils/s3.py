from datetime import datetime
from celery_conf import celery
import boto3
import botocore


import os
import redis
import subprocess
import shutil
from zipfile import ZipFile
# from app import logger
import time
from celery import current_task
from utils.schema.models import db, S3_file_manager, S3_file_manager_logger
from sentry_sdk import capture_exception
from configg.config import s3_bucket_name, local_path

import gzip
import io
from botocore.exceptions import ClientError
import logging
logger = logging.getLogger(__name__)
# AWS Credentials
aws_access_key_id = "your aws key id"
aws_secret_access_key_id = "your aws secret key id"

aws_region = 'us-east-2'
bucket_name = s3_bucket_name

s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key_id, region_name=aws_region)
s3_resource = boto3.resource('s3', aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key_id, region_name=aws_region)


# Local Path
file_path_tmp = f'{local_path}/tmp/'
file_path_downloaded = f'{local_path}/'
file_type = ('.zip', '.csv', '.xlsx', '.xls')


# Celery tasks
class Celery_Task:

    # @celery.task(queue='high_priority')
    # def high():
    #     time.sleep(10)
    #     queue_name = current_task.request.delivery_info['routing_key']
    #     return f'Task {current_task.name} running in queue {queue_name}'
    # @celery.task(queue='low_priority')
    # def low():
    #     time.sleep(10)
    #     queue_name = current_task.request.delivery_info['routing_key']

    #     return f'Task {current_task.name} running in queue {queue_name}'

    # @celery.task()
    # def simple_task():
    #     return 'Hi! from simple task'
    # @celery.task(name ="periodic_task")
    # def periodic_task():
    #     return 'Hi! from periodic_task'

    @celery.task(bind=True, queue='other_functionality')
    def send_email(self, subject, body, recipient,cc=None):
        try:
            from utils.utils import IssueManager
            issue_manager = IssueManager()
            client = boto3.client('ses', aws_access_key_id=aws_access_key_id,
                                  aws_secret_access_key=aws_secret_access_key_id, region_name='us-east-1')
            message = {
                "Subject": {"Data": subject},
                "Body": {
                    "Html": {"Data": body}}
            }
            client.send_email(
                Source="contact@digitalshelfiq.com",
                # Destination={"ToAddresses": [recipient]},
                Destination={
                "ToAddresses": recipient,
                "CcAddresses": [cc]  # Add CC addresses here
                },
                Message=message
            )
            return self.request.id
        except botocore.exceptions.ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'MessageRejected':
                # Handle the case where the email address is not verified
                # You can log the error, send a notification, or take any other action you need
                logger.error(f"Email address not verified in AWS SES.")
                message=f"{e}"
                issue_name="Email Verification Error"
                issue_manager.create_issue_ticket(name=issue_name, description=message, priority=3, associated_with='Send Email Functionality')
                return self.request.id
            else:
                # Handle other AWS SES errors
                capture_exception(e)
                logger.error(f"Error sending email {str(e)}")
                return self.request.id
        except Exception as e:
            # Handle other exceptions (not related to AWS SES)
            capture_exception(e)
            logger.error(f"Error sending email {str(e)}")
            return self.request.id
    def get_queue_length(queue_name):
        redis_url = f'redis://:{os.environ.get("REDIS_PASSWORD")}@localhost:6379/0',
        client = redis.StrictRedis.from_url(redis_url)
        queue_length = client.llen(queue_name)
        return queue_length

    def create_worker(worker_name):
        command = ["celery", "-A", "celery_worker.celery", "worker", "--concurrency=16",
                   "-Q", "high_priority", "-n", worker_name, "--loglevel=info"]
        worker_process = subprocess.Popen(command)

    def delete_worker(worker_name):
        kill_command = f"pkill -f 'celery -A celery_worker.celery worker --concurrency=16 -Q high_priority -n {worker_name} --loglevel=info'"
        subprocess.run(kill_command, shell=True)


#
#
# s3 resource class
#
#

class FileExtraction:
    def __init__(self):
        self.file_names = []

    def clear_flie_names(self):
        self.file_names = []

    def get_file_names(self, bucket_name=bucket_name):
        try:
            bucket = s3_resource.Bucket(bucket_name)
            # file_names = []
            for obj in bucket.objects.all():
                self.file_names.append(obj.key)
            if self.file_names:
                return self.file_names
            else:
                return None
        except Exception as e:
            capture_exception(e)
            return f"Error while getting file names {str(e)}"

    def get_local_file_names(self, file_path):
        local_file_names = []
        for file in os.listdir(file_path):
            if file.endswith(file_type):
                local_file_names.append(file)

        return local_file_names

    @celery.task(bind=True)
    def move_files_locally(self, *args, **kwargs):
        try:
            obj = FileExtraction()
            files = obj.get_local_file_names(file_path_downloaded)
            for file in files:
                if file.endswith(file_type):
                    shutil.move(file, file_path_tmp+file)
                    print(f'{file} has been moved to tmp folder')
            return self.request.id
        except Exception as e:
            capture_exception(e)
            logger.error(f"Error moving files locally {str(e)}")
            return "Error moving files to tmp folder"

    # Extraction of the files from the s3 bucket
    @celery.task(bind=True)
    def download_files(self, bucket_name=bucket_name, *args, **kwargs):
        try:
            obj = FileExtraction()
            file_name = obj.get_file_names()
            if not file_name:
                raise Exception("File not available in bucket")
            all_files = file_name
            if all_files is None:
                raise Exception("File not available in bucket")
            else:
                for file_name in all_files:
                    if FileExtraction.file_exists(file_name):
                        print(f'{file_name} already exists')
                    else:
                        s3_client.download_file(
                            bucket_name, file_name, file_name)
                        #create entry in s3 manager table
                        s3_manager_entry = S3_file_manager(
                            file_name=file_name,
                            process_status='started',
                        )
                        db.session.add(s3_manager_entry)
                        db.session.commit()
                        if file_name.endswith('.zip'):
                            FileExtraction.extract_files(file_name)
                        logger.info(
                            "Files downloaded from s3 and deleted from s3")
            return self.request.id
        except Exception as e:
            capture_exception(e)
            logger.error(f'Error downloading files from s3 {str(e)}')
            raise Exception(f"Error downloading files from s3 {str(e)}")

    # Local File Operations
    def extract_files(file_name):
        with ZipFile(file_name, 'r') as zip:
            zip.extractall()
            print(f'{file_name} has been extracted')

    def file_exists(file):
        if os.path.exists(file):
            return True
        else:
            return False

    @celery.task(bind=True)
    def delete_file_from_local(self, *args, **kwargs):
        try:
            obj = FileExtraction()
            file_names = obj.get_local_file_names(file_path_tmp)
            # if not file_names:
            #     raise Exception("Files are not available for extraction in tmp folder.")
            for file in file_names:
                if FileExtraction.file_exists(file_path_tmp+file):
                    os.remove(file_path_tmp+file)
                    print(f'{file} has been deleted')
                elif FileExtraction.file_exists(file_path_downloaded+file):
                    os.remove(file_path_downloaded+file)
                else:
                    print(f'{file} does not exist')
            return self.request.id
        except Exception as e:
            capture_exception(e)
            logger.error(f"Error deleting file from local storage || {str(e)}")
            raise Exception('Error deleting file from local storage')

    # S3 Bucket Operations
    def delete_file_from_s3(bucket_name=bucket_name):
        try:
            completed_records = S3_file_manager.query.filter_by(process_status='completed').all()
            for record in completed_records:
                s3_client.delete_object(Bucket=bucket_name, Key=record.file_name)
                create_logger_entry = S3_file_manager_logger(
                    file_name=record.file_name,
                    process_status=record.process_status,
                    is_deleted = True
                )
                db.session.add(create_logger_entry)
                delete_record = S3_file_manager.query.get(record.id)
                db.session.add(delete_record)
                db.session.delete(delete_record)
                db.session.commit()
                print(f'{record.file_name} has been deleted from s3')
        except Exception as e:
            capture_exception(e)
            db.session.rollback()
            logger.error(f'Error deleting file from s3: {str(e)}')
            raise Exception(f'Error deleting file from s3: {str(e)}')

    def bucket_list_names(self,):
        bucket_list = []
        response = s3_client.list_buckets()
        for bucket in response['Buckets']:
            bucket_list.append(bucket['Name'])
        return bucket_list

    def convert_zip_to_gzip(self,bucket_name, file_key):
        try:
            # Fetch the file from S3
            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            file_content = response['Body'].read()

            # Determine the file type (zip or csv)
            if file_key.endswith('.zip'):
                # Convert zip content to gzip content
                gzip_buffer = io.BytesIO()
                with gzip.GzipFile(mode='wb', fileobj=gzip_buffer) as gz:
                    gz.write(file_content)

                # Upload the gzipped content back to S3 with .gz extension
                new_key = file_key.replace('.zip', '.gz')
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=new_key,
                    Body=gzip_buffer.getvalue(),
                    ContentEncoding='gzip'
                )

                # Delete the original .zip file
                s3_client.delete_object(Bucket=bucket_name, Key=file_key)
                


            elif file_key.endswith('.csv') or file_key.endswith('.xlsx'):
                # Convert csv content to gzip content
                gzip_buffer = io.BytesIO()
                with gzip.GzipFile(mode='wb', fileobj=gzip_buffer) as gz:
                    gz.write(file_content)

                # Upload the gzipped content back to S3 with .gz extension
                new_key = file_key.replace('.csv', '.gz')
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=new_key,
                    Body=gzip_buffer.getvalue(),
                    ContentEncoding='gzip'
                )

                # Delete the original .csv file
                s3_client.delete_object(Bucket=bucket_name, Key=file_key)

            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return f"The file '{file_key}' does not exist in the bucket."
            else:
                return f"An error occurred: {e}"

    @celery.task(bind=True)
    def move_files_to_s3(self, *args, **kwargs):
        try:
            import configg.config as config
            bucket=s3_bucket_name
            obj = FileExtraction()
            file_name = obj.get_local_file_names(file_path_tmp)
            for file in file_name:
                if file.endswith('.zip') or file.endswith('.csv'):
                    s3_client.upload_file(file_path_tmp+file,
                                        bucket, file)
                    print(f'{file} has been uploaded to s3')
                    obj.convert_zip_to_gzip(bucket, file)

            return self.request.id
        except Exception as e:
            capture_exception(e)
            logger.error(f"Error moving file to s3 || {str(e)}")
            raise Exception("Error moving file to s3 from local storage")


    @celery.task(bind=True)
    def revoke_tasks_if_error(self, request, chain_task, *args, **kwargs):
        print(f"All tasks have been revoked {request}")
        chain_task.revoke(terminate=True)


class s3_lifecycle:
    @celery.task(bind=True, queue='other_functionality')
    def lifecycle_Management(self):
        try:
            bucket_name = 'processeddatas'

            response = s3_client.list_objects_v2(Bucket=bucket_name)
            for obj in response.get('Contents'):
                # if obj['Key'].startswith('processed/'):

                upload_date = obj['LastModified'].replace(tzinfo=None)
                current_date = datetime.now()
                difference = (current_date - upload_date).days

                currnet_class_name = s3_lifecycle.get_storage_class(
                    bucket_name, obj['Key'])
                if currnet_class_name is None:
                    if difference >= 30:
                        s3_lifecycle.change_storage_class(
                            bucket_name, obj['Key'], 'DEEP_ARCHIVE')
                        print(
                            f"File Name: {obj['Key']}, Upload Date: {upload_date}")

                if difference >= 365:
                    s3_lifecycle.delete_file_from_s3(bucket_name, obj['Key'])

            return self.request.id

        except Exception as e:
            capture_exception(e)
            print(f"An error occurred: {e}")

    def change_storage_class(bucket_name, file_key, new_storage_class):
        try:
            copy_source = {
                'Bucket': bucket_name,
                'Key': file_key
            }
            s3_client.copy_object(
                Bucket=bucket_name,
                CopySource=copy_source,
                Key=file_key,
                StorageClass=new_storage_class
            )
            print(
                f"Storage class of {file_key} changed to {new_storage_class}")
        except s3_client.exceptions.InvalidObjectState as e:
            capture_exception(e)
            print(
                f"An error occurred: {e}. \n The object may be in a state like GLACIER or Deep Archive so that does not allow direct modification.")
        except Exception as e:
            capture_exception(e)
            print(f"An error occurred: {e}")

    def get_storage_class(bucket_name, file_key):
        try:
            response = s3_client.head_object(Bucket=bucket_name, Key=file_key)
            storage_class = response.get('StorageClass')
            return storage_class
        except Exception as e:
            capture_exception(e)
            print(f"An error occurred: {e}")

    def delete_file_from_s3(bucket_name, file_key):
        try:
            # Delete the object
            s3_client.delete_object(Bucket=bucket_name, Key=file_key)
            print(f"File '{file_key}' deleted successfully.")

        except Exception as e:
            capture_exception(e)
            print(f"An error occurred: {e}")

    def filter_bucket(bucket_name, min_date=None, max_date=None,report_type=None):
        try:
            response = s3_client.list_objects_v2(Bucket=bucket_name)
            file_names = []
            for obj in response['Contents']:
                if obj['Key'].startswith("BelSkai"):
                    date = obj['Key'].split('_')[1].split('.')[0]
                    origanal_report_type=obj['Key'].split('_')[0].replace("BelSkai","")


                    # Check if date is within the specified range
                if (min_date is None or date >= min_date) and (max_date is None or date <= max_date) and (report_type is None or origanal_report_type == report_type):
                    file_names.append(obj['Key'])
                    # print(obj['Key'])
                    
            return file_names
        except Exception as e:
            print(f"An error occurred: {e}")
            return []
