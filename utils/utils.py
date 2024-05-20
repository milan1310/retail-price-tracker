from datetime import timedelta, timezone
import datetime
from flask import Blueprint
import sentry_sdk
import boto3
from configg.config import aws_access_key_id, aws_secret_access_key_id,source_email, s3_bucket_name
import botocore
from datetime import datetime, timedelta
from celery_conf import celery
from utils.s3 import Celery_Task
from utils.schema.models import Issue_Ticket, S3_file_manager, S3_file_manager_logger, Task_Logger, db
from celery.signals import after_task_publish, task_failure, task_postrun, task_prerun, task_success, task_retry, task_revoked, before_task_publish
# import datetime
# from app import logger
import logging

logger = logging.getLogger(__name__)


class Helper_Response():

    def get_response(self, status_code, message_header, message, status=None, extra={}):
        response = {
            'status': status if status else status_code,
            'message_code': status_code,
            'message_header': message_header,
            'message': message
        }
        return response | extra


class Email_helpers:
    @celery.task(bind=True)
    def send_email(self, subject, body, recipient, cc=None):
        try:
            client = boto3.client('ses', aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key_id, region_name='us-east-1')
            message = {
                "Subject": {"Data": subject},
                "Body": {"Html": {"Data": body}}
            }
            destination = {"ToAddresses": [recipient]}
            if cc:
                destination["CcAddresses"] = [cc]
            client.send_email(
                Source=source_email,
                Destination=destination,
                Message=message
            )
            return 'success'
        except botocore.exceptions.ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'MessageRejected':
                return f'error {e}'
            else:
                return f'error {e}'
        except Exception as e:
            return f'error {e}'



class CRUD_Utils():
    def insert_data(self,table_name, form_data_list):
        try:
            table_model_map = {

            }

            model_class = table_model_map.get(table_name)

            if model_class:
                # Get all column names from the model class
                column_names = [column.name for column in model_class.__table__.columns]

                # Filter form data to only include keys present in the model columns
                for form_data in form_data_list:
                    if isinstance(form_data, dict):
                        filtered_form_data = {key: value for key, value in form_data.items() if key in column_names}

                        # Create a new instance of the model class with filtered form data
                        new_record = model_class(**filtered_form_data)
                        db.session.add(new_record)
                    else:
                        print("Invalid form data:", form_data)
                db.session.commit()
                return True, new_record
            else:
                return False, f"Table '{table_name}' not found or not supported"
        except Exception as e:
            sentry_sdk.capture_exception(e)
            return False, f"Error inserting record {e}"
    


    def update_data_with_history_logger(self, table_name, form_data_list, field_name=None, field_value=None,  filter_condition=None):
        try:
            table_model_map = {
            }

            model_class = table_model_map.get(table_name)
            if model_class:
                try:
                    if not isinstance(field_name, str):
                        raise ValueError("Field name must be a string")
                    base_query = model_class.query

                    if filter_condition:
                        records_to_update = base_query.filter(filter_condition).all()
                    if field_name and field_value:
                        filter_condition = {field_name: field_value}
                        records_to_update = base_query.filter_by(**filter_condition).all()

                    if records_to_update:
                        total_records_updated=0
                        total_changes=0
                        for record in records_to_update:
                            # Capture changes for history log
                            changes = {}  # Store changes to log later
                            for form_data in form_data_list:
                                for key, value in form_data.items():
                                    if getattr(record, key) != value:
                                        changes[key] = {'old_value': getattr(record, key), 'new_value': value}
                                        total_changes+=1
                                    if value is not None:
                                        setattr(record, key, value)

                            # Log changes into history log table
                            self.log_changes_to_history(table_name, changes)
                            total_records_updated+=1

                        db.session.commit()
                        return True, f"{total_records_updated} records updated successfully. Total changes: {total_changes}"
                        
                    else:
                        return False, "Records not found"
                except Exception as e:
                    print(e)
                    return False, "Error updating records"
            else:
                return False, "Invalid table name"
        except Exception as e:
            sentry_sdk.capture_exception(e)
            print(e)

    # def log_changes_to_history(self,table_name, changes):
    #     try:
    #         if changes:
    #             for field_name, change_info in changes.items():
    #                 history_log_entry = History_log_record(
    #                     table_name=table_name,
    #                     field_name=field_name,
    #                     new_field=str(change_info['new_value']),
    #                     prev_field=str(change_info['old_value']),
                        
    #                 )
    #                 db.session.add(history_log_entry)
    #             db.session.commit()
    #     except Exception as e:
    #         sentry_sdk.capture_exception(e)
    #         print(e)


class Helpers():

    def calculate_timedifference_minutes(self, created_date,created_time):
        '''
        Provide give time in datetime object
        '''
        # Get the current datetime
        current_datetime = datetime.now()
        # Combine created_date and created_time to create the timestamp
        created_timestamp = datetime.combine(created_date,created_time)
        # Calculate the time difference
        time_difference = current_datetime - created_timestamp
        # Convert time difference to minutes
        minutes_difference = time_difference.total_seconds() / 60
        return minutes_difference
    
    def upload_to_s3(self, file_path, file_name):
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key_id
            )
        try:
            s3_client.upload_file(file_path, s3_bucket_name, file_name)
            return True
        # except botocore.exceptions.FileNotFoundError:
        #     return False
        # except botocore.exceptions.NoCredentialsError:
        #     return False
        except Exception as e:
            print(e)
            sentry_sdk.capture_exception(e)
            return False

class TaskMonitorHelper():
    # @task_prerun.connect
    # def task_prerun_handler(task_id, task=None, **kwargs):
    #     task_manager = Task_Manager.query.filter_by(
    #         name=task.request.task).first()
    #     if not task_manager:
    #         celery.control.revoke(task.request.id, terminate=True)

    # @after_task_publish.connect
    # def after_task_publish_handler(sender=None, headers=None, body=None, **kwargs):
    #     task_manager = Task_Manager.query.filter_by(name=sender).first()
    #     if headers['retries'] < 1:
    #         logger_entry = Task_Logger(
    #             name=sender,
    #             status='Started',
    #             PID=os.getpid(),
    #             retries=0,
    #             celery_task_id=headers['id'],
    #             start_date=datetime.now(),
    #             task_id=task_manager.id
    #         )

    #         db.session.add(logger_entry)
    #         db.session.commit()

    @task_retry.connect
    def task_retry_handler(request=None, reason=None, **kwargs):
        logger = Task_Logger.query.filter_by(celery_task_id=request.id).first()
        logger.retries = int(logger.retries) + 1
        db.session.add(logger)
        db.session.commit()

    @task_revoked.connect
    def task_revoked_handler(request=None, terminated=None, signum=None, **kwargs):
        from utils.s3 import FileExtraction
        obj = FileExtraction()
        obj.delete_file_from_local.delay()
        logger = Task_Logger.query.filter_by(celery_task_id=request.id).first()
        if logger:
            logger.status = 'revoked'
            logger.issue = 'Task revoked due to some error'
            db.session.add(logger)
            db.session.commit()
        else:
            print("task is not initiated")

    @task_postrun.connect
    def task_postrun_handler(task_id, task=None, **kwargs):
        logger = Task_Logger.query.filter_by(celery_task_id=task_id).first()
        if logger:
            logger.end_date = datetime.now()
            db.session.add(logger)
            db.session.commit()
        else:
            print("Task is not initiated")

    @task_success.connect
    def task_success_handler(result=None, **kwargs):
        try:
            print(result)
            logger = Task_Logger.query.filter_by(celery_task_id=result).first()
            if logger:
                print("**************Logger with record",logger)
                logger.status = 'Success'
                db.session.add(logger)
                db.session.commit()
            else:
                print("**************Logger with no record",logger)
        except Exception as e:
            print(e)
        

    @task_failure.connect
    def task_failure_handler(task_id=None, exception=None, traceback=None, **kwargs):
        try:
            from utils.s3 import FileExtraction
            obj = FileExtraction()
            file_names = obj.get_file_names()
            print("File names==============",file_names)
            if file_names != None:
                obj.delete_file_from_local.delay()
            logger = Task_Logger.query.filter_by(celery_task_id=task_id).first()
            if logger:
                logger.status = 'Failed'
                logger.issue = str(exception)
                db.session.add(logger)

            #file manager
            if file_names:
                records = S3_file_manager.query.filter(
                    S3_file_manager.process_status == 'started',
                    S3_file_manager.file_name.in_(file_names)
                ).all()
                if records:
                    for record in records:
                        logger_entry = S3_file_manager_logger(
                            file_name=record.file_name,
                            process_status='archived',
                            is_deleted = False
                        )
                        db.session.add(logger_entry)
                    records_to_delte = S3_file_manager.query.filter(
                        S3_file_manager.process_status == 'started',
                        S3_file_manager.file_name.in_(file_names)
                    )
                    records_to_delte.delete()
                db.session.add(records)
                db.session.commit()
        except Exception as e:
            db.session.rollback()

class IssueManager:
    def create_issue_ticket(self, name, status='Pending', description=None, source='TPNETL', priority=None, associated_with=None):
        try:
            if name != "Email Verification Error":
                email_subject = "New Issue Ticket Created in Data Automation Process"
                email_body = f"<h1>{name}</h1>\n</br>Description :<b> {description} </b>,\n</br>Priority : <b>{priority}<b> ,\n</br>Associated With : <b>{associated_with}</b>"
                Celery_Task.send_email.delay(subject=email_subject, body=email_body, recipient=['harshil@mediaamp.co.in'],cc="milan@mediaamp.in")
                
            issue_ticket_entry = Issue_Ticket(
                name=name,
                status=status,
                description=description,
                source=source,
                priority=priority,
                associated_with=associated_with
            )
            db.session.add(issue_ticket_entry)
            db.session.commit()
        except Exception as e:
            sentry_sdk.capture_exception(e)
            logger.error(f"Error creating issue ticket || {str(e)}")

    def update_issue_ticket_status(self, id, status):
        try:
            issue_ticket = Issue_Ticket.query.filter_by(id=id).first()
            issue_ticket.status = status
            db.session.add(issue_ticket)
            db.session.commit()
        except Exception as e:
            sentry_sdk.capture_exception(e)
            logger.error(f"Error updating issue ticket || {str(e)}")

    def assign_issue_ticket(self, issue_ticket_id, issue_representative_id):
        try:
            issue_ticket = Issue_Ticket.query.filter_by(
                id=issue_ticket_id).first()
            issue_ticket.assign_to = issue_representative_id
            db.session.add(issue_ticket)
            db.session.commit()
        except Exception as e:
            sentry_sdk.capture_exception(e)
            logger.error(f"Error assign issue ticket || {str(e)}")

    def get_issue_ticket(self):
        try:
            issue_tickes = Issue_Ticket.query.get_all()
            return issue_tickes
        except Exception as e:
            sentry_sdk.capture_exception(e)
            logger.error(f"Error getting issue ticket || {str(e)}")