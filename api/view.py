import time
from celery import chain
from flask import Blueprint,jsonify, request
from utils.etl import Transformation
from utils.s3 import FileExtraction, s3_lifecycle
# from utils.
from utils.schema.models import createdb, db
import sentry_sdk
from utils.utils import Helper_Response, TaskMonitorHelper
import logging
from run_scrapper import run_pdp

api = Blueprint('api', __name__)


@api.route('/upload_report', methods=['POST'])
def upload_report():
    try:
        logger = logging.getLogger(__name__)
        if request.data:
            logger.info(
                f"Received request of report post--{time.strftime('%Y-%m-%d %H:%M:%S')}")
            message = request.data
            logger.info(
                f"form_data------{message}-{time.strftime('%Y-%m-%d %H:%M:%S')}")
            # with open('message_log.txt', 'a') as file:
            #     file.write(
            #         f"{message} - {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        TaskMonitorHelper()
        obj = FileExtraction()

        etlobj = Transformation()

        obj = FileExtraction()
        lifecycle_obj = s3_lifecycle()

        # etlobj.load_files_to_dataframes()

        chain_task = chain(
        #    run_pdp.s(),
           # obj.move_files_locally.s(),
           etlobj.load_files_to_dataframes.s(),
           obj.move_files_to_s3.s(),
           obj.delete_file_from_local.s(),
        )

        chain_task.apply_async()
        # lifecycle_obj.lifecycle_Management.delay()

        return jsonify("This task has been sent to celery!!!")
    except Exception as e:
        sentry_sdk.capture_exception(e)
        logger.error(f"Error while performing tpnetl {e}")
        print(e)
        return jsonify("This task has been sent to celery error!")


    
@api.route('/dbcreate',methods=['GET'])
def dbcreate():
    createdb()
    return 'Database Created'

@api.route("/")
def hello_world():
    return "<p>Hello, World!</p>"