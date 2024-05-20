import datetime
import math
import os
import numpy as np
import pandas as pd
import psycopg2
from psycopg2 import DataError, Error, IntegrityError
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
import json
# from utils.utils import IssueManager
from utils.s3 import Celery_Task, FileExtraction
from celery_conf import celery
from sentry_sdk import capture_exception
from configg.config import local_path, user, password, host, port, database
from utils.schema.models import db, Task_Logger, S3_file_manager, DimSeller
import logging
from flask_sqlalchemy import SQLAlchemy

from utils.utils import IssueManager 
logger = logging.getLogger(__name__)

# Local Path
localdir = f'{local_path}/tmp/'
local_files = []
file_list = {}



campaign_file_list=[]


class Transformation_helper:
    def __init__(self):
        self.local_files = []
        self.file_list = {}

    def get_all_file_names(self, localdir=localdir):
        try:
            for file in os.listdir(localdir):
                # local_files=[]
                if file.endswith('.csv') or file.endswith('.xlsx'):
                    self.local_files.append(file)
            logger.info('Getting all file names')
            return self.local_files
        except Exception as e:
            capture_exception(e)
            logger.error(f'Error while get_all_file_names || {str(e)}')
            raise Exception(f"Error while get_all_file_names || {str(e)}")

    def prepare_file_structure(self):
        try:
            allfiles = self.get_all_file_names()
            # if allfiles:
            for file in allfiles:
                self.file_list[file] = {
                    'file_name': file.split('_')[0],
                    'file_date': file.split('_')[1].replace(file.split('.')[-1], ''),
                    'file_path': localdir,
                    'file_type': file.split('.')[-1]
                }
            logger.info('Preparing file structure')
            return self.file_list
        except Exception as e:
            capture_exception(e)
            logger.error(f'Error while prepare_file_structure || {str(e)}')
            raise Exception(f"Error while prepare_file_structure || {str(e)}")


    def rds_get_data(self, table_name,file_name):
        try:
            connection = conn
            cursor = connection.cursor()
            if file_name in campaign_file_list:
                data=f"SELECT DISTINCT(campaign_name), client_id, retailer_id, brand_id, client_entity_id ,channel , vendor ,platform ,relationship  from {table_name}"
            else:
                data = f'SELECT DISTINCT(campaign_name), client_id, retailer_id, brand_id, client_entity_id from {table_name}'
            cursor.execute(data)
            data = cursor.fetchall()

            df = pd.DataFrame(data)
            logger.info('Connecting to database')
            return df

        except Exception as e:
            capture_exception(e)
            logger.error(f"Error while rds_get_data || {str(e)}")
            raise Exception(
                f"Error while rds_get_data || {str(e)}")

    def rds_delete_data(self, sql_update):
        try:
            connection = conn

            cursor = connection.cursor()
            retailer_delete_spend = sql_update
            print(f"****{retailer_delete_spend}")
            cursor.execute(retailer_delete_spend)
            connection.commit()
            logger.info('Connecting to database')
            return True

        except Exception as e:
            capture_exception(e)
            logger.error(f"Error while rds_delete_data || {str(e)}")
            raise Exception(
                f"Error while rds_delete_data || {str(e)}")


class DF_helper:
    def rename(self, df, columns, inplace=True):
        try:
            df.rename(columns=columns, inplace=inplace)
            logger.info('Renaming columns')
            return df
        except Exception as e:
            capture_exception(e)
            logger.error(f"Error while rename || {str(e)}")
            raise Exception(f"Error while rename || {str(e)}")

    def merge(self, df, df_mergewith, how, left_on, right_on):
        try:
            logger.info('Merging columns')
            return df.merge(df_mergewith, how=how, left_on=left_on, right_on=right_on)
        except Exception as e:
            logger.error(f"Error while merge || {str(e)}")
            raise Exception(f"Error while merge || {str(e)}")

    def drop(self, df, columns, inplace=True):
        try:
            df.drop(columns=columns, inplace=inplace)
            logger.info('Droping columns')
            return df
        except Exception as e:
            logger.error(f"Error while drop || {str(e)}")
            raise Exception(f"Error while drop  || {str(e)}")

    def reindex(self, df, columns):
        try:
            df.reindex(columns=columns)
            logger.info('Reindexing columns')
            return df
        except Exception as e:
            logger.error(f"Error while reindex || {str(e)}")
            raise Exception(f"Error while reindex || {str(e)}")


class Transformation_util:
    def __init__(self):
        self.engine = create_engine(f'postgresql://{user}:{password}@{host}/{database}')

    def load_dateframes(self, celery_id, file_name, df_columns, df_rename, df_reindex, expected_columns_table, database_column_validation_table, table_name, delete_column_name,marketplace_name):
        try:
            log_messages = []
            helper = Transformation_helper()
            file_list = helper.prepare_file_structure()
            validate = Validate_helper()
            issue_manager = IssueManager()
            calculate = Data_Counts()

            if not file_list:
                msg = f"Could not prepare file structure"
                logger.error(msg)
                log_messages.append({"type": "error", "message": msg})
                raise Exception(msg)    

            for key, value in file_list.items():
                if value['file_name'] == file_name:
                    # Loading Data into DF
                    df = pd.read_csv(value['file_path'] + key)
                    # Adding Columns
                    for column in df_columns:
                        df[column] = None
                    
                    dataframe_helper = DF_helper()
                    # Renaming Columns
                    df = dataframe_helper.rename(df=df, columns=df_rename)
                    csv_validate_headers = validate.validate_column_headers(
                        expected_columns_table, value['file_path'] + key)
                    if csv_validate_headers:
                        issue_manager.create_issue_ticket(
                            name=file_name, description=csv_validate_headers, priority=2, associated_with='incoming csv files from s3')
                        msg = f"Invalid column(s) headers"
                        logger.error(msg)
                        log_messages.append({"type": "error", "message": msg})
                        raise Exception(msg)
                    else:
                        msg = f"Valid column(s) headers"
                        logger.info(msg)
                        log_messages.append({"type": "info", "message": msg})

                    csv_validate_datatype = validate.validate_data_types(
                        expected_columns_table, value['file_path'] + key)
                    if csv_validate_datatype:
                        issue_manager.create_issue_ticket(
                            name=file_name, description=csv_validate_datatype, priority=2, associated_with='incoming csv file from, s3')
                        msg = f"Invalid column(s) data types"
                        logger.error(msg)
                        log_messages.append({"type": "error", "message": msg})
                        raise Exception(msg)
                    else:
                        msg = f"Valid column(s) data types"
                        logger.info(msg)
                        log_messages.append({"type": "info", "message": msg})
                    #feed the dim_seller table
                    print('first check')
                    if file_name in (['amazon3p','walmart3p']):
                        print('second------')
                        # Fill missing 'seller_name' with 'supplier_name' and vice versa
                        df['seller_name'] = df['seller_name'].fillna(df['supplier_name'])
                        df['supplier_name'] = df['supplier_name'].fillna(df['seller_name'])
                        distinct_pairs = df[['seller_name', 'supplier_name']].drop_duplicates()
                        db_pairs = pd.read_sql_table('dim_seller', self.engine)
                        # Merge the dataframes on 'seller_name' and 'supplier_name'
                        merged_for_distinct = pd.merge(distinct_pairs, db_pairs, on=['seller_name', 'supplier_name'], how='outer')

                        # Find pairs in distinct_pairs that are not in db_pairs
                        new_pairs = merged_for_distinct[merged_for_distinct['seller_id'].isnull()][['seller_name', 'supplier_name']]  

                        new_pairs = dataframe_helper.rename(new_pairs, columns={'sold_by': 'seller_name', 'shipped_from': 'supplier_name'})
                        new_pairs = new_pairs.assign(marketplace=marketplace_name)
                        # Insert new pairs into the database
                        new_pairs.to_sql('dim_seller', self.engine, if_exists='append', index=False)
                        df1 = pd.read_sql_table('dim_seller', self.engine)
                        df2 = df.merge(df1, how='left',
                                    on=['seller_name', 'supplier_name'])
                    elif file_name in (['amazonpdp']):
                        # Fill missing 'is_aplus' and 'is_variation' with False
                        df['is_aplus'] = df['is_aplus'].fillna(False)
                        df['is_variation'] = df['is_variation'].fillna(False)
                        df['bulleting'] = df['bulleting'].fillna('no info')
                        df['description'] = df['description'].fillna('no info')

                        # Fill missing 'buy_box_winner' with 'no info'
                        df['buy_box_winner'] = df['buy_box_winner'].fillna('no info')
                        df2 = df
                    elif file_name in (['walmartpdp']):
                        df['list_price'] = df['list_price'].str.replace('"','').replace('$', '')
                        df['list_price'] = df['list_price'].str.replace('$','')
                        # df['product_id']=df['product_id'].astype('string)
                        df['brand'] = df['brand'].str.replace('"','')
                        df['product_name'] = df['product_name'].str.replace('"','')
                        df['rating'] = df['rating'].str.replace('"','')
                        df['list_price'] = df['list_price'].astype(float) 
                        df2 = df
                    else:
                        df2 = df

                    df2 = dataframe_helper.drop(
                        df=df2, columns=delete_column_name)
                    if file_name in (['amazon3p','walmart3p']):
                        # Assign current date to a new column
                        df2['price_date'] = datetime.datetime.now().date()
                        # Assign current time to a new column
                        df2['price_time'] = datetime.datetime.now().strftime('%H:%M:00')
                        df2['currency'] = 'USD'
                    
                    df2 = df2.reindex(columns=df_reindex)
                    check_null_values = validate.validate_null_values(
                        database_column_validation_table, df2
                    )
                    if check_null_values:
                        issue_manager.create_issue_ticket(
                            name=file_name, description=check_null_values, priority=2, associated_with='Final CSV entering to database')
                        msg = f"Getting NULL values for {file_name}"
                        logger.error(msg)
                        log_messages.append({"type": "error", "message": msg})
                        raise Exception(msg)
                    else:
                        msg = f"Not any NULL values."
                        logger.info(msg)
                        log_messages.append({"type": "info", "message": msg})
                    
                        


                    final_validate_headers = validate.validate_column_headers(
                        database_column_validation_table, df2)
                    if final_validate_headers:
                        issue_manager.create_issue_ticket(
                            name=file_name, description=final_validate_headers, priority=2, associated_with='Final CSV entering to database')
                        msg = f"Invalid column(s) headers for {file_name}"
                        logger.error(msg)
                        log_messages.append({"type": "error", "message": msg})
                        raise Exception(msg)
                    else:
                        msg = f"Valid column(s) headers"
                        logger.info(msg)
                        log_messages.append({"type": "info", "message": msg})

                    final_validate_datatype = validate.validate_data_types(
                        database_column_validation_table, df2)
                    if final_validate_datatype:
                        issue_manager.create_issue_ticket(
                            name=file_name, description=final_validate_datatype, priority=2, associated_with='Final CSV entering to database')
                        msg = f"Invalid column(s) data types for {file_name}"
                        logger.error(msg)
                        log_messages.append({"type": "error", "message": msg})
                        raise Exception(msg)
                    else:
                        msg = f"Valid column(s) data types"
                        logger.info(msg)
                        log_messages.append({"type": "info", "message": msg})
                    df2.to_sql(table_name, self.engine,
                                index=False, if_exists='append')
                    msg = f"{table_name} Data Loaded Successfully"
                    logger.info(msg)
                    log_messages.append({"type": "info", "message": msg})
                    # update file manager status
                    records = S3_file_manager.query.filter(and_(
                        S3_file_manager.process_status == 'started', S3_file_manager.file_name.startswith(f'%{file_name}%'))).all()
                    for record in records:
                        record.process_status = 'completed'
                        db.session.add(record)
                    db.session.commit()
                    email_subject = "Reports Added"
                    email_body = f"<h1>Reports generated</h1>\n {key} report is Loaded Successfully. \n thank you"
                    Celery_Task.send_email.delay(subject=email_subject, body=email_body, recipient=['harshil@mediaamp.co.in'],cc="harshil@mediaamp.co.in")
                    
            log_messages_json = json.dumps(log_messages)

            # task_logger = Task_Logger.query.filter_by(
            #     celery_task_id.first()
            # task_logger.data_load_logger = log_messages_json
            # db.session.add(task_logger)
            # db.session.commit()

        except Exception as e:
            capture_exception(e)
            msg = f"Error while load_dateframes || {str(e)}"
            logger.error(msg)
            print(e)
            raise Exception(msg)
        
    

class Transformation:
    def __init__(self) -> None:
        # self.db = db
        pass

    @celery.task(bind=True)
    def load_files_to_dataframes(self,*args, **kwargs):
        try:
            helper = Transformation_helper()
            file_list = helper.prepare_file_structure()
            if not file_list:
                from utils.s3 import FileExtraction
                obj = FileExtraction()
                obj.delete_file_from_local.delay()
                logger.error(f"No files available to load in database")
                raise Exception("No files available to load in database")
            with open('utils/etl_args.json', 'r') as campaign_data:
                arguments_data = json.load(campaign_data)
            for key, value in file_list.items():
                file_name = value['file_name']
                print(f"------------------{file_name}---------------------")
                if file_name in ['amazon3p','walmart3p','amazonpdp','autozonepdp','advanceautopartspdp','walmartpdp']:
                    print("====================+++++++++", file_name)
                    campaingn_args = arguments_data.get(file_name)
                    transformation_util = Transformation_util()
                    transformation_util.load_dateframes(self.request.id,**campaingn_args)
            logger.info(f"load_files_to_dataframes successfully")
            return self.request.id
        except Exception as e:
            capture_exception(e)
            logger.error(f"Error while load_files_to_dataframes || {e}")
            raise Exception(f"Error while loading data to ETL {e}")



class Validate_helper:
    def parse_date(self, date_str):
        try:
            parsed_date = datetime.strptime(date_str, '%d-%m-%Y')
            formatted_date = parsed_date.strftime('%Y-%m-%d')
            logger.info(f"parse_date successfully")
            return formatted_date
        except (ValueError, TypeError) as e:
            logger.error(
                f"Error while parse_date || {str(e)}")
            return None

    def validate_column_headers(self, table_name, csv_filename):
        try:
            with open('utils/column_headers.json', 'r') as header_names:
                table_headers = json.load(header_names)

            if table_name not in table_headers:
                return json.dumps({"error": f"Invalid table name: {table_name}"}, indent=2)

            if isinstance(csv_filename, str) and (csv_filename.lower().endswith('.csv') or csv_filename.lower().endswith('.xlsx')):
                if csv_filename.lower().endswith('.csv'):
                    df = pd.read_csv(csv_filename)
                    actual_columns = list(df.columns)
                else:
                    df = pd.read_excel(csv_filename)
                    actual_columns = list(df.columns)
            else:
                df = csv_filename
                actual_columns = list(df.columns) if hasattr(
                    df, 'columns') else df

            expected_sequence = [column['name']
                                 for column in table_headers[table_name]]

            invalid_columns = [
                column for column in actual_columns if column not in expected_sequence]
            if invalid_columns:
                error_msg = {
                    "error": f"Invalid column(s) for table '{table_name}': {', '.join(invalid_columns)}",
                    "expected": expected_sequence,
                    "actual": actual_columns
                }
                logger.error(
                    f"Error while validate_column_headers || {str(error_msg)}")
                return json.dumps(error_msg, indent=2)
            if actual_columns != expected_sequence:
                error_msg = {
                    "error": f"Column sequence mismatch for table '{table_name}'.",
                    "expected": expected_sequence,
                    "actual": actual_columns
                }
                logger.error(
                    f"Error while validate_column_headers || {str(error_msg)}")
                return json.dumps(error_msg, indent=2)
            logger.info(f"Column headers validated for {table_name}")
            return None
        except Exception as e:
            logger.error(
                f"Error while validate_column_headers || {str(e)}")

    # def validate_data_types(self, table_name, csv_filename):
    #     try:
    #         with open('utils/column_headers.json', 'r') as header_names:
    #             table_headers = json.load(header_names)

    #         if isinstance(csv_filename, str) and (csv_filename.lower().endswith('.csv') or csv_filename.lower().endswith('.xlsx')):
    #             if csv_filename.lower().endswith('.csv'):
    #                 df = pd.read_csv(csv_filename)
    #                 actual_columns = list(df.columns)
    #             else:
    #                 df = pd.read_excel(csv_filename)
    #                 actual_columns = list(df.columns)
    #         else:
    #             df = csv_filename
    #             actual_columns = list(df.columns) if hasattr(
    #                 df, 'columns') else df
    #         # df = pd.read_csv(csv_filename)
    #         # actual_columns = list(df.columns)

    #         expected_type = [column['type']
    #                          for column in table_headers[table_name]]

    #         # Define a mapping between data types in JSON and Pandas
    #         type_mapping = {
    #             "BigInteger": ["int64"],
    #             "Integer": ["int64"],
    #             "String": ["object"],
    #             "Date": ["datetime64[ns]"],
    #             "DateTime": ["datetime64[ns]"],
    #             "Float": ["float64"],
    #             "Boolean": ["bool"]
    #         }

    #         # Iterate through each column and verify data type
    #         for column_name, expected_data_type in zip(actual_columns, expected_type):
    #             if expected_data_type in ["BigInteger", "Integer"]:
    #                 if df[column_name].dtype == 'O':
    #                     df[column_name] = df[column_name].str.replace(',', '')
    #                 df[column_name].fillna(0, inplace=True)
    #                 df[column_name] = df[column_name].astype("int64")
    #                 df[column_name] = df[column_name].fillna(0).astype("int64")
    #         for column_name, expected_data_type in zip(actual_columns, expected_type):
    #             actual_data_type = str(df[column_name].dtype)

    #             if expected_data_type in ["BigInteger", "Integer", "Float"]:
    #                 try:
    #                     if df[column_name].dtype == 'O':
    #                         df[column_name] = df[column_name].str.replace(',', '')
    #                     df[column_name] = pd.to_numeric(df[column_name], errors='coerce')

    #                     if expected_data_type != "Float":
    #                         df[column_name] = df[column_name].apply(
    #                             lambda x: int(math.floor(x)) if not pd.isna(x) else 0)
    #                         df[column_name] = df[column_name].fillna(0).astype("int64")
    #                         df[column_name] = df[column_name].astype("Int64")

    #                 except ValueError:
    #                     df[column_name] = 0

    #             # Handle integer-to-float conversion
    #             elif expected_data_type == "Float" and actual_data_type == "int64":
    #                 try:
    #                     df[column_name] = df[column_name].astype("float64")
    #                 except ValueError:
    #                     df[column_name] = 0.00

    #             # Handle integer-to-string conversion
    #             elif expected_data_type == "String" and (actual_data_type == "int64" or actual_data_type == "float64"):
    #                 try:
    #                     df[column_name] = df[column_name].astype("object")
    #                 except ValueError:
    #                     df[column_name] = ""

    #             # Handle string-to-integer conversion
    #             elif expected_data_type == "Integer" and actual_data_type == "object":
    #                 try:
    #                     df[column_name] = df[column_name].str.replace(',', '')
    #                     df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
    #                     df[column_name] = df[column_name].astype("int64")
    #                 except ValueError:
    #                     df[column_name] = 0

    #             # Handle float-to-integer conversion
    #             elif expected_data_type == "Integer" and (actual_data_type == "float64" or actual_data_type == "int64"):
    #                 try:
    #                     df[column_name] = df[column_name].apply(
    #                         lambda x: int(math.floor(x)) if not pd.isna(x) else 0)
    #                     df[column_name] = df[column_name].astype("int64")
    #                 except ValueError:
    #                     df[column_name] = 0

    #             elif expected_data_type == "Integer" and actual_data_type == "int64":
    #                 try:
    #                     df[column_name] = df[column_name].astype("int64")
    #                 except ValueError:
    #                     df[column_name] = 0

    #             elif expected_data_type == "Boolean" and actual_data_type == "object":
    #                 try:
    #                     df[column_name] = df[column_name].astype("bool")
    #                 except ValueError:
    #                     df[column_name] = False

    #             # Handle date format conversion
    #             elif expected_data_type == "Date":
    #                 try:
    #                     if df[column_name].dtype == 'object':
    #                         # Try to infer the date format and convert to datetime
    #                         df[column_name] = df[column_name].str.replace('/', '-')
    #                         df[column_name] = pd.to_datetime(df[column_name], format='%d-%m-%Y', errors='coerce').strftime('%d-%m-%Y')
    #                     elif df[column_name].dtype == 'datetime64[ns]':
    #                         df[column_name] = df[column_name].dt.strftime('%d-%m-%Y')

    #                     df[column_name] = pd.to_datetime(
    #                         df[column_name])
    #                 except ValueError:
    #                     error_msg = {
    #                         "error": f"Invalid date format in column '{column_name}' of table '{table_name}'."
    #                     }
    #                     df[column_name] = ""

    #             elif df[column_name].dtype != expected_data_type.lower() and \
    #                     df[column_name].dtype not in type_mapping.get(expected_data_type, []):
    #                 error_msg = {
    #                     "error": f"Data type mismatch for column '{column_name}' in table '{table_name}'. Expected type: {expected_data_type}, Actual type: {actual_data_type}",
    #                 }
    #                 logger.error(
    #                     f"Error while validate_data_types || {str(error_msg)}")
    #                 return json.dumps(error_msg, indent=2)
    #         logger.info(f"Column data types are validated for {table_name}")
    #         return None
    #     except Exception as e:
    #         logger.error(
    #             f"Error while validate_data_types || {str(e)}")

    def validate_data_types(self, table_name, csv_filename):
        try:
            with open('utils/column_headers.json', 'r') as header_names:
                table_headers = json.load(header_names)
            db_expected_type = [column['type'] for column in table_headers[table_name]]
            print(db_expected_type)
            if isinstance(csv_filename, pd.DataFrame):
                df=csv_filename
                if 'ad_spend' in df.columns:
                    df['ad_spend'] = df['ad_spend'].replace('[\$,]', '', regex=True).replace('', 0).astype(float)
                if 'ad_sales' in df.columns:
                    df['ad_sales'] = df['ad_sales'].replace('[\$,]', '', regex=True).replace('', 0).astype(float)
                if 'impressions' in df.columns:
                    df['impressions'] = df['impressions'].str.replace(',', '')
                if 'clicks' in df.columns:
                    df['clicks'] = df['clicks'].str.replace(',', '')
                if 'ad_units' in df.columns:
                    df['ad_units'] = df['ad_units'].str.replace(',', '')
            else:
                if csv_filename.endswith(".xlsx"):
                    df = pd.read_excel(csv_filename)
                else:
                    df = pd.read_csv(csv_filename)
                
                # print(df)
                # print(df.iloc[0])
                if isinstance(csv_filename, str):
                    entire_file_name= csv_filename.split('/')[-1]
                    fileName=entire_file_name.split('.')[0].split('_')[0]
                    if fileName in ['AholdCampaign','AholdPlacement','AholdKeyword','LorealEmailTgtDspCampaign','WMTDSSPlatform','TGTKioskCampaign','BrightPetNoneAmazonCampaign']:
                        if 'CPC' in df.columns:
                            df['CPC'] = df['CPC'].replace('[\$,]', '', regex=True).replace('', 0).astype(float)
                        if 'AdSpendUSD' in df.columns:
                            df['AdSpendUSD'] = df['AdSpendUSD'].replace('[\$,]', '', regex=True).replace('', 0).astype(float)
                        if 'Total Sales' in df.columns:
                            df['Total Sales'] = df['Total Sales'].replace('[\$,]', '', regex=True).replace('', 0).astype(float)
                        if 'Impressions' in df.columns:
                            df['Impressions'] = df['Impressions'].str.replace(',', '')
                        if 'Clicks' in df.columns:
                            df['Clicks'] = df['Clicks'].str.replace(',', '')
                        if 'Conversions' in df.columns:
                            df['Conversions'] = df['Conversions'].str.replace(',', '')
                        if 'Advertiser Cost (USD)' in df.columns:
                            df['Advertiser Cost (USD)'] = df['Advertiser Cost (USD)'].str.replace(',', '')
                        if 'Media Cost (USD)' in df.columns:
                            df['Media Cost (USD)'] = df['Media Cost (USD)'].str.replace(',', '')
                        if 'Advertiser Cost (Adv Currency)' in df.columns:
                            df['Advertiser Cost (Adv Currency)'] = df['Advertiser Cost (Adv Currency)'].str.replace(',', '')
                        if 'Spend' in df.columns:
                            df['Spend'] = df['Spend'].replace('[\$,]', '', regex=True).replace('', 0)
                        if 'Total Attributed Sales' in df.columns:
                            df['Total Attributed Sales'] = df['Total Attributed Sales'].replace('[\$,]', '', regex=True).replace('', 0)
                        if 'Actualized Vendor Spend' in df.columns:
                            df['Actualized Vendor Spend'] = df['Actualized Vendor Spend'].replace('[\$,]', '', regex=True).replace('', 0)
                        # BrightpetChewy
                        if 'Total cost' in df.columns:
                            df['Total cost'] = df['Total cost'].replace('[\$,]', '', regex=True).replace('', 0).astype(float)
                        if 'Product sales' in df.columns:
                            df['Product sales'] = df['Product sales'].replace('[\$,]', '', regex=True).replace('', 0).astype(float)
                        if 'Total product sales' in df.columns:
                            df['Total product sales'] = df['Total product sales'].replace('[\$,]', '', regex=True).replace('', 0).astype(float)
                        
                    

                
                csv_columns=list(df.columns)
                csv_columns_type=list(df.dtypes)
                
                print(csv_columns_type)
                print(csv_columns)
                type_mapping = {
                    "BigInteger": "int64",
                    "Integer": "int64",
                    "String": "object",
                    "Date": "datetime64[ns]",
                    "DateTime": "datetime64[ns]",
                    "Float": "float64",
                    "Boolean": "bool"
                }

                for csv_data_type, db_data_type,csv_columns in zip(csv_columns_type, db_expected_type,csv_columns):
                        # print(f"{csv_columns}--------------{db_data_type}---{csv_data_type} ---------------- {type_mapping[db_data_type]}")

                    if csv_data_type != type_mapping[db_data_type]:
                        # print(csv_columns)
                        if csv_data_type == "float64" and type_mapping[db_data_type] == "int64":
                            df[csv_columns] = df[csv_columns].fillna(0).astype("int64")

                        else:
                            df[csv_columns]=df[csv_columns].astype(type_mapping[db_data_type])
                            print(f"{csv_columns}--------------{db_data_type}---{csv_data_type} ---------------- {type_mapping[db_data_type]}")

                csv_columns=list(df.columns)
                print("---------------------------------------------------------")
                for db_data_type,csv_columns in zip( db_expected_type,csv_columns):
                    print(f"{db_data_type}---{df[csv_columns].dtype}")
                    # print(csv_columns)
                # print(df.iloc[0])
                
        except Exception as e:
            logger.error(f"Error while validate_data_types || {str(e)}")
        


    def validate_campaign_name(self, table_name, df):
        try:
            # Connect to your database and execute the query to get distinct campaign names
            connection = conn
            cursor = connection.cursor()
            distinct_campaign_names_db = []

            get_campaignname_db = """SELECT DISTINCT "campaign_name" FROM campaign_master_connector"""
            cursor.execute(get_campaignname_db)
            distinct_campaign_names_db = cursor.fetchall()

            distinct_campaign_names_db = [item[0]
                                          for item in distinct_campaign_names_db]

            actual_columns = list(df.columns)
            distinct_campaign_names_csv = []
            with open('utils/column_headers.json', 'r') as header_names:
                table_headers = json.load(header_names)

            expected_columnnames = [column['name']
                                    for column in table_headers[table_name]]

            if 'campaign_name' in expected_columnnames:
                distinct_campaign_names_csv = df['campaign_name'].unique()
            
            if 'Campaign Name' in expected_columnnames:
                distinct_campaign_names_csv = df['Campaign Name'].unique() 

            if 'CampaignName' in expected_columnnames:
                distinct_campaign_names_csv = df['CampaignName'].unique()  

            if 'Campaign name' in expected_columnnames:
                distinct_campaign_names_csv = df['Campaign name'].unique()

            if 'Campaign' in expected_columnnames:
                distinct_campaign_names_csv = df['Campaign'].unique()                  

            # Check for campaign names in CSV that are not in the database
            invalid_campaign_names = [
                campaign_name for campaign_name in distinct_campaign_names_csv if campaign_name not in distinct_campaign_names_db]
            print("*invalid_campaign_names", invalid_campaign_names)

            # If there are invalid campaign names, raise an issue
            if invalid_campaign_names:
                connection = conn
                cursor = connection.cursor()
                for campaign_name in invalid_campaign_names:
                    # Create SQL query to insert invalid campaign names with all other columns as NULL
                    new_campaign_name = campaign_name.replace("'","''")
                    insert_query = f"INSERT INTO campaign_master_connector (campaign_name) VALUES ('{new_campaign_name}');"
                    print(insert_query)
                    # Execute the insert query
                    cursor.execute(insert_query)
                    connection.commit()
                    error_msg = {
                        "error": f"{campaign_name} is not exist for Table:{table_name}, {campaign_name} is added in campaign_master_connector table.",
                    }
                    logger.error(
                        f"Error while validate_campaign_name || {str(error_msg)}")
                return json.dumps(error_msg, indent=2)

            logger.info(f"Column data are validated for {table_name}")
            return None
        except Exception as e:
            logger.error(
                f"Error while validate_campaign_name || {str(e)}")

    def validate_null_values(self, table_name, df):
        try:
            # Get column names
            column_names = df.columns.tolist()
            columns_to_check = ['campaign_name', 'client_id',
                                'retailer_id', 'brand_id', 'client_entity_id']
            existing_columns = [
                col for col in columns_to_check if col in column_names]

            # Check for NULL and 0 values
            null_values = df[existing_columns].isnull().sum()
            zero_values = (df[existing_columns] == 0).sum()

            error_messages = []

            for col in existing_columns:
                if null_values[col] > 0:
                    error_messages.append(
                        f"Total {null_values[col]} NULL values found in TABLE:{table_name} for COLUMN {col}")
                if zero_values[col] > 0:
                    error_messages.append(
                        f"Total {zero_values[col]} NULL values found in TABLE:{table_name} for COLUMN {col}")

            if error_messages:
                error_msg = {
                    "error": "NULL VALUES - {}".format(', '.join(error_messages))
                }
                logger.error(
                    f"Error in validate_null_values: {str(error_msg)}")
                return json.dumps(error_msg, indent=2)

            logger.info(f"No NULL values found in columns for {table_name}")
            return None
        except Exception as e:
            logger.error(f"Error in validate_null_values: {str(e)}")


class Data_Counts:
    def calculate_data(self, table_name, csv_filename, col_to_map):
        difference_percentage = None
        try:
            if isinstance(csv_filename, str) and (csv_filename.lower().endswith('.csv') or csv_filename.lower().endswith('.xlsx')):
                if csv_filename.lower().endswith('.csv'):
                    df = pd.read_csv(csv_filename)
                    df_columns = list(df.columns)
                else:
                    df = pd.read_excel(csv_filename)
                    df_columns = list(df.columns)
            else:
                df = csv_filename
                df_columns = list(df.columns) if hasattr(
                    df, 'columns') else df
            if 'date' in df_columns:
                # Filter out rows with NaN values in the 'date' column
                df = df.dropna(subset=['date'])
                try:
                    connection = conn
                    cursor = connection.cursor()
                    # Attempt to infer the Date format
                    df['date'] = pd.to_datetime(
                        df['date'], format='%Y-%m-%d')

                    # Calculate minimum and maximum dates from CSV
                    min_date_csv = pd.to_datetime(
                        df['date'].min())
                    max_date_csv = pd.to_datetime(
                        df['date'].max())

                    min_date_csv = min_date_csv.strftime(
                        '%Y-%m-%d') if min_date_csv else None
                    max_date_csv = max_date_csv.strftime(
                        '%Y-%m-%d') if max_date_csv else None

                    # Fetch minimum and maximum dates from the database
                    min_date_db_query = f"SELECT MIN(date) FROM {table_name}"
                    max_date_db_query = f"SELECT MAX(date) FROM {table_name}"
                    # Calculate total of columns for CSV and database
                    for col in col_to_map:
                        total_csv = round(df[col].sum(), 2)
                        total_db_query = f"SELECT SUM({col}) FROM {table_name} WHERE date >= '{min_date_csv}' AND date <= '{max_date_csv}'"
                        total_db_result = cursor.execute(total_db_query)
                        total_db_result = cursor.fetchone()
                        total_db = round(
                            total_db_result[0], 2) if total_db_result[0] else None
                        difference = None
                        if total_csv is not None and total_db is not None:
                            difference = round(
                                ((total_db - total_csv) / total_db) * 100, 2)
                        logger.info(
                            f"Columns Calculations for Table {table_name}, Column {col}")
                        print(f"TABLE: {table_name}")
                        print(f"Total {col} (CSV): {total_csv}")
                        print(f"Total {col} (Database): {total_db}")
                        print(f"Difference of {col}: {difference}%")

                    min_date_db_result = cursor.execute(min_date_db_query)
                    min_date_db_result = cursor.fetchone()
                    min_date_db_result = min_date_db_result[0].strftime(
                        '%Y-%m-%d') if min_date_db_result[0] else None

                    max_date_db_result = cursor.execute(max_date_db_query)
                    max_date_db_result = cursor.fetchone()
                    max_date_db_result = max_date_db_result[0].strftime(
                        '%Y-%m-%d') if max_date_db_result[0] else None

                    # Convert to the desired date format
                    min_date_db = min_date_db_result if min_date_db_result is not None else None
                    max_date_db = max_date_db_result if max_date_db_result is not None else None

                    # Count total rows between min_date and max_date from CSV
                    total_rows_csv = len(df[df['date'].notnull() & (
                        df['date'] >= min_date_csv) & (df['date'] <= max_date_csv)])

                    # Count total rows between min_date and max_date from database
                    total_rows_db_query = f"SELECT COUNT(*) FROM {table_name} WHERE date >= '{min_date_db}' AND date <= '{max_date_db}'"
                    total_rows_db_csvdate = f"SELECT COUNT(*) FROM {table_name} WHERE date >= '{min_date_csv}' AND date <= '{max_date_csv}'"

                    total_rows_db_result = cursor.execute(total_rows_db_query)
                    total_rows_db_result = cursor.fetchone()
                    total_rows_db_result = total_rows_db_result[0] if total_rows_db_result[0] else None

                    total_rows_db_csvdate_result = cursor.execute(
                        total_rows_db_csvdate)
                    total_rows_db_csvdate_result = cursor.fetchone()
                    total_rows_db_csvdate_result = total_rows_db_csvdate_result[
                        0] if total_rows_db_csvdate_result[0] else None

                    total_rows_db = int(
                        total_rows_db_result) if total_rows_db_result else None
                    total_rows_csvdate_db = int(
                        total_rows_db_csvdate_result) if total_rows_db_csvdate_result else None

                    # Calculate the threshold percentage
                    if total_rows_csv is not None and total_rows_csvdate_db is not None:
                        difference_percentage = round((
                            (total_rows_csvdate_db - total_rows_csv) / total_rows_csvdate_db) * 100, 2)

                    logger.info(
                        f"calculate_data successfully")
                except ValueError as e:
                    capture_exception(e)
                    logger.error(
                        f"Error while calculate_data || {str(e)}")
                    return str(e), None, None
            else:
                min_date_csv = None
                max_date_csv = None
                total_rows_csv = None

                min_date_db = None
                max_date_db = None
                total_rows_db = None
                total_rows_csvdate_db = None

        except Exception as e:
            logger.error(
                f"Error while calculate_data || {str(e)}")
            return str(e), None, None
        return min_date_csv, max_date_csv, total_rows_csv, min_date_db, max_date_db, total_rows_db, total_rows_csvdate_db, difference_percentage

    def count_csv_rows(self, csv_filename):
        if csv_filename.lower().endswith('.csv'):
            df = pd.read_csv(csv_filename)
            row_count = len(df)
        else:
            df = pd.read_excel(csv_filename)
            row_count = len(df)
        return row_count

    def count_db_rows(self, table_name):
        table_model = globals()[table_name]
        row_count = table_model.query.count()
        return row_count


class Load_data:

    def map_data_type(self, data_type):
        data_type_mapping = {
            'String': 'object',
            'Integer': 'int64',
            'BigInteger': 'int64',
            'Date': 'object',
            'DateTime': 'object',
            'Float': 'float64',
            'Boolean': 'bool',
        }
        return data_type_mapping.get(data_type, str)

    def load_data_to_table(self, table_name, csv_filename):
        with open('utils/column_headers.json', 'r') as json_file:
            column_headers = json.load(json_file)

        # Get the column names and data types for the specified table
        columns_info = column_headers.get(table_name, [])
        column_dtype_map = {info["name"]: self.map_data_type(info["type"])
                            for info in columns_info}

        if csv_filename.lower().endswith('.csv'):
            df = pd.read_csv(csv_filename)
        else:
            df = pd.read_excel(csv_filename)
        # df = pd.read_csv(csv_filename, dtype=column_dtype_map)

        # Handle the 'date' column if it exists in the DataFrame
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df['date'] = df['date'].apply(
                lambda x: np.nan if pd.isna(x) else x)

        error_messages = []

        # Loop through the DataFrame rows and add them to the database
        for index, row in df.iterrows():
            table_model = globals()[table_name]

            # Check for missing or invalid values in date columns
            for column_name, column_data_type in column_dtype_map.items():
                # if column_data_type == 'datetime64[ns]' or column_data_type == 'object':
                if pd.isna(row[column_name]):
                    row[column_name] = None
                    error_message = f"Warning: '{column_name}' in {table_name} is missing or invalid in Row {index + 1}"
                    error_messages.append(error_message)
                    continue

            table_entry = table_model(**row.to_dict())
            try:
                db.session.add(table_entry)
                db.session.commit()
            except IntegrityError as e:
                capture_exception(e)
                db.session.rollback()
                error_message = f"Warning: Duplicate data found in table '{table_name}' for row {index+1}"
                error_messages.append(error_message)
            except DataError as e:
                capture_exception(e)
                db.session.rollback()
                error_message = f"Warning: Data error in table '{table_name}' for row {index+1}"
                error_messages.append(error_message)

        if error_messages:
            logger.error(
                f"Error while load_data_to_table || {str(error_messages)}")
            return error_messages
            # return jsonify(errors=error_messages), 400

        else:
            logger.info(f"Data loaded successfully for {table_name}")
            return []
            # return jsonify(message=f"Data inserted successfully into table: {table_name}")
