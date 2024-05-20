import psycopg2
from flask import Blueprint,jsonify
from flask_sqlalchemy import SQLAlchemy
# from utils.utils import Authentication_Utils
from sqlalchemy import TIME, Column,Date,BigInteger, Integer, Boolean, String, Float, DateTime, ForeignKey,CheckConstraint,case,event
from datetime import datetime


bp=Blueprint('model',__name__)
db = SQLAlchemy()


class TimeStamp(object):
    created_date = Column(Date, default=datetime.now())
    created_time = Column(TIME, default=datetime.now())
    updated_date = Column(Date, default=datetime.now(), onupdate=datetime.now())
    updated_time = Column(TIME, default=datetime.now(), onupdate=datetime.now())

    
class DimDate(db.Model):
    __tablename__ = 'dim_date'

    date_id = Column(Integer, primary_key=True, nullable=False)
    date_actual = Column(Date, nullable=False)
    epoch = Column(Integer, nullable=False)
    day_suffix = Column(String(4), nullable=False)
    day_name = Column(String(9), nullable=False)
    day_of_week = Column(Integer, nullable=False)
    day_of_month = Column(Integer, nullable=False)
    day_of_quarter = Column(Integer, nullable=False)
    day_of_year = Column(Integer, nullable=False)
    week_of_month = Column(Integer, nullable=False)
    week_of_year = Column(Integer, nullable=False)
    week_of_year_iso = Column(String(10), nullable=False)
    month_actual = Column(Integer, nullable=False)
    month_name = Column(String(9), nullable=False)
    month_name_abbreviated = Column(String(3), nullable=False)
    quarter_actual = Column(Integer, nullable=False)
    quarter_name = Column(String(9), nullable=False)
    year_actual = Column(Integer, nullable=False)
    first_day_of_week = Column(Date, nullable=False)
    last_day_of_week = Column(Date, nullable=False)
    first_day_of_month = Column(Date, nullable=False)
    last_day_of_month = Column(Date, nullable=False)
    first_day_of_quarter = Column(Date, nullable=False)
    last_day_of_quarter = Column(Date, nullable=False)
    first_day_of_year = Column(Date, nullable=False)
    last_day_of_year = Column(Date, nullable=False)
    mmyyyy = Column(String(6), nullable=False)
    mmddyyyy = Column(String(10), nullable=False)
    weekend_indr = Column(Boolean, nullable=False)

class DimTime(db.Model):
    __tablename__ = 'dim_time'

    time_id = db.Column(db.Integer, primary_key=True)
    time_actual = db.Column(db.Time, nullable=False)
    hour = db.Column(db.Integer, nullable=False)
    minute = db.Column(db.Integer, nullable=False)
    second = db.Column(db.Integer, nullable=False)
    am_pm = db.Column(db.String(2), nullable=False)
    hour12 = db.Column(db.Integer, nullable=False)
    hour24 = db.Column(db.Integer, nullable=False)
    minute_of_day = db.Column(db.Integer, nullable=False)
    second_of_day = db.Column(db.Integer, nullable=False)
    iso_time = db.Column(db.String(12), nullable=False)
    is_morning = db.Column(db.Boolean, nullable=False)
    is_afternoon = db.Column(db.Boolean, nullable=False)
    is_evening = db.Column(db.Boolean, nullable=False)
    is_night = db.Column(db.Boolean, nullable=False)

    def __repr__(self):
        return f'<DimTime {self.time_actual}>'

class DimSeller(db.Model, TimeStamp):
    __tablename__ = 'dim_seller'
    
    seller_id = db.Column(db.Integer, primary_key=True, nullable=False)
    seller_name = db.Column(db.String(255), nullable=False)
    supplier_name = db.Column(db.String(255))
    marketplace = db.Column(db.String(100), nullable=False)
    average_price = db.Column(db.Numeric(10, 2))

    def __repr__(self):
        return f'<DimSeller {self.seller_name}>'

class DimProduct(db.Model, TimeStamp):
    __tablename__ = 'dim_product'
    id = db.Column(db.Integer, primary_key=True)
    product_id = db.Column(db.String, unique=True)
    product_name = db.Column(db.String(255))
    description = db.Column(db.Text)
    bulleting = db.Column(db.Text)
    buy_box_winner = db.Column(db.String())
    is_aplus = db.Column(db.String())
    is_variation = db.Column(db.String())
    num_images = db.Column(db.Integer())
    list_price = db.Column(db.Numeric(10, 2))
    rating = db.Column(db.String())
    brand = db.Column(db.String(100))
    category = db.Column(db.String(100))
    sub_category = db.Column(db.String(100))
    supplier_id = db.Column(db.Integer)

    def __repr__(self):
        return f'<DimProduct {self.product_name}>'
    
class DimPrice(db.Model):
    __tablename__ = 'dim_price'

    price_id = db.Column(db.Integer, primary_key=True)
    product_id = db.Column(db.String, db.ForeignKey('dim_product.product_id'), nullable=False)
    seller_id = db.Column(db.Integer, db.ForeignKey('dim_seller.seller_id'), nullable=False)
    price_date = db.Column(db.Date, nullable=False)
    price_time = db.Column(db.Time, nullable=False)
    list_price = db.Column(db.Numeric(10, 2), nullable=False)
    sale_price = db.Column(db.Numeric(10, 2))
    currency = db.Column(db.String(3), nullable=False)

    def __repr__(self):
        return f'<DimPrice {self.price_id} - Product {self.product_id}>'
    
class Task_Logger(db.Model, TimeStamp):
    __tablename__ = "task_logger"
    name = db.Column(String)
    start_date = db.Column(DateTime)
    end_date = db.Column(DateTime)
    status = db.Column(String)
    issue = db.Column(String)
    PID = db.Column(Integer)
    celery_task_id = db.Column(String, primary_key=True)
    retries = db.Column(Integer)
    data_load_logger = db.Column(String)


class Issue_Ticket(db.Model, TimeStamp):
    __tablename__ = "issue_ticket"
    id = db.Column(BigInteger, primary_key=True)
    name = db.Column(String)
    pipeline = db.Column(String)
    status = db.Column(String)
    description = db.Column(String)
    source = db.Column(String)
    owner = db.Column(String)
    priority = db.Column(BigInteger)
    associated_with = db.Column(String)

class S3_file_manager(db.Model, TimeStamp):
    __tablename__ = "s3_file_manager"
    id = db.Column(Integer, primary_key=True)
    file_name = db.Column(String)
    process_status = db.Column(String) # started, completed, archived
    is_deleted = db.Column(Boolean, default=False)

class S3_file_manager_logger(db.Model, TimeStamp):
    __tablename__ = "s3_file_manager_logger"
    id = db.Column(Integer, primary_key=True)
    file_name = db.Column(String)
    process_status = db.Column(String) # started, completed, archived
    is_deleted = db.Column(Boolean)




def createdb():
    db.create_all() 