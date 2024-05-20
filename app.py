from flask import Flask
from flask_cors import CORS
import logging
from flask_sqlalchemy import SQLAlchemy
from api.view import api
from utils.schema.models import bp,db
from configg.config import user, password, host, port, database
import sentry_sdk
from celery_conf import celery
# from kombu.common import Broadcast, Queue, Exchange

sentry_sdk.init(
    dsn="sentery dsn",
    traces_sample_rate=1.0,
    profiles_sample_rate=1.0,
)


app=Flask(__name__)
CORS(app, resources={
    r"/*" : {
    "origins":
        "*"
        }
    }
)


log_filename = 'app.log'
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(filename=log_filename,
                    level=logging.DEBUG, format=log_format)
file_handler = logging.FileHandler('app.log')
file_handler.setFormatter(log_format)
app.logger.addHandler(file_handler)


app.config["JWT_SECRET_KEY"] = "4354354"
app.config['JWT_BLACKLIST_ENABLED'] = True



def create_app():
    app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql://{user}:{password}@{host}:{port}'
    db.init_app(app)

    app.register_blueprint(api)
    app.register_blueprint(bp)


    return app