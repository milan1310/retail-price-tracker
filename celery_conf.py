from celery import Celery
from configg.config import user, password, host, port, database, redispassword

def make_celery(app_name=__name__):
    return Celery(
        app_name,
        broker=f'redis://:{redispassword}@localhost:6379/0',
        backend=f'db+postgresql://{user}:{password}@{host}:{port}/{database}'
    )
celery = make_celery()
celery.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Kolkata',
    result_extended=True,
    enable_utc=True,
    broker_connection_retry_on_startup=True,
    task_default_queue='other_functionality',
)
