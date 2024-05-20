from app import celery, create_app
from celery_conf import celery
app = create_app()
app.app_context().push()