import os
from celery import Celery

#: Set default configuration module name
os.environ.setdefault('CELERY_CONFIG_MODULE', 'kanjo_pipeline.config.celeryconfig')

app = Celery()
app.config_from_envvar('CELERY_CONFIG_MODULE')