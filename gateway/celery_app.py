# gateway/celery_app.py 

from celery import Celery 
from celery.schedules import crontab 
from flask import Flask 
from config import get_celery_result_backend 
import os
from tasks import check_and_enqueue_requests




 

def make_celery(app): 
    celery = Celery( 
        app.import_name, 
        backend=get_celery_result_backend(trust_mode=True),  # 开发阶段使用 trust 模式 
        broker=app.config['broker_url'] 
    ) 

    celery.conf.update(app.config) 

    celery.conf.beat_schedule = { 
        'check-database-every-minute': { 
            'task': 'tasks.check_and_enqueue_requests', 
            'schedule': crontab(minute='*'), 
        }, 

    } 
    celery.conf.timezone = 'UTC' 
    return celery 

 

def create_app(): 
    app = Flask(__name__) 
    app.config.update( 
        broker_url=os.getenv("BROKER_URL", "redis://redis:6379/0") 
    ) 
    return app 

 
flask_app = create_app() 

celery = make_celery(flask_app) 

