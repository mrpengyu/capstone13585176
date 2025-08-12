# gateway/config.py 

import os 
from sqlalchemy import create_engine

class Config: 

    GROK_API_URL = "https://api.x.ai/v1/chat/completions" 
    GROK_API_KEY = os.getenv("GROK_API_KEY", "aip key")  
    GROK_MODEL = "grok-3" 

 
 

def get_postgres_config(): 

    return { 

        "user": os.getenv("POSTGRES_USER", "postgres"), 

        "host": os.getenv("POSTGRES_HOST", "database"), 

        "port": os.getenv("POSTGRES_PORT", "5432"), 

        "db": os.getenv("POSTGRES_DB", "inforobot"), 

        "password": os.getenv("POSTGRES_PASSWORD", "")  

    } 

 

def get_db_url(trust_mode=True): 

    config = get_postgres_config() 

    if trust_mode: 

        return f"postgresql+psycopg2://{config['user']}@{config['host']}:{config['port']}/{config['db']}" 

    return f"postgresql+psycopg2://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['db']}" 

 

def get_celery_result_backend(trust_mode=True): 

    config = get_postgres_config() 

    if trust_mode: 

        return f"db+postgresql://{config['user']}@{config['host']}:{config['port']}/{config['db']}" 

    return f"db+postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['db']}" 

# initialize the database engine directly
db_url = get_db_url(trust_mode=True)   
db_engine = create_engine(db_url)   