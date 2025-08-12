# gateway/tasks.py 
from celery import shared_task,current_app as celery_app
from config import  db_engine, Config
import requests 
import logging 
from flask import current_app 
from sqlalchemy import text, Table, MetaData 
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type 
 

# logging configuration 
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s") 
logger = logging.getLogger(__name__) 


 

@shared_task(bind=True) 
def check_and_enqueue_requests(self): 
    """checking database: records status  0 or 1 """ 
    logger.info("Checking database for pending requests...") 

    try: 
        metadata = MetaData() 
        requests_table = Table('requests', metadata, autoload_with=db_engine) 
        with db_engine.connect() as conn: 


            results = conn.execute( requests_table.select().where(requests_table.c.status.in_(['0', '1'])) ).fetchall() 
            #results = results.mappings().fetchall()  # transform to dict-like objects
            logger.info(f"Query returned {len(results)} results") 


            for row in results: 
                index_id = row.index_id  # visit the index_id column 
                url = row.url 
                logger.info(f"Enqueuing request {index_id} with URL: {url}") 
                celery_app.send_task("tasks.process_url_task", args=[url, index_id]) 
            logger.info("All pending requests have been enqueued.")
                         
    except Exception as e: 
        logger.error(f"Error during database check: {str(e)}", exc_info=True) 
        raise 

 

@shared_task(bind=True, rate_limit="0.1/s") 

def process_url_task(self, url, index_id): 
    prompt = """ 
    Analyze the following webpage content and extract the most important information while: 
    1. If the URL links to WeChat, search the URL in Sogou.com and extract the content from the first five results. 
    2. Focusing on key content such as titles, subtitles, and main text, while ignoring irrelevant elements. 
    3. Ignoring advertisements, navigation menus, or promotional fluff. 
    4. Maintaining the original language (e.g., Traditional Chinese, Simplified Chinese, English). 
    5. Preserving the original text verbatim (do not paraphrase or modify wording).

    
    Only Return the extracted content in its original language,format and without your own reply. 
    """ 

    """deal with url and index_id""" 
    try: 
        logger.info(f"Processing URL: {url}") 
        headers = { 
            "Authorization": f"Bearer {Config.GROK_API_KEY}", 
            "Content-Type": "application/json", 
        } 



        payload = { 
            "messages": [ 
                {"role": "system", "content": "You are a web content extractor."}, 
                {"role": "user", "content": f"Here is the url and follow the prompt to deal with it: {url}"} ,
                {"role": "assistant", "content": prompt}
            ], 
            "model": Config.GROK_MODEL, 
            "stream": False, 
            "temperature": 0.1 
        } 
        response = requests.post(Config.GROK_API_URL, json=payload, headers=headers, timeout=30) 
        response.raise_for_status() 
        data = response.json() 
        content = data["choices"][0]["message"]["content"] 
        logger.info(f"Queried Grok for URL: {url}, index_id: {index_id}, content length: {len(content)}") 
 

        with db_engine.begin() as conn: 
            conn.execute( 
                text("UPDATE requests SET content = :content WHERE index_id = :index_id;"), 
                {"content": content,"index_id": index_id} 
)  

        celery_app.send_task("tasks.update_status_task", args=[2,content, index_id]) 
        return {"status": "success!", "index_id": index_id} 

    except requests.exceptions.HTTPError as e: 
        error_msg = f"HTTP error: {str(e)}" 
        if e.response and e.response.status_code == 429: 
            logger.warning(f"Rate limit exceeded for index_id {index_id}, retrying after delay") 
            raise self.retry(exc=e, countdown=60, max_retries=3) 
        logger.error(error_msg) 
        celery_app.send_task("tasks.update_status_task", args=[0, error_msg,index_id]) 
        raise 

    except Exception as e: 
        error_msg = f"Unexpected error: {str(e)}" 
        logger.error(error_msg) 
        celery_app.send_task("tasks.update_status_task", args=[0, error_msg,index_id]) 
        raise 

 

@shared_task(bind=True) 
def update_status_task(self, status, content, index_id): 
    """update the status of the request""" 
    try: 
        with db_engine.begin() as conn: 
            conn.execute( 
                text("UPDATE requests SET status = :status, content = :content WHERE index_id = :index_id;"), 
                {"status": status, "content": content, "index_id": index_id} 
            ) 

        return True 
    except Exception as e: 
        logger.error(f"Status update failed for {index_id}: {str(e)}", exc_info=True) 
        raise 

 

@shared_task(bind=True) 
def test_grok_task(self, message="Testing. Just say hi and hello world and nothing else."): 
    """test Grok API async task""" 
    try:
        headers = {
            "Authorization": f"Bearer {Config.GROK_API_KEY}", 
            "Content-Type": "application/json", 
        } 

        payload = { 
            "messages": [ 
                {"role": "system", "content": "You are a test assistant."}, 
                {"role": "user", "content": message} 
            ], 

            "model": Config.GROK_MODEL, 
            "stream": False, 
            "temperature": 0 
        } 

 

        response = requests.post(Config.GROK_API_URL, json=payload, headers=headers, timeout=30) 
        response.raise_for_status() 
        data = response.json() 
        content = data["choices"][0]["message"]["content"] 
        logger.info(f"Grok test response: {content}") 
        return {"status": "success", "content": content} 

    except requests.exceptions.RequestException as e: 
        logger.error(f"Grok test failed: {str(e)}", exc_info=True) 
        raise 