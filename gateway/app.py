#gateway/app.py 
import random
from flask import Flask, request, jsonify 
from celery.result import AsyncResult 
from sqlalchemy import create_engine, text 
from sqlalchemy.exc import SQLAlchemyError 
import uuid 
from config import get_db_url, Config, db_engine
from celery_app import celery, flask_app 
import logging 

 

# Flask innitialization 
app = flask_app 

 

# logging configuration 
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s") 
logger = logging.getLogger(__name__) 

 



 

@app.route("/") 
def home(): 
    return jsonify({"message": "Flask app is running with Celery!"}) 

 

@app.route("/health") 
def health_check(): 

    """ 
    health check endpoint 
    """ 

    try: 
        with db_engine.connect() as conn: 
            result = conn.execute(text("SELECT 1")).scalar() 
            return jsonify({"status": "success", "db_result": result}), 200 
    except Exception as e: 

        logger.error(f"Health check failed: {str(e)}") 

        return jsonify({"status": "error", "message": str(e)}), 500 

 

@app.route("/requests", methods=["POST"]) 
def create_request(): 
    """ 
    add a new request to the database 
    """ 

    data = request.json
    url = data.get("url")
    content = "the url is waiting for processing"  # predefined content
    status = 0  # 0 means waiting for processing

    try: 
        with db_engine.begin() as conn: 
            # generate a unique index_id 
            while True: 
                index_id = str(random.randint(100000, 999999))  # generate a random index_id 
                # check if index_id already exists 
                exists = conn.execute( 
                    text("SELECT 1 FROM requests WHERE index_id = :index_id"),
                    {"index_id": index_id} 
                ).scalar() 
                if not exists: 
                    break              

            # insert the new request into the database 
            conn.execute( 
                text(""" 
                    INSERT INTO requests 
                    (url, index_id, content, status)  
                    VALUES (:url, :index_id, :content, :status); 
                """), 
                { 
                    "url": url,  
                    "index_id": index_id,  
                    "content": content, 
                    "status": status 
                } 
            ) 
        response = {"index_id": index_id}
        return jsonify(response), 200 
    except SQLAlchemyError as e: 
        logger.error(f"Error creating request: {str(e)}", exc_info=True) 
        return jsonify({"status": "error", "message": str(e)}), 500 

 

@app.route("/requests/<string:index_id>", methods=["GET"]) 

def get_request_by_index_id(index_id): 
    """ 
    check request by index_id 
    """ 

    try: 
        with db_engine.connect() as conn: 
            result = conn.execute( 
                text("SELECT * FROM requests WHERE index_id = :index_id"), 
                {"index_id": index_id} 
            ).fetchone()  

            if not result: 
                return jsonify({"status": "error", "message": "Request not found"}), 404 
 
            request_data = dict(result._mapping) 
            return jsonify({"data": request_data}), 200 
    except SQLAlchemyError as e: 
        logger.error(f"Error fetching request {index_id}: {str(e)}", exc_info=True) 
        return jsonify({"status": "error", "message": str(e)}), 500 

 

@app.route("/requests/<string:index_id>", methods=["PUT"]) 

def update_request_content(index_id): 

    """ 

    update request content by index_id 

    """ 

    data = request.json 

    content = data.get("content", None) 

 

    if content is None: 

        return jsonify({"status": "error", "message": "Content is required"}), 400 

 

    try: 

        with db_engine.begin() as conn: 

            result = conn.execute( 

                text("UPDATE requests SET content = :content WHERE index_id = :index_id RETURNING index_id"), 

                {"content": content, "index_id": index_id} 

            ).fetchone() 

 

            if not result: 

                return jsonify({"status": "error", "message": "Request not found"}), 404 

 

            return jsonify({"status": "success", "message": "Content updated successfully"}), 200 

    except SQLAlchemyError as e: 

        logger.error(f"Error updating request {index_id}: {str(e)}", exc_info=True) 

        return jsonify({"status": "error", "message": str(e)}), 500 

 

@app.route("/requests/<string:index_id>", methods=["DELETE"]) 

def delete_request(index_id): 

    """ 

    delete request by index_id 

    """ 

    try: 

        with db_engine.begin() as conn: 

            result = conn.execute( 

                text("DELETE FROM requests WHERE index_id = :index_id RETURNING index_id"), 

                {"index_id": index_id} 

            ).fetchone() 

 

            if not result: 

                return jsonify({"status": "error", "message": "Request not found"}), 404 

 

            return jsonify({"status": "success", "message": "Request deleted successfully"}), 200 

    except SQLAlchemyError as e: 

        logger.error(f"Error deleting request {index_id}: {str(e)}", exc_info=True) 

        return jsonify({"status": "error", "message": str(e)}), 500 

 

@app.route("/submit", methods=["POST"]) 

def submit_url(): 

    """ 

    submit a URL for processing

    """ 

    data = request.json 

    url = data.get("url") 

    if not url: 

        return jsonify({"status": "error", "message": "URL is required"}), 400 

 

    try: 

        index_id = str(uuid.uuid4()) 

        with db_engine.begin() as conn: 

            conn.execute( 

                text("INSERT INTO requests (url, index_id, status) VALUES (:url, :index_id, :status)"), 

                {"url": url, "index_id": index_id, "status": 0} 

            ) 

 

        task = celery.send_task("tasks.process_url_task", args=[url, index_id]) 

        celery.send_task("tasks.update_status_task", args=[index_id, "processing"]) 

        return jsonify({ 

            "status": "queued", 

            "index_id": index_id, 

            "task_id": task.id 

        }), 202 

    except SQLAlchemyError as e: 

        logger.error(f"Error submitting URL: {str(e)}", exc_info=True) 

        return jsonify({"status": "error", "message": str(e)}), 500 

 

@app.route("/status/<task_id>") 

def task_status(task_id): 

    """ 

    check the status of a Celery task

    """ 

    task = AsyncResult(task_id, app=celery) 

    response = {"status": task.state} 

    if task.state == "SUCCESS": 

        response["result"] = task.result 

    elif task.state == "FAILURE": 

        response["error"] = str(task.result) 

    return jsonify(response) 

@app.route("/index", methods=["POST"])
def get_index_data():
    """
    Fetch index_id and address from the database ordered by id.
    """
    try:
        with db_engine.connect() as conn:
            # Query to fetch index_id and address ordered by id
            result = conn.execute(
                text("SELECT index_id, url FROM requests ORDER BY id;")
            ).fetchall()

            if not result:
                return jsonify({"status": "error", "message": "No data found"}), 404

            # Convert the result into a list of dictionaries
            data = [{"index_id": row._mapping["index_id"], "address": row._mapping["url"]} for row in result]

            return jsonify({"status": "success", "data": data}), 200
    except SQLAlchemyError as e:
        logger.error(f"Error fetching index data: {str(e)}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500
 

if __name__ == "__main__": 

    app.run(host="0.0.0.0", port=8080, debug=True) 