import logging
import redis
import pika
import json
import time
import random
from datetime import datetime
import psycopg2
import requests

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    force=True
)

logger = logging.getLogger("service-c")

# ----------------------------
# Postgres Connection
# ----------------------------
def get_db_connection():

    return psycopg2.connect(
        host="postgres",
        database="opssim",
        user="opsuser",
        password="opspassword"
    )

# ----------------------------
# Redis
# ----------------------------
r = redis.Redis(
    host="redis",
    port=6379,
    decode_responses=True
)

# ----------------------------
# Structured Logging
# ----------------------------
def log_event(level, trace_id, request_id, state, message):

    log_data = {
        "timestamp": datetime.utcnow().isoformat(),
        "trace_id": trace_id,
        "request_id": request_id,
        "service": "service-c",
        "state": state,
        "message": message
    }

    print(json.dumps(log_data), flush=True)


# ----------------------------
# Persist Workflow Event
# ----------------------------
def persist_event(trace_id, request_id, state, message):

    conn = get_db_connection()

    cur = conn.cursor()

    cur.execute("""
        INSERT INTO workflow_events (
            trace_id,
            request_id,
            service_name,
            state,
            message
        )
        VALUES (%s, %s, %s, %s, %s)
    """, (
        trace_id,
        request_id,
        "service-c",
        state,
        message
    ))

    conn.commit()

    cur.close()
    conn.close()


# ----------------------------
# RabbitMQ Connection Retry
# ----------------------------
connection = None

while connection is None:

    try:

        logger.info("Attempting RabbitMQ connection...")

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="rabbitmq")
        )

        logger.info("Connected to RabbitMQ")

    except pika.exceptions.AMQPConnectionError:

        logger.error("RabbitMQ not ready. Retrying in 5 seconds...")

        time.sleep(5)


channel = connection.channel()

channel.queue_declare(queue="workflow_queue_c")


# ----------------------------
# Consumer Callback
# ----------------------------
def callback(ch, method, properties, body):

    message = json.loads(body)

    trace_id = message["trace_id"]
    request_id = message["request_id"]

    log_event(
        level="info",
        trace_id=trace_id,
        request_id=request_id,
        state="PROCESSING_C",
        message="message consumed from workflow_queue_c"
    )
    
    # persist event to Postgres
    persist_event(
        trace_id,
        request_id,
        "PROCESSING_C",
        "workflow consumed by workflow_queue_c"
    )

    # update redis state
    r.set(f"workflow:{request_id}", "PROCESSING_C")

    # call external API
    try: 
        response = requests.get(
            "http://external-api:8003/process",
            params={"request_id": request_id},
            timeout=2
        )

        if response.status_code == 200 and response.json().get("status") == "SUCCESS":

            state_text="COMPLETED_C"
            message_text="external API call succeeded"
            log_level="info"

        else:

            state_text="FAILED_C"
            message_text=f"external API call failed with status code {response.status_code}"
            log_level="error"

        r.set(f"workflow:{request_id}", state_text)
            
        log_event(
            level=log_level,
            trace_id=trace_id,
            request_id=request_id,
            state=state_text,
            message=message_text
        )

        persist_event(
            trace_id,
            request_id,
            state_text,
            message_text
        )

    except Exception as e:

        r.set(f"workflow:{request_id}", "ERRORED_C")

        log_event(
            level="error",
            trace_id=trace_id,
            request_id=request_id,
            state="ERRORED_C",
            message=f"external API call failed: {str(e)}"
        )

        persist_event(
            trace_id,
            request_id,
            "ERRORED_C",
            f"external API call failed: {str(e)}"
        )

        return
    
    # cleanup redis after completion
    time.sleep(2)

    r.expire(f"workflow:{request_id}", 3600)

    log_event(
        level="info",
        trace_id=trace_id,
        request_id=request_id,
        state="CACHE_CLEANUP",
        message="redis workflow cache deleted"
    )

    persist_event(
        trace_id,
        request_id,
        "COMPLETED",
        "workflow completed successfully"
    )
    
    return
# ----------------------------
# Start Consumer
# ----------------------------
channel.basic_consume(
    queue="workflow_queue_c",
    on_message_callback=callback,
    auto_ack=True
)

logger.info("service-c waiting for RabbitMQ messages...")

channel.start_consuming()