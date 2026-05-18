import logging
import redis
import pika
import json
import time
import random
from datetime import datetime
import psycopg2
import requests
import threading

#----------------------------
# Service Status 
#----------------------------
SERVICE_STATUS = {
    "service": "service-c",
    "ready": False,
    "dependencies": {
        "redis": False,
        "postgres": False,
        "rabbitmq": False,
        "external_api": False
    }
}

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

#----------------------------
# Postgres Health Check
#----------------------------
def check_postgres():
    try:
        conn = get_db_connection()
        conn.close()
        return True
    except:
        return False
    
# ----------------------------
# Redis
# ----------------------------
r = redis.Redis(
    host="redis",
    port=6379,
    decode_responses=True
)

#----------------------------
# Redis Health Check
#----------------------------
def check_redis():
    try:
        r.ping()
        return True
    except Exception:
        return False

# ----------------------------
# External API Health Check
# ----------------------------
def check_external_api():
    try:
        response = requests.get(
            "http://external-api:8003/health", 
            timeout=2
        )
        return response.status_code == 200
    
    except Exception:

        return False

# ----------------------------
# RabbitMQ connection holder and readiness check
# ----------------------------
rmq_connection = None
 
def check_rabbitmq():
    try:
        return rmq_connection is not None and rmq_connection.is_open
    except Exception:
        return False
 
# ----------------------------
# Readiness Check
# ----------------------------
def ready():
    SERVICE_STATUS["dependencies"]["redis"] = check_redis()
    SERVICE_STATUS["dependencies"]["postgres"] = check_postgres()
    SERVICE_STATUS["dependencies"]["rabbitmq"] = check_rabbitmq()
    SERVICE_STATUS["dependencies"]["external_api"] = check_external_api()
 
    SERVICE_STATUS["ready"] = all(SERVICE_STATUS["dependencies"].values())
 
    r.set("service-c:ready", json.dumps(SERVICE_STATUS), ex=60)
    return SERVICE_STATUS
 
# ----------------------------
# Heartbeat to check dependencies and update status in Redis
# ----------------------------
def heartbeat_loop():
    
    while True:
        try:
            ready()
        except Exception as e:
            logger.error(f"Heartbeat error: {e}")
        time.sleep(10)

# ----------------------------
# Retry Config (NEW)
# ----------------------------
MAX_RETRIES = 3
RETRY_KEY_PREFIX = "retry:"

def get_retry_count(request_id):
    return int(r.get(f"{RETRY_KEY_PREFIX}{request_id}") or 0)

def increment_retry(request_id):
    count = get_retry_count(request_id) + 1
    r.set(f"{RETRY_KEY_PREFIX}{request_id}", count)
    return count

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
# DLQ 
# ----------------------------
def send_to_dlq(ch, message):

    ch.basic_publish(
        exchange="",
        routing_key="workflow_dlq",
        body=json.dumps(message)
    )

# ----------------------------
# Consumer Callback
# ----------------------------
def callback(ch, method, properties, body):

    message = json.loads(body)

    trace_id = message["trace_id"]
    request_id = message["request_id"]

    # ----------------------------
    # PROCESSING C
    # ----------------------------
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

    # simulate processing delay
    time.sleep(random.randint(1, 3))

    # ----------------------------
    # FAILURE SIMULATION (NEW)
    # ----------------------------
    should_fail = random.randint(1, 10) > 8

    if should_fail:

        retry_count = increment_retry(request_id)

        r.set(f"workflow:{request_id}", "FAILED_C")

        log_event(
            level="error",
            trace_id=trace_id,
            request_id=request_id,
            state="FAILED_C",
            message=f"service-c failed (retry {retry_count}/{MAX_RETRIES})"
        )

        persist_event(
            trace_id,
            request_id,
            "FAILED_C",
            f"workflow failed in service c (retry {retry_count}/{MAX_RETRIES})"
        )

        # ----------------------------
        # RETRY LOGIC (NEW)
        # ----------------------------
        if retry_count <= MAX_RETRIES:

            log_event(
                level="warning",
                trace_id=trace_id,
                request_id=request_id,
                state="RETRY_C",
                message=f"retrying service-c ({retry_count}/{MAX_RETRIES})"
            )

            ch.basic_publish(
                exchange="",
                routing_key="workflow_queue_c",
                body=json.dumps(message)
            )

        else:

            send_to_dlq(ch, message)

            log_event(
                level="error",
                trace_id=trace_id,
                request_id=request_id,
                state="DLQ_C",
                message="max retries exceeded, sent to DLQ"
            )

        return

    # ----------------------------
    # EXTERNAL API CALL (SUCCESS PATH)
    # ----------------------------
    try:

        response = requests.get(
            "http://external-api:8003/process",
            params={"request_id": request_id},
            timeout=2
        )

        if response.status_code == 200 and response.json().get("status") == "SUCCESS":

            state_text = "COMPLETED_C"
            message_text = "external API call succeeded"
            log_level = "info"

        else:

            state_text = "FAILED_C"
            message_text = "external API call failed"
            log_level = "error"

    except Exception as e:

        state_text = "ERRORED_C"
        message_text = f"external API error: {str(e)}"
        log_level = "error"

    # ----------------------------
    # FINAL STATE UPDATE
    # ----------------------------
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

    # ----------------------------
    # REDIS CLEANUP  (keep visibility longer now)
    # ----------------------------
    r.expire(f"workflow:{request_id}", 3600)

    log_event(
        level="info",
        trace_id=trace_id,
        request_id=request_id,
        state="CACHE_CLEANUP",
        message="redis workflow cache set to expire"
    )

    persist_event(
        trace_id,
        request_id,
        "CACHE_CLEANUP",
        "workflow marked for cleanup"
    )

# ----------------------------
# RabbitMQ Connection
# ----------------------------
def connect_rabbitmq():
    global rmq_connection
    while True:
        try:
            logger.info("Attempting RabbitMQ connection...")
            rmq_connection = pika.BlockingConnection(
                pika.ConnectionParameters(host="rabbitmq")
            )
            logger.info("Connected to RabbitMQ")
            SERVICE_STATUS["dependencies"]["rabbitmq"] = True
            return rmq_connection
        except pika.exceptions.AMQPConnectionError:
            logger.error("RabbitMQ not ready. Retrying in 5 seconds...")
            SERVICE_STATUS["dependencies"]["rabbitmq"] = False
            time.sleep(5)
 
# ----------------------------
# Start Consumer
# ----------------------------
connection = connect_rabbitmq()
channel = connection.channel()
 
channel.queue_declare(queue="workflow_queue_c")
channel.queue_declare(queue="workflow_dlq")
 
ready()   # write initial ready state after full setup

channel.basic_consume(
    queue="workflow_queue_c",
    on_message_callback=callback,
    auto_ack=True
)
 
threading.Thread(target=heartbeat_loop, daemon=True).start()

logger.info("service-c waiting for RabbitMQ messages...")

channel.start_consuming()