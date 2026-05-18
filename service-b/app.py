import logging
import redis
import pika
import json
import time
import random
from datetime import datetime
import psycopg2
import threading

#----------------------------
# Service Status 
#----------------------------
SERVICE_STATUS = {
    "service": "service-b",
    "ready": False,
    "dependencies": {
        "rabbitmq": False,
        "redis": False,
        "postgres": False
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

logger = logging.getLogger("service-b")

# ----------------------------
# Redis
# ----------------------------
r = redis.Redis(
    host="redis",
    port=6379,
    decode_responses=True
)

#----------------------------
# Redis Readiness Check
#----------------------------
def check_redis():
    try:
        r.ping()
        return True
    except Exception:
        return False
    
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
# Postgres Readiness Check
#----------------------------
def check_postgres():
    try:
        conn = get_db_connection()
        conn.close()
        return True
    except:
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
# RMQ Readiness Check
# ----------------------------
def ready():
    SERVICE_STATUS["dependencies"]["redis"] = check_redis()
    SERVICE_STATUS["dependencies"]["postgres"] = check_postgres()
    SERVICE_STATUS["dependencies"]["rabbitmq"] = check_rabbitmq()

    SERVICE_STATUS["ready"] = all(SERVICE_STATUS["dependencies"].values())

    r.set("service-b:ready", json.dumps(SERVICE_STATUS), ex=60)
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
# Structured Logging
# ----------------------------
def log_event(level, trace_id, request_id, state, message):

    log_data = {
        "timestamp": datetime.utcnow().isoformat(),
        "trace_id": trace_id,
        "request_id": request_id,
        "service": "service-b",
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
        "service-b",
        state,
        message
    ))

    conn.commit()

    cur.close()
    conn.close()

# ----------------------------
# Retry + DLQ CONFIG
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
# DLQ 
# ----------------------------
def send_to_dlq(channel, message):

    channel.basic_publish(
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
    # Step B: Processing start
    # ----------------------------
    log_event(
        level="info",
        trace_id=trace_id,
        request_id=request_id,
        state="PROCESSING_B",
        message="message consumed from workflow_queue_b"
    )

    # update redis state
    r.set(f"workflow:{request_id}", "PROCESSING_B")

    # persist event to Postgres
    persist_event(
        trace_id,
        request_id,
        "PROCESSING_B",
        "workflow consumed by service b"
    )

    # simulate processing
    time.sleep(random.randint(1, 4))

    # ----------------------------
    # FAILURE SIMULATION (NEW STEP 2)
    # ----------------------------
    should_fail = random.randint(1, 10) > 8

    if should_fail:

        # increment retry counter
        retry_count = increment_retry(request_id)

        r.set(f"workflow:{request_id}", "FAILED_B")

        log_event(
            level="error",
            trace_id=trace_id,
            request_id=request_id,
            state="FAILED_B",
            message=f"service-b failed (retry {retry_count}/{MAX_RETRIES})"
        )

        # persist event to Postgres
        persist_event(
            trace_id,
            request_id,
            "FAILED_B",
            f"workflow failed in service b (retry {retry_count}/{MAX_RETRIES})"
        )

        # ----------------------------
        # RETRY LOGIC (NEW)
        # ----------------------------
        if retry_count <= MAX_RETRIES:

            log_event(
                level="warning",
                trace_id=trace_id,
                request_id=request_id,
                state="RETRY_B",
                message=f"retrying workflow (attempt {retry_count})"
            )

            # requeue message
            ch.basic_publish(
                exchange="",
                routing_key="workflow_queue_b",
                body=json.dumps(message)
            )

        else:

            # send to DLQ after max retries
            send_to_dlq(ch, message)

            log_event(
                level="error",
                trace_id=trace_id,
                request_id=request_id,
                state="DLQ_B",
                message="max retries exceeded, sent to DLQ"
            )

        return

    # ----------------------------
    # SUCCESS PATH
    # ----------------------------

    r.set(f"workflow:{request_id}", "COMPLETED_B")

    log_event(
        level="info",
        trace_id=trace_id,
        request_id=request_id,
        state="COMPLETED_B",
        message="processing completed in service-b"
    )

    # persist event to Postgres
    persist_event(
        trace_id,
        request_id,
        "COMPLETED_B",
        "workflow completed in service b"
    )
    
    # publish to next queue
    next_message = {
        "trace_id": trace_id,
        "request_id": request_id,
        "state": "PROCESSING_C",
        "timestamp": datetime.utcnow().isoformat()
    }

    ch.basic_publish(
        exchange="",
        routing_key="workflow_queue_c",
        body=json.dumps(next_message)
    )

    log_event(
        level="info",
        trace_id=trace_id,
        request_id=request_id,
        state="PUBLISHED_TO_C",
        message="event published to workflow_queue_c"
    )

    # persist event to Postgres
    persist_event(
        trace_id,
        request_id,
        "PUBLISHED_TO_C",
        "workflow published to workflow_queue_c"
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

channel.queue_declare(queue="workflow_queue_b")

channel.queue_declare(queue="workflow_queue_c")

channel.queue_declare(queue="workflow_dlq")

ready()   # write initial ready state after full setup

channel.basic_consume(
    queue="workflow_queue_b",
    on_message_callback=callback,
    auto_ack=True
)

# start before consuming
threading.Thread(target=heartbeat_loop, daemon=True).start()

logger.info("service-b waiting for RabbitMQ messages...")

channel.start_consuming()