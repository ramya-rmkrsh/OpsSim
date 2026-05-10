import logging
import redis
import pika
import json
import time
import random
from datetime import datetime
import psycopg2

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

# incoming queue
channel.queue_declare(queue="workflow_queue")

# outgoing queue
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

    # simulate random failure
    if random.randint(1, 10) > 8:

        r.set(f"workflow:{request_id}", "FAILED_B")

        log_event(
            level="error",
            trace_id=trace_id,
            request_id=request_id,
            state="FAILED_B",
            message="processing failed in service-b"
        )

        # persist event to Postgres
        persist_event(
            trace_id,
            request_id,
            "FAILED_B",
            "workflow failed in service b"
        )

        return

    # completed processing in B
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

    channel.basic_publish(
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
# Start Consumer
# ----------------------------
channel.basic_consume(
    queue="workflow_queue",
    on_message_callback=callback,
    auto_ack=True
)

logger.info("service-b waiting for RabbitMQ messages...")

channel.start_consuming()