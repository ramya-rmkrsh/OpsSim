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
# Create workflow_events table
# ----------------------------
conn = get_db_connection()

cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS workflow_events (
    id SERIAL PRIMARY KEY,
    trace_id TEXT,
    request_id TEXT,
    service_name TEXT,
    state TEXT,
    message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

conn.commit()

cur.close()
conn.close()

print("workflow_events table ready", flush=True)


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

    # update redis state
    r.set(f"workflow:{request_id}", "PROCESSING_C")

    # simulate external processing
    time.sleep(random.randint(2, 5))

    # simulate occasional downstream failure
    if random.randint(1, 10) > 8:

        r.set(f"workflow:{request_id}", "FAILED_C")

        log_event(
            level="error",
            trace_id=trace_id,
            request_id=request_id,
            state="FAILED_C",
            message="external dependency failure in service-c"
        )

        persist_event(
            trace_id,
            request_id,
            "FAILED_C",
            "external dependency failure in service-c"
        )
        return

    # mark completed
    r.set(f"workflow:{request_id}", "COMPLETED")

    log_event(
        level="info",
        trace_id=trace_id,
        request_id=request_id,
        state="COMPLETED",
        message="workflow completed successfully"
    )

    # cleanup redis after completion
    time.sleep(2)

    r.delete(f"workflow:{request_id}")

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