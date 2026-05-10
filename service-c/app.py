import logging
import redis
import pika
import json
import time
import random
from datetime import datetime

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

    if level == "info":
        logger.info(json.dumps(log_data, flush=True))

    elif level == "error":
        logger.error(json.dumps(log_data, flush=True))


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