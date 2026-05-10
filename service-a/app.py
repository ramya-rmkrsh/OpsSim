from fastapi import FastAPI
import logging
import redis
import uuid
import pika
import json
from datetime import datetime

# ----------------------------
# Logging Configuration
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    force=True
)

logger = logging.getLogger("service-a")

app = FastAPI()

# ----------------------------
# Redis Connection
# ----------------------------
r = redis.Redis(
    host="redis",
    port=6379,
    decode_responses=True
)


# ----------------------------
# Structured Logging Helper
# ----------------------------
def log_event(level, trace_id, request_id, state, message):

    log_data = {
        "timestamp": datetime.utcnow().isoformat(),
        "trace_id": trace_id,
        "request_id": request_id,
        "service": "service-a",
        "state": state,
        "message": message
    }

    print(json.dumps(log_data), flush=True)


# ----------------------------
# RabbitMQ Publisher
# ----------------------------
def publish_message(message):

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="rabbitmq")
    )

    channel = connection.channel()

    channel.queue_declare(queue="workflow_queue")

    channel.basic_publish(
        exchange="",
        routing_key="workflow_queue",
        body=json.dumps(message)
    )

    connection.close()


# ----------------------------
# API Endpoint
# ----------------------------
@app.get("/work")
def work():

    request_id = str(uuid.uuid4())
    trace_id = str(uuid.uuid4())

    # workflow started
    r.set(f"workflow:{request_id}", "PROCESSING_A")

    log_event(
        level="info",
        trace_id=trace_id,
        request_id=request_id,
        state="PROCESSING_A",
        message="workflow received"
    )

    # publish event to RabbitMQ
    message = {
        "trace_id": trace_id,
        "request_id": request_id,
        "state": "PROCESSING_B",
        "timestamp": datetime.utcnow().isoformat()
    }

    publish_message(message)

    log_event(
        level="info",
        trace_id=trace_id,
        request_id=request_id,
        state="PUBLISHED_TO_QUEUE",
        message="event published to workflow_queue_b"
    )

    return {
        "trace_id": trace_id,
        "request_id": request_id,
        "status": "QUEUED"
    }