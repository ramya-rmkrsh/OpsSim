import logging
import redis
import pika
import json
import time
import random
from datetime import datetime
import psycopg2
import threading

from prometheus_client import Histogram, start_http_server

from opentelemetry import trace
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource

from opentelemetry.propagate import inject, extract
from opentelemetry.trace import SpanKind

# ----------------------------
# Auto instrumentation
# ----------------------------
RedisInstrumentor().instrument()
Psycopg2Instrumentor().instrument()
RequestsInstrumentor().instrument()

# ----------------------------
# OpenTelemetry Setup
# ----------------------------
resource = Resource.create({"service.name": "service-b"})

provider = TracerProvider(resource=resource)
provider.add_span_processor(
    BatchSpanProcessor(
        OTLPSpanExporter(
            endpoint="http://otel-collector:4317",
            insecure=True
        )
    )
)

trace.set_tracer_provider(provider)
tracer = trace.get_tracer("service-b")

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
# Service Status
# ----------------------------
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
# Redis
# ----------------------------
r = redis.Redis(host="redis", port=6379, decode_responses=True)

def check_redis():
    try:
        r.ping()
        return True
    except:
        return False

# ----------------------------
# Postgres
# ----------------------------
def get_db_connection():
    return psycopg2.connect(
        host="postgres",
        database="opssim",
        user="opsuser",
        password="opspassword"
    )

def check_postgres():
    try:
        conn = get_db_connection()
        conn.close()
        return True
    except:
        return False

# ----------------------------
# RabbitMQ
# ----------------------------
rmq_connection = None

def check_rabbitmq():
    try:
        return rmq_connection is not None and rmq_connection.is_open
    except:
        return False

# ----------------------------
# Ready check
# ----------------------------
def ready():
    SERVICE_STATUS["dependencies"]["redis"] = check_redis()
    SERVICE_STATUS["dependencies"]["postgres"] = check_postgres()
    SERVICE_STATUS["dependencies"]["rabbitmq"] = check_rabbitmq()

    SERVICE_STATUS["ready"] = all(SERVICE_STATUS["dependencies"].values())

    r.set("service-b:ready", json.dumps(SERVICE_STATUS), ex=60)
    return SERVICE_STATUS

# ----------------------------
# Heartbeat
# ----------------------------
def heartbeat_loop():
    while True:
        try:
            ready()
        except Exception as e:
            logger.error(f"Heartbeat error: {e}")
        time.sleep(10)

# ----------------------------
# Logging helper
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
# Persist event
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
        ) VALUES (%s, %s, %s, %s, %s)
    """, (trace_id, request_id, "service-b", state, message))

    conn.commit()
    cur.close()
    conn.close()

# ----------------------------
# Retry
# ----------------------------
MAX_RETRIES = 3
RETRY_KEY_PREFIX = "retry:"

def get_retry_count(request_id):
    return int(r.get(f"{RETRY_KEY_PREFIX}{request_id}") or 0)

def increment_retry(request_id):
    count = get_retry_count(request_id) + 1
    r.set(f"{RETRY_KEY_PREFIX}{request_id}", count, ex=3600)
    return count

# ----------------------------
# DLQ
# ----------------------------
def send_to_dlq(channel, message):
    headers = {}
    inject(headers)

    channel.basic_publish(
        exchange="",
        routing_key="workflow_dlq",
        body=json.dumps(message),
        properties=pika.BasicProperties(headers=headers)
    )

# ----------------------------
# Metrics
# ----------------------------
workflow_latency = Histogram(
    "workflow_duration_seconds",
    "End to end workflow duration",
    buckets=[0.1, 0.5, 1, 2, 5, 10]
)

# ----------------------------
# Callback (FIXED TRACING)
# ----------------------------
def callback(ch, method, properties, body):

    message = json.loads(body)
    request_id = message["request_id"]

    # ✅ FIX: propagate context from RabbitMQ
    context = extract(properties.headers if properties else {})

    with workflow_latency.time():

        with tracer.start_as_current_span(
            "rmq.consume",
            context=context,
            kind=SpanKind.CONSUMER
        ) as span:

            span.set_attribute("queue", "workflow_queue_b")
            span.set_attribute("request_id", request_id)

            trace_id = format(span.get_span_context().trace_id, "032x")

            log_event("info", trace_id, request_id,
                      "PROCESSING_B", "message received")

            # ---------------- DB SPAN ----------------
            with tracer.start_as_current_span("db.persist"):
                persist_event(trace_id, request_id,
                              "PROCESSING_B",
                              "stored event in service-b")

            # ---------------- REDIS SPAN ----------------
            with tracer.start_as_current_span("redis.set"):
                r.set(f"workflow:{request_id}", "PROCESSING_B", ex=3600)

            time.sleep(random.randint(1, 3))

            # ---------------- FAILURE SIMULATION ----------------
            should_fail = random.randint(1, 10) > 8

            if should_fail:

                retry_count = increment_retry(request_id)

                state = "FAILED_B"

                log_event("error", trace_id, request_id, state,
                          f"retry {retry_count}/{MAX_RETRIES}")

                persist_event(trace_id, request_id, state,
                              "failure in service-b")

                if retry_count <= MAX_RETRIES:

                    headers = properties.headers.copy() if properties and properties.headers else {}
                    inject(headers)

                    ch.basic_publish(
                        exchange="",
                        routing_key="workflow_queue_b",
                        body=json.dumps(message),
                        properties=pika.BasicProperties(headers=headers)
                    )

                else:
                    send_to_dlq(ch, message)

                return

            # ---------------- SUCCESS PATH ----------------
            r.set(f"workflow:{request_id}", "COMPLETED_B", ex=3600)

            log_event("info", trace_id, request_id,
                      "COMPLETED_B", "completed in service-b")

            persist_event(trace_id, request_id,
                          "COMPLETED_B",
                          "success in service-b")

            # ---------------- SEND TO C ----------------
            next_message = {
                "trace_id": trace_id,
                "request_id": request_id,
                "state": "PROCESSING_C",
                "timestamp": datetime.utcnow().isoformat()
            }

            headers = properties.headers.copy() if properties and properties.headers else {}
            inject(headers)

            ch.basic_publish(
                exchange="",
                routing_key="workflow_queue_c",
                body=json.dumps(next_message),
                properties=pika.BasicProperties(headers=headers)
            )

            log_event("info", trace_id, request_id,
                      "PUBLISHED_TO_C", "sent to service-c")

# ----------------------------
# RabbitMQ connection
# ----------------------------
def connect_rabbitmq():
    global rmq_connection
    while True:
        try:
            logger.info("Connecting to RabbitMQ...")
            rmq_connection = pika.BlockingConnection(
                pika.ConnectionParameters(host="rabbitmq")
            )
            logger.info("Connected to RabbitMQ")
            SERVICE_STATUS["dependencies"]["rabbitmq"] = True
            return rmq_connection
        except:
            SERVICE_STATUS["dependencies"]["rabbitmq"] = False
            time.sleep(5)

# ----------------------------
# Startup
# ----------------------------
start_http_server(8001)

connection = connect_rabbitmq()
channel = connection.channel()

channel.queue_declare(queue="workflow_queue_b")
channel.queue_declare(queue="workflow_queue_c")
channel.queue_declare(queue="workflow_dlq")

ready()

channel.basic_consume(
    queue="workflow_queue_b",
    on_message_callback=callback,
    auto_ack=True
)

threading.Thread(target=heartbeat_loop, daemon=True).start()

logger.info("service-b waiting for messages...")
channel.start_consuming()