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
# OpenTelemetry Setup (Tempo via Collector)
# ----------------------------
resource = Resource.create({"service.name": "service-c"})

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
tracer = trace.get_tracer("service-c")

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
# Service Status
# ----------------------------
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
# External API
# ----------------------------
def check_external_api():
    try:
        res = requests.get(
            "http://external-api:8003/health", 
            timeout=2
        )
        return res.status_code == 200
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
    SERVICE_STATUS["dependencies"]["external_api"] = check_external_api()

    SERVICE_STATUS["ready"] = all(SERVICE_STATUS["dependencies"].values())
    r.set("service-c:ready", json.dumps(SERVICE_STATUS), ex=60)
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
# Retry logic
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
# Logging helper
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
# DB persist
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
    """, (trace_id, request_id, "service-c", state, message))

    conn.commit()
    cur.close()
    conn.close()

# ----------------------------
# DLQ
# ----------------------------
def send_to_dlq(ch, message):
    headers = {}
    inject(headers)

    ch.basic_publish(
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
# Consumer Callback (FIXED TRACING)
# ----------------------------
def callback(ch, method, properties, body):

    message = json.loads(body)
    request_id = message["request_id"]

    # ✅ FIX: extract parent context from RabbitMQ headers
    context = extract(properties.headers if properties else {})

    with workflow_latency.time():

        with tracer.start_as_current_span(
            "rmq.consume",
            context=context,
            kind=SpanKind.CONSUMER
        ) as span:

            span.set_attribute("queue", "workflow_queue_c")
            span.set_attribute("request_id", request_id)

            trace_id = format(span.get_span_context().trace_id, "032x")

            log_event("info", trace_id, request_id, "PROCESSING_C",
                      "message consumed from workflow_queue_c")

            # ---------------- DB SPAN ----------------
            with tracer.start_as_current_span("db.persist"):
                persist_event(trace_id, request_id,
                              "PROCESSING_C",
                              "workflow consumed by service c")

            # ---------------- REDIS SPAN ----------------
            with tracer.start_as_current_span("redis.set"):
                r.set(f"workflow:{request_id}", "PROCESSING_C", ex=3600)

            time.sleep(random.randint(1, 3))

            # ---------------- FAILURE SIMULATION ----------------
            should_fail = random.randint(1, 10) > 8

            if should_fail:
                retry_count = increment_retry(request_id)

                state = "FAILED_C"
                log_event("error", trace_id, request_id, state,
                          f"retry {retry_count}/{MAX_RETRIES}")

                persist_event(trace_id, request_id, state,
                              f"failed retry {retry_count}")

                if retry_count <= MAX_RETRIES:

                    headers = properties.headers.copy() if properties and properties.headers else {}
                    inject(headers)

                    ch.basic_publish(
                        exchange="",
                        routing_key="workflow_queue_c",
                        body=json.dumps(message),
                        properties=pika.BasicProperties(headers=headers)
                    )

                else:
                    send_to_dlq(ch, message)
                    log_event("error", trace_id, request_id,
                              "DLQ_C", "max retries exceeded")

                return

            # ---------------- EXTERNAL API ----------------
            try:
                response = requests.get(
                    "http://external-api:8003/process",
                    params={"request_id": request_id},
                    timeout=2
                )

                if response.status_code == 200 and response.json().get("status") == "SUCCESS":
                    state = "COMPLETED_C"
                    msg = "external API success"
                    level = "info"
                else:
                    state = "FAILED_C"
                    msg = "external API failed"
                    level = "error"

            except Exception as e:
                state = "ERRORED_C"
                msg = str(e)
                level = "error"

            # ---------------- FINAL UPDATE ----------------
            r.set(f"workflow:{request_id}", state, ex=3600)

            log_event(level, trace_id, request_id, state, msg)

            persist_event(trace_id, request_id, state, msg)

            r.expire(f"workflow:{request_id}", 3600)

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
            logger.info("RabbitMQ connected")
            SERVICE_STATUS["dependencies"]["rabbitmq"] = True
            return rmq_connection
        except:
            SERVICE_STATUS["dependencies"]["rabbitmq"] = False
            time.sleep(5)

# ----------------------------
# Start system
# ----------------------------
start_http_server(8001)

connection = connect_rabbitmq()
channel = connection.channel()

channel.queue_declare(queue="workflow_queue_c")
channel.queue_declare(queue="workflow_dlq")

ready()

channel.basic_consume(
    queue="workflow_queue_c",
    on_message_callback=callback,
    auto_ack=True
)

threading.Thread(target=heartbeat_loop, daemon=True).start()

logger.info("service-c waiting for messages...")
channel.start_consuming()