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
def log_event(level, component, operation, trace_id, request_id, state, message):
    log_data = {
        "level": level,
        "timestamp": datetime.utcnow().isoformat(),
        "service": "service-b",
        "component": component,
        "operation": operation,
        "state": state,
        "message": message,
        "trace_id": trace_id,
        "request_id": request_id
    }
    print(json.dumps(log_data), flush=True)

#----------------------------
# RabbitMQ Publish with Tracing
#----------------------------

def publish_message(channel, routing_key, message, trace_id, request_id, existing_headers=None, state=None, log_message=None):
    with tracer.start_as_current_span(
        "rmq.publish",
        kind=SpanKind.PRODUCER
    ) as span:
        span.set_attribute("messaging.system", "rabbitmq")
        span.set_attribute("messaging.destination", routing_key)
        span.set_attribute("messaging.operation", "publish")
        span.set_attribute("request_id", request_id)

        headers = existing_headers.copy() if existing_headers else {}
        inject(headers)  # inject AFTER span is active so it propagates the publish span, not the consume span

        channel.basic_publish(
            exchange="",
            routing_key=routing_key,
            body=json.dumps(message),
            properties=pika.BasicProperties(headers=headers)
        )

        log_event(
            "info",
            "rmq", 
            "publish", 
            trace_id, 
            request_id,
            state,
            log_message
        )

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
        )
        VALUES (%s, %s, %s, %s, %s)
    """, (
        trace_id,
        request_id,
        "service-b",
        state,
        message
    ))

    log_event(
        "info",
        "db",
        "insert",
        trace_id, 
        request_id, 
        state,
        message
        ) 
   
    conn.commit()
    cur.close()
    conn.close()

# ----------------------------
# Retry
# ----------------------------
MAX_RETRIES = 5
RETRY_KEY_PREFIX = "retry:service-b:"

def get_retry_count(request_id):
    return int(r.get(f"{RETRY_KEY_PREFIX}{request_id}") or 0)

def increment_retry(request_id):
    count = get_retry_count(request_id) + 1
    r.set(f"{RETRY_KEY_PREFIX}{request_id}", count, ex=3600)
    return count

# ----------------------------
# DLQ
# ----------------------------
def send_to_dlq(channel, message, trace_id, request_id, existing_headers):

    persist_event(
        trace_id,
        request_id,
        "DLQ",
        "Message sent to Service-B DLQ"
    )

    publish_message(
        channel,
        "workflow_dlq", 
        message, 
        trace_id, 
        request_id,
        existing_headers,
        "DLQ"
        "Message sent to DLQ"
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

            # ---------------- REDIS SPAN ----------------
            with tracer.start_as_current_span("redis.set"):
                r.set(f"workflow:{request_id}", "PROCESSING_B", ex=3600)
                log_event(
                    "info",
                    "redis",
                    "set",
                    trace_id, 
                    request_id, 
                    "PROCESSING_B",
                    "updated state in redis to PROCESSING_B"
                )
            time.sleep(random.randint(1, 3))

            # ---------------- FAILURE SIMULATION ----------------
            should_fail = random.randint(1, 10) > 6

            if should_fail:
                log_event(
                    "info",
                    "service-b",
                    "processing",
                    trace_id,
                    request_id,
                    f"should_fail={should_fail}",
                    "should_fail value > 6, simulating failure in service-b"
                )

                retry_count = increment_retry(request_id)

                state = "FAILED_B"

                with tracer.start_as_current_span("redis.set"):
                    r.set(f"workflow:{request_id}", state, ex=3600)
                    log_event(
                        "info",
                        "redis",
                        "set",
                        trace_id, 
                        request_id, 
                        state,
                        f"retry {retry_count}/{MAX_RETRIES}"
                    )

                """ with tracer.start_as_current_span("db.persist"):
                    persist_event(
                        trace_id, 
                        request_id, 
                        state,
                        f"retry {retry_count}/{MAX_RETRIES}"
                    ) """

                if retry_count <= MAX_RETRIES:

                    publish_message(
                        ch,
                        "workflow_queue_b",
                        message,
                        trace_id,
                        request_id,
                        properties.headers,
                        "FAILED_B",
                        f"Failed in service-b, retrying {retry_count}/{MAX_RETRIES}"
                    )

                    r.expire(f"workflow:{request_id}", 3600)

                else:
                    send_to_dlq(ch, message, trace_id, request_id, properties.headers)
                return

            # ---------------- SUCCESS PATH ----------------
            with tracer.start_as_current_span("redis.set"):
                r.set(f"workflow:{request_id}", "COMPLETED_B", ex=3600)
                log_event(
                    "info",
                    "redis",
                    "set",
                    trace_id, 
                    request_id,
                    "COMPLETED_B", 
                    "completed in service-b"
                )

            with tracer.start_as_current_span("db.persist"):
                persist_event(
                    trace_id, 
                    request_id,
                    "COMPLETED_B",
                    "success in service-b"
                )

            # ---------------- SEND TO C ----------------
            next_message = {
                "trace_id": trace_id,
                "request_id": request_id,
                "state": "PROCESSING_C",
                "timestamp": datetime.utcnow().isoformat()
            }

            publish_message(
                ch, 
                "workflow_queue_c", 
                next_message, 
                trace_id, 
                request_id, 
                properties.headers,
                "PROCESSING_C",
                "forwarded to service-c"
            )

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