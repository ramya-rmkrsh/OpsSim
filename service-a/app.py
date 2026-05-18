from fastapi import FastAPI
import logging
import redis
import uuid
import pika
import json
import psycopg2
import time

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

            return rmq_connection
        
        except pika.exceptions.AMQPConnectionError:

            logger.error("RabbitMQ not ready. Retrying in 5 seconds...")
            
            time.sleep(5)


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
        "service-a",
        state,
        message
    ))

    conn.commit()

    cur.close()
    conn.close()

# ----------------------------
# Health Check Endpoint
# ----------------------------
@app.get("/health")
def health():
    return {"status": "healthy"}

# ----------------------------
# Readiness Check Endpoint
# ----------------------------
@app.get("/ready")
def ready():

    dependencies = {
        "redis": False,
        "rabbitmq": False,
        "postgres": False
    }

    # Redis check
    try:
        r.ping()
        dependencies["redis"] = True
    except:
        pass

    # RabbitMQ check
    try:
        #connection = pika.BlockingConnection(
        #    pika.ConnectionParameters(host="rabbitmq")
        #)
        connection=connect_rabbitmq()
        dependencies["rabbitmq"] = True
        connection.close()
    except:
        pass

    # Postgres check
    try:
        conn = get_db_connection()
        conn.close()
        dependencies["postgres"] = True

    except:
        return False
      
    ready = all(dependencies.values())

    return {
        "ready": ready,
        "dependencies": dependencies
    }

#----------------------------
# System Status Endpoint
#----------------------------
@app.get("/system/status")
def system_status():
    return {
        "service-a": ready(),
        "service-b": json.loads(r.get("service-b:ready") or "{}"),
        "service-c": json.loads(r.get("service-c:ready") or "{}")
    }
    
# ----------------------------
# Status endpoint (Redis)
# ----------------------------
@app.get("/status/{request_id}")
def get_status(request_id: str):

    state = r.get(f"workflow:{request_id}")

    if state is None:
        state = "UNKNOWN"

    return {
        "request_id": request_id,
        "status": state
    }

# ----------------------------
# Workflow history endpoint (Postgres)
# ----------------------------
@app.get("/workflow/{request_id}")
def workflow_history(request_id: str):

    conn = get_db_connection()

    cur = conn.cursor()

    cur.execute("""
        SELECT
            trace_id,
            request_id,
            service_name,
            state,
            message,
            created_at
        FROM workflow_events
        WHERE request_id = %s
        ORDER BY created_at ASC
    """, (request_id,))

    rows = cur.fetchall()

    cur.close()
    conn.close()

    events = []

    for row in rows:

        events.append({
            "trace_id": row[0],
            "request_id": row[1],
            "service": row[2],
            "state": row[3],
            "message": row[4],
            "created_at": str(row[5])
        })

    return {
        "request_id": request_id,
        "workflow_history": events
    }

# ----------------------------
# Final Result Endpoint
# ----------------------------
FINAL_STATES = [
    "FAILED_B",
    "FAILED_C",
    "ERRORED_C",
    "COMPLETED_C"
]

@app.get("/result/{request_id}")
def get_result(request_id: str):

    state = r.get(f"workflow:{request_id}")

    if state is None:
        state = "UNKNOWN"

    return {
        "request_id": request_id,
        "current_state": state,
        "workflow_completed": state in FINAL_STATES
    }

# ----------------------------
# Workflow entry point
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

    persist_event(
        trace_id,
        request_id,
        "PROCESSING_A",
        "workflow received"
    )

    # publish event to RabbitMQ
    message = {
        "trace_id": trace_id,
        "request_id": request_id,
        "state": "PROCESSING_B",
        "timestamp": datetime.utcnow().isoformat()
    }

    connection = connect_rabbitmq()
    channel = connection.channel()
    channel.queue_declare(queue="workflow_queue_b")

    channel.basic_publish(
        exchange="",
        routing_key="workflow_queue_b",
        body=json.dumps(message)
    )

    log_event(
        level="info",
        trace_id=trace_id,
        request_id=request_id,
        state="PUBLISHED_TO_B",
        message="event published to workflow_queue_b"
    )

    persist_event(
        trace_id,
        request_id,
        "PUBLISHED_TO_B",
        "event published to workflow_queue_b"
    )

    connection.close() # ensure rmq connection is closed after publishing

    return {
        "trace_id": trace_id,
        "request_id": request_id,
        "message": "workflow started",
        "next_steps": {
            "status": f"/status/{request_id}",
            "workflow_history": f"/workflow/{request_id}",
            "final_result": f"/result/{request_id}"
        }
    }