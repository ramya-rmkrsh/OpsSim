from fastapi import FastAPI, Request
import redis
import logging
import uuid
import requests
import time

app = FastAPI()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("service-a")

trace_id = str(uuid.uuid4())

r = redis.Redis(host="redis", port=6379, decode_responses=True)


@app.get("/work")
def work():

    request_id = str(uuid.uuid4())

    logger.info(f"[A] START {request_id}")

    # initial state only
    r.set(f"workflow:{request_id}", "PROCESSING_A")

    time.sleep(1)

    try:
        response = requests.get(
        f"http://service-b:8000/process/{request_id}",
        headers={
            "X-Trace-ID": trace_id
            },
            timeout=10
        )

        logger.info(f"[TRACE={trace_id}] Service[A] Service B response: {response.json()}")

    except Exception as e:
        logger.error(f"[TRACE={trace_id}] Service [A] error: {str(e)}")
        r.set(f"workflow:{request_id}", "FAILED")
        return {"status": "FAILED"}

    # IMPORTANT: DO NOT mark completed here anymore
    logger.info(f"[TRACE={trace_id}] Service[A] forwarded workflow {request_id}")

    return {
        "request_id": request_id,
        "status": "IN_PROGRESS"
    }