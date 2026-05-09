from fastapi import FastAPI, Request
import redis
import logging
import requests
import time
import random

app = FastAPI()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("service-b")

r = redis.Redis(host="redis", port=6379, decode_responses=True)


@app.get("/process/{request_id}")
def process(request_id: str, request: Request):

    trace_id = request.headers.get("X-Trace-ID")
    logger.info(f"[TRACE={trace_id}] Service [B] START {request_id}")

    r.set(f"workflow:{request_id}", "PROCESSING_B")

    time.sleep(random.randint(1, 2))

    try:
        response = requests.get(
            f"http://service-c:8000/process/{request_id}",
             headers={
                "X-Trace-ID": trace_id
            },
            timeout=10
        )

        logger.info(f"[TRACE={trace_id}] Service [B] Service C response: {response.json()}")

    except Exception as e:
        logger.error(f"[TRACE={trace_id}] Service [B] error: {str(e)}")
        r.set(f"workflow:{request_id}", "FAILED")
        return {"status": "FAILED"}

    logger.info(f"[TRACE={trace_id}] Service [B] COMPLETED {request_id}")
    return {
        "request_id": request_id,
        "status": "OK"
    }