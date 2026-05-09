from fastapi import FastAPI, Request
import redis
import logging
import time
import random

app = FastAPI()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("service-c")

r = redis.Redis(host="redis", port=6379, decode_responses=True)


@app.get("/process/{request_id}")
def process(request_id: str, request: Request):

    trace_id = request.headers.get("X-Trace-ID")

    logger.info(f"[TRACE={trace_id}] Service [C] START {request_id}")

    r.set(f"workflow:{request_id}", "PROCESSING_C")

    time.sleep(random.randint(1, 3))

    # simulate failure
    if random.randint(1, 10) > 8:
        r.set(f"workflow:{request_id}", "FAILED")
        logger.error(f"[TRACE={trace_id}] Service [C] FAILED {request_id}")
        return {"status": "FAILED"}

    # FINAL STATE
    r.set(f"workflow:{request_id}", "COMPLETED")
    logger.info(f"[TRACE={trace_id}] Service [C] COMPLETED {request_id}")

    # CLEANUP Redis (important design decision)
    time.sleep(1)

    r.delete(f"workflow:{request_id}")
    logger.info(f"[TRACE={trace_id}] Service [C] CLEANUP DONE {request_id}")

    return {
        "request_id": request_id,
        "status": "COMPLETED"
    }