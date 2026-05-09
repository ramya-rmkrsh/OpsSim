from fastapi import FastAPI
import logging
import redis
import uuid
import requests
import time

# -----------------------
# Logging setup
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger("service-a")

app = FastAPI()

# Redis connection
r = redis.Redis(host="redis", port=6379, decode_responses=True)


@app.get("/work")
def work():

    request_id = str(uuid.uuid4())

    logger.info(f"[START] request_id={request_id}")

    # 1. initial state
    r.set(f"workflow:{request_id}", "RECEIVED")
    logger.info(f"Redis state -> RECEIVED ({request_id})")

    # 2. simulate small processing delay
    time.sleep(1)

    # 3. call service-b (dependency)
    try:
        logger.info(f"Calling service-b for request_id={request_id}")

        response = requests.get(
            f"http://service-b:8000/process/{request_id}",
            timeout=10
        )

        logger.info(
            f"service-b response: request_id={request_id}, status={response.json()}"
        )

    except Exception as e:
        logger.error(f"service-b call failed: {str(e)}")

        r.set(f"workflow:{request_id}", "FAILED")

        return {
            "request_id": request_id,
            "status": "FAILED",
            "reason": "service-b unreachable"
        }

    # 4. final state update based on service-b response
    r.set(f"workflow:{request_id}", "DELEGATED_TO_B")

    logger.info(f"[END] request_id={request_id}")

    return {
        "request_id": request_id,
        "status": "delegated",
        "service_b_result": response.json()
    }