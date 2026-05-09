from fastapi import FastAPI
import redis
import logging
import random
import time

app = FastAPI()

# -----------------------
# Logging setup
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger("service-b")

# -----------------------
# Redis connection
# -----------------------
r = redis.Redis(host="redis", port=6379, decode_responses=True)


# -----------------------
# Health endpoint
# -----------------------
@app.get("/health")
def health():
    logger.info("Health check called")
    return {"status": "ok", "service": "service-b"}


# -----------------------
# Core processing endpoint
# -----------------------
@app.get("/process/{request_id}")
def process(request_id: str):

    logger.info(f"Received request: {request_id}")

    # mark state in Redis
    r.set(f"workflow:{request_id}", "PROCESSING")
    logger.info(f"Redis updated: workflow:{request_id} -> PROCESSING")

    # simulate latency
    delay = random.randint(0, 5)
    logger.info(f"Simulating processing delay: {delay}s")
    time.sleep(delay)

    # simulate failure scenarios
    failure_roll = random.randint(1, 10)

    if failure_roll >= 9:
        r.set(f"workflow:{request_id}", "FAILED")
        logger.error(f"Processing FAILED for {request_id}")

        return {
            "request_id": request_id,
            "status": "FAILED",
            "reason": "downstream processing error"
        }

    elif failure_roll >= 7:
        r.set(f"workflow:{request_id}", "DEGRADED")
        logger.warning(f"Processing DEGRADED for {request_id}")

        return {
            "request_id": request_id,
            "status": "DEGRADED",
            "reason": "slow processing"
        }

    # success path
    r.set(f"workflow:{request_id}", "COMPLETED")
    logger.info(f"Processing COMPLETED for {request_id}")

    return {
        "request_id": request_id,
        "status": "COMPLETED"
    }