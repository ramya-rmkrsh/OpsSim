from fastapi import FastAPI
import time
import random
import logging
from datetime import datetime

app = FastAPI()

#----------------------------
# Health Check Endpoint
#----------------------------
@app.get("/health")
def health():
    return {"status": "ok"}

#----------------------------
# Simulated External API Endpoint
#----------------------------
@app.get("/process")
def process(request_id: str):

    start = time.time()

    # simulate latency
    time.sleep(random.uniform(0.5, 3.0))

    # simulate failure
    if random.randint(1, 10) > 7:
        logging.error(f"External API call failed for request_id: {request_id}")
        return {
            "request_id": request_id,
            "status": "FAILED",
            "error": "external system error"
        }

    duration = time.time() - start
    logging.info(f"External API call succeeded for request_id: {request_id} in {duration:.2f} seconds")
    return {
        "request_id": request_id,
        "status": "SUCCESS",
        "processing_time": duration,
        "timestamp": datetime.utcnow().isoformat()
    }