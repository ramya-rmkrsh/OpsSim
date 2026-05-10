CREATE TABLE IF NOT EXISTS workflow_events (

    id SERIAL PRIMARY KEY,

    trace_id TEXT,
    request_id TEXT,

    service_name TEXT,

    state TEXT,

    message TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS external_transactions (

    id SERIAL PRIMARY KEY,

    request_id TEXT,

    external_status TEXT,

    response_code INTEGER,

    processing_time FLOAT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);