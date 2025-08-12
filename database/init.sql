CREATE TABLE IF NOT EXISTS requests (
    id SERIAL PRIMARY KEY,
    url VARCHAR(500) NOT NULL UNIQUE,
    index_id VARCHAR(50) NOT NULL UNIQUE,
    content TEXT,
    status SMALLINT DEFAULT 0 CHECK (status BETWEEN 0 AND 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON COLUMN requests.status IS '0-waiting 1-inprogress 2-completed';

CREATE INDEX IF NOT EXISTS idx_requests_status ON requests(status);