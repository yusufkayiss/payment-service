CREATE TABLE payments (
                          id BIGSERIAL PRIMARY KEY,
                          order_id BIGINT NOT NULL UNIQUE,
                          user_email VARCHAR(255) NOT NULL,
                          amount NUMERIC(19, 2) NOT NULL,
                          status VARCHAR(30) NOT NULL,
                          provider_reference VARCHAR(100),
                          failure_reason VARCHAR(500),
                          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_payments_status ON payments(status);

CREATE TABLE outbox_events (
                               id BIGSERIAL PRIMARY KEY,
                               aggregate_type VARCHAR(100) NOT NULL,
                               aggregate_id VARCHAR(100) NOT NULL,
                               event_type VARCHAR(100) NOT NULL,
                               exchange_name VARCHAR(150) NOT NULL,
                               routing_key VARCHAR(150) NOT NULL,
                               payload TEXT NOT NULL,
                               trace_context TEXT,
                               status VARCHAR(30) NOT NULL,
                               retry_count INT NOT NULL DEFAULT 0,
                               last_error TEXT,
                               created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                               published_at TIMESTAMP
);

CREATE INDEX idx_outbox_events_status_created_at ON outbox_events(status, created_at);