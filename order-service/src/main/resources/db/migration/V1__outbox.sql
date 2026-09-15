CREATE TABLE outbox_events (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic         VARCHAR(128) NOT NULL,
    message_key   VARCHAR(16)  NOT NULL,
    event_type    VARCHAR(64)  NOT NULL,
    payload       JSONB        NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL,
    published_at  TIMESTAMPTZ,
    attempts      INT          NOT NULL DEFAULT 0,
    last_error    TEXT
);

CREATE INDEX idx_outbox_events_unpublished ON outbox_events (id) WHERE published_at IS NULL;
