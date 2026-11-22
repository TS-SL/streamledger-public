-- Every log position must leave durable evidence, including identical redeliveries.
-- Progress is a property of the database's projection, not of one consumer group:
-- a new group replaying the topic must hit the same fence.

ALTER TABLE consumer_progress DROP CONSTRAINT consumer_progress_pkey;
ALTER TABLE consumer_progress RENAME COLUMN consumer_group TO last_owner_group;
ALTER TABLE consumer_progress ADD PRIMARY KEY (topic, partition);

-- Identical redeliveries (same event_id, same digest) at a new log position.
CREATE TABLE duplicate_deliveries (
    topic       text    NOT NULL,
    partition   integer NOT NULL,
    "offset"    bigint  NOT NULL,
    event_id    text    NOT NULL REFERENCES received_events (event_id),
    broker_ts   timestamptz NOT NULL,
    detected_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, partition, "offset")
);
CREATE INDEX duplicate_deliveries_event_idx ON duplicate_deliveries (event_id);

-- One row per processed log position, whichever table holds it.
-- Reconciliation checks that each position appears exactly once and that the
-- count per partition equals consumer_progress.next_offset.
CREATE VIEW position_ledger AS
    SELECT topic, partition, "offset", event_id, status, reason FROM received_events
    UNION ALL
    SELECT topic, partition, "offset", event_id, 'DUPLICATE', NULL FROM duplicate_deliveries
    UNION ALL
    SELECT topic, partition, "offset", event_id, 'CONFLICT', 'EVENT_ID_CONFLICT' FROM event_conflicts
    UNION ALL
    SELECT topic, partition, "offset", event_id, 'INVALID', reason FROM processing_errors;
