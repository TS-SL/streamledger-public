-- StreamLedger core schema. Money is BIGINT minor units everywhere.
-- Invariants live in the database, not only in Python:
--   * row-level CHECKs for amounts, statuses and per-row bounds
--   * deferred constraint triggers for cross-row sums (checked at COMMIT)
--   * a composite deferred FK tying pending_dependencies to PENDING events

-- Positional progress per partition; authoritative over Kafka committed offsets.
CREATE TABLE consumer_progress (
    consumer_group text    NOT NULL,
    topic          text    NOT NULL,
    partition      integer NOT NULL CHECK (partition >= 0),
    next_offset    bigint  NOT NULL CHECK (next_offset >= 0),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, topic, partition)
);

-- One row per distinct event_id: the first occurrence in admission order.
CREATE TABLE received_events (
    event_id     text        PRIMARY KEY CHECK (event_id ~ '^[A-Za-z0-9_-]{1,64}$'),
    payload      bytea       NOT NULL,                 -- canonical JSON bytes
    digest       char(64)    NOT NULL,
    event_type   text        NOT NULL CHECK (event_type IN
                     ('ORDER_CREATED','PAYMENT_CAPTURED','PAYMENT_FAILED','REFUND_ISSUED','ORDER_CANCELLED')),
    order_id     text        NOT NULL,
    payment_id   text,
    refund_id    text,
    currency     char(3)     NOT NULL CHECK (currency IN ('USD','EUR','GBP')),
    amount_minor bigint      NOT NULL CHECK (amount_minor BETWEEN 0 AND 1000000000000),
    event_time   timestamptz NOT NULL,
    topic        text        NOT NULL,
    partition    integer     NOT NULL,
    "offset"     bigint      NOT NULL,
    broker_ts    timestamptz NOT NULL,
    status       text        NOT NULL CHECK (status IN ('APPLIED','PENDING','REJECTED')),
    reason       text,
    received_at  timestamptz NOT NULL DEFAULT now(),
    resolved_at  timestamptz,
    UNIQUE (topic, partition, "offset"),
    UNIQUE (event_id, status),                         -- target of the pending FK below
    CONSTRAINT digest_matches_payload CHECK (digest = encode(sha256(payload), 'hex')),
    CONSTRAINT reason_iff_not_applied CHECK ((status = 'APPLIED') = (reason IS NULL)),
    CONSTRAINT pending_is_unresolved CHECK (status <> 'PENDING' OR resolved_at IS NULL)
);
CREATE INDEX received_events_order_idx ON received_events (order_id);

-- Later deliveries that reused an event_id with a different payload. Never applied.
CREATE TABLE event_conflicts (
    topic       text     NOT NULL,
    partition   integer  NOT NULL,
    "offset"    bigint   NOT NULL,
    event_id    text     NOT NULL REFERENCES received_events (event_id),
    payload     bytea    NOT NULL,
    digest      char(64) NOT NULL,
    broker_ts   timestamptz NOT NULL,
    detected_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, partition, "offset"),
    CHECK (digest = encode(sha256(payload), 'hex'))
);

-- Records that could not be interpreted at all (schema/validation failures).
CREATE TABLE processing_errors (
    topic       text     NOT NULL,
    partition   integer  NOT NULL,
    "offset"    bigint   NOT NULL,
    broker_ts   timestamptz NOT NULL,
    reason      text     NOT NULL,
    detail      text,
    event_id    text,                                  -- if extractable; diagnostics only
    raw_key     bytea,
    raw_value   bytea,
    detected_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, partition, "offset")
);

CREATE TABLE orders (
    order_id           text    PRIMARY KEY,
    currency           char(3) NOT NULL CHECK (currency IN ('USD','EUR','GBP')),
    total_minor        bigint  NOT NULL CHECK (total_minor > 0),
    status             text    NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN','CANCELLED')),
    captured_minor     bigint  NOT NULL DEFAULT 0 CHECK (captured_minor >= 0),
    refunded_minor     bigint  NOT NULL DEFAULT 0 CHECK (refunded_minor >= 0),
    created_event_id   text    NOT NULL UNIQUE REFERENCES received_events (event_id),
    cancelled_event_id text    UNIQUE REFERENCES received_events (event_id),
    CONSTRAINT no_overcapture CHECK (captured_minor <= total_minor),
    CONSTRAINT no_net_negative CHECK (refunded_minor <= captured_minor),
    CONSTRAINT cancel_has_event CHECK ((status = 'CANCELLED') = (cancelled_event_id IS NOT NULL))
);

CREATE TABLE payments (
    order_id       text    NOT NULL REFERENCES orders (order_id),
    payment_id     text    NOT NULL,
    status         text    NOT NULL CHECK (status IN ('CAPTURED','FAILED')),
    amount_minor   bigint  NOT NULL CHECK (amount_minor > 0),
    refunded_minor bigint  NOT NULL DEFAULT 0 CHECK (refunded_minor >= 0),
    event_id       text    NOT NULL UNIQUE REFERENCES received_events (event_id),
    PRIMARY KEY (order_id, payment_id),
    CONSTRAINT no_over_refund CHECK (refunded_minor <= CASE WHEN status = 'CAPTURED' THEN amount_minor ELSE 0 END)
);

CREATE TABLE refunds (
    order_id     text   NOT NULL,
    refund_id    text   NOT NULL,
    payment_id   text   NOT NULL,
    amount_minor bigint NOT NULL CHECK (amount_minor > 0),
    event_id     text   NOT NULL UNIQUE REFERENCES received_events (event_id),
    PRIMARY KEY (order_id, refund_id),
    FOREIGN KEY (order_id, payment_id) REFERENCES payments (order_id, payment_id)
);
CREATE INDEX refunds_payment_idx ON refunds (order_id, payment_id);

-- Money by event-time period (UTC date). late_* = subset that reached the broker
-- after period end + grace (a restatement of an already-closed period).
CREATE TABLE financial_projection (
    period_date         date    NOT NULL,
    currency            char(3) NOT NULL CHECK (currency IN ('USD','EUR','GBP')),
    captured_minor      bigint  NOT NULL DEFAULT 0 CHECK (captured_minor >= 0),
    refunded_minor      bigint  NOT NULL DEFAULT 0 CHECK (refunded_minor >= 0),
    late_captured_minor bigint  NOT NULL DEFAULT 0 CHECK (late_captured_minor BETWEEN 0 AND captured_minor),
    late_refunded_minor bigint  NOT NULL DEFAULT 0 CHECK (late_refunded_minor BETWEEN 0 AND refunded_minor),
    PRIMARY KEY (period_date, currency)
);

CREATE TABLE pending_dependencies (
    event_id         text    PRIMARY KEY,
    status           text    NOT NULL DEFAULT 'PENDING' CHECK (status = 'PENDING'),
    order_id         text    NOT NULL,
    missing          text    NOT NULL CHECK (missing IN ('MISSING_ORDER','MISSING_PAYMENT')),
    payment_id       text,
    admission_offset bigint  NOT NULL,
    since            timestamptz NOT NULL DEFAULT now(),
    CHECK ((missing = 'MISSING_PAYMENT') = (payment_id IS NOT NULL)),
    -- the referenced event must be PENDING; deferred so a release can update the
    -- event status and delete this row in one transaction
    FOREIGN KEY (event_id, status) REFERENCES received_events (event_id, status)
        ON UPDATE NO ACTION DEFERRABLE INITIALLY DEFERRED
);
CREATE INDEX pending_by_order ON pending_dependencies (order_id, admission_offset);

-- Cross-row invariant: denormalized totals equal the sums of their detail rows.
-- Checked once per affected order at COMMIT, so intermediate states are allowed.
CREATE FUNCTION check_order_totals() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    oid text := COALESCE(NEW.order_id, OLD.order_id);
    o   orders%ROWTYPE;
    cap bigint;
    ref bigint;
    bad_payment text;
BEGIN
    SELECT * INTO o FROM orders WHERE order_id = oid;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    SELECT COALESCE(sum(amount_minor) FILTER (WHERE status = 'CAPTURED'), 0) INTO cap
      FROM payments WHERE order_id = oid;
    SELECT COALESCE(sum(amount_minor), 0) INTO ref FROM refunds WHERE order_id = oid;
    IF o.captured_minor <> cap OR o.refunded_minor <> ref THEN
        RAISE EXCEPTION 'order % totals (captured %, refunded %) disagree with detail rows (%, %)',
            oid, o.captured_minor, o.refunded_minor, cap, ref USING ERRCODE = 'check_violation';
    END IF;
    SELECT p.payment_id INTO bad_payment
      FROM payments p
      LEFT JOIN refunds r ON r.order_id = p.order_id AND r.payment_id = p.payment_id
     WHERE p.order_id = oid
     GROUP BY p.payment_id, p.refunded_minor
    HAVING p.refunded_minor <> COALESCE(sum(r.amount_minor), 0)
     LIMIT 1;
    IF bad_payment IS NOT NULL THEN
        RAISE EXCEPTION 'payment %/% refunded_minor disagrees with refunds', oid, bad_payment
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END $$;

CREATE CONSTRAINT TRIGGER orders_totals AFTER INSERT OR UPDATE ON orders
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_order_totals();
CREATE CONSTRAINT TRIGGER payments_totals AFTER INSERT OR UPDATE OR DELETE ON payments
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_order_totals();
CREATE CONSTRAINT TRIGGER refunds_totals AFTER INSERT OR UPDATE OR DELETE ON refunds
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_order_totals();
