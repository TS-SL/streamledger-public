-- Deferred events still waiting, oldest admission first, with what they wait for.
SELECT p.order_id, p.event_id, e.event_type, p.missing, p.payment_id,
       e.partition, e."offset", e.event_time, e.broker_ts, now() - p.since AS waiting_for
FROM pending_dependencies p
JOIN received_events e USING (event_id)
ORDER BY p.since, e.partition, e."offset";
