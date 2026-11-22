-- Cancelled orders that still hold money (expected to be refunded); reported, never hidden.
SELECT order_id, currency, total_minor, captured_minor, refunded_minor,
       captured_minor - refunded_minor AS balance_minor
FROM orders
WHERE status = 'CANCELLED' AND captured_minor <> refunded_minor
ORDER BY balance_minor DESC;
