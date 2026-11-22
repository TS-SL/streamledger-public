-- Money per event-time day and currency; late_* = amounts that arrived after the day closed (+1h grace).
SELECT period_date, currency, captured_minor, refunded_minor,
       captured_minor - refunded_minor AS net_minor,
       late_captured_minor, late_refunded_minor
FROM financial_projection
ORDER BY period_date, currency;
