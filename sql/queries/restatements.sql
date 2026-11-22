-- Periods whose totals changed after they closed, and by how much.
SELECT period_date, currency, late_captured_minor, late_refunded_minor,
       late_captured_minor - late_refunded_minor AS net_restatement_minor
FROM financial_projection
WHERE late_captured_minor > 0 OR late_refunded_minor > 0
ORDER BY period_date, currency;
