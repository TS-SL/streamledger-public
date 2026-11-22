-- Outcome counts per topic: the operational overview.
SELECT topic, status, reason, count(*) AS positions
FROM position_ledger
GROUP BY topic, status, reason
ORDER BY topic, positions DESC;
