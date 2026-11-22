-- Every processed log position must appear exactly once, contiguously from 0 to next_offset - 1.
-- Any row returned is a defect.
WITH per_partition AS (
    SELECT topic, partition, count(*) AS rows, count(DISTINCT "offset") AS distinct_offsets,
           min("offset") AS first_offset, max("offset") AS last_offset
    FROM position_ledger GROUP BY topic, partition
)
SELECT c.topic, c.partition, c.next_offset, l.rows, l.distinct_offsets, l.first_offset, l.last_offset
FROM consumer_progress c
LEFT JOIN per_partition l USING (topic, partition)
WHERE c.next_offset > 0
  AND (l.rows IS DISTINCT FROM c.next_offset
       OR l.distinct_offsets IS DISTINCT FROM c.next_offset
       OR l.first_offset IS DISTINCT FROM 0
       OR l.last_offset IS DISTINCT FROM c.next_offset - 1);
