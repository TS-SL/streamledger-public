# Event contract (schema v1)

All data is synthetic. No real payment details are generated, stored, or accepted.

## Wire format

One Kafka record = one JSON object, UTF-8, maximum 16 KiB. The record **key** must
equal `order_id`, because partitioning by order is what makes per-order ordering
meaningful (see consistency-contract.md section 4).

| field            | type              | rule |
|------------------|-------------------|------|
| `event_id`       | string            | `[A-Za-z0-9_-]{1,64}`, intended to be globally unique |
| `event_type`     | string            | one of the five types below |
| `schema_version` | integer           | only `1` is supported |
| `order_id`       | string            | id pattern |
| `payment_id`     | string or null    | required for PAYMENT_* and REFUND_ISSUED, otherwise null |
| `refund_id`      | string or null    | required for REFUND_ISSUED, otherwise null |
| `event_time`     | string            | `YYYY-MM-DDTHH:MM:SS.mmmZ`, UTC, when the business fact happened |
| `producer_id`    | string            | the service that emitted the event (diagnostics only) |
| `producer_seq`   | integer ≥ 0       | per-producer counter (diagnostics only; **not** used for ordering) |
| `currency`       | string            | `USD`, `EUR` or `GBP` |
| `amount_minor`   | integer           | integer minor units (cents). Floats, strings and booleans are rejected |

All fields must be present. Unknown fields are rejected: a producer that needs a
new field must bump `schema_version`.

**Canonical form and digest.** After validation an event is re-serialized with
sorted keys, no whitespace, UTF-8 (`canonical_json`). `digest = sha256(canonical form)`.
Two deliveries count as *the same event* only if they have the same `event_id`
**and** the same digest. Key order and whitespace don't matter. Any field difference does.

## Event types and financial effect

`payment_id` and `refund_id` are scoped to their order: the identity of a payment
is `(order_id, payment_id)`. This keeps every decision local to one order, and so
to one partition.

| type | requires | effect if accepted | money effect |
|------|----------|--------------------|--------------|
| ORDER_CREATED | — | order exists, status OPEN, `total_minor = amount` | none |
| PAYMENT_CAPTURED | order | payment CAPTURED for `amount` | `+amount` captured |
| PAYMENT_FAILED | order | payment FAILED | none (attempt recorded) |
| REFUND_ISSUED | order and a CAPTURED payment | refund recorded against the payment | `+amount` refunded |
| ORDER_CANCELLED | order | status CANCELLED | none |

`net_paid = captured − refunded`, per order, per payment, and per reporting period.

## Semantics for the awkward cases

| case | outcome | reason code |
|------|---------|-------------|
| identical redelivery (same id, same digest) | DUPLICATE, no effect | — |
| same `event_id`, different payload | CONFLICT, no effect, both payloads kept as evidence | `EVENT_ID_CONFLICT` |
| second ORDER_CREATED for an existing order | REJECTED | `ORDER_ALREADY_EXISTS` |
| capture of an already-captured payment_id (new event_id) | REJECTED | `DUPLICATE_CAPTURE` |
| capture of a payment_id already FAILED | REJECTED | `PAYMENT_ALREADY_FAILED` |
| failure of a payment_id already CAPTURED | REJECTED | `PAYMENT_ALREADY_CAPTURED` |
| failure of a payment_id already FAILED | REJECTED | `DUPLICATE_FAILURE` |
| multiple payments per order | allowed, provided the total captured stays ≤ order total | — |
| captures exceeding the order total | REJECTED | `OVERCAPTURE` |
| partial refund | allowed while the refunds on a payment stay ≤ its captured amount | — |
| refund exceeding the payment's remaining refundable amount | REJECTED | `OVER_REFUND` |
| refund of a FAILED payment | REJECTED | `REFUND_OF_FAILED_PAYMENT` |
| refund_id reused within the order | REJECTED | `DUPLICATE_REFUND_ID` |
| any non-create event whose currency ≠ order currency | REJECTED | `CURRENCY_MISMATCH` |
| second ORDER_CANCELLED | REJECTED | `ALREADY_CANCELLED` |
| payment / refund / cancel before its order exists | DEFERRED until the order arrives | `MISSING_ORDER` |
| refund before its payment is captured or failed | DEFERRED until the payment is resolved | `MISSING_PAYMENT` |
| more than 32 deferred events on one order | REJECTED | `PENDING_LIMIT` |
| negative, zero, non-integer or > 10¹² amount | INVALID | `BAD_AMOUNT` |
| unknown `schema_version` | INVALID | `UNSUPPORTED_SCHEMA_VERSION` |
| unknown event type, malformed JSON, missing or extra field, key ≠ order_id | INVALID | various |

**Cancelled orders.** Cancellation changes status only. Captures and refunds that
come after it are still accepted, because they report money that has actually
moved. An order that is CANCELLED with `net_paid ≠ 0` is reported with the
diagnostic flag `CANCELLED_WITH_BALANCE`. It is not hidden and it is not rejected.

**Late corrections.** v1 has no "edit" event. A correction is a new business event,
such as a refund or a cancellation. Reporting assigns money to the UTC date of
`event_time`. When an event's broker timestamp is more than the grace period
(1 hour) after the end of its period, its amount is also counted in the period's
`late_*` columns, so a report that was already read can be identified as restated.
Lateness is computed from the record's own broker timestamp. It is never computed
from when a deferred event happened to be released, because that is processing-time
dependent.
