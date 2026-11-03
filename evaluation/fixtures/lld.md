# billing-service low-level design

## Webhook handler

The handler reads the raw request body, verifies the signature header against the shared secret and only then parses the JSON. In the LLD the handler is part of phase six and is the only code path allowed to create payment records from gateway events.

## Idempotency table

The LLD stores idempotency keys in a table named idempotency_keys with columns key, request_hash, response_body and created_at. A repeated key with a different request hash is rejected with status 422, and a repeated key with the same hash replays the stored response.

## Retry worker

The retry worker scans for payments in the failed state every five minutes. It applies the schedule from the dunning phase and records every attempt in the payment_attempts table.

## Invoice numbering

Invoice numbers are allocated from a database sequence per year, formatted as INV-YYYY-NNNNNN, and are never reused.
