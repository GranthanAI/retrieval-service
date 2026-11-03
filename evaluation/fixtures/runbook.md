# billing-service operations runbook

## Retry configuration

The retry worker reads MAX_RETRY_ATTEMPTS from the environment. The default is 5 and the hard ceiling is 12. Setting MAX_RETRY_ATTEMPTS to zero disables retries entirely.

## Incident notes

The double-charge incident was traced to transaction TXN-102394, which was replayed by the gateway after a timeout. Finance refunded the duplicate within one business day.

## Public endpoints

Customers manage their plan through /api/v1/subscriptions. The endpoint accepts GET to list subscriptions and POST to create one. Cancellation uses /api/v1/subscriptions/{id}/cancel.

## Support contacts

Billing questions go to the finance-ops queue. Gateway outages go to the platform on-call.

## Alerting

The invoice-lag alert pages the on-call engineer when invoice generation is more than 60 minutes behind schedule. The alert is named INVOICE_LAG_HIGH.

## Rate limits

The public API allows 120 requests per minute per API key. Clients that exceed the limit receive status 429 with a Retry-After header. The limit is set by RATE_LIMIT_PER_MINUTE.

## Backups

Database backups run nightly at 02:30 UTC and are kept for 35 days. Restores are rehearsed once a quarter.

## Data retention

Invoice records are kept for seven years. Raw webhook payloads are deleted after 90 days.

## Escalation

If the on-call engineer does not acknowledge a page within 15 minutes, it escalates to the engineering manager.
