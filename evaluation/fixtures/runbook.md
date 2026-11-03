# billing-service operations runbook

## Retry configuration

The retry worker reads MAX_RETRY_ATTEMPTS from the environment. The default is 5 and the hard ceiling is 12. Setting MAX_RETRY_ATTEMPTS to zero disables retries entirely.

## Incident notes

The double-charge incident was traced to transaction TXN-102394, which was replayed by the gateway after a timeout. Finance refunded the duplicate within one business day.

## Public endpoints

Customers manage their plan through /api/v1/subscriptions. The endpoint accepts GET to list subscriptions and POST to create one. Cancellation uses /api/v1/subscriptions/{id}/cancel.

## Support contacts

Billing questions go to the finance-ops queue. Gateway outages go to the platform on-call.
