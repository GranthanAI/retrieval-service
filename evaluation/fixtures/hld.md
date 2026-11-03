# billing-service high-level design

## Overview

The billing-service owns plans, subscriptions, invoices and payments. It is a stateless API in front of PostgreSQL and talks to the payment gateway only through the webhook handler and the charge client.

## Delivery phases

The work is split into twelve phases, described in the roadmap. The first four phases build the data model and the subscription lifecycle. Phases five to eight cover money movement: invoices, gateway webhooks, usage metering and dunning. The last phases cover tax, administration, observability and rollout.

## Idempotency

Every state-changing operation carries an idempotency key. The HLD requires that a repeated request with the same key returns the original result and never produces a second side effect. Gateway webhooks use the gateway event id as their key.

## Failure handling

When the payment gateway is unavailable the service queues the charge and retries with exponential backoff. The HLD sets the ceiling at one day, after which the subscription moves to the past-due state.

## Security

All admin endpoints require the support role. Secrets are read from the environment and never logged.

## Currency handling

Prices are stored in minor units (cents) as integers. The service never stores floating point money amounts.

## Data stores

PostgreSQL is the system of record. Redis is used only for rate limiting and never holds the only copy of any data.

## Scalability

The API scales horizontally behind a load balancer. The retry worker runs as a single leader elected through a database advisory lock.
