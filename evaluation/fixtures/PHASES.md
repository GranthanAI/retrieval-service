# billing-service roadmap

This file tracks how the billing-service is built. Each phase has a goal, a task list and exit criteria. Phases are worked in order and a phase is closed only when its exit criteria hold.

## billing-service: coding phases

### Phase 1: Repository scaffolding

In this phase the team builds the repository layout, linting and the continuous integration pipeline.

#### Tasks

- Create the monorepo layout and the shared Makefile
- Wire up the lint and type-check jobs

#### Exit criteria

- A pull request runs lint, types and unit tests in under six minutes

### Phase 2: Database schema

In this phase the team builds the PostgreSQL schema and the migration tooling.

#### Tasks

- Model accounts, plans and subscriptions
- Add Alembic migrations with a rollback check

#### Exit criteria

- Every migration applies and rolls back cleanly on an empty database

### Phase 3: Plan catalogue

In this phase the team builds the catalogue of sellable plans and their price books.

#### Tasks

- Store plan prices per currency
- Expose the catalogue through a read-only endpoint

#### Exit criteria

- The catalogue returns all three plans with prices in four currencies

### Phase 4: Subscription lifecycle

In this phase the team builds the state machine that moves a subscription between trial, active, paused and cancelled.

#### Tasks

- Implement the lifecycle transitions
- Reject transitions that skip a state

#### Exit criteria

- Illegal transitions return a conflict error and leave the subscription unchanged

### Phase 5: Invoice generation

In this phase the team builds monthly invoice generation and PDF rendering.

#### Tasks

- Generate invoices on the first of each month
- Render invoice PDFs from a template

#### Exit criteria

- A month of synthetic subscriptions produces matching invoices with no rounding drift

### Phase 6: Payment gateway webhooks

In this phase the team builds webhook ingestion from the payment gateway with idempotent processing. The gateway signs every webhook with a shared secret, and the handler must treat the signing key as write-only.

#### Tasks

- Verify the webhook signature header on every call
- Store each gateway event id so a replay is ignored
- Map gateway statuses onto internal payment states

#### Exit criteria

- Replaying the same webhook ten times produces exactly one payment record
- A webhook with a bad signature is rejected with status 401 and logged

### Phase 7: Usage metering

In this phase the team builds metering of API calls and storage so overage can be billed.

#### Tasks

- Aggregate usage events hourly
- Expose the running total per account

#### Exit criteria

- Hourly aggregates reconcile with raw events to within one event

### Phase 8: Dunning and retries

In this phase the team builds retry schedules for failed card payments and the reminder emails that go with them. Dunning is the one place where the service sends email on its own initiative.

#### Tasks

- Retry a failed payment after one, three and seven days
- Send a reminder email before each retry

#### Exit criteria

- A failed payment is retried on schedule and the account is suspended after the last retry

### Phase 9: Tax calculation

In this phase the team builds tax rules per region and the tax lines on invoices. Tax rules change often, so the rate table is data, not code.

#### Tasks

- Look up the tax rate by billing country
- Show tax as its own invoice line

#### Exit criteria

- Invoices for the five test regions carry the expected tax amounts

### Phase 10: Admin API

In this phase the team builds internal endpoints that support staff use to credit, refund and pause accounts.

#### Tasks

- Add credit notes and refunds
- Require the support role for every admin call

#### Exit criteria

- Every admin call is audited with the acting user and a reason

### Phase 11: Observability

In this phase the team builds metrics, dashboards and alerts for the billing pipeline.

#### Tasks

- Export invoice and payment counters
- Alert when invoice generation lags by more than an hour

#### Exit criteria

- The on-call dashboard shows invoice lag and payment failure rate

### Phase 12: Load testing and rollout

In this phase the team builds load tests at ten times expected volume and the staged production rollout.

#### Tasks

- Run the load test against a staging copy
- Roll out to five percent of accounts first

#### Exit criteria

- The load test sustains ten times expected volume with p95 latency under 400 ms

## Open items

- Decide whether credit notes expire.
- Confirm the data retention period with legal.
- Pick the on-call rotation for the rollout week.
