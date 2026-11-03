"""Regenerates the committed synthetic fixtures (all content is invented; nothing here comes from a real document).

    uv run python evaluation/fixtures/_generate.py

Outputs are committed so the benchmark never depends on this script at run time; re-run it only when the corpus changes
(and then update questions.py, whose gold evidence strings must still appear verbatim)."""
import csv
import pathlib

HERE = pathlib.Path(__file__).parent

# (title, what the team builds, task bullets, exit criteria). Wording is deliberately repetitive across phases, the way real
# phase plans are, so a lexical or semantic match on "phase" alone cannot pick the right one.
PHASES = [
    ("Repository scaffolding", "the repository layout, linting and the continuous integration pipeline",
     ["Create the monorepo layout and the shared Makefile", "Wire up the lint and type-check jobs"],
     ["A pull request runs lint, types and unit tests in under six minutes"]),
    ("Database schema", "the PostgreSQL schema and the migration tooling",
     ["Model accounts, plans and subscriptions", "Add Alembic migrations with a rollback check"],
     ["Every migration applies and rolls back cleanly on an empty database"]),
    ("Plan catalogue", "the catalogue of sellable plans and their price books",
     ["Store plan prices per currency", "Expose the catalogue through a read-only endpoint"],
     ["The catalogue returns all three plans with prices in four currencies"]),
    ("Subscription lifecycle", "the state machine that moves a subscription between trial, active, paused and cancelled",
     ["Implement the lifecycle transitions", "Reject transitions that skip a state"],
     ["Illegal transitions return a conflict error and leave the subscription unchanged"]),
    ("Invoice generation", "monthly invoice generation and PDF rendering",
     ["Generate invoices on the first of each month", "Render invoice PDFs from a template"],
     ["A month of synthetic subscriptions produces matching invoices with no rounding drift"]),
    ("Payment gateway webhooks", "webhook ingestion from the payment gateway with idempotent processing",
     ["Verify the webhook signature header on every call", "Store each gateway event id so a replay is ignored",
      "Map gateway statuses onto internal payment states"],
     ["Replaying the same webhook ten times produces exactly one payment record",
      "A webhook with a bad signature is rejected with status 401 and logged"]),
    ("Usage metering", "metering of API calls and storage so overage can be billed",
     ["Aggregate usage events hourly", "Expose the running total per account"],
     ["Hourly aggregates reconcile with raw events to within one event"]),
    ("Dunning and retries", "retry schedules for failed card payments and the reminder emails that go with them",
     ["Retry a failed payment after one, three and seven days", "Send a reminder email before each retry"],
     ["A failed payment is retried on schedule and the account is suspended after the last retry"]),
    ("Tax calculation", "tax rules per region and the tax lines on invoices",
     ["Look up the tax rate by billing country", "Show tax as its own invoice line"],
     ["Invoices for the five test regions carry the expected tax amounts"]),
    ("Admin API", "internal endpoints that support staff use to credit, refund and pause accounts",
     ["Add credit notes and refunds", "Require the support role for every admin call"],
     ["Every admin call is audited with the acting user and a reason"]),
    ("Observability", "metrics, dashboards and alerts for the billing pipeline",
     ["Export invoice and payment counters", "Alert when invoice generation lags by more than an hour"],
     ["The on-call dashboard shows invoice lag and payment failure rate"]),
    ("Load testing and rollout", "load tests at ten times expected volume and the staged production rollout",
     ["Run the load test against a staging copy", "Roll out to five percent of accounts first"],
     ["The load test sustains ten times expected volume with p95 latency under 400 ms"]),
]

# Wording that differs per phase so each section has a distinctive handle in addition to its title.
EXTRA = {
    6: "The gateway signs every webhook with a shared secret, and the handler must treat the signing key as write-only.",
    9: "Tax rules change often, so the rate table is data, not code.",
    8: "Dunning is the one place where the service sends email on its own initiative.",
}


def phases_md() -> str:
    out = ["# billing-service roadmap", "",
           "This file tracks how the billing-service is built. Each phase has a goal, a task list and exit criteria. "
           "Phases are worked in order and a phase is closed only when its exit criteria hold.", "",
           "## billing-service: coding phases", ""]
    for n, (title, goal, tasks, exit_) in enumerate(PHASES, start=1):
        out += [f"### Phase {n}: {title}", "",
                f"In this phase the team builds {goal}. {EXTRA.get(n, '')}".strip(), "",
                "#### Tasks", ""] + [f"- {t}" for t in tasks] + ["", "#### Exit criteria", ""] + [f"- {e}" for e in exit_] + [""]
    out += ["## Open items", "",
            "- Decide whether credit notes expire.", "- Confirm the data retention period with legal.",
            "- Pick the on-call rotation for the rollout week.", ""]
    return "\n".join(out)


HLD = """# billing-service high-level design

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
"""

LLD = """# billing-service low-level design

## Webhook handler

The handler reads the raw request body, verifies the signature header against the shared secret and only then parses the JSON. In the LLD the handler is part of phase six and is the only code path allowed to create payment records from gateway events.

## Idempotency table

The LLD stores idempotency keys in a table named idempotency_keys with columns key, request_hash, response_body and created_at. A repeated key with a different request hash is rejected with status 422, and a repeated key with the same hash replays the stored response.

## Retry worker

The retry worker scans for payments in the failed state every five minutes. It applies the schedule from the dunning phase and records every attempt in the payment_attempts table.

## Invoice numbering

Invoice numbers are allocated from a database sequence per year, formatted as INV-YYYY-NNNNNN, and are never reused.

## Money type

Amounts are held in a Money value object with an integer amount_minor and a three-letter currency code. Arithmetic across different currencies raises CurrencyMismatchError.

## Pagination

List endpoints use cursor pagination with an opaque cursor and a maximum page size of 100.

## Error format

Errors are returned as JSON with the fields code, message and request_id.
"""

RUNBOOK = """# billing-service operations runbook

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
"""

ONBOARDING = """# Customer onboarding guide

## Account setup

New customers receive an invitation email that expires after 72 hours. The first user to accept becomes the account owner.

## Free trial

Every new account starts with a 14 day trial of the Team plan. No card is required during the trial.

## Support hours

Support is staffed from 08:00 to 20:00 UTC on weekdays. Enterprise customers have a 24 hour line.

## Service levels

The target first response time is 4 hours for Team customers and 1 hour for Enterprise customers.

## Cancelling

A customer can cancel at any time. The subscription stays active until the end of the paid period and is not refunded pro rata.

## Data export

Customers can request a full export of their invoices as CSV. Exports are ready within one business day.
"""

INVOICES = [("invoice_id", "customer", "plan", "amount_usd", "status")] + [
    (f"INV-20{i:02d}", name, plan, amount, status) for i, (name, plan, amount, status) in enumerate([
        ("Northwind Traders", "Team", 199, "paid"), ("Contoso Ltd", "Enterprise", 1499, "paid"),
        ("Fabrikam Inc", "Starter", 49, "overdue"), ("Adventure Works", "Team", 199, "paid"),
        ("Tailspin Toys", "Starter", 49, "paid"), ("Wingtip Group", "Enterprise", 1499, "refunded"),
        ("Litware Corp", "Team", 199, "overdue"), ("Proseware", "Starter", 49, "paid"),
    ], start=1)]


_ADJ = ["Blue", "Red", "Green", "Silver", "Golden", "Northern", "Southern", "Eastern", "Western", "Rapid", "Quiet", "Bright", "Solid"]
_NOUN = ["Harbor", "Mills", "Labs", "Works", "Freight", "Foods", "Systems", "Media", "Energy", "Optics"]
_PLANS = [("Starter", 49), ("Team", 199), ("Enterprise", 1499)]
_STATUS = ["paid", "paid", "paid", "overdue", "refunded"]
for _i in range(9, 61):
    _plan, _amt = _PLANS[_i % 3]
    INVOICES.append((f"INV-20{_i:02d}", f"{_ADJ[_i % len(_ADJ)]} {_NOUN[(_i * 3) % len(_NOUN)]}", _plan, _amt, _STATUS[(_i * 7) % len(_STATUS)]))


def pdf_pages() -> list[list[str]]:
    """Plain text lines per page; lines starting with '# ' are headings. The 'Settlement reconciliation' section starts on
    page 3 and runs through page 5; the pricing table is on page 6."""
    filler = "Finance reviews the settlement report every morning and compares it with the ledger totals. "
    return [
        ["# Billing handbook", "", "This handbook describes how the billing team runs settlement, reconciliation and pricing.",
         "It is written for new finance operations staff."],
        ["# How billing works", "", "Customers are charged at the start of each billing period.",
         "Charges are collected by the payment gateway and settled into the company bank account in daily batches."],
        ["# Settlement reconciliation", "", "Reconciliation confirms that every settled batch matches the ledger.",
         filler * 2, "Step one is to download the settlement report for the previous day."],
        [filler * 3, "Step two is to match each settled payment to a ledger entry by payment reference.",
         "Step three is to list every unmatched payment on the exceptions sheet."],
        [filler * 2, "Unmatched payments older than 14 days are escalated to the finance-ops queue.",
         "After escalation the exceptions sheet is archived by the team lead."],
        ["# Pricing plans", "", "The table below lists the three plans and their limits.", "TABLE"],
    ]


PRICING = [("Plan", "Price per month", "Storage limit", "Seats"),
           ("Starter", "$49", "50 GB", "3"), ("Team", "$199", "500 GB", "25"), ("Enterprise", "$1499", "5 TB", "unlimited")]


def _esc(t: str) -> str:
    return t.replace("\\", r"\\").replace("(", r"\(").replace(")", r"\)")


def build_pdf(pages: list[list[str]]) -> bytes:
    """Hand-built PDF (no PDF library is installed). Text uses Helvetica; the table is drawn with ruled lines so a layout
    model can see its grid."""
    objs: list[tuple[int, bytes]] = []
    page_ids = []
    first_page_obj = 4  # 1 catalog, 2 pages, 3 font
    for i, lines in enumerate(pages):
        ops, y = [], 740
        for line in lines:
            if line == "TABLE":
                x0, colw, rowh = 60, [90, 130, 110, 90], 26
                for r, row in enumerate(PRICING):
                    ty = y - r * rowh
                    cx = x0
                    for c, cell in enumerate(row):
                        ops.append(f"{cx} {ty - rowh} {colw[c]} {rowh} re S")
                        ops.append(f"BT /F1 11 Tf {cx + 6} {ty - 17} Td ({_esc(cell)}) Tj ET")
                        cx += colw[c]
                continue
            size = 20 if line.startswith("# ") else 11
            text = line[2:] if line.startswith("# ") else line
            words, cur, wrapped = text.split(), "", []
            for w in words:
                if len(cur) + len(w) + 1 > 90:
                    wrapped.append(cur)
                    cur = w
                else:
                    cur = (cur + " " + w).strip()
            wrapped.append(cur)
            for w_line in wrapped:
                ops.append(f"BT /F1 {size} Tf 60 {y} Td ({_esc(w_line)}) Tj ET")
                y -= size + 6
            y -= 4 if size == 11 else 10
        stream = "\n".join(ops).encode("latin-1")
        page_obj = first_page_obj + 2 * i
        page_ids.append(page_obj)
        objs.append((page_obj, f"<< /Type /Page /Parent 2 0 R /Resources << /Font << /F1 3 0 R >> >> /MediaBox [0 0 612 792] /Contents {page_obj + 1} 0 R >>".encode()))
        objs.append((page_obj + 1, f"<< /Length {len(stream)} >>\nstream\n".encode() + stream + b"\nendstream"))
    kids = " ".join(f"{p} 0 R" for p in page_ids)
    all_objs = [(1, b"<< /Type /Catalog /Pages 2 0 R >>"), (2, f"<< /Type /Pages /Kids [{kids}] /Count {len(page_ids)} >>".encode()),
                (3, b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")] + objs
    out, offsets = bytearray(b"%PDF-1.4\n"), {}
    for num, body in all_objs:
        offsets[num] = len(out)
        out += f"{num} 0 obj\n".encode() + body + b"\nendobj\n"
    xref = len(out)
    out += f"xref\n0 {len(all_objs) + 1}\n0000000000 65535 f \n".encode()
    for num, _ in all_objs:
        out += f"{offsets[num]:010d} 00000 n \n".encode()
    out += f"trailer\n<< /Size {len(all_objs) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    return bytes(out)


def main() -> None:
    (HERE / "PHASES.md").write_text(phases_md(), encoding="utf-8", newline="\n")
    (HERE / "hld.md").write_text(HLD, encoding="utf-8", newline="\n")
    (HERE / "lld.md").write_text(LLD, encoding="utf-8", newline="\n")
    (HERE / "runbook.md").write_text(RUNBOOK, encoding="utf-8", newline="\n")
    (HERE / "onboarding.md").write_text(ONBOARDING, encoding="utf-8", newline="\n")
    with open(HERE / "invoices.csv", "w", encoding="utf-8", newline="") as f:
        csv.writer(f, lineterminator="\n").writerows(INVOICES)
    (HERE / "billing-handbook.pdf").write_bytes(build_pdf(pdf_pages()))


if __name__ == "__main__":
    main()
