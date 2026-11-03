"""Labelled benchmark questions over the synthetic fixtures in evaluation/fixtures/.

A returned chunk is *relevant* to a question when its text contains one of the question's gold `evidence` strings (compared
case-insensitively with whitespace collapsed). Gold is defined on text, not on chunk ids, on purpose: Phase 2 changes the
chunker, so chunk ids and boundaries change between index generations while the answer text does not.

Fields
    category        phase | phase_title | phase_exit | hinglish | multi_file | identifier | table | csv | page | unanswerable
    files           the files attached to the (simulated) conversation, i.e. the request's file_ids
    evidence        [(file, text)] gold passages; any one in a chunk makes that chunk relevant
    pages           1-based pages on which the gold passage is printed (paged files only)
    expected_files  for multi-file questions: every file that must be represented in the answer set
    follow_up_of    reserved for Phase 5 (rewritten-query cases)
"""
from dataclasses import dataclass, field

ALL = ["PHASES.md", "hld.md", "lld.md", "runbook.md", "billing-handbook.pdf", "invoices.csv"]
DESIGN_DOCS = ["PHASES.md", "hld.md", "lld.md"]  # the three attached docs from the original bug report


@dataclass(frozen=True)
class Question:
    id: str
    category: str
    query: str
    files: list[str]
    evidence: list[tuple[str, str]] = field(default_factory=list)
    pages: list[int] = field(default_factory=list)
    expected_files: list[str] = field(default_factory=list)

    @property
    def answerable(self) -> bool:
        return bool(self.evidence)


def _g(n: int) -> tuple[str, str]:  # goal sentence of phase n in PHASES.md
    return ("PHASES.md", GOALS[n])


GOALS = {
    3: "the catalogue of sellable plans and their price books",
    6: "webhook ingestion from the payment gateway with idempotent processing",
    8: "retry schedules for failed card payments and the reminder emails that go with them",
    11: "metrics, dashboards and alerts for the billing pipeline",
}
PH6_EXIT = ("PHASES.md", "Replaying the same webhook ten times produces exactly one payment record")
PH9_EXIT = ("PHASES.md", "Invoices for the five test regions carry the expected tax amounts")
PH10_EXIT = ("PHASES.md", "Every admin call is audited with the acting user and a reason")

QUESTIONS: list[Question] = [
    # --- the bug report: three design docs attached, ask about one phase -------------------------------------------------
    Question("p6-about", "phase", "what is phase 6 about?", DESIGN_DOCS, [_g(6)]),
    Question("p6-title", "phase_title", "Phase 6: Payment gateway webhooks", DESIGN_DOCS, [_g(6)]),
    Question("p6-exit", "phase_exit", "what are the exit criteria of phase 6?", DESIGN_DOCS, [PH6_EXIT]),
    Question("p6-hinglish", "hinglish", "Phase 6 mein kya karna hai?", DESIGN_DOCS, [_g(6)]),
    Question("p9-hinglish-exit", "hinglish", "Phase 9 ka exit criteria kya hai?", DESIGN_DOCS, [PH9_EXIT]),
    Question("p3-about", "phase", "what is phase 3 about?", DESIGN_DOCS, [_g(3)]),
    Question("p11-about", "phase", "what happens in phase 11?", DESIGN_DOCS, [_g(11)]),
    Question("p8-title", "phase_title", "Phase 8: Dunning and retries", DESIGN_DOCS, [_g(8)]),
    Question("p10-exit", "phase_exit", "exit criteria for phase 10", DESIGN_DOCS, [PH10_EXIT]),
    # --- several files: the answer needs a passage from each -------------------------------------------------------------
    Question("mf-idempotency", "multi_file", "how do the HLD and the LLD each describe idempotency?", DESIGN_DOCS,
             [("hld.md", "returns the original result and never produces a second side effect"),
              ("lld.md", "a repeated key with the same hash replays the stored response")],
             expected_files=["hld.md", "lld.md"]),
    Question("mf-webhook", "multi_file", "what do the roadmap and the LLD say about the webhook handler?", DESIGN_DOCS,
             [_g(6), ("lld.md", "the handler is part of phase six")], expected_files=["PHASES.md", "lld.md"]),
    Question("mf-phases", "multi_file", "compare how the roadmap and the HLD describe the phases", DESIGN_DOCS,
             [("PHASES.md", "Phases are worked in order and a phase is closed only when its exit criteria hold"),
              ("hld.md", "The work is split into twelve phases")], expected_files=["PHASES.md", "hld.md"]),
    # --- exact identifiers -----------------------------------------------------------------------------------------------
    Question("id-retry", "identifier", "what does MAX_RETRY_ATTEMPTS do?", ALL,
             [("runbook.md", "The retry worker reads MAX_RETRY_ATTEMPTS from the environment")]),
    Question("id-retry-default", "identifier", "what is the default value of MAX_RETRY_ATTEMPTS?", ALL,
             [("runbook.md", "The default is 5 and the hard ceiling is 12")]),
    Question("id-txn", "identifier", "TXN-102394", ALL, [("runbook.md", "traced to transaction TXN-102394")]),
    Question("id-path", "identifier", "/api/v1/subscriptions", ALL,
             [("runbook.md", "Customers manage their plan through /api/v1/subscriptions")]),
    # --- tables ----------------------------------------------------------------------------------------------------------
    Question("tbl-storage", "table", "what is the storage limit of the Enterprise plan?", ALL,
             [("billing-handbook.pdf", "Enterprise | $1499 | 5 TB | unlimited")], pages=[6]),
    Question("tbl-seats", "table", "how many seats does the Team plan include?", ALL,
             [("billing-handbook.pdf", "Team | $199 | 500 GB | 25")], pages=[6]),
    Question("csv-invoice", "csv", "which customer has invoice INV-2006 and what is its status?", ALL,
             [("invoices.csv", "INV-2006 | Wingtip Group | Enterprise | 1499 | refunded")]),
    # --- page attribution: one section spans pages 3-5 ---------------------------------------------------------------------
    Question("pg-escalate", "page", "after how many days are unmatched payments escalated?", ALL,
             [("billing-handbook.pdf", "Unmatched payments older than 14 days are escalated to the finance-ops queue")],
             pages=[5]),
    Question("pg-step2", "page", "what is step two of the settlement reconciliation?", ALL,
             [("billing-handbook.pdf", "Step two is to match each settled payment to a ledger entry")], pages=[4]),
    Question("pg-step1", "page", "what is step one of the settlement reconciliation?", ALL,
             [("billing-handbook.pdf", "Step one is to download the settlement report for the previous day")], pages=[3]),
    # --- unanswerable: nothing in the corpus answers these -----------------------------------------------------------------
    Question("na-colour", "unanswerable", "what is the CEO's favourite colour?", ALL),
    Question("na-k8s", "unanswerable", "how is the Kubernetes autoscaler configured?", ALL),
    Question("na-crypto", "unanswerable", "what is the refund policy for cryptocurrency payments?", ALL),
    Question("na-football", "unanswerable", "who won the football world cup in 2018?", ALL),
    Question("na-encryption", "unanswerable", "which algorithm encrypts the database at rest?", ALL),
]
