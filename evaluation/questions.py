"""Labelled benchmark questions over the synthetic fixtures in evaluation/fixtures/ (the TUNING set; see heldout.py for the other one).

A returned chunk is *relevant* to a question when its text contains one of the question's gold `evidence` strings (compared
case-insensitively with whitespace collapsed). Gold is defined on text, not on chunk ids, on purpose: Phase 2 changes the
chunker, so chunk ids and boundaries change between index generations while the answer text does not.

Fields
    category        phase | phase_title | phase_exit | hinglish | multi_file | identifier | table | csv | page | unanswerable
    files           the files attached to the (simulated) conversation, i.e. the request's file_ids
    evidence        [(file, text)] gold passages; any one in a chunk makes that chunk relevant
    pages           1-based pages on which the gold passage is printed (paged files only)
    expected_files  for multi-file questions: every file that must be represented in the answer set
"""
import importlib.util
import pathlib
from dataclasses import dataclass, field

_GEN = pathlib.Path(__file__).resolve().parent / "fixtures" / "_generate.py"
_spec = importlib.util.spec_from_file_location("_eval_fixture_generator", _GEN)
_gen = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_gen)

ALL = ["PHASES.md", "hld.md", "lld.md", "runbook.md", "onboarding.md", "billing-handbook.pdf", "invoices.csv"]
DESIGN_DOCS = ["PHASES.md", "hld.md", "lld.md"]  # the three attached docs from the original bug report
PDF = "billing-handbook.pdf"


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


def goal(n: int) -> tuple[str, str]:
    """Gold: the sentence fragment describing what phase n builds (under its `### Phase n` heading in PHASES.md)."""
    return ("PHASES.md", _gen.PHASES[n - 1][1])


def exit_(n: int, i: int = 0) -> tuple[str, str]:
    """Gold: the i-th exit criterion of phase n."""
    return ("PHASES.md", _gen.PHASES[n - 1][3][i])


def task(n: int, i: int = 0) -> tuple[str, str]:
    return ("PHASES.md", _gen.PHASES[n - 1][2][i])


def csv_row(invoice_no: int) -> tuple[str, str]:
    row = next(r for r in _gen.INVOICES if r[0] == f"INV-{invoice_no}")
    return ("invoices.csv", " | ".join(str(c) for c in row))


def price_row(plan: str) -> tuple[str, str]:
    row = next(r for r in _gen.PRICING if r[0] == plan)
    return (PDF, " | ".join(row))


def Q(id, category, query, evidence=(), files=None, pages=(), expected=()):
    return Question(id, category, query, list(files or (DESIGN_DOCS if category in PHASE_CATS else ALL)), list(evidence), list(pages), list(expected))


PHASE_CATS = {"phase", "phase_title", "phase_exit", "hinglish", "multi_file"}

QUESTIONS: list[Question] = [
    # --- phase: what is phase N about (8) --------------------------------------------------------------------------------
    Q("p6-about", "phase", "what is phase 6 about?", [goal(6)]),
    Q("p3-about", "phase", "what is phase 3 about?", [goal(3)]),
    Q("p11-about", "phase", "what happens in phase 11?", [goal(11)]),
    Q("p1-about", "phase", "what is phase 1 about?", [goal(1)]),
    Q("p2-about", "phase", "what does phase 2 cover?", [goal(2)]),
    Q("p4-about", "phase", "describe phase 4", [goal(4)]),
    Q("p7-about", "phase", "what is phase 7 about?", [goal(7)]),
    Q("p12-about", "phase", "tell me about phase 12", [goal(12)]),
    # --- phase_title: the heading pasted as the question (8) -------------------------------------------------------------
    Q("p6-title", "phase_title", "Phase 6: Payment gateway webhooks", [goal(6)]),
    Q("p8-title", "phase_title", "Phase 8: Dunning and retries", [goal(8)]),
    Q("p1-title", "phase_title", "Phase 1: Repository scaffolding", [goal(1)]),
    Q("p4-title", "phase_title", "Phase 4: Subscription lifecycle", [goal(4)]),
    Q("p9-title", "phase_title", "Phase 9: Tax calculation", [goal(9)]),
    Q("p10-title", "phase_title", "Phase 10: Admin API", [goal(10)]),
    Q("p12-title", "phase_title", "Phase 12: Load testing and rollout", [goal(12)]),
    Q("p5-title", "phase_title", "Phase 5: Invoice generation", [goal(5)]),
    # --- phase_exit (8) --------------------------------------------------------------------------------------------------
    Q("p6-exit", "phase_exit", "what are the exit criteria of phase 6?", [exit_(6)]),
    Q("p10-exit", "phase_exit", "exit criteria for phase 10", [exit_(10)]),
    Q("p1-exit", "phase_exit", "exit criteria for phase 1", [exit_(1)]),
    Q("p4-exit", "phase_exit", "what are the exit criteria of phase 4?", [exit_(4)]),
    Q("p7-exit", "phase_exit", "what are the exit criteria of phase 7", [exit_(7)]),
    Q("p12-exit", "phase_exit", "phase 12 exit criteria", [exit_(12)]),
    Q("p3-exit", "phase_exit", "what are the exit criteria of phase 3?", [exit_(3)]),
    Q("p8-exit", "phase_exit", "when is phase 8 done?", [exit_(8)]),
    # --- hinglish (8) ----------------------------------------------------------------------------------------------------
    Q("p6-hinglish", "hinglish", "Phase 6 mein kya karna hai?", [goal(6)]),
    Q("p9-hinglish-exit", "hinglish", "Phase 9 ka exit criteria kya hai?", [exit_(9)]),
    Q("p4-hinglish", "hinglish", "Phase 4 mein kya hota hai?", [goal(4)]),
    Q("p11-hinglish", "hinglish", "Phase 11 ka goal kya hai?", [goal(11)]),
    Q("p8-hinglish", "hinglish", "phase 8 mein kya karna hai", [goal(8)]),
    Q("p2-hinglish-exit", "hinglish", "Phase 2 ke exit criteria kya hain?", [exit_(2)]),
    Q("retry-hinglish", "hinglish", "MAX_RETRY_ATTEMPTS ka kya kaam hai?", [("runbook.md", "The retry worker reads MAX_RETRY_ATTEMPTS from the environment")], files=ALL),
    Q("enterprise-hinglish", "hinglish", "Enterprise plan ki storage limit kitni hai?", [price_row("Enterprise")], files=ALL),
    # --- multi_file: the answer needs a passage from each of several files (8) ---------------------------------------------
    Q("mf-idempotency", "multi_file", "how do the HLD and the LLD each describe idempotency?",
      [("hld.md", "returns the original result and never produces a second side effect"), ("lld.md", "a repeated key with the same hash replays the stored response")],
      expected=["hld.md", "lld.md"]),
    Q("mf-webhook", "multi_file", "what do the roadmap and the LLD say about the webhook handler?", [goal(6), ("lld.md", "the handler is part of phase six")],
      expected=["PHASES.md", "lld.md"]),
    Q("mf-phases", "multi_file", "compare how the roadmap and the HLD describe the phases",
      [("PHASES.md", "Phases are worked in order and a phase is closed only when its exit criteria hold"), ("hld.md", "The work is split into twelve phases")],
      expected=["PHASES.md", "hld.md"]),
    Q("mf-retry", "multi_file", "how do the HLD and the LLD describe retrying failed payments?",
      [("hld.md", "retries with exponential backoff"), ("lld.md", "scans for payments in the failed state every five minutes")], expected=["hld.md", "lld.md"]),
    Q("mf-money", "multi_file", "what do the HLD and the LLD say about how money amounts are stored?",
      [("hld.md", "Prices are stored in minor units"), ("lld.md", "integer amount_minor")], expected=["hld.md", "lld.md"]),
    Q("mf-dunning", "multi_file", "what do the roadmap and the LLD say about dunning?",
      [goal(8), ("lld.md", "It applies the schedule from the dunning phase")], expected=["PHASES.md", "lld.md"]),
    Q("mf-retry-limit", "multi_file", "compare the runbook and the LLD on retries",
      [("runbook.md", "The default is 5 and the hard ceiling is 12"), ("lld.md", "records every attempt in the payment_attempts table")],
      files=ALL, expected=["runbook.md", "lld.md"]),
    Q("mf-support", "multi_file", "what do the runbook and the onboarding guide say about where to get support?",
      [("runbook.md", "Billing questions go to the finance-ops queue"), ("onboarding.md", "Support is staffed from 08:00 to 20:00 UTC")],
      files=ALL, expected=["runbook.md", "onboarding.md"]),
    # --- identifier (8) --------------------------------------------------------------------------------------------------
    Q("id-retry", "identifier", "what does MAX_RETRY_ATTEMPTS do?", [("runbook.md", "The retry worker reads MAX_RETRY_ATTEMPTS from the environment")]),
    Q("id-retry-default", "identifier", "what is the default value of MAX_RETRY_ATTEMPTS?", [("runbook.md", "The default is 5 and the hard ceiling is 12")]),
    Q("id-txn", "identifier", "TXN-102394", [("runbook.md", "traced to transaction TXN-102394")]),
    Q("id-path", "identifier", "/api/v1/subscriptions", [("runbook.md", "Customers manage their plan through /api/v1/subscriptions")]),
    Q("id-alert", "identifier", "INVOICE_LAG_HIGH", [("runbook.md", "The alert is named INVOICE_LAG_HIGH")]),
    Q("id-ratelimit", "identifier", "what sets RATE_LIMIT_PER_MINUTE?", [("runbook.md", "The limit is set by RATE_LIMIT_PER_MINUTE")]),
    Q("id-currency", "identifier", "when is CurrencyMismatchError raised?", [("lld.md", "Arithmetic across different currencies raises CurrencyMismatchError")]),
    Q("id-table", "identifier", "idempotency_keys", [("lld.md", "table named idempotency_keys")]),
    # --- table: pricing table in the PDF (8) -----------------------------------------------------------------------------
    Q("tbl-storage", "table", "what is the storage limit of the Enterprise plan?", [price_row("Enterprise")], pages=[6]),
    Q("tbl-seats", "table", "how many seats does the Team plan include?", [price_row("Team")], pages=[6]),
    Q("tbl-starter-price", "table", "how much does the Starter plan cost per month?", [price_row("Starter")], pages=[6]),
    Q("tbl-starter-storage", "table", "what is the storage limit of the Starter plan?", [price_row("Starter")], pages=[6]),
    Q("tbl-enterprise-seats", "table", "how many seats does the Enterprise plan include?", [price_row("Enterprise")], pages=[6]),
    Q("tbl-team-price", "table", "what is the monthly price of the Team plan?", [price_row("Team")], pages=[6]),
    Q("tbl-team-storage", "table", "how much storage does the Team plan have?", [price_row("Team")], pages=[6]),
    Q("tbl-enterprise-price", "table", "how much does the Enterprise plan cost?", [price_row("Enterprise")], pages=[6]),
    # --- csv: row lookups (8) --------------------------------------------------------------------------------------------
    Q("csv-invoice", "csv", "which customer has invoice INV-2006 and what is its status?", [csv_row(2006)]),
    Q("csv-2003", "csv", "what is the status of invoice INV-2003?", [csv_row(2003)]),
    Q("csv-2017", "csv", "which plan is invoice INV-2017 for?", [csv_row(2017)]),
    Q("csv-2033", "csv", "who is the customer on invoice INV-2033?", [csv_row(2033)]),
    Q("csv-2041", "csv", "how much was invoice INV-2041 for?", [csv_row(2041)]),
    Q("csv-2052", "csv", "what is the status of invoice INV-2052?", [csv_row(2052)]),
    Q("csv-2058", "csv", "which customer was billed on invoice INV-2058?", [csv_row(2058)]),
    Q("csv-2007", "csv", "is invoice INV-2007 paid?", [csv_row(2007)]),
    # --- page: attribution in the 6-page PDF; the reconciliation section spans pages 3-5 (8) ------------------------------
    Q("pg-escalate", "page", "after how many days are unmatched payments escalated?",
      [(PDF, "Unmatched payments older than 14 days are escalated to the finance-ops queue")], pages=[5]),
    Q("pg-step2", "page", "what is step two of the settlement reconciliation?", [(PDF, "Step two is to match each settled payment to a ledger entry")], pages=[4]),
    Q("pg-step1", "page", "what is step one of the settlement reconciliation?", [(PDF, "Step one is to download the settlement report for the previous day")], pages=[3]),
    Q("pg-step3", "page", "what is step three of the reconciliation?", [(PDF, "Step three is to list every unmatched payment on the exceptions sheet")], pages=[4]),
    Q("pg-archive", "page", "who archives the exceptions sheet?", [(PDF, "the exceptions sheet is archived by the team lead")], pages=[5]),
    Q("pg-settle", "page", "how are card payments settled?", [(PDF, "settled into the company bank account in daily batches")], pages=[2]),
    Q("pg-audience", "page", "who is the handbook written for?", [(PDF, "It is written for new finance operations staff")], pages=[1]),
    Q("pg-plans", "page", "which plans are listed in the pricing section?", [(PDF, "The table below lists the three plans and their limits")], pages=[6]),
    # --- unanswerable: nothing in the corpus answers these (8) -----------------------------------------------------------
    Q("na-colour", "unanswerable", "what is the CEO's favourite colour?"),
    Q("na-k8s", "unanswerable", "how is the Kubernetes autoscaler configured?"),
    Q("na-crypto", "unanswerable", "what is the refund policy for cryptocurrency payments?"),
    Q("na-football", "unanswerable", "who won the football world cup in 2018?"),
    Q("na-encryption", "unanswerable", "which algorithm encrypts the database at rest?"),
    Q("na-upload", "unanswerable", "what is the maximum file upload size?"),
    Q("na-vendor", "unanswerable", "which payment gateway vendor does the company use?"),
    Q("na-employees", "unanswerable", "how many employees does the company have?"),
]
