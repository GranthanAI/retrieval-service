"""HELD-OUT questions. Written with different phrasing from questions.py: no heading words copied into the question, paraphrases,
typos, mixed Hindi/English, and answers that sit in the body of a section rather than under a heading that matches the question.

RULES
  * Never tune a setting, a boost, a threshold or the retrieval code against these results.
  * Run them only to report (`python -m evaluation.run --sets heldout ...`, or `--sets main,heldout`). The runner prints no
    per-question rows for this set.
  * Do not edit a question after seeing how the system answers it. To add questions, write new ones; do not "fix" old ones.
Same corpus and same gold-by-evidence-text convention as questions.py."""
from evaluation.questions import ALL, DESIGN_DOCS, PDF, Q, csv_row, exit_, goal, price_row, task

H = lambda id, cat, query, ev=(), **kw: Q("h-" + id, cat, query, ev, files=kw.pop("files", ALL), **kw)  # noqa: E731

HELDOUT = [
    # paraphrase: the heading words are not in the question
    H("stage6", "paraphrase", "what's the sixth stage of the roadmap all about", [goal(6)]),
    H("tax-stage", "paraphrase", "which stage deals with taxes per region?", [goal(9)]),
    H("replay", "paraphrase", "how do we make sure a replayed gateway callback does not double book a payment?",
      [exit_(6), ("hld.md", "returns the original result and never produces a second side effect"), task(6, 1)]),
    H("monitoring-stage", "paraphrase", "which stage sets up dashboards and alerts?", [goal(11)]),
    H("floats", "paraphrase", "is money kept as floating point numbers?", [("hld.md", "The service never stores floating point money amounts")]),
    # typos
    H("typo-reminder", "typo", "wen do we send remindr emails befor retries", [task(8, 1), goal(8)]),
    H("typo-unmatched", "typo", "unmatchd payments older than two weeks - what then?", [(PDF, "Unmatched payments older than 14 days are escalated")], pages=[5]),
    H("typo-backup", "typo", "how long do databse backups stay arround", [("runbook.md", "are kept for 35 days")]),
    H("typo-trial", "typo", "do i need a credt card for the triall", [("onboarding.md", "No card is required during the trial")]),
    # mixed Hindi / English
    H("hi-loadtest", "hinglish", "kis phase mein load testing hoti hai?", [goal(12)]),
    H("hi-backup", "hinglish", "kitne din tak database backups rakhe jaate hain?", [("runbook.md", "are kept for 35 days")]),
    H("hi-signature", "hinglish", "webhook ki signature verify kaise hoti hai", [task(6, 0), ("lld.md", "verifies the signature header against the shared secret")]),
    H("hi-refund", "hinglish", "cancel karne par paisa wapas milta hai kya?", [("onboarding.md", "is not refunded pro rata")]),
    # answer in the body, question does not name the section
    H("body-rate", "body", "what happens if a client sends more than 120 calls a minute", [("runbook.md", "Clients that exceed the limit receive status 429")]),
    H("body-retention", "body", "how long are old invoices kept", [("runbook.md", "Invoice records are kept for seven years")]),
    H("body-invite", "body", "how long can a new customer wait before the invite lapses", [("onboarding.md", "invitation email that expires after 72 hours")]),
    H("body-escalation", "body", "when is a page passed on to the manager", [("runbook.md", "does not acknowledge a page within 15 minutes")]),
    H("body-hours", "body", "what are the working hours of support", [("onboarding.md", "Support is staffed from 08:00 to 20:00 UTC")]),
    H("body-pagesize", "body", "how many items can one list request return at most", [("lld.md", "maximum page size of 100")]),
    H("body-errors", "body", "which fields does an error response contain", [("lld.md", "the fields code, message and request_id")]),
    H("body-poll", "body", "how often does the worker look for failed payments", [("lld.md", "scans for payments in the failed state every five minutes")]),
    H("body-sla", "body", "how fast is the first reply for the biggest customers", [("onboarding.md", "1 hour for Enterprise customers")]),
    # tables / csv / page
    H("tbl-biggest", "table", "what does it cost per month to be on the biggest tier", [price_row("Enterprise")], pages=[6]),
    H("tbl-middle", "table", "how much space does the middle plan give", [price_row("Team")], pages=[6]),
    H("tbl-cheapest", "table", "how many people can use the cheapest subscription", [price_row("Starter")], pages=[6]),
    H("csv-refund", "csv", "which invoice was refunded to Wingtip", [csv_row(2006)]),
    H("csv-fabrikam", "csv", "is the Fabrikam invoice paid or overdue", [csv_row(2003)]),
    H("pg-bank", "page", "where does the money from card charges end up", [(PDF, "settled into the company bank account in daily batches")], pages=[2]),
    # multi-file
    H("mf-ceiling", "multi_file", "compare what the design docs and the runbook say about retry limits",
      [("runbook.md", "the hard ceiling is 12"), ("hld.md", "The HLD sets the ceiling at one day")], expected=["runbook.md", "hld.md"]),
    H("mf-retry-charges", "multi_file", "how are failed card charges retried, per the runbook and the low-level design",
      [("runbook.md", "Setting MAX_RETRY_ATTEMPTS to zero disables retries entirely"), ("lld.md", "scans for payments in the failed state every five minutes")],
      expected=["runbook.md", "lld.md"]),
    H("mf-contact", "multi_file", "between the onboarding guide and the runbook, who do I contact and when",
      [("onboarding.md", "Support is staffed from 08:00 to 20:00 UTC"), ("runbook.md", "Billing questions go to the finance-ops queue")],
      expected=["onboarding.md", "runbook.md"]),
    H("mf-money-layers", "multi_file", "how do the high-level and low-level designs differ on where money amounts live",
      [("hld.md", "Prices are stored in minor units"), ("lld.md", "integer amount_minor")], expected=["hld.md", "lld.md"]),
    H("mf-late-payers", "multi_file", "what do the roadmap and the handbook say about chasing late payers",
      [goal(8), (PDF, "Unmatched payments older than 14 days are escalated")], expected=["PHASES.md", PDF]),
    H("mf-new-customer", "multi_file", "what does a new customer learn about plans and the trial from the handbook and the onboarding guide",
      [(PDF, "The table below lists the three plans and their limits"), ("onboarding.md", "Every new account starts with a 14 day trial")],
      expected=[PDF, "onboarding.md"]),
    H("mf-paging", "multi_file", "which documents talk about alerts or paging, and what do they say",
      [("runbook.md", "pages the on-call engineer when invoice generation is more than 60 minutes behind schedule"), task(11, 1)],
      expected=["runbook.md", "PHASES.md"]),
    # phase_exit-like: asked without "phase" or "exit criteria" (added after Phase 2 was first measured, written before seeing their results)
    H("pe-webhook", "phase_exit", "when can the team call the webhook stage finished", [exit_(6, 0), exit_(6, 1)]),
    H("pe-tax", "phase_exit", "what has to be true before the tax stage can be closed", [exit_(9)]),
    H("pe-admin", "phase_exit", "how do we know the admin stage is done", [exit_(10)]),
    H("pe-loadtest", "phase_exit", "load testing wali stage kab complete maani jaati hai", [exit_(12)]),
    H("pe-dunning", "phase_exit", "what must the dunning stage achieve to count as complete", [exit_(8)]),
    H("pe-schema", "phase_exit", "database schema wali stage ko khatam karne ke liye kya check karna hai", [exit_(2)]),
    # unanswerable
    H("na-region", "unanswerable", "which cloud region hosts the database?"),
    H("na-ceo", "unanswerable", "what is the name of the chief executive?"),
    H("na-password", "unanswerable", "how do I reset my password?"),
    H("na-logo", "unanswerable", "what colour is the company logo?"),
]
