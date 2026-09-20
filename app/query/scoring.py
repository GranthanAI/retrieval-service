from dataclasses import dataclass


@dataclass
class FusedCandidate:
    """HLD §18.2's bookkeeping — every score tracked explicitly and kept
    internally through the whole pipeline, even though only `rrf_score`
    ever reaches the wire (`response_mapper.py`). Keeping all of them
    (not just the final number) is what makes the pipeline debuggable: a
    chunk that scored well on BM25 but got excluded by MMR's diversity
    penalty is a very different situation from one that never made the
    RRF-fused candidate set at all.

    Provenance fields (`document_id`, `filename`, `content`, `page_number`,
    `section`, `chunk_index`) are carried on the same object rather than
    looked up again later — `fusion.py`/`dense_search.py`/`bm25_search.py`
    populate them from the store hit, `response_mapper.py` reads them
    straight off, no second store round trip.
    """

    chunk_id: str
    document_id: str = ""
    filename: str = ""
    content: str = ""
    page_number: int | None = None
    section: str | None = None
    chunk_index: int = 0

    dense_score: float | None = None
    dense_rank: int | None = None
    bm25_score: float | None = None
    bm25_rank: int | None = None
    rrf_score: float = 0.0
    mmr_score: float | None = None  # selection-order-dependent; internal tracking only, HLD §18.2
