"""In-memory stand-ins for the Milvus and OpenSearch *clients* (not for the repositories).

The real `MilvusRepository` / `OpenSearchRepository` run unchanged on top of these, so the query bodies and search params the
service builds are what gets evaluated, and a change to them (Phase 1) shows up in the benchmark. They implement only the
subset of each API the repositories use. The OpenSearch fake scores with BM25 (k1=1.2, b=0.75, index-wide statistics, i.e. a
single shard) and follows the bool/should/minimum_should_match semantics of the real engine, including the sub-field analyzers
that Phase 2 introduces (`.english`, `.exact`)."""
import math
import re
from collections import Counter, defaultdict

import numpy as np

_STOPWORDS = frozenset("a an and are as at be but by for if in into is it no not of on or such that the their then there these they this to was will with".split())


# --------------------------------------------------------------------------------------------------------------------------
# Analyzers
# --------------------------------------------------------------------------------------------------------------------------
def _standard(text: str) -> list[str]:
    # The standard tokenizer keeps underscores inside a word and splits on hyphens, slashes and dots.
    return [t.lower() for t in re.findall(r"\w+", text)]


def _stem(tok: str) -> str:
    for suffix in ("ing", "ed", "es", "s", "ly"):
        if tok.endswith(suffix) and len(tok) - len(suffix) >= 3:
            return tok[: -len(suffix)]
    return tok


def _english(text: str) -> list[str]:
    return [_stem(t) for t in _standard(text) if t not in _STOPWORDS]


def _exact(text: str) -> list[str]:
    """Identifiers kept whole (MAX_RETRY_ATTEMPTS, TXN-102394, /api/v1/x) plus their snake_case / camelCase parts."""
    out: list[str] = []
    for raw in re.findall(r"[\w\-/.{}]+", text):
        raw = raw.strip(".-")
        if not raw:
            continue
        out.append(raw.lower())
        for part in re.split(r"[_\-/.{}]+|(?<=[a-z])(?=[A-Z])", raw):
            if part and part.lower() != raw.lower():
                out.append(part.lower())
    return out


_ANALYZERS = {"standard": _standard, "english": _english, "exact": _exact}


def _analyze(field: str, text: str) -> list[str]:
    sub = field.split(".", 1)[1] if "." in field else "standard"
    return _ANALYZERS[sub](text)


# --------------------------------------------------------------------------------------------------------------------------
# OpenSearch
# --------------------------------------------------------------------------------------------------------------------------
class FakeOpenSearchClient:
    K1, B = 1.2, 0.75

    class _Indices:
        """Just enough of client.indices for the repository's existence/mapping guard: the fake index always exists, correctly mapped."""

        def exists(self, index):
            return True

        def get_mapping(self, index):
            from app.stores.index_schema import REQUIRED_OPENSEARCH_TYPES

            return {index: {"mappings": {"properties": {k: {"type": v} for k, v in REQUIRED_OPENSEARCH_TYPES.items()}}}}

    def __init__(self) -> None:
        self.indices = self._Indices()
        self.docs: dict[str, dict] = {}
        self.queries: list[dict] = []  # request bodies seen, for tests
        self._stats: dict[str, tuple[dict[str, Counter], dict[str, list[str]], float, dict[str, int]]] = {}

    # -- API used by OpenSearchRepository --------------------------------------------------------------------------------
    def bulk(self, body: list[dict], refresh: bool = False) -> dict:
        for action, doc in zip(body[0::2], body[1::2]):
            self.docs[action["index"]["_id"]] = doc
        self._stats.clear()
        return {"errors": False}

    def delete_by_query(self, index: str, body: dict, refresh: bool = False) -> dict:
        term = body["query"]["term"]
        (key, value), = term.items()
        for cid in [i for i, d in self.docs.items() if d.get(key) == value]:
            del self.docs[cid]
        self._stats.clear()
        return {}

    def info(self) -> dict:
        return {"version": {"number": "fake"}}

    def search(self, index: str, body: dict, request_timeout: float | None = None) -> dict:
        self.queries.append(body)
        scored = []
        for cid, doc in self.docs.items():
            score = self._eval(body["query"], cid, doc)
            if score is not None:
                scored.append((score, cid))
        scored.sort(key=lambda x: (-x[0], x[1]))
        hits = [{"_id": cid, "_score": s, "_source": self.docs[cid]} for s, cid in scored[: body.get("size", 10)]]
        return {"hits": {"hits": hits}}

    # -- query evaluation ------------------------------------------------------------------------------------------------
    def _field_stats(self, field: str):
        if field not in self._stats:
            base = field.split(".", 1)[0]
            tf: dict[str, Counter] = {}
            toks: dict[str, list[str]] = {}
            df: Counter = Counter()
            for cid, doc in self.docs.items():
                t = _analyze(field, str(doc.get(base) or ""))
                toks[cid], tf[cid] = t, Counter(t)
                df.update(set(t))
            avg = (sum(len(t) for t in toks.values()) / len(toks)) if toks else 1.0
            self._stats[field] = (tf, toks, avg, df)
        return self._stats[field]

    def _bm25(self, field: str, cid: str, term: str) -> float:
        tf, toks, avg, df = self._field_stats(field)
        f = tf[cid].get(term, 0)
        if not f:
            return 0.0
        n = len(self.docs)
        idf = math.log(1 + (n - df[term] + 0.5) / (df[term] + 0.5))
        return idf * f * (self.K1 + 1) / (f + self.K1 * (1 - self.B + self.B * len(toks[cid]) / avg))

    def _eval(self, q: dict, cid: str, doc: dict) -> float | None:
        """Score, or None when the document does not match."""
        (kind, spec), = q.items()
        if kind == "term":
            (key, value), = spec.items()
            return 0.0 if doc.get(key) == value else None
        if kind == "multi_match":
            best = 0.0
            for f in spec["fields"]:
                name, _, boost = f.partition("^")
                boost = float(boost or 1)
                s = boost * sum(self._bm25(name, cid, t) for t in set(_analyze(name, spec["query"])))
                best = max(best, s)
            return best if best > 0 else None
        if kind == "match_phrase":
            (name, v), = spec.items()
            boost = float(v.get("boost", 1)) if isinstance(v, dict) else 1.0
            phrase = _analyze(name, v["query"] if isinstance(v, dict) else v)
            toks = self._field_stats(name)[1][cid]
            n = len(phrase)
            if n and any(toks[i:i + n] == phrase for i in range(len(toks) - n + 1)):
                return boost * sum(self._bm25(name, cid, t) for t in set(phrase))
            return None
        if kind == "bool":
            total = 0.0
            for clause in _as_list(spec.get("must")) + _as_list(spec.get("filter")):
                s = self._eval(clause, cid, doc)
                if s is None:
                    return None
                total += s
            shoulds = [s for s in (self._eval(c, cid, doc) for c in _as_list(spec.get("should"))) if s is not None]
            required = spec.get("minimum_should_match")
            if required is None:
                required = 1 if (spec.get("should") and not spec.get("must") and not spec.get("filter")) else 0
            if len(shoulds) < int(required):
                return None
            return total + sum(shoulds)
        raise NotImplementedError(f"fake OpenSearch does not implement {kind!r}")


def _as_list(x) -> list:
    return [] if x is None else (x if isinstance(x, list) else [x])


# --------------------------------------------------------------------------------------------------------------------------
# Milvus
# --------------------------------------------------------------------------------------------------------------------------
class FakeMilvusClient:
    """Exact (brute-force) cosine search: no approximation, so it is an upper bound on what HNSW returns."""

    def __init__(self, index_type: str = "HNSW") -> None:
        self.rows: dict[str, dict] = {}
        self.index_type = index_type
        self.search_calls: list[dict] = []  # kwargs of every search(), for tests

    def upsert(self, collection_name: str, data: list[dict]) -> None:
        for r in data:
            self.rows[r["chunk_id"]] = r

    def flush(self, collection_name: str) -> None:
        pass

    def delete(self, collection_name: str, filter: str) -> None:
        doc = re.search(r'document_id == "([^"]+)"', filter).group(1)
        for cid in [i for i, r in self.rows.items() if r["document_id"] == doc]:
            del self.rows[cid]

    def search(self, collection_name, data, anns_field, search_params, limit, filter, output_fields, timeout=None, consistency_level=None):
        self.search_calls.append({"search_params": search_params, "limit": limit, "filter": filter})
        scope = {(d, int(v)) for d, v in re.findall(r'document_id == "([^"]+)" and content_version == (\d+)', filter)}
        rows = [r for r in self.rows.values() if (r["document_id"], r["content_version"]) in scope]
        if not rows:
            return [[]]
        q = np.asarray(data[0], dtype=np.float32)
        mat = np.asarray([r["embedding"] for r in rows], dtype=np.float32)
        sims = (mat @ q) / (np.linalg.norm(mat, axis=1) * np.linalg.norm(q) + 1e-12)
        order = np.argsort(-sims, kind="stable")[:limit]
        return [[{"distance": float(sims[i]), "entity": {f: rows[i][f] for f in output_fields}} for i in order]]

    def query(self, collection_name, filter, output_fields, consistency_level=None):
        ids = set(re.findall(r'"([^"]+)"', filter))
        return [{f: r[f] for f in output_fields} for cid, r in self.rows.items() if cid in ids]

    def list_collections(self):
        return ["fake"]


def group_by_doc(rows: dict[str, dict]) -> dict[str, int]:
    out: dict[str, int] = defaultdict(int)
    for r in rows.values():
        out[r["document_id"]] += 1
    return dict(out)
