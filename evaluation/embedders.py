"""Embedders for the benchmark. Both expose the two methods `pipeline.run` / the ingestion path call on `EmbeddingClient`."""
import asyncio
import hashlib
import math
import re

DIM = 384


class FakeEmbedder:
    """Deterministic and model-free, for CI: signed feature hashing of word unigrams and bigrams into 384 dims, L2-normalised.
    It is lexical, not semantic, so fake-mode numbers are a regression signal for the pipeline, never a quality claim."""

    name = "fake-hash-384"
    dimension = DIM

    @staticmethod
    def _vec(text: str) -> list[float]:
        words = re.findall(r"\w+", text.lower())
        feats = words + [f"{a}_{b}" for a, b in zip(words, words[1:])]
        v = [0.0] * DIM
        for f in feats:
            h = int.from_bytes(hashlib.md5(f.encode()).digest()[:8], "big")
            v[h % DIM] += 1.0 if (h >> 63) & 1 else -1.0
        n = math.sqrt(sum(x * x for x in v)) or 1.0
        return [x / n for x in v]

    async def embed_query(self, text: str) -> list[float]:
        return self._vec(text)

    async def embed_chunks(self, texts: list[str]) -> list[list[float]]:
        return [self._vec(t) for t in texts]


class RealEmbedder:
    """all-MiniLM-L6-v2 loaded locally through sentence-transformers, configured like embedding-service (normalised vectors, no
    prefixes, the model's own 256-wordpiece truncation). Loaded from the local Hugging Face cache; never downloads silently."""

    name = "all-MiniLM-L6-v2"
    dimension = DIM

    def __init__(self, model: str = "sentence-transformers/all-MiniLM-L6-v2") -> None:
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:  # pragma: no cover - environment dependent
            raise SystemExit("--real needs sentence-transformers: run `uv sync --group eval`") from exc
        self._model = SentenceTransformer(model, device="cpu")
        self.max_seq_length = self._model.max_seq_length

    def _encode(self, texts: list[str]) -> list[list[float]]:
        return self._model.encode(texts, normalize_embeddings=True, batch_size=32, show_progress_bar=False).tolist()

    async def embed_query(self, text: str) -> list[float]:
        return (await asyncio.to_thread(self._encode, [text]))[0]

    async def embed_chunks(self, texts: list[str]) -> list[list[float]]:
        return await asyncio.to_thread(self._encode, texts) if texts else []
