"""The embedding model's own tokenizer, used to size chunks so that what is indexed is what the model actually reads.

all-MiniLM-L6-v2 reads 256 wordpieces and silently drops the rest, so chunk limits are expressed in ITS tokens, not words. The model's
name, dimension, max_seq_length and tokenizer id come from embedding-service's `GetModelInfo` RPC (`ModelProfile`); the tokenizer is
loaded locally from that id and the model name is validated against EMBEDDING_MODEL_EXPECTED.

Ingestion is off the request path, so a tokenizer download is allowed here (unlike `app/query/tokens.py`)."""
import math
import re
from dataclasses import dataclass
from functools import lru_cache
from typing import Protocol

from app.core.config import settings
from app.core.exceptions import EmbeddingModelMismatchError
from app.core.logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class ModelProfile:
    model: str
    dimension: int
    max_seq_length: int
    tokenizer_id: str


class TokenCounter(Protocol):
    name: str

    def count(self, text: str) -> int: ...

    def split(self, text: str, max_tokens: int) -> list[str]:
        """Last-resort split of `text` into consecutive pieces of at most `max_tokens` tokens."""
        ...


class HfTokenCounter:
    """Counts with the Hugging Face tokenizer of the embedding model (special tokens excluded; the model adds [CLS]/[SEP], which is
    why chunk limits leave a margin below max_seq_length)."""

    def __init__(self, tokenizer_id: str) -> None:
        from huggingface_hub import hf_hub_download
        from tokenizers import Tokenizer

        try:
            path = hf_hub_download(tokenizer_id, "tokenizer.json", local_files_only=True)
        except Exception:  # noqa: BLE001 - not cached yet: ingestion may download
            path = hf_hub_download(tokenizer_id, "tokenizer.json")
        tok = Tokenizer.from_file(path)
        tok.no_truncation()
        tok.no_padding()
        self._tok = tok
        self.name = f"hf:{tokenizer_id}"

    def count(self, text: str) -> int:
        return len(self._tok.encode(text, add_special_tokens=False).ids) if text else 0

    def split(self, text: str, max_tokens: int) -> list[str]:
        enc = self._tok.encode(text, add_special_tokens=False)
        offsets = enc.offsets
        pieces: list[str] = []
        start_char = 0
        for i in range(0, len(offsets), max_tokens):
            last = offsets[min(i + max_tokens, len(offsets)) - 1][1]
            end_char = len(text) if i + max_tokens >= len(offsets) else last
            piece = text[start_char:end_char].strip()
            if piece:
                pieces.append(piece)
            start_char = end_char
        return pieces


class WordTokenCounter:
    """Fallback when no tokenizer can be loaded (offline CI): about 1.35 wordpieces per word. Clearly an estimate; chunks built with
    it are sized with extra margin by the caller and the generation record says which counter was used."""

    name = "words*1.35"

    def count(self, text: str) -> int:
        return math.ceil(len(text.split()) * 1.35) if text.strip() else 0

    def split(self, text: str, max_tokens: int) -> list[str]:
        words = text.split()
        per = max(1, int(max_tokens / 1.35))
        return [" ".join(words[i:i + per]) for i in range(0, len(words), per)]


@lru_cache(maxsize=4)
def load_counter(tokenizer_id: str) -> TokenCounter:
    try:
        return HfTokenCounter(tokenizer_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("tokenization.fallback_to_word_estimate", tokenizer=tokenizer_id, error=str(exc))
        return WordTokenCounter()


def validate_profile(profile: ModelProfile) -> None:
    """Refuse to chunk for a model other than the one this service is configured for (a silent embedding-service upgrade would
    otherwise index vectors from a different model into the same search space)."""
    if profile.model != settings.EMBEDDING_MODEL_EXPECTED or profile.dimension != settings.EMBEDDING_DIMENSION_EXPECTED:
        raise EmbeddingModelMismatchError(
            f"embedding-service reports model={profile.model!r} dimension={profile.dimension}, expected "
            f"model={settings.EMBEDDING_MODEL_EXPECTED!r} dimension={settings.EMBEDDING_DIMENSION_EXPECTED}"
        )


_SENTENCE = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9\"'(\[])")


def sentences(text: str) -> list[str]:
    return [s for s in _SENTENCE.split(text.strip()) if s.strip()]
