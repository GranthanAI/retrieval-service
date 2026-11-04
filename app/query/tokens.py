from functools import lru_cache

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

_WORDS_PER_TOKEN = 0.75  # only used when the tokenizer cannot be loaded


def _load(local_only: bool):
    from huggingface_hub import hf_hub_download
    from tokenizers import Tokenizer

    path = hf_hub_download(settings.TOKEN_COUNT_TOKENIZER_ID, "tokenizer.json", local_files_only=local_only)
    tok = Tokenizer.from_file(path)
    # The shipped tokenizer.json truncates to 128 and pads to 128, which would make every count 128 or less.
    tok.no_truncation()
    tok.no_padding()
    return tok


@lru_cache(maxsize=1)
def _tokenizer():
    """The configured tokenizer from the LOCAL Hugging Face cache only. A query must never wait on the network (a cold-cache download
    inside a request blew the 1.8 s pipeline deadline); `warm()` at startup is where a download is allowed."""
    try:
        return _load(local_only=True)
    except Exception as exc:  # noqa: BLE001 - not cached locally: degrade to the estimate
        logger.warning("tokens.tokenizer_not_cached", tokenizer=settings.TOKEN_COUNT_TOKENIZER_ID, error=type(exc).__name__)
        return None


def warm() -> bool:
    """Called once at startup, off the request path. Downloads the tokenizer if it is not cached yet, then primes the cache.
    Returns whether real token counting is available."""
    if _tokenizer() is None:
        try:
            _load(local_only=False)
        except Exception as exc:  # noqa: BLE001
            logger.warning("tokens.warm_failed", tokenizer=settings.TOKEN_COUNT_TOKENIZER_ID, error=str(exc))
            return False
        _tokenizer.cache_clear()
    return _tokenizer() is not None


def count(text: str) -> int:
    """Token count used for `max_tokens` and `total_tokens`. A proxy for the LLM's tokenizer (unknown here), but a real one."""
    if not text:
        return 0
    tok = _tokenizer()
    if tok is None:
        return max(1, int(len(text.split()) / _WORDS_PER_TOKEN))
    return len(tok.encode(text, add_special_tokens=False).ids)
