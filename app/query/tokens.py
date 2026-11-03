from functools import lru_cache

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

_WORDS_PER_TOKEN = 0.75  # only used when the tokenizer cannot be loaded


@lru_cache(maxsize=1)
def _tokenizer():
    """The configured Hugging Face tokenizer, or None (with one warning) if it cannot be loaded, e.g. offline with a cold cache."""
    try:
        from tokenizers import Tokenizer

        tok = Tokenizer.from_pretrained(settings.TOKEN_COUNT_TOKENIZER_ID)
        # The shipped tokenizer.json truncates to 128 and pads to 128, which would make every count 128 or less.
        tok.no_truncation()
        tok.no_padding()
        return tok
    except Exception as exc:  # noqa: BLE001 - any load failure degrades to the estimate
        logger.warning("tokens.tokenizer_unavailable", tokenizer=settings.TOKEN_COUNT_TOKENIZER_ID, error=str(exc))
        return None


def count(text: str) -> int:
    """Token count used for `max_tokens` and `total_tokens`. A proxy for the LLM's tokenizer (unknown here), but a real one."""
    if not text:
        return 0
    tok = _tokenizer()
    if tok is None:
        return max(1, int(len(text.split()) / _WORDS_PER_TOKEN))
    return len(tok.encode(text, add_special_tokens=False).ids)
