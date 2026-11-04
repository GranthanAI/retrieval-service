"""Index generations: which Milvus collection / OpenSearch index a read or a write goes to, and how that generation was built.

A generation is a complete, self-consistent index built by one (parser, chunker, analyzer, embedding model) combination. Generation N
lives in `<MILVUS_COLLECTION_NAME>_g<N>` and `<OPENSEARCH_INDEX_NAME>_g<N>`; vectors and chunk boundaries from different generations are
never mixed in one search space. Exactly one generation is `active` (queries and new ingestion use it); `make reindex GENERATION=N`
fills a `building` one without touching the active one; `make activate GENERATION=N` switches only if the benchmark allows it.

The registry lives in Postgres (`index_generations`). Until a row exists (a fresh checkout, tests, no database) generation 1 is the
legacy un-suffixed pair named by the settings, so nothing changes for an installation that has not adopted generations."""
import contextlib
from contextvars import ContextVar
from dataclasses import dataclass, replace

from app.core.config import settings
from app.stores.index_schema import SCHEMA_V1, SCHEMA_V2

BUILDING, ACTIVE, RETIRED = "building", "active", "retired"


@dataclass(frozen=True)
class GenerationInfo:
    generation: int
    milvus_collection: str
    opensearch_index: str
    parser_version: str = "1"
    chunker_version: str = "1"
    analyzer_version: str = "1"
    embedding_model: str = ""
    embedding_dimension: int = 384
    status: str = ACTIVE
    opensearch_alias: str | None = None   # reads go through the alias when set, so activation is an atomic alias swap

    @property
    def schema_version(self) -> str:
        return SCHEMA_V1 if self.analyzer_version == "1" else SCHEMA_V2

    @property
    def opensearch_read_target(self) -> str:
        return self.opensearch_alias or self.opensearch_index


def names_for(generation: int) -> tuple[str, str]:
    """(milvus collection, opensearch index) of generation N."""
    return f"{settings.MILVUS_COLLECTION_NAME}_g{generation}", f"{settings.OPENSEARCH_INDEX_NAME}_g{generation}"


def legacy() -> GenerationInfo:
    """Generation 1 as it exists before generations are adopted: the un-suffixed names from the settings. Read at call time because
    tests and the benchmark point the settings at throwaway names."""
    return GenerationInfo(1, settings.MILVUS_COLLECTION_NAME, settings.OPENSEARCH_INDEX_NAME,
                          embedding_model=settings.EMBEDDING_MODEL_EXPECTED, embedding_dimension=settings.EMBEDDING_DIMENSION_EXPECTED)


_registered: list[GenerationInfo] = []              # every generation whose physical indexes still exist, active first
_override: ContextVar[GenerationInfo | None] = ContextVar("generation_override", default=None)


def current() -> GenerationInfo:
    """The generation reads use and new ingestion writes to."""
    return _override.get() or next((g for g in _registered if g.status == ACTIVE), None) or legacy()


def all_live() -> list[GenerationInfo]:
    """Every generation that still holds data (active, building and retired-but-not-dropped): a deleted file must vanish from all of
    them, or rolling back to a retired generation would resurrect it."""
    if _override.get():
        return [_override.get()]
    return list(_registered) or [legacy()]


def set_registered(infos: list[GenerationInfo]) -> None:
    _registered[:] = sorted(infos, key=lambda g: (g.status != ACTIVE, g.generation))


@contextlib.contextmanager
def use(info: GenerationInfo):
    """Pin reads and writes in this task to `info` (the benchmark and `make reindex` build a generation this way)."""
    token = _override.set(info)
    try:
        yield info
    finally:
        _override.reset(token)


def with_status(info: GenerationInfo, status: str) -> GenerationInfo:
    return replace(info, status=status)
