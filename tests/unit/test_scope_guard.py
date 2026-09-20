import pytest

from app.core.enums import RetrievalScope
from app.query import scope_guard


class TestScopeGuard:
    def test_files_scope_with_file_ids_is_accepted(self) -> None:
        assert scope_guard.check(RetrievalScope.FILES, ["doc-a"]) is True

    def test_files_scope_with_empty_file_ids_is_rejected(self) -> None:
        assert scope_guard.check(RetrievalScope.FILES, []) is False

    @pytest.mark.parametrize("scope", [RetrievalScope.KNOWLEDGE_BASE, RetrievalScope.ALL])
    def test_non_files_scope_is_rejected_even_with_file_ids(self, scope: RetrievalScope) -> None:
        assert scope_guard.check(scope, ["doc-a"]) is False

    def test_non_files_scope_with_no_file_ids_is_also_rejected(self) -> None:
        assert scope_guard.check(RetrievalScope.KNOWLEDGE_BASE, []) is False
