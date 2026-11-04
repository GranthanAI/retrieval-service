"""retrieval.proto is vendored byte-for-byte into llm-service (its Makefile/CI diff it). If both repos are checked out side by side,
they must not drift; embedding.proto is vendored here from embedding-service the same way."""
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("mine,theirs", [
    ("protos/retrieval.proto", "../llm-service/proto/retrieval.proto"),
    ("protos/embedding.proto", "../embedding-service/protos/embedding.proto"),
])
def test_vendored_proto_is_identical(mine, theirs):
    other = (ROOT / theirs).resolve()
    if not other.exists():
        pytest.skip(f"{theirs} not checked out next to this repo")
    assert (ROOT / mine).read_bytes().replace(b"\r\n", b"\n") == other.read_bytes().replace(b"\r\n", b"\n")
