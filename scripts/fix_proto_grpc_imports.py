"""Post-processes grpc_tools.protoc's generated `*_pb2_grpc.py` files so
their `import X_pb2 as X__pb2` lines become `from app.grpc import X_pb2 as
X__pb2` — protoc emits flat top-level imports that don't match this
package's layout (`app/grpc/`), matching the fixup llm-service/
embedding-service also apply.

Deliberately a real script, not a one-line `python -c` fixup: an earlier
version of that one-liner opened each file in write mode (truncating it)
as part of evaluating the same expression that read it, silently wiping
every generated `_pb2_grpc.py` file it "fixed." Read-then-write as two
explicit, sequential steps to make that class of bug structurally
impossible to reintroduce.
"""

import glob
import re

PATTERN = re.compile(r"import (\w+_pb2) as (\w+__pb2)")
REPLACEMENT = r"from app.grpc import \1 as \2"


def main() -> None:
    for path in glob.glob("app/grpc/*_pb2_grpc.py"):
        with open(path, encoding="utf-8") as f:
            content = f.read()

        fixed = PATTERN.sub(REPLACEMENT, content)

        with open(path, "w", encoding="utf-8") as f:
            f.write(fixed)


if __name__ == "__main__":
    main()
