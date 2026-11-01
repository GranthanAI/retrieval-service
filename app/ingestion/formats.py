"""Which parser a file goes to, decided from its name (the stored object has no trustworthy type of its own).

  pdf          Docling with table structure (as before)
  docling      Word (docx), PowerPoint (pptx), Excel (xlsx) and HTML: structure-aware extraction by Docling
  markdown     split on headings
  csv          rows grouped under the header row
  text         plain text and source code: grouped by line ranges so a citation points at "Lines 120-240"
  legacy_doc   binary Word (.doc): best effort through an external converter, otherwise a clear error
  archive      .zip: every readable member is parsed with the same rules
  unsupported  anything else (images, executables, ...)
"""
import posixpath

PDF = {"pdf"}
DOCLING = {"docx", "pptx", "xlsx", "html", "htm"}
MARKDOWN = {"md", "markdown", "mdx"}
CSV = {"csv", "tsv"}
LEGACY_DOC = {"doc"}
ARCHIVE = {"zip"}

# Plain text and source code. Deliberately no ".env": it holds secrets and has no business in a search index.
TEXT = {
    "txt", "text", "log", "rst", "adoc", "asciidoc", "tex", "org", "srt", "vtt",
    "json", "jsonl", "ndjson", "yaml", "yml", "toml", "ini", "cfg", "conf", "properties", "xml", "ipynb", "gql", "graphql", "proto",
    "sql", "py", "pyi", "js", "jsx", "mjs", "cjs", "ts", "tsx", "java", "kt", "kts", "scala", "groovy", "gradle",
    "c", "h", "cc", "cpp", "cxx", "hpp", "hh", "cs", "go", "rs", "rb", "php", "swift", "m", "mm", "dart", "lua", "r", "jl", "pl", "pm",
    "sh", "bash", "zsh", "fish", "ps1", "bat", "cmd", "vue", "svelte", "css", "scss", "sass", "less", "tf", "tfvars", "hcl",
    "ex", "exs", "erl", "hs", "clj", "cljs", "fs", "ml", "nim", "zig", "v", "sol", "asm", "s", "cmake", "mk", "makefile",
}
# Well-known extensionless text files, matched on the whole (lower-cased) name.
TEXT_NAMES = {"dockerfile", "makefile", "readme", "license", "licence", "changelog", "authors", "contributing", "notice", "procfile",
              "gemfile", "rakefile", "jenkinsfile", "vagrantfile", "codeowners", "requirements"}

# Never worth indexing from inside an archive (dependency trees, VCS data, OS metadata, build output).
NOISE_DIRS = {"__macosx", ".git", ".svn", ".hg", "node_modules", "__pycache__", ".venv", "venv", ".idea", ".vscode", "dist", "build",
              "target", ".next", ".gradle", ".tox", ".mypy_cache", ".pytest_cache", "site-packages"}
NOISE_FILES = {".ds_store", "thumbs.db", "desktop.ini", ".env"}


def extension(filename: str) -> str:
    """Lower-cased extension without the dot ("" when there is none)."""
    base = posixpath.basename(filename.replace("\\", "/")).lower()
    return base.rsplit(".", 1)[1] if "." in base.strip(".") else ""


def classify(filename: str) -> str:
    ext = extension(filename)
    base = posixpath.basename(filename.replace("\\", "/")).lower()
    if ext in PDF:
        return "pdf"
    if ext in DOCLING:
        return "docling"
    if ext in MARKDOWN:
        return "markdown"
    if ext in CSV:
        return "csv"
    if ext in LEGACY_DOC:
        return "legacy_doc"
    if ext in ARCHIVE:
        return "archive"
    if ext in TEXT or (not ext and base in TEXT_NAMES) or base in TEXT_NAMES:
        return "text"
    return "unsupported"


def is_noise(path: str) -> bool:
    """A path inside an archive that should be skipped without comment (see NOISE_*)."""
    parts = [p.lower() for p in path.replace("\\", "/").split("/") if p]
    if not parts:
        return True
    return parts[-1] in NOISE_FILES or any(p in NOISE_DIRS for p in parts[:-1])
