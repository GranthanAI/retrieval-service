"""Reading a .zip safely, so every readable file inside it is indexed with the same rules as an upload of its own.

Nothing is extracted to disk (so there is no path traversal to exploit): members are read into memory with hard caps, and the
limits below bound the work an archive can cause, whatever its headers claim:

  * ARCHIVE_MAX_ENTRIES          more entries than this: refuse the archive (a zip of a million empty files)
  * ARCHIVE_MAX_MEMBER_BYTES     a member is read through a capped stream; over the cap it is skipped, never fully expanded
  * ARCHIVE_MAX_TOTAL_BYTES      total bytes read across the archive; reading stops there
  * ARCHIVE_MAX_COMPRESSION_RATIO  a large member that compresses better than this is treated as a decompression bomb and skipped
  * ARCHIVE_MAX_DEPTH            archives inside archives are opened up to this depth
  * encrypted members and symbolic links are skipped; absolute paths, drive letters and ".." are neutralised in the name that is shown
"""
import io
import posixpath
import stat
import zipfile
from collections import Counter
from dataclasses import dataclass, field
from typing import Callable

from app.core.config import settings
from app.ingestion import formats
from app.ingestion.parsed import ParsedDocument, ParsedSection, ParserError

_BOMB_CHECK_MIN_BYTES = 1_000_000          # tiny files can compress extremely well without being a threat
_HEAVY_KINDS = {"pdf", "docling", "legacy_doc"}
_HEADING_LIMIT = 200


@dataclass
class _Budget:
    total_read: int = 0
    heavy_members: int = 0
    included: list[str] = field(default_factory=list)
    skipped: Counter = field(default_factory=Counter)


def safe_member_path(name: str) -> str | None:
    """The name shown for a member: forward slashes, no leading slash, no drive letter, no "." / ".." segments. None when
    nothing usable is left (or the name tries to climb out of the archive)."""
    name = name.replace("\\", "/")
    if len(name) > 1 and name[1] == ":":
        name = name[2:]
    parts = [p for p in name.split("/") if p not in ("", ".")]
    if not parts or any(p == ".." for p in parts):
        return None
    return "/".join(parts)[:_HEADING_LIMIT]


def parse_archive(raw: bytes, filename: str, parse_member: Callable[[bytes, str], ParsedDocument], depth: int = 0) -> ParsedDocument:
    """Parse every readable member and return their sections, each headed by the member's path. `parse_member(data, name)`
    parses one non-archive file (it raises ParserError for a file that cannot be read; that file is skipped, not fatal)."""
    budget = _Budget()
    sections = _walk(raw, filename, parse_member, depth, "", budget)
    if not sections:
        reasons = ", ".join(f"{count} {why}" for why, count in budget.skipped.most_common()) or "it is empty"
        raise ParserError(f"{filename!r} has no readable files ({reasons}). Supported: documents, text, Markdown, CSV and code.")
    listing = "\n".join(budget.included[:500]) + (f"\n... and {len(budget.included) - 500} more" if len(budget.included) > 500 else "")
    skipped = "; ".join(f"{count} skipped ({why})" for why, count in budget.skipped.most_common())
    overview = f"Files in {filename} ({len(budget.included)} indexed" + (f", {skipped}" if skipped else "") + "):\n" + listing
    return ParsedDocument([ParsedSection(heading="Archive contents", page_number=None, text=overview), *sections])


def _walk(raw: bytes, archive_name: str, parse_member, depth: int, prefix: str, budget: _Budget) -> list[ParsedSection]:
    try:
        zf = zipfile.ZipFile(io.BytesIO(raw))
    except zipfile.BadZipFile as exc:
        raise ParserError(f"{archive_name!r} is not a valid zip file") from exc
    infos = zf.infolist()
    if len(infos) > settings.ARCHIVE_MAX_ENTRIES:
        raise ParserError(f"{archive_name!r} has {len(infos)} entries; the limit is {settings.ARCHIVE_MAX_ENTRIES}")

    out: list[ParsedSection] = []
    for info in infos:
        if info.is_dir():
            continue
        path = safe_member_path(info.filename)
        if path is None:
            budget.skipped["unsafe name"] += 1
            continue
        if formats.is_noise(path):
            continue                                                   # dependency trees, VCS data, OS metadata: not counted
        shown = prefix + path
        if info.flag_bits & 0x1:
            budget.skipped["password-protected"] += 1
            continue
        if stat.S_ISLNK(info.external_attr >> 16):
            budget.skipped["symbolic link"] += 1
            continue
        kind = formats.classify(path)
        if kind == "unsupported":
            budget.skipped["unsupported type"] += 1
            continue
        if info.file_size > settings.ARCHIVE_MAX_MEMBER_BYTES:
            budget.skipped["too large"] += 1
            continue
        if (info.file_size > _BOMB_CHECK_MIN_BYTES and info.compress_size > 0
                and info.file_size / info.compress_size > settings.ARCHIVE_MAX_COMPRESSION_RATIO):
            budget.skipped["suspicious compression ratio"] += 1
            continue
        if budget.total_read >= settings.ARCHIVE_MAX_TOTAL_BYTES:
            budget.skipped["archive size limit reached"] += 1
            continue
        if kind in _HEAVY_KINDS and budget.heavy_members >= settings.ARCHIVE_MAX_DOCUMENT_MEMBERS:
            budget.skipped["too many documents"] += 1
            continue

        try:
            with zf.open(info) as handle:
                data = handle.read(settings.ARCHIVE_MAX_MEMBER_BYTES + 1)         # capped stream: headers can lie, this cannot
        except (zipfile.BadZipFile, NotImplementedError, RuntimeError, OSError, EOFError):
            budget.skipped["unreadable"] += 1
            continue
        if len(data) > settings.ARCHIVE_MAX_MEMBER_BYTES:
            budget.skipped["too large"] += 1
            continue
        budget.total_read += len(data)

        if kind == "archive":
            if depth + 1 >= settings.ARCHIVE_MAX_DEPTH:
                budget.skipped["nested too deep"] += 1
                continue
            try:
                out.extend(_walk(data, shown, parse_member, depth + 1, shown + "/", budget))
            except ParserError:
                budget.skipped["unreadable"] += 1
            continue

        if kind in _HEAVY_KINDS:
            budget.heavy_members += 1
        try:
            parsed = parse_member(data, path)
        except ParserError:
            budget.skipped["unreadable"] += 1
            continue
        except Exception:                                              # one broken member never fails the whole archive
            budget.skipped["unreadable"] += 1
            continue
        member_sections = [s for s in parsed.sections if s.text.strip()]
        if not member_sections:
            budget.skipped["no text"] += 1
            continue
        budget.included.append(shown)
        for s in member_sections:
            label = f"{shown} › {s.heading}" if s.heading else shown
            out.append(ParsedSection(heading=label[:_HEADING_LIMIT], page_number=s.page_number, text=s.text))
    return out
