"""Keep nobara-sync's result protocol separate from visible update messages."""
from __future__ import annotations

import json
import re

_ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
_PROGRESS = re.compile(r"\bNOBARA_UPDATE_PROGRESS(?:\s+|$)")
_MARKER = re.compile(r"\bNOBARA_UPDATE_RESULT(?:\s+|$)")


def result_line(line: str) -> tuple[bool, str, dict]:
    """Recognize status lines, including logging prefixes and terminal colors."""
    clean = _ANSI.sub("", line)
    marker = _MARKER.search(clean)
    if marker is None:
        return False, "", {}
    try:
        result = json.loads(clean[marker.end():].strip())
        if isinstance(result, dict) and isinstance(result.get("message"), str):
            return True, clean[:marker.start()], result
    except ValueError:
        pass
    return True, clean[:marker.start()], {}


def progress_line(line: str) -> tuple[bool, dict]:
    clean = _ANSI.sub("", line)
    marker = _PROGRESS.search(clean)
    if marker is None:
        return False, {}
    try:
        result = json.loads(clean[marker.end():].strip())
        if isinstance(result, dict) and result.get("version") == 1 and result.get("event") in {"plan", "package"}:
            return True, result
    except ValueError:
        pass
    return True, {}


def internal_line(line: str) -> bool:
    return result_line(line)[0] or progress_line(line)[0]


def sync_arguments(values) -> list[str]:
    """Whitelist the frontend contract; never forward arbitrary DNF/CLI options."""
    args = []
    for value in values:
        if value in {"--all", "--progress"}:
            args.append(value)
        elif isinstance(value, str) and value.startswith("--package="):
            name = value[len("--package="):]
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9+_.-]{0,254}", name):
                raise ValueError("Invalid selected package name.")
            args.append(value)
        else:
            raise ValueError("Unsupported system update argument.")
    return list(dict.fromkeys(args))


def last_result(lines: list[str]) -> dict:
    for line in reversed(lines):
        _, _, result = result_line(line)
        if result:
            return result
    return {}


def visible_text(text: str) -> str:
    """A final display guard for legacy helpers and multiline error messages."""
    lines = []
    for line in text.splitlines():
        if progress_line(line)[0]:
            continue
        internal, prefix, result = result_line(line)
        if not internal:
            lines.append(line)
        elif result:
            lines.append(prefix + result["message"])
    return "\n".join(lines).strip()
