"""Keep nobara-sync's result protocol separate from visible update messages."""
from __future__ import annotations

import json
import re

_ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
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
        internal, prefix, result = result_line(line)
        if not internal:
            lines.append(line)
        elif result:
            lines.append(prefix + result["message"])
    return "\n".join(lines).strip()
