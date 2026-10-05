from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Iterable


@dataclass(slots=True)
class AppEntry:
    appstream_id: str
    name: str
    summary: str
    description: str
    pkg_names: list[str] = field(default_factory=list)
    categories: list[str] = field(default_factory=list)
    keywords: list[str] = field(default_factory=list)
    screenshots: list[str] = field(default_factory=list)
    launchables: list[str] = field(default_factory=list)
    icon_name: str | None = None
    icon_path: str | None = None
    icon_url: str | None = None
    homepage_url: str | None = None
    kind: str = "DESKTOP_APP"
    installed: bool = False
    installed_version: str | None = None
    candidate_version: str | None = None
    repo_ids: list[str] = field(default_factory=list)

    @property
    def primary_pkg(self) -> str | None:
        return self.pkg_names[0] if self.pkg_names else None


def search_terms(text: str | None) -> list[str]:
    """Split a search into words. Like `dnf search`, every word has to match, in any order."""
    return str(text or "").casefold().split()


def filter_search(
    apps: Iterable[AppEntry],
    terms: list[str],
    matches: Callable[[AppEntry, str], bool],
) -> list[AppEntry]:
    """Keep the apps that match every word; each word narrows the results of the one before."""
    items = list(apps)
    for term in terms:
        items = [app for app in items if matches(app, term)]
    return items


def search_sort_key(
    terms: list[str],
    matches: Callable[[AppEntry, str], bool],
    rank_key: Callable[[AppEntry, str], tuple],
) -> Callable[[AppEntry], tuple]:
    """Return a sort key for a search. Results containing the whole search text come
    first, in their usual order; results that only contain each word follow, ranked
    by their worst-matching word."""
    phrase = " ".join(terms)
    if len(terms) == 1:
        return lambda app: rank_key(app, phrase)

    def key(app: AppEntry) -> tuple:
        if matches(app, phrase):
            return (0, rank_key(app, phrase))
        return (1, max(rank_key(app, term) for term in terms))

    return key


def is_hidden_debug_package_name(name: str | None) -> bool:
    if not name:
        return False
    value = str(name).casefold()
    return value.endswith("-debuginfo") or value.endswith("-debugsource") or value in {"debuginfo", "debugsource"}


def is_non_app_package_name(name: str | None) -> bool:
    if not name:
        return False
    value = str(name).casefold()
    return (
        is_hidden_debug_package_name(value)
        or value.endswith("-devel")
        or value.endswith("-static")
        or value in {"devel", "static"}
    )


def is_likely_library_only_name(name: str | None) -> bool:
    if not name:
        return False
    value = str(name).casefold()
    if not value.startswith("lib"):
        return False
    allow = ("libreoffice", "librewolf", "librecad", "libation")
    return not value.startswith(allow)


def should_hide_from_standard_catalog(app: AppEntry) -> bool:
    pkg_names = [pkg for pkg in app.pkg_names if pkg]
    if any(is_non_app_package_name(pkg) for pkg in pkg_names):
        return True

    if app.kind == "PACKAGE":
        primary = app.primary_pkg or app.name
        summary = (app.summary or "").casefold()
        description = (app.description or "").casefold()
        if is_likely_library_only_name(primary):
            return True
        if "shared library" in summary or "shared library" in description:
            return True
        if "runtime library" in summary or "runtime library" in description:
            return True
        if summary.startswith("libraries ") or " libraries" in summary:
            return True
        if description.startswith("libraries ") or " libraries" in description:
            return True

    return False
