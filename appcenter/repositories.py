"""Use Nobara's metadata recovery when available, with standalone DNF fallback."""
from __future__ import annotations


def load_repositories(create_base):
    try:
        from nobara_updater.update_repositories import load_repositories as recover
    except ImportError:
        # App Center also runs outside Nobara and alongside older updaters.
        base = create_base()
        try:
            base.get_repo_sack().load_repos()
        except TypeError:
            base.get_repo_sack().load_repos(True)
        return base
    return recover(create_base)
