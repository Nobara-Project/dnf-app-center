from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable
import json
import os
import subprocess
import sys
import threading
from pathlib import Path

import gi
gi.require_version("Gio", "2.0")
from gi.repository import Gio

from .models import AppEntry


class DnfUnavailable(RuntimeError):
    pass


@dataclass(slots=True)
class PackageState:
    installed: bool
    installed_version: str | None = None
    candidate_version: str | None = None
    repo_ids: list[str] | None = None
    installed_arch: str | None = None
    candidate_arch: str | None = None


class DnfBackend:
    def __init__(self) -> None:
        try:
            import libdnf5  # type: ignore
        except Exception as exc:  # pragma: no cover - environment dependent
            raise DnfUnavailable(
                "Could not import libdnf5 Python bindings. Install 'python3-libdnf5'."
            ) from exc

        self.libdnf5 = libdnf5
        self.base = None
        self._package_search_cache: dict[str, AppEntry] | None = None
        self._desktop_entry_cache: dict[str, dict] | None = None
        self._helper_proc: subprocess.Popen[str] | None = None
        self._helper_lock = threading.Lock()
        self._repo_priority_cache: dict[str, tuple[int, str]] = {}
        self.cache_authorization = True
        self.reload_state()

    def _create_base(self, force_refresh: bool = False):
        base = self.libdnf5.base.Base()
        base.load_config()
        base.setup()

        repo_sack = base.get_repo_sack()
        repo_sack.create_repos_from_system_configuration()
        if force_refresh:
            repo_query = self.libdnf5.repo.RepoQuery(base)
            try:
                repo_query.filter_enabled(True)
            except Exception:
                pass
            for repo in repo_query:
                try:
                    repo.expire()
                except Exception:
                    pass
        try:
            repo_sack.load_repos()
        except TypeError:
            repo_sack.load_repos(True)
        return base

    def reload_state(self, force_refresh: bool = False) -> None:
        self.base = self._create_base(force_refresh=force_refresh)
        self._repo_priority_cache = self._build_repo_priority_cache()
        self._invalidate_package_search_cache()

    def set_cache_authorization(self, enabled: bool) -> None:
        self.cache_authorization = bool(enabled)
        if not self.cache_authorization:
            self.shutdown()

    def shutdown(self) -> None:
        with self._helper_lock:
            proc = self._helper_proc
            self._helper_proc = None
            if proc is None:
                return
            try:
                if proc.stdin is not None:
                    proc.stdin.write(json.dumps({"cmd": "quit"}) + "\n")
                    proc.stdin.flush()
            except Exception:
                pass
            try:
                proc.terminate()
            except Exception:
                pass
            try:
                proc.wait(timeout=2)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

    def get_package_state(self, pkg_name: str, repo_id: str = "__all__") -> PackageState:
        installed_pkg = self._get_installed_package(pkg_name)
        installed_arch = self._get_pkg_arch(installed_pkg)
        available_pkg = self._select_best_available_package(pkg_name, repo_id=repo_id, preferred_arch=installed_arch)

        repo_ids: list[str] = []
        if available_pkg is not None:
            repo_value = self._safe_pkg_text(available_pkg, "get_repo_id")
            if repo_value:
                repo_ids.append(repo_value)

        return PackageState(
            installed=installed_pkg is not None,
            installed_version=self._get_pkg_evr(installed_pkg),
            candidate_version=self._get_pkg_evr(available_pkg),
            repo_ids=repo_ids,
            installed_arch=self._get_pkg_arch(installed_pkg),
            candidate_arch=self._get_pkg_arch(available_pkg),
        )

    def enrich_apps(self, apps: Iterable) -> None:
        for app in apps:
            self.refresh_app(app)

    def refresh_apps(self, apps: Iterable) -> None:
        self.reload_state()
        self.enrich_apps(apps)

    def refresh_app(self, app) -> None:
        state = None
        for pkg_name in app.pkg_names:
            current = self.get_package_state(pkg_name)
            if current.installed or current.candidate_version:
                state = current
                break
        if state is None:
            app.installed = False
            app.installed_version = None
            app.candidate_version = None
            app.repo_ids = []
            return
        app.installed = state.installed
        app.installed_version = state.installed_version
        app.candidate_version = state.candidate_version
        app.repo_ids = state.repo_ids or []

    def _invalidate_package_search_cache(self) -> None:
        self._package_search_cache = None

    def _get_installed_package(self, pkg_name: str):
        installed_q = self.libdnf5.rpm.PackageQuery(self.base)
        installed_q.filter_name([pkg_name])
        installed_q.filter_installed()

        # For packages with multiple versions installed (e.g., kernels),
        # use filter_latest_evr to get only the newest version(s)
        try:
            installed_q.filter_latest_evr()
        except TypeError:
            installed_q.filter_latest_evr(True)

        return next(iter(installed_q), None)

    def _available_packages_for_name(self, pkg_name: str, repo_id: str = "__all__", preferred_arch: str | None = None) -> list:
        query = self.libdnf5.rpm.PackageQuery(self.base)
        query.filter_name([pkg_name])
        filter_available = getattr(query, "filter_available", None)
        if callable(filter_available):
            try:
                filter_available()
            except TypeError:
                try:
                    filter_available(True)
                except Exception:
                    pass
            except Exception:
                pass
        if repo_id != "__all__":
            try:
                query.filter_repo_id([repo_id])
            except Exception:
                pass
        packages = []
        preferred = (preferred_arch or "").strip()
        for pkg in query:
            repo_value = self._safe_pkg_text(pkg, "get_repo_id")
            if not repo_value or repo_value.startswith("@"):
                continue
            pkg_arch = self._get_pkg_arch(pkg)
            if preferred:
                # Updates/candidates must match the installed package arch.
                # Allow exact matches and noarch as a fallback.
                if pkg_arch not in {preferred, "noarch"}:
                    continue
            packages.append(pkg)
        return packages

    def _build_repo_priority_cache(self) -> dict[str, tuple[int, str]]:
        cache: dict[str, tuple[int, str]] = {}
        repo_query = self.libdnf5.repo.RepoQuery(self.base)
        for repo in repo_query:
            try:
                enabled = bool(repo.is_enabled())
            except Exception:
                enabled = True
            if not enabled:
                continue
            repo_id = str(getattr(repo, "get_id", lambda: "")() or "")
            if not repo_id:
                continue
            priority = self._repo_priority_value(repo)
            name = str(getattr(repo, "get_name", lambda: "")() or repo_id)
            cache[repo_id] = (priority, name)
        return cache

    def _repo_priority_value(self, repo) -> int:
        getter = getattr(repo, "get_priority", None)
        raw_priority = 0
        if getter is not None:
            try:
                raw_priority = int(getter())
            except Exception:
                raw_priority = 0
        # Match requested semantics:
        #   1   = highest priority
        #   100 = lowest explicit priority
        #   0 / unset = treated as 101 (below all explicit priorities)
        if raw_priority <= 0:
            return 101
        if raw_priority > 100:
            return 100
        return raw_priority

    def _repo_priority_for_id(self, repo_id: str) -> int:
        return self._repo_priority_cache.get(repo_id, (0, repo_id))[0]

    def _select_best_available_package(self, pkg_name: str, repo_id: str = "__all__", preferred_arch: str | None = None):
        packages = self._available_packages_for_name(pkg_name, repo_id=repo_id, preferred_arch=preferred_arch)
        if not packages:
            return None

        best_priority = min(self._repo_priority_for_id(self._safe_pkg_text(pkg, "get_repo_id")) for pkg in packages)
        best_repo_ids = []
        for pkg in packages:
            repo_value = self._safe_pkg_text(pkg, "get_repo_id")
            if self._repo_priority_for_id(repo_value) == best_priority and repo_value not in best_repo_ids:
                best_repo_ids.append(repo_value)

        best_query = self.libdnf5.rpm.PackageQuery(self.base)
        best_query.filter_name([pkg_name])
        try:
            best_query.filter_repo_id(best_repo_ids)
        except Exception:
            pass
        if preferred_arch:
            try:
                best_query.filter_arch([preferred_arch, "noarch"])
            except Exception:
                pass
        filter_available = getattr(best_query, "filter_available", None)
        if callable(filter_available):
            try:
                filter_available()
            except TypeError:
                try:
                    filter_available(True)
                except Exception:
                    pass
            except Exception:
                pass
        try:
            best_query.filter_latest_evr()
        except TypeError:
            try:
                best_query.filter_latest_evr(True)
            except Exception:
                pass
        best_pkg = next((pkg for pkg in best_query if not self._safe_pkg_text(pkg, "get_repo_id").startswith("@")), None)
        if best_pkg is not None:
            return best_pkg

        candidates = [pkg for pkg in packages if self._repo_priority_for_id(self._safe_pkg_text(pkg, "get_repo_id")) == best_priority]
        return next(iter(candidates), None)


    def get_repositories(self) -> list[dict[str, str]]:
        repo_query = self.libdnf5.repo.RepoQuery(self.base)
        rows: list[dict[str, str]] = []
        for repo in repo_query:
            try:
                enabled = bool(repo.is_enabled())
            except Exception:
                enabled = True
            rows.append(
                {
                    "id": str(getattr(repo, "get_id", lambda: "")()),
                    "name": str(getattr(repo, "get_name", lambda: "")() or ""),
                    "baseurl": self._first_non_empty(
                        self._sequence_to_list(getattr(repo, "get_mirrors", lambda: [])()),
                        self._sequence_to_list(getattr(repo, "get_baseurl", lambda: [])()),
                    ),
                    "enabled": enabled,
                }
            )
        rows.sort(key=lambda item: (item["name"] or item["id"]).casefold())
        return rows

    def get_enabled_repos(self) -> list[dict[str, str]]:
        return [repo for repo in self.get_repositories() if repo.get("enabled")]


    def get_installed_packages(self, repo_id: str = "__all__") -> list[AppEntry]:
        cache: dict[str, AppEntry] = {}
        installed_q = self.libdnf5.rpm.PackageQuery(self.base)
        installed_q.filter_installed()
        for pkg in installed_q:
            self._ingest_pkg_into_cache(cache, pkg, installed=True)

        items = list(cache.values())
        if repo_id != "__all__":
            items = [app for app in items if repo_id in app.repo_ids or "@System" in app.repo_ids or "@system" in app.repo_ids]
        items.sort(key=lambda app: app.name.casefold())
        return items

    def get_upgradable_packages(self, repo_id: str = "__all__") -> list[AppEntry]:
        """Return all packages from the solver-backed upgrade transaction.

        This mirrors the updater logic used by Nobara Sync much more closely:
        run an upgrade-all goal, inspect the resolved transaction, and include
        every inbound package action that DNF5 would actually perform.
        """
        cache: dict[str, AppEntry] = {}
        transaction = self._resolve_upgrade_transaction()
        if transaction is None:
            return []

        for tspkg in self._iter_transaction_packages(transaction):
            pkg = self._transaction_package_payload(tspkg)
            if pkg is None:
                continue
            action = self._transaction_package_action(tspkg)
            if not self._is_update_list_action(action):
                continue
            pkg_repo_id = self._safe_pkg_text(pkg, "get_repo_id")
            if repo_id != "__all__" and pkg_repo_id != repo_id:
                continue

            name = self._safe_pkg_text(pkg, "get_name")
            if not name:
                continue
            installed_pkg = self._get_installed_package(name)
            installed_arch = self._get_pkg_arch(installed_pkg)
            pkg_arch = self._get_pkg_arch(pkg)
            if installed_arch and pkg_arch not in {installed_arch, "noarch"}:
                continue

            self._ingest_pkg_into_cache(cache, pkg, installed=False)
            if installed_pkg is not None:
                self._ingest_pkg_into_cache(cache, installed_pkg, installed=True)

        items = list(cache.values())
        # Filter out packages where:
        # 1. Installed version matches candidate version exactly (same version from different repo)
        # 2. Candidate version is older than installed (downgrade, not update)
        filtered_items = []
        for app in items:
            if app.installed_version and app.candidate_version:
                # Skip if versions match exactly
                if app.installed_version == app.candidate_version:
                    continue
                # Skip if candidate is actually older (downgrade)
                # This happens when a higher version was installed from @commandline
                # but repo priority wants to "upgrade" to an older repo version
                if self._compare_evr(app.candidate_version, app.installed_version) < 0:
                    continue
            filtered_items.append(app)
        filtered_items.sort(key=lambda app: app.name.casefold())
        return filtered_items


    def _resolve_upgrade_transaction(self):
        goal = self.libdnf5.base.Goal(self.base)
        try:
            goal.add_upgrade("*")
        except TypeError:
            try:
                goal.add_upgrade()
            except Exception:
                try:
                    goal.add_upgrade("")
                except Exception:
                    return None
        except Exception:
            return None

        try:
            install_only_names = getattr(self.base.get_config(), "installonlypkgs", [])
        except Exception:
            install_only_names = []
        for name in install_only_names or []:
            try:
                goal.add_upgrade(name)
            except Exception:
                pass

        try:
            transaction = goal.resolve()
        except Exception:
            return None
        try:
            if list(transaction.get_problems() or []):
                return None
        except Exception:
            pass
        return transaction

    def _iter_transaction_packages(self, transaction) -> list:
        getter = getattr(transaction, "get_transaction_packages", None)
        if getter is None:
            return []
        try:
            items = getter()
        except Exception:
            return []
        return self._swig_sequence_to_list(items)

    def _transaction_package_payload(self, tspkg):
        getter = getattr(tspkg, "get_package", None)
        if getter is None:
            return None
        try:
            return getter()
        except Exception:
            return None

    def _transaction_package_action(self, tspkg):
        getter = getattr(tspkg, "get_action", None)
        if getter is None:
            return None
        try:
            return getter()
        except Exception:
            return None

    def _is_update_list_action(self, action) -> bool:
        transaction_mod = getattr(self.libdnf5, "transaction", None)
        constants = []
        if transaction_mod is not None:
            for attr in (
                "TransactionItemAction_UPGRADE",
                "TransactionItemAction_INSTALL",
                "TransactionItemAction_REINSTALL",
                "TransactionItemAction_DOWNGRADE",
                "TransactionItemAction_SWITCH",
            ):
                value = getattr(transaction_mod, attr, None)
                if value is not None:
                    constants.append(value)
        if action in constants:
            return True
        to_string = getattr(transaction_mod, "transaction_item_action_to_string", None) if transaction_mod is not None else None
        if callable(to_string):
            try:
                action_text = str(to_string(action) or "").strip().casefold()
            except Exception:
                action_text = ""
            return action_text in {"upgrade", "install", "reinstall", "downgrade", "switch"}
        return str(action).strip().casefold() in {"upgrade", "install", "reinstall", "downgrade", "switch"}

    def _swig_sequence_to_list(self, value) -> list:
        if value is None:
            return []
        try:
            return [item for item in value]
        except TypeError:
            pass
        get_size = getattr(value, "size", None) or getattr(value, "get_size", None)
        get = getattr(value, "get", None) or getattr(value, "index", None) or getattr(value, "index_safe", None)
        if callable(get_size) and callable(get):
            try:
                return [get(i) for i in range(int(get_size()))]
            except Exception:
                return []
        return []

    def _build_desktop_entry_cache(self) -> dict[str, dict]:
        if self._desktop_entry_cache is not None:
            return self._desktop_entry_cache

        cache: dict[str, dict] = {}
        roots = [Path('/usr/share/applications'), Path.home() / '.local/share/applications']
        for root in roots:
            if not root.exists():
                continue
            for desktop_file in root.glob('*.desktop'):
                try:
                    info = Gio.DesktopAppInfo.new_from_filename(str(desktop_file))
                except Exception:
                    info = None
                if not info:
                    continue
                desktop_id = getattr(info, 'get_id', lambda: desktop_file.name)() or desktop_file.name
                name = (getattr(info, 'get_name', lambda: '')() or '').strip()
                categories = (getattr(info, 'get_categories', lambda: None)() or '')
                exec_line = (getattr(info, 'get_string', lambda key: None)('Exec') or '')
                icon_value = None
                try:
                    gicon = info.get_icon()
                    icon_value = gicon.to_string() if gicon else None
                except Exception:
                    icon_value = None

                entry = {
                    'desktop_id': desktop_id,
                    'name': name,
                    'categories': [c for c in str(categories).split(';') if c],
                    'exec': exec_line,
                    'icon': icon_value,
                }

                keys = set()
                stem = desktop_file.stem.casefold()
                keys.add(stem)
                keys.add(desktop_id.casefold())
                if name:
                    keys.add(name.casefold())
                    keys.add(name.casefold().replace(' ', ''))
                    keys.add(name.casefold().replace(' ', '-'))
                exec_cmd = exec_line.strip().split()[0] if exec_line.strip() else ''
                if exec_cmd:
                    exec_base = Path(exec_cmd).name.casefold()
                    keys.add(exec_base)
                for key in list(keys):
                    if key.endswith('.desktop'):
                        keys.add(key[:-8])
                    if key.startswith('org.') or key.startswith('dev.') or key.startswith('com.') or key.startswith('io.'):
                        keys.add(key.split('.')[-1])
                for key in keys:
                    cache.setdefault(key, entry)

        self._desktop_entry_cache = cache
        return cache

    def _lookup_desktop_entry(self, pkg_name: str) -> dict | None:
        cache = self._build_desktop_entry_cache()
        key = (pkg_name or '').casefold()
        if not key:
            return None
        if key in cache:
            return cache[key]
        simplified = key.replace('_', '-').replace(' ', '-')
        if simplified in cache:
            return cache[simplified]
        for prefix in ('python3-', 'gnome-', 'kde-'):
            if key.startswith(prefix) and key[len(prefix):] in cache:
                return cache[key[len(prefix):]]
        return None

    def search_packages(self, query: str, repo_id: str = "__all__", limit: int = 200) -> list[AppEntry]:
        needle = (query or "").strip().casefold()
        if not needle:
            return []
        cache = self._build_package_search_cache()
        items = [
            app for app in cache.values()
            if (needle in app.name.casefold())
            or (needle in app.summary.casefold())
            or any(needle in pkg.casefold() for pkg in app.pkg_names)
        ]
        if repo_id != "__all__":
            items = [app for app in items if repo_id in app.repo_ids]
        items.sort(key=lambda app: self._package_search_rank_key(app, needle))
        return items[:limit]

    def _build_package_search_cache(self) -> dict[str, AppEntry]:
        if self._package_search_cache is not None:
            return self._package_search_cache

        cache: dict[str, AppEntry] = {}

        latest_q = self.libdnf5.rpm.PackageQuery(self.base)
        try:
            latest_q.filter_latest_evr()
        except TypeError:
            latest_q.filter_latest_evr(True)

        for pkg in latest_q:
            self._ingest_pkg_into_cache(cache, pkg, installed=False)

        installed_q = self.libdnf5.rpm.PackageQuery(self.base)
        installed_q.filter_installed()
        for pkg in installed_q:
            self._ingest_pkg_into_cache(cache, pkg, installed=True)

        self._package_search_cache = cache
        return cache

    def _ingest_pkg_into_cache(self, cache: dict[str, AppEntry], pkg, installed: bool) -> None:
        name = self._safe_pkg_text(pkg, "get_name")
        if not name:
            return
        app = cache.get(name)
        if app is None:
            summary = self._safe_pkg_text(pkg, "get_summary") or "System package"
            description = self._safe_pkg_text(pkg, "get_description") or summary
            app = AppEntry(
                appstream_id=f"pkg:{name}",
                name=name,
                summary=summary,
                description=description,
                pkg_names=[name],
                icon_name="application-x-executable",
                kind="PACKAGE",
                repo_ids=[],
            )
            desktop_entry = self._lookup_desktop_entry(name)
            if desktop_entry:
                desktop_name = desktop_entry.get('name') or ''
                if desktop_name:
                    app.name = desktop_name
                desktop_id = desktop_entry.get('desktop_id')
                if desktop_id:
                    app.launchables = [desktop_id]
                desktop_categories = desktop_entry.get('categories') or []
                if desktop_categories:
                    app.categories = [str(cat) for cat in desktop_categories]
                desktop_icon = desktop_entry.get('icon')
                if desktop_icon:
                    app.icon_name = desktop_icon
            cache[name] = app

        repo_id = self._safe_pkg_text(pkg, "get_repo_id")
        if repo_id and repo_id not in app.repo_ids and not repo_id.startswith("@"):
            app.repo_ids.append(repo_id)

        evr = self._get_pkg_evr(pkg)
        if installed:
            app.installed = True
            if evr:
                app.installed_version = evr
        else:
            if evr:
                app.candidate_version = evr

        if not app.summary or app.summary == "System package":
            app.summary = self._safe_pkg_text(pkg, "get_summary") or app.summary
        if not app.description or app.description == app.summary:
            app.description = self._safe_pkg_text(pkg, "get_description") or app.description

    def _package_search_rank_key(self, app: AppEntry, needle: str) -> tuple[int, int, int, str]:
        name = app.name.casefold()
        summary = app.summary.casefold()
        exact_name = name == needle
        prefix_name = name.startswith(needle)
        name_pos = name.find(needle) if needle in name else 9999
        summary_pos = summary.find(needle) if needle in summary else 9999

        if exact_name:
            rank = 0
        elif prefix_name:
            rank = 1
        elif name_pos != 9999:
            rank = 2
        elif summary_pos != 9999:
            rank = 3
        else:
            rank = 4
        return (rank, min(name_pos, summary_pos), len(name), name)

    def _safe_pkg_text(self, pkg, method_name: str) -> str:
        method = getattr(pkg, method_name, None)
        if method is None:
            return ""
        try:
            value = method()
        except Exception:
            return ""
        return str(value or "")

    def set_repository_enabled(self, repo_id: str, enabled: bool, event_cb: Callable[[dict], None] | None = None) -> tuple[bool, str]:
        payload = {"cmd": "repo-toggle", "repo_id": str(repo_id), "enabled": bool(enabled)}
        ok, message = self._run_privileged_helper_payload(payload, event_cb=event_cb)
        if ok:
            self.reload_state()
        return ok, message

    def execute_action(self, action: str, pkg_name: str | list[str], event_cb: Callable[[dict], None] | None = None) -> tuple[bool, str]:
        if action == 'install-rpms':
            ok, message = self._install_rpm_files([str(pkg) for pkg in (pkg_name if isinstance(pkg_name, list) else [pkg_name]) if pkg], event_cb)
        elif os.geteuid() == 0:
            ok, message = self._run_local_action(action, pkg_name, event_cb)
        else:
            ok, message = self._run_privileged_helper(action, pkg_name, event_cb)
        if ok:
            self._invalidate_package_search_cache()
        return ok, message



    def _run_command_with_logs(self, cmd: list[str], event_cb: Callable[[dict], None] | None = None) -> tuple[int, list[str]]:
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding='utf-8', errors='replace', bufsize=1)
        except FileNotFoundError:
            raise
        output_lines: list[str] = []
        assert process.stdout is not None
        for raw in process.stdout:
            line = raw.rstrip('\n')
            output_lines.append(line)
            if event_cb is not None and line:
                event_cb({'event': 'log', 'message': line})
        return process.wait(), output_lines

    def _install_rpm_files(self, rpm_paths: list[str], event_cb: Callable[[dict], None] | None = None) -> tuple[bool, str]:
        if not rpm_paths:
            return False, 'No RPM files were specified.'
        if os.geteuid() == 0:
            return self._run_local_rpm_install(rpm_paths, event_cb)
        payload = {'cmd': 'install-rpms', 'paths': list(rpm_paths)}
        return self._run_privileged_helper_payload(payload, event_cb=event_cb)

    def _run_local_rpm_install(self, rpm_paths: list[str], event_cb: Callable[[dict], None] | None = None) -> tuple[bool, str]:
        if event_cb is not None:
            event_cb({'event': 'log', 'message': 'Preflighting local RPM install transaction...'})
        preflight_cmd = ['dnf5', 'install', '-y', '--setopt=tsflags=test', *rpm_paths]
        try:
            rc, output_lines = self._run_command_with_logs(preflight_cmd, event_cb)
        except FileNotFoundError:
            return False, 'dnf5 is not installed.'
        except Exception as exc:
            return False, str(exc)
        benign_needles = ('Nothing to do.', 'Transaction test succeeded.', 'Complete!', 'Operation aborted', 'Exiting due to strict setting.')
        if self._looks_like_dependency_conflict(output_lines):
            return False, 'Transaction cancelled before execution because dependency/conflict issues were detected.\n' + '\n'.join(output_lines)
        if rc != 0 and not any(any(n in line for n in benign_needles) for line in output_lines):
            return False, '\n'.join(output_lines) or f'Preflight failed with exit code {rc}.'
        if event_cb is not None:
            event_cb({'event': 'log', 'message': 'Preflight check passed. Running real transaction...'})
        install_cmd = ['dnf5', 'install', '-y', *rpm_paths]
        try:
            rc, output_lines = self._run_command_with_logs(install_cmd, event_cb)
        except FileNotFoundError:
            return False, 'dnf5 is not installed.'
        except Exception as exc:
            return False, str(exc)
        if self._looks_like_dependency_conflict(output_lines):
            return False, '\n'.join(output_lines) or 'RPM install reported dependency/conflict issues.'
        if rc == 0:
            return True, 'RPM install completed successfully.'
        return False, '\n'.join(output_lines) or f'RPM install failed with exit code {rc}.'


    def _conflict_needles(self) -> tuple[str, ...]:
        return (
            "Problem ",
            "Skipping packages with conflicts",
            "Skipping packages with broken dependencies",
            "conflicts",
            "broken dependencies",
            "cannot install",
            "Transaction check error",
            "Error:",
        )

    def _looks_like_dependency_conflict(self, lines: list[str]) -> bool:
        needles = self._conflict_needles()
        return any(any(n in line for n in needles) for line in lines)

    def _preflight_transaction(self, action: str, pkg_names: list[str], event_cb: Callable[[dict], None] | None = None) -> tuple[bool, str]:
        if action not in {"install", "update"}:
            return True, ""
        if not pkg_names:
            return False, "No packages were specified."

        action_map = {"install": "install", "update": "upgrade"}
        description = f"Preflighting {action} transaction..."
        if event_cb is not None:
            event_cb({"event": "log", "message": description})

        cmd = [
            "dnf5",
            action_map[action],
            "-y",
            "--setopt=tsflags=test",
            *pkg_names,
        ]

        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )
        except FileNotFoundError:
            return False, "dnf5 is not installed."
        except Exception as exc:
            return False, str(exc)

        output_lines: list[str] = []
        assert process.stdout is not None
        for raw in process.stdout:
            line = raw.rstrip("\n")
            output_lines.append(line)
            if event_cb is not None and line:
                event_cb({"event": "log", "message": line})

        rc = process.wait()

        if self._looks_like_dependency_conflict(output_lines):
            return False, (
                "Transaction cancelled before execution because dependency/conflict issues were detected.\n"
                + "\n".join(output_lines)
            )

        benign_needles = (
            "Nothing to do.",
            "Transaction test succeeded.",
            "Complete!",
            "Operation aborted",
            "Exiting due to strict setting.",
        )
        if rc != 0 and not any(any(n in line for n in benign_needles) for line in output_lines):
            return False, "\n".join(output_lines) or f"Preflight failed with exit code {rc}."

        if event_cb is not None:
            event_cb({"event": "log", "message": "Preflight check passed. Running real transaction..."})
        return True, ""

    def _run_local_action(self, action: str, pkg_name: str | list[str], event_cb: Callable[[dict], None] | None = None) -> tuple[bool, str]:
        if action == "system-update":
            return self._run_nobara_sync_cli(event_cb=event_cb)

        goal = self.libdnf5.base.Goal(self.base)
        pkg_names = [pkg_name] if isinstance(pkg_name, str) else [pkg for pkg in pkg_name if pkg]
        if not pkg_names:
            return False, "No packages were specified."
        if action == "install":
            for name in pkg_names:
                goal.add_install(name)
            description = f"Install {', '.join(pkg_names)}" if len(pkg_names) <= 3 else f"Install {len(pkg_names)} packages"
        elif action == "remove":
            for name in pkg_names:
                goal.add_remove(name)
            description = f"Remove {', '.join(pkg_names)}" if len(pkg_names) <= 3 else f"Remove {len(pkg_names)} packages"
        elif action == "update":
            for name in pkg_names:
                goal.add_upgrade(name)
            description = f"Update {', '.join(pkg_names)}" if len(pkg_names) <= 3 else f"Update {len(pkg_names)} packages"
            return self._run_transaction(goal, description, event_cb=event_cb)
        else:
            return False, f"Unsupported action: {action}"

    def _start_privileged_helper(self, event_cb: Callable[[dict], None] | None = None) -> tuple[bool, str]:
        helper = Path(__file__).with_name("privileged_helper.py").resolve()
        python_bin = sys.executable or "/usr/bin/python3"
        user_home = os.path.expanduser("~")
        cmd = ["pkexec", python_bin, "-u", str(helper), "--server", "--user-home", user_home]
        env = dict(os.environ)
        env.setdefault("PYTHONUNBUFFERED", "1")
        try:
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
                bufsize=1,
            )
        except FileNotFoundError:
            return False, "pkexec is not installed. Install polkit to authorize package changes."
        except Exception as exc:
            return False, str(exc)

        message = "Authorization failed."
        if proc.stdout is None:
            proc.terminate()
            return False, "Could not start privileged helper."

        for raw_line in proc.stdout:
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except Exception:
                message = line or message
                continue
            event = payload.get("event")
            if event == "ready":
                self._helper_proc = proc
                if event_cb is not None:
                    event_cb({"event": "log", "message": str(payload.get("message") or "Authorization granted.")})
                return True, str(payload.get("message") or "Privileged helper ready.")
            if event_cb is not None:
                event_cb(payload)
            if event == "result":
                message = str(payload.get("message") or message)

        proc.wait()
        self._helper_proc = None
        if proc.returncode == 126:
            return False, "Authentication was dismissed."
        if proc.returncode == 127:
            return False, "Authorization failed."
        return False, message or f"Privileged helper exited with code {proc.returncode}."

    def _run_privileged_helper_payload(self, payload: dict, event_cb: Callable[[dict], None] | None = None) -> tuple[bool, str]:
        with self._helper_lock:
            cached_mode = bool(self.cache_authorization)
            proc = self._helper_proc if cached_mode else None
            if proc is None or proc.poll() is not None:
                self._helper_proc = None
                ok, message = self._start_privileged_helper(event_cb)
                if not ok:
                    return False, message
                proc = self._helper_proc
            if proc is None or proc.stdin is None or proc.stdout is None:
                self._helper_proc = None
                return False, "Privileged helper is unavailable."

            try:
                proc.stdin.write(json.dumps(payload) + "\n")
                proc.stdin.flush()
            except BrokenPipeError:
                self._helper_proc = None
                return False, "Privileged helper exited unexpectedly."
            except Exception as exc:
                self._helper_proc = None
                return False, str(exc)

            result_ok = False
            result_message = "Transaction failed."
            for raw_line in proc.stdout:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    resp_payload = json.loads(line)
                except Exception:
                    if event_cb is not None:
                        event_cb({"event": "log", "message": line})
                    continue
                event = resp_payload.get("event")
                if event == "result":
                    result_ok = bool(resp_payload.get("ok"))
                    result_message = str(resp_payload.get("message") or result_message)
                    break
                if event_cb is not None:
                    event_cb(resp_payload)

            if not cached_mode:
                try:
                    if proc.stdin is not None:
                        proc.stdin.write(json.dumps({"cmd": "quit"}) + "\n")
                        proc.stdin.flush()
                except Exception:
                    pass
                try:
                    proc.terminate()
                except Exception:
                    pass
                self._helper_proc = None

            if proc.poll() is not None and not result_ok and result_message == "Transaction failed.":
                self._helper_proc = None
                if proc.returncode == 126:
                    return False, "Authentication was dismissed."
                if proc.returncode == 127:
                    return False, "Authorization failed."
                return False, f"Privileged helper exited with code {proc.returncode}."
            return result_ok, result_message

    def _run_privileged_helper(self, action: str, pkg_name: str | list[str], event_cb: Callable[[dict], None] | None = None) -> tuple[bool, str]:
        return self._run_privileged_helper_payload({"cmd": "action", "action": action, "pkg_names": ([pkg_name] if isinstance(pkg_name, str) else list(pkg_name))}, event_cb=event_cb)

    def install(self, pkg_name: str) -> tuple[bool, str]:
        return self.execute_action("install", pkg_name)

    def remove(self, pkg_name: str) -> tuple[bool, str]:
        return self.execute_action("remove", pkg_name)

    def update_packages(self, pkg_names: list[str]) -> tuple[bool, str]:
        return self.execute_action("update", pkg_names)


    def _run_nobara_sync_cli(self, event_cb: Callable[[dict], None] | None = None) -> tuple[bool, str]:
        cmd = ["nobara-sync", "cli"]
        if event_cb is not None:
            event_cb({"event": "log", "message": "Running system update via nobara-sync cli..."})
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )
        except FileNotFoundError:
            return False, "nobara-sync is not installed."
        except Exception as exc:
            return False, str(exc)

        lines: list[str] = []
        assert proc.stdout is not None
        for raw in proc.stdout:
            line = raw.rstrip("\n")
            lines.append(line)
            if event_cb is not None and line:
                event_cb({"event": "log", "message": line})
        rc = proc.wait()
        if rc == 0:
            return True, "System update completed successfully."
        if self._looks_like_dependency_conflict(lines):
            return False, "\n".join(lines) or "System update reported conflicts/broken dependencies."
        return False, "\n".join(lines) or f"nobara-sync cli failed with exit code {rc}."

    def _transaction_success_value(self):
        transaction_cls = self.libdnf5.base.Transaction
        for attr in ("TransactionRunResult_SUCCESS", "SUCCESS"):
            value = getattr(transaction_cls, attr, None)
            if value is not None:
                return value
        nested = getattr(transaction_cls, "TransactionRunResult", None)
        if nested is not None:
            return getattr(nested, "SUCCESS", None)
        return None

    def _run_transaction(self, goal, description: str, event_cb: Callable[[dict], None] | None = None) -> tuple[bool, str]:
        if event_cb is not None:
            event_cb({"event": "log", "message": f"Resolving transaction for {description}..."})
        transaction = goal.resolve()
        problems = list(transaction.get_problems() or [])
        if problems:
            return False, "\n".join(str(problem) for problem in problems)

        try:
            if event_cb is not None:
                event_cb({"event": "log", "message": f"Downloading packages for {description}..."})
            transaction.download()
        except Exception:
            pass

        if event_cb is not None:
            event_cb({"event": "log", "message": f"Running transaction: {description}"})
        result = transaction.run()
        success_value = self._transaction_success_value()
        if success_value is not None and result == success_value:
            if event_cb is not None:
                event_cb({"event": "log", "message": f"{description} completed successfully."})
            return True, f"{description} completed successfully."

        details = []
        try:
            details.extend(str(item) for item in transaction.get_transaction_problems() or [])
        except Exception:
            pass
        try:
            details.extend(str(item) for item in transaction.get_resolve_logs_as_strings() or [])
        except Exception:
            pass
        if not details:
            details.append(f"Transaction failed with result code: {result}")
        return False, "\n".join(details)

    def _get_pkg_arch(self, pkg) -> str | None:
        if pkg is None:
            return None
        for method_name in ("get_arch", "get_arch_name"):
            method = getattr(pkg, method_name, None)
            if method is None:
                continue
            try:
                value = method()
            except Exception:
                continue
            if value:
                return str(value)
        return None

    def _compare_evr(self, evr1: str, evr2: str) -> int:
        """Compare two EVR strings.

        Returns:
            -1 if evr1 < evr2 (evr1 is older)
             0 if evr1 == evr2 (same version)
             1 if evr1 > evr2 (evr1 is newer)
        """
        # Try using the rpm module for proper version comparison
        try:
            import rpm
            # rpm.labelCompare expects tuples of (epoch, version, release)
            # Parse EVR strings into components
            def parse_evr(evr_str):
                if ':' in evr_str:
                    epoch, vr = evr_str.split(':', 1)
                    epoch = epoch or '0'
                else:
                    epoch = '0'
                    vr = evr_str
                if '-' in vr:
                    parts = vr.rsplit('-', 1)
                    version = parts[0]
                    release = parts[1] if len(parts) > 1 else ''
                else:
                    version = vr
                    release = ''
                return (epoch, version, release)

            evr1_tuple = parse_evr(evr1)
            evr2_tuple = parse_evr(evr2)
            return rpm.labelCompare(evr1_tuple, evr2_tuple)
        except ImportError:
            pass
        except Exception:
            pass

        # Fallback: simple string comparison (not accurate for all cases)
        if evr1 == evr2:
            return 0
        return 1 if evr1 > evr2 else -1

    def _get_pkg_evr(self, pkg) -> str | None:
        if pkg is None:
            return None
        try:
            epoch = pkg.get_epoch()
            version = pkg.get_version()
            release = pkg.get_release()
            if epoch and str(epoch) != "0":
                return f"{epoch}:{version}-{release}"
            return f"{version}-{release}"
        except Exception:
            pass
        for method_name in ("get_evr", "get_nevra"):
            method = getattr(pkg, method_name, None)
            if method is None:
                continue
            try:
                return str(method())
            except Exception:
                continue
        return None

    def _sequence_to_list(self, value) -> list[str]:
        if value is None:
            return []
        try:
            return [str(item) for item in value if item]
        except TypeError:
            pass
        get_size = getattr(value, "size", None) or getattr(value, "get_size", None)
        get = getattr(value, "get", None) or getattr(value, "index", None) or getattr(value, "index_safe", None)
        if callable(get_size) and callable(get):
            try:
                return [str(get(i)) for i in range(int(get_size())) if get(i)]
            except Exception:
                return []
        return []

    def _first_non_empty(self, *values: list[str]) -> str:
        for seq in values:
            for item in seq:
                if item:
                    return item
        return ""
