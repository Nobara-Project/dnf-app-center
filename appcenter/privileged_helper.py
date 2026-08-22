#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import pwd
import subprocess
import sys

def _force_user_env():
    """Extracts --user-home from sys.argv before formal parsing to fix HOME immediately."""
    for i, arg in enumerate(sys.argv):
        if arg == "--user-home" and i + 1 < len(sys.argv):
            user_home = sys.argv[i+1]
            os.environ["HOME"] = user_home
            os.environ["XDG_DATA_HOME"] = os.path.join(user_home, ".local", "share")
            os.environ["XDG_CONFIG_HOME"] = os.path.join(user_home, ".config")
            os.environ["XDG_CACHE_HOME"] = os.path.join(user_home, ".cache")
            break

_force_user_env()

def emit(event: str, **payload) -> None:
    payload = {"event": event, **payload}
    print(json.dumps(payload), flush=True)


def _build_backend():
    try:
        import libdnf5  # type: ignore
    except Exception as exc:
        return None, {"ok": False, "message": f"Could not import libdnf5: {exc}"}

    base = libdnf5.base.Base()
    base.load_config()
    config = base.get_config()
    config.metadata_expire = 0
    base.setup()

    repo_sack = base.get_repo_sack()
    repo_sack.create_repos_from_system_configuration()
    try:
        repo_sack.load_repos()
    except TypeError:
        repo_sack.load_repos(True)
    return (libdnf5, base), None


def _transaction_success_value(libdnf5):
    transaction_cls = libdnf5.base.Transaction
    for attr in ("TransactionRunResult_SUCCESS", "SUCCESS"):
        value = getattr(transaction_cls, attr, None)
        if value is not None:
            return value
    nested = getattr(transaction_cls, "TransactionRunResult", None)
    if nested is not None:
        return getattr(nested, "SUCCESS", None)
    return None


def _conflict_needles() -> tuple[str, ...]:
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


def _looks_like_dependency_conflict(lines: list[str]) -> bool:
    needles = _conflict_needles()
    return any(any(n in line for n in needles) for line in lines)


def _looks_like_nobara_sync_failure(lines: list[str]) -> bool:
    needles = (
        "ERROR: DNF Package update are incomplete or failed due to conflicts/broken dependencies.",
        "ERROR: Please see ~/.local/share/logs/nobara-sync.log for more details",
        "ERROR: You can press the 'Open Log File' button on the Update System app to view it.",
        "Skipping packages with conflicts",
        "Skipping packages with broken dependencies",
        "Transaction check error",
    )
    return any(any(n in line for n in needles) for line in lines)


def _preflight_transaction(action: str, pkg_names: list[str]) -> tuple[bool, str]:
    if action not in {"install", "update"}:
        return True, ""
    if not pkg_names:
        return False, "No packages were specified."

    action_map = {"install": "install", "update": "upgrade"}
    emit("log", message=f"Preflighting {action} transaction...")
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
        if line:
            emit("log", message=line)

    rc = process.wait()

    if _looks_like_dependency_conflict(output_lines):
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

    emit("log", message="Preflight check passed. Running real transaction...")
    return True, ""


def _run_command_with_logs(cmd: list[str]) -> tuple[int, list[str]]:
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding='utf-8',
        errors='replace',
        bufsize=1,
    )
    output_lines: list[str] = []
    assert process.stdout is not None
    for raw in process.stdout:
        line = raw.rstrip('\n')
        output_lines.append(line)
        if line:
            emit('log', message=line)
    return process.wait(), output_lines


def _run_rpm_file_install(paths: list[str]) -> tuple[bool, str]:
    if not paths:
        return False, 'No RPM files were specified.'
    emit('log', message='Preflighting local RPM install transaction...')
    try:
        rc, output_lines = _run_command_with_logs(['dnf5', 'install', '-y', '--setopt=tsflags=test', *paths])
    except FileNotFoundError:
        return False, 'dnf5 is not installed.'
    except Exception as exc:
        return False, str(exc)
    benign_needles = ('Nothing to do.', 'Transaction test succeeded.', 'Complete!', 'Operation aborted', 'Exiting due to strict setting.')
    if _looks_like_dependency_conflict(output_lines):
        return False, 'Transaction cancelled before execution because dependency/conflict issues were detected.\n' + '\n'.join(output_lines)
    if rc != 0 and not any(any(n in line for n in benign_needles) for line in output_lines):
        return False, '\n'.join(output_lines) or f'Preflight failed with exit code {rc}.'
    emit('log', message='Preflight check passed. Running real transaction...')
    try:
        rc, output_lines = _run_command_with_logs(['dnf5', 'install', '-y', *paths])
    except FileNotFoundError:
        return False, 'dnf5 is not installed.'
    except Exception as exc:
        return False, str(exc)
    if _looks_like_dependency_conflict(output_lines):
        return False, '\n'.join(output_lines) or 'RPM install reported dependency/conflict issues.'
    if rc == 0:
        return True, 'RPM install completed successfully.'
    return False, '\n'.join(output_lines) or f'RPM install failed with exit code {rc}.'


def _system_update_args(pkg_name: str | list[str]) -> list[str]:
    pkg_names = [pkg_name] if isinstance(pkg_name, str) else list(pkg_name)
    return ["--all"] if "--all" in {str(pkg) for pkg in pkg_names} else []


def _run_system_update(sync_args: list[str] | None = None) -> tuple[bool, str]:
    sync_cmd = ["nobara-sync", "cli", *(sync_args or [])]
    emit("log", message=f"Running system update via {' '.join(sync_cmd)}...")
    target_home = os.environ.get("HOME", "/root")
    cmd = [
        "/usr/bin/env",
        f"HOME={target_home}",
        f"XDG_DATA_HOME={target_home}/.local/share",
        *sync_cmd,
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
            env=os.environ.copy()
        )
    except FileNotFoundError:
        return False, "nobara-sync is not installed."
    except Exception as exc:
        return False, str(exc)

    output_lines: list[str] = []
    assert process.stdout is not None
    for raw in process.stdout:
        line = raw.rstrip("\n")
        output_lines.append(line)
        if line:
            emit("log", message=line)
    rc = process.wait()
    if rc == 0:
        return True, "System update completed successfully."
    if _looks_like_dependency_conflict(output_lines):
        return False, "\n".join(output_lines) or "System update reported conflicts/broken dependencies."
    return False, "\n".join(output_lines) or f"nobara-sync cli failed with exit code {rc}."


def _run_transaction(libdnf5, base, action: str, pkg_name: str | list[str]) -> tuple[bool, str]:
    if action == "system-update":
        return _run_system_update(sync_args=_system_update_args(pkg_name))

    goal = libdnf5.base.Goal(base)
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
    else:
        return False, f"Unsupported action: {action}"

    ok, message = _preflight_transaction(action, pkg_names)
    if not ok:
        return False, message

    emit("log", message=f"Resolving transaction for {description}...")
    transaction = goal.resolve()
    problems = list(transaction.get_problems() or [])
    if problems:
        return False, "\n".join(str(problem) for problem in problems)

    try:
        emit("log", message=f"Downloading packages for {description}...")
        transaction.download()
    except Exception:
        pass

    emit("log", message=f"Running transaction: {description}")
    try:
        result = transaction.run()
    except Exception as exc:
        return False, str(exc)

    success_value = _transaction_success_value(libdnf5)
    if success_value is not None and result == success_value:
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




def _set_repository_enabled(repo_id: str, enabled: bool) -> tuple[bool, str]:
    emit("log", message=f"{'Enabling' if enabled else 'Disabling'} repository {repo_id}...")
    commands = [
        ["dnf5", "config-manager", "setopt", f"{repo_id}.enabled={'1' if enabled else '0'}"],
        ["dnf", "config-manager", "setopt", f"{repo_id}.enabled={'1' if enabled else '0'}", "--save"],
    ]
    last_message = "Could not update repository configuration."
    for cmd in commands:
        try:
            process = subprocess.run(cmd, text=True, capture_output=True, check=False)
        except FileNotFoundError:
            continue
        except Exception as exc:
            last_message = str(exc)
            continue
        combined = "\n".join(part for part in [(process.stdout or "").strip(), (process.stderr or "").strip()] if part).strip()
        if combined:
            for line in combined.splitlines():
                emit("log", message=line)
        if process.returncode == 0:
            return True, f"Repository {repo_id} {'enabled' if enabled else 'disabled'}."
        last_message = combined or f"Command {' '.join(cmd)} failed with exit code {process.returncode}."
    return False, last_message

def _handle_command(libdnf5, base, payload: dict) -> bool:
    cmd = str(payload.get("cmd") or "")
    if cmd == "quit":
        emit("result", ok=True, message="Helper exiting.")
        return False
    if cmd == "repo-toggle":
        repo_id = str(payload.get("repo_id") or "")
        enabled = bool(payload.get("enabled"))
        if not repo_id:
            emit("result", ok=False, message="Missing repository id.")
            return True
        ok, message = _set_repository_enabled(repo_id, enabled)
        emit("result", ok=ok, message=message)
        return True
    if cmd == 'install-rpms':
        paths = payload.get('paths')
        if not isinstance(paths, list):
            emit('result', ok=False, message='Missing RPM file paths.')
            return True
        ok, message = _run_rpm_file_install([str(path) for path in paths if path])
        emit('result', ok=ok, message=message)
        return True
    if cmd != "action":
        emit("result", ok=False, message=f"Unsupported helper command: {cmd}")
        return True
    action = str(payload.get("action") or "")
    pkg_names = payload.get("pkg_names")
    if not isinstance(pkg_names, list):
        pkg_names = [str(payload.get("pkg_name") or "")]
    ok, message = _run_transaction(libdnf5, base, action, [str(pkg) for pkg in pkg_names if pkg])
    emit("result", ok=ok, message=message)
    return True


def server_main() -> int:
    backend, error = _build_backend()
    if error is not None:
        print(json.dumps(error), flush=True)
        return 1

    libdnf5, base = backend
    emit("ready", message="Privileged helper ready")
    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except Exception as exc:
            emit("result", ok=False, message=f"Invalid helper payload: {exc}")
            continue
        if not _handle_command(libdnf5, base, payload):
            break
    return 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--server", action="store_true")
    parser.add_argument("--user-home", type=str)
    args, remaining = parser.parse_known_args(argv[1:])
    if args.server:
        return server_main()
    if not remaining:
        print(json.dumps({"ok": False, "message": "Usage: privileged_helper.py <action> [package]"}), flush=True)
        return 2
    action = remaining[0]
    pkg_name = remaining[1] if len(remaining) >= 2 else ""

    backend, error = _build_backend()
    if error is not None:
        print(json.dumps(error), flush=True)
        return 1
    libdnf5, base = backend
    ok, message = _run_transaction(libdnf5, base, action, pkg_name)
    emit("result", ok=ok, message=message)
    return 0 if ok else 1

    if len(argv) >= 2 and argv[1] == "--server":
        return server_main()

    if len(argv) < 2:
        print(json.dumps({"ok": False, "message": "Usage: privileged_helper.py <install|remove|update|system-update> [package]"}), flush=True)
        return 2

    action = argv[1]
    pkg_name = argv[2] if len(argv) >= 3 else ""
    backend, error = _build_backend()
    if error is not None:
        print(json.dumps(error), flush=True)
        return 1

    libdnf5, base = backend
    ok, message = _run_transaction(libdnf5, base, action, pkg_name)
    emit("result", ok=ok, message=message)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
