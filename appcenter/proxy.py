"""Proxy-only helper IPC, including standalone/older-updater installations.

Validation follows jalsarraf0's nobara-core-packages PR #144. Keep the parser
compatible with nobara_updater.update_state (covered by contract tests).
"""
import contextlib
import os
import re
import urllib.parse

PROXY_VARIABLES = ("http_proxy", "https_proxy", "HTTPS_PROXY", "all_proxy", "ALL_PROXY", "no_proxy", "NO_PROXY")
PROXY_SCHEMES = {"http", "https", "socks4", "socks4a", "socks5", "socks5h"}


def valid_proxy_setting(name: str, value: str) -> bool:
    """Accept a proxy URL or a no_proxy host list; refuse anything else."""
    if not isinstance(value, str) or not 0 < len(value) <= 2048:
        return False
    if name.lower() == "no_proxy":
        return re.fullmatch(r"[A-Za-z0-9.:*/%_\[\], -]+", value) is not None
    if not re.fullmatch(r"[!-~]+", value):
        return False
    try:
        # Like curl, read a proxy without a scheme as http://.
        url = urllib.parse.urlsplit(value if "://" in value else "http://" + value)
        port = url.port
    except ValueError:
        return False
    return (url.scheme in PROXY_SCHEMES and re.fullmatch(r"[A-Za-z0-9._:%-]+", url.hostname or "") is not None
            and port != 0 and url.path in {"", "/"} and not url.query and not url.fragment)


def proxy_settings(environ) -> tuple[dict[str, str], list[str]]:
    """Return the valid proxy variables in environ, and the names refused."""
    settings, refused = {}, []
    for name in PROXY_VARIABLES:
        value = environ.get(name)
        if value and valid_proxy_setting(name, value):
            settings[name] = value
        elif value:
            refused.append(name)
    return settings, refused


@contextlib.contextmanager
def proxy_environment(settings):
    """Apply one authorized request's proxy settings; never retain credentials."""
    if settings is None:
        # Backward compatibility with clients predating the proxy IPC field.
        yield
        return
    if not isinstance(settings, dict):
        raise ValueError("Proxy settings must be an object.")
    proxies, refused = proxy_settings(settings)
    if refused:
        raise ValueError("Invalid proxy settings: " + ", ".join(refused))
    previous = {name: os.environ.get(name) for name in PROXY_VARIABLES}
    try:
        for name in PROXY_VARIABLES:
            os.environ.pop(name, None)
        os.environ.update(proxies)
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
