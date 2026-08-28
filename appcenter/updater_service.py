
from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

import gi
gi.require_version("Gtk", "3.0")
try:
    gi.require_version("AppIndicator3", "0.1")
    from gi.repository import AppIndicator3  # type: ignore
except ValueError:
    gi.require_version("AyatanaAppIndicator3", "0.1")
    from gi.repository import AyatanaAppIndicator3 as AppIndicator3  # type: ignore
from gi.repository import GLib, Gtk

try:
    import dbus
    import dbus.mainloop.glib
    import dbus.service
except Exception:
    dbus = None

from .dnf_backend import DnfBackend
from .updater_config import load_updater_settings, updater_interval_seconds
from .i18n import _

LOGGER = logging.getLogger("dnf_app_store_updater")
APP_NAME = _("DNF App Center")
APP_ICON = str((Path(__file__).with_name("assets") / "nobara-updater.svg").resolve())
TICK_INTERVAL = 60
BUS_NAME = "org.dnf.AppCenter.UpdateService"
OBJECT_PATH = "/org/dnf/AppCenter/UpdateService"
DISALLOWED_USERS = {"liveuser", "gnome-initial-setup"}


def _current_user() -> str:
    try:
        return os.getenv("USER") or os.getlogin()
    except Exception:
        return os.getenv("USER", "")


def _notifications_allowed(settings: dict | None = None) -> bool:
    settings = load_updater_settings() if settings is None else settings
    return bool(settings.get("notifications", True)) and _current_user() not in DISALLOWED_USERS


def _spawn_app(*extra: str) -> None:
    env = os.environ.copy()
    cmd = ["dnf-app-center", *extra]
    subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)


class Indicator:
    def __init__(self, refresh_func):
        self.refresh_func = refresh_func
        self._indicator = AppIndicator3.Indicator.new(
            "dnf-app-center-updater",
            APP_ICON,
            AppIndicator3.IndicatorCategory.APPLICATION_STATUS,
        )
        try:
            self._indicator.set_icon_full(APP_ICON, APP_NAME)
        except Exception:
            pass
        self._indicator.set_status(AppIndicator3.IndicatorStatus.PASSIVE)
        self._indicator.set_title(_("No updates available"))
        self._indicator.set_menu(self._build_menu())
        self.last_updates = 0

    def _build_menu(self):
        menu = Gtk.Menu()
        refresh_item = Gtk.MenuItem(label=_("Check for Updates"))
        refresh_item.connect("activate", lambda *_: self.refresh_func(True))
        menu.append(refresh_item)

        update_item = Gtk.MenuItem(label=_("Update System"))
        update_item.connect("activate", lambda *_: _spawn_app("--update"))
        menu.append(update_item)

        open_item = Gtk.MenuItem(label=_("Open App Center"))
        open_item.connect("activate", lambda *_: _spawn_app())
        menu.append(open_item)
        menu.show_all()
        return menu

    def set_updates(self, count: int) -> None:
        self.last_updates = count
        if count > 0:
            self._indicator.set_status(AppIndicator3.IndicatorStatus.ACTIVE)
            title = f"{count} update(s) available"
            self._indicator.set_title(title)
            try:
                self._indicator.set_label(str(count), "updates")
            except Exception:
                pass
        else:
            self._indicator.set_title(_("No updates available"))
            try:
                self._indicator.set_label("", "updates")
            except Exception:
                pass
            self._indicator.set_status(AppIndicator3.IndicatorStatus.PASSIVE)


class Notification:
    def __init__(self):
        self.last_value = 0
        self.iface = None
        if dbus is None:
            return
        try:
            bus = dbus.SessionBus()
            self.iface = dbus.Interface(
                bus.get_object("org.freedesktop.Notifications", "/org/freedesktop/Notifications"),
                dbus_interface="org.freedesktop.Notifications",
            )
        except Exception:
            self.iface = None

    def send(self, count: int) -> None:
        if not self.iface or count <= 0 or count == self.last_value:
            self.last_value = count
            return
        try:
            self.iface.Notify(
                APP_NAME,
                0,
                APP_ICON,
                _("Updates are available"),
                _("Package update(s) are available."),
                [],
                {},
                5000,
            )
        except Exception:
            pass
        self.last_value = count


class UpdateService(dbus.service.Object if dbus is not None else object):
    def __init__(self, *args, refresh_callback=None, **kwargs):
        self.refresh_callback = refresh_callback
        if dbus is not None:
            super().__init__(*args, **kwargs)

    if dbus is not None:
        @dbus.service.method(dbus_interface=BUS_NAME, in_signature="b", out_signature="")
        def RefreshUpdates(self, refresh: bool) -> None:
            GLib.idle_add(self.refresh_callback, bool(refresh))


class Updater:
    def __init__(self):
        self.indicator = Indicator(self.refresh_updates)
        self.notification = Notification()
        self._lock = threading.Lock()
        self._refreshing = False
        self._refresh_pending: bool | None = None
        self._last_check_monotonic = 0.0

    def _check_updates(self, refresh: bool) -> int:
        backend = DnfBackend()
        try:
            if refresh:
                backend.reload_state(force_refresh=True)
            updates = backend.get_upgradable_packages()
            return len(updates)
        finally:
            backend.shutdown()

    def refresh_updates(self, refresh: bool = False) -> bool:
        settings = load_updater_settings()
        if not settings.get("enabled", True) and not refresh:
            GLib.idle_add(self._apply_update_count, 0)
            return False
        with self._lock:
            if self._refreshing:
                # A check is already running and may have captured state
                # from before whatever just triggered this call (e.g. a
                # transaction that finished mid-check), so it can't be
                # trusted to reflect this request. Remember to run one
                # more pass right after the in-flight one finishes instead
                # of silently dropping it; `True` (forced metadata reload)
                # wins over `False` if requests of both kinds pile up.
                self._refresh_pending = self._refresh_pending or refresh
                return False
            self._refreshing = True

        def worker() -> None:
            try:
                LOGGER.info("Checking for updates (refresh=%s)", refresh)
                count = self._check_updates(refresh)
                self._last_check_monotonic = time.monotonic()
                GLib.idle_add(self._apply_update_count, count)
            except Exception:
                LOGGER.exception("Failed checking for updates")
                GLib.idle_add(self._apply_update_count, 0)
            finally:
                pending = None
                with self._lock:
                    self._refreshing = False
                    if self._refresh_pending is not None:
                        pending = self._refresh_pending
                        self._refresh_pending = None
                if pending is not None:
                    GLib.idle_add(self.refresh_updates, pending)

        threading.Thread(target=worker, daemon=True).start()
        return False

    def _apply_update_count(self, count: int) -> bool:
        settings = load_updater_settings()
        self.indicator.set_updates(count)
        if _notifications_allowed(settings):
            self.notification.send(count)
        else:
            self.notification.last_value = count
        return False

    def schedule(self) -> bool:
        settings = load_updater_settings()
        if not settings.get("enabled", True):
            self._apply_update_count(0)
            return True
        interval = updater_interval_seconds(settings)
        now = time.monotonic()
        if self._last_check_monotonic <= 0 or now - self._last_check_monotonic >= interval:
            self.refresh_updates(False)
        return True


def manual_update_check(refresh: bool, json_out: bool = False) -> int:
    """Check updates once without starting the tray service.

    Exit codes:
      0   = no updates
      100 = updates available
      2   = error
    """
    backend = DnfBackend()
    try:
        if refresh:
            backend.reload_state(force_refresh=True)
        updates = backend.get_upgradable_packages()
        total = len(updates)
        if json_out:
            print(json.dumps({"total": total}))
        else:
            print(f"Total: {total}")
        return 100 if total > 0 else 0
    except Exception as exc:
        print(f"Error checking updates: {exc}", file=sys.stderr)
        return 2
    finally:
        backend.shutdown()



def cli_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="Check for updates once and exit")
    parser.add_argument("--refresh", action="store_true", help="Force metadata refresh when used with --check")
    parser.add_argument("--json", action="store_true", help="Print result as JSON when used with --check")
    args = parser.parse_args(argv)
    if args.check:
        return manual_update_check(args.refresh, json_out=args.json)
    return main()

def main() -> int:
    logging.basicConfig(level=logging.INFO, format='(%(name)s) %(message)s')
    if dbus is not None:
        dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)
        try:
            session_bus = dbus.SessionBus()
            name = dbus.service.BusName(BUS_NAME, session_bus)
            updater = Updater()
            UpdateService(session_bus, OBJECT_PATH, refresh_callback=updater.refresh_updates)
        except Exception:
            updater = Updater()
    else:
        updater = Updater()

    updater.refresh_updates(False)
    GLib.timeout_add_seconds(TICK_INTERVAL, updater.schedule)
    loop = GLib.MainLoop()
    loop.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(cli_main())
