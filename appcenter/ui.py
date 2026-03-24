from __future__ import annotations

import hashlib
import html
import os
from dataclasses import dataclass, field
from html.parser import HTMLParser
import subprocess
import sys
import threading
import traceback
import urllib.error
import urllib.request
from pathlib import Path

import gi

gi.require_version("Gtk", "4.0")
gi.require_version("Adw", "1")
gi.require_version("GdkPixbuf", "2.0")
from gi.repository import Adw, Gdk, GdkPixbuf, Gio, GLib, GObject, Gtk, Pango

from .appstream_catalog import AppStreamCatalog, AppStreamUnavailable
from .dnf_backend import DnfBackend, DnfUnavailable

import json
from pathlib import Path as _Path

_UPDATER_CONFIG_PATH = _Path.home() / ".config" / "dnf-app-center" / "updater.json"

def load_updater_settings() -> dict:
    defaults = {
        "enabled": True,
        "interval_unit": "hours",
        "interval_value": 6,
    }
    try:
        if _UPDATER_CONFIG_PATH.exists():
            data = json.loads(_UPDATER_CONFIG_PATH.read_text())
            if isinstance(data, dict):
                defaults.update({k: data[k] for k in defaults.keys() if k in data})
    except Exception:
        pass
    return defaults

def save_updater_settings(settings: dict) -> None:
    _UPDATER_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "enabled": bool(settings.get("enabled", True)),
        "interval_unit": str(settings.get("interval_unit", "hours")),
        "interval_value": int(settings.get("interval_value", 6)),
    }
    _UPDATER_CONFIG_PATH.write_text(json.dumps(payload, indent=2))

from .i18n import _
from .updater_config import load_updater_settings, save_updater_settings, VALID_UNITS
from .models import AppEntry, should_hide_from_standard_catalog


class _NewsHTMLToMarkupParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.blocks: list[str] = []
        self._parts: list[str] = []
        self._href_stack: list[str] = []
        self._list_depth = 0

    def _append(self, text: str) -> None:
        if text:
            self._parts.append(text)

    def _flush_block(self) -> None:
        text = ''.join(self._parts).strip()
        if text:
            self.blocks.append(text)
        self._parts = []

    def handle_starttag(self, tag: str, attrs) -> None:
        attrs = dict(attrs)
        tag = tag.lower()
        if tag in {'b', 'strong'}:
            self._append('<b>')
        elif tag in {'i', 'em'}:
            self._append('<i>')
        elif tag == 'u':
            self._append('<u>')
        elif tag in {'s', 'strike', 'del'}:
            self._append('<s>')
        elif tag in {'tt', 'code'}:
            self._append('<tt>')
        elif tag == 'a':
            href = attrs.get('href', '')
            self._href_stack.append(href)
            if href:
                self._append(f'<a href="{GLib.markup_escape_text(href)}">')
        elif tag in {'p', 'div', 'section', 'article', 'header'}:
            self._flush_block()
        elif tag in {'br', 'hr'}:
            self._append('\n')
        elif tag in {'ul', 'ol'}:
            self._flush_block()
            self._list_depth += 1
        elif tag == 'li':
            if self._parts and not ''.join(self._parts).endswith('\n'):
                self._append('\n')
            indent = '  ' * max(self._list_depth - 1, 0)
            self._append(f'{indent}• ')

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {'b', 'strong'}:
            self._append('</b>')
        elif tag in {'i', 'em'}:
            self._append('</i>')
        elif tag == 'u':
            self._append('</u>')
        elif tag in {'s', 'strike', 'del'}:
            self._append('</s>')
        elif tag in {'tt', 'code'}:
            self._append('</tt>')
        elif tag == 'a':
            href = self._href_stack.pop() if self._href_stack else ''
            if href:
                self._append('</a>')
        elif tag == 'li':
            self._append('\n')
        elif tag in {'ul', 'ol'}:
            self._list_depth = max(0, self._list_depth - 1)
            self._flush_block()
        elif tag in {'p', 'div', 'section', 'article', 'header'}:
            self._flush_block()

    def handle_data(self, data: str) -> None:
        self._append(GLib.markup_escape_text(data))

    def handle_entityref(self, name: str) -> None:
        self._append(GLib.markup_escape_text(html.unescape(f'&{name};')))

    def handle_charref(self, name: str) -> None:
        self._append(GLib.markup_escape_text(html.unescape(f'&#{name};')))


def _markup_blocks_from_text(raw_text: str) -> list[str]:
    text = (raw_text or '').strip()
    if not text:
        return ['No news available.']
    if '<' in text and '>' in text:
        parser = _NewsHTMLToMarkupParser()
        try:
            parser.feed(text)
            parser.close()
        except Exception:
            parser.blocks = []
        if parser.blocks:
            return parser.blocks
    sections = [GLib.markup_escape_text(part.strip()) for part in text.replace('\r\n', '\n').split('\n\n') if part.strip()]
    return sections or ['No news available.']


@dataclass
class QueueItem:
    app: AppEntry
    action: str
    status: str = "queued"
    message: str = "Waiting"
    pkg_names: list[str] = field(default_factory=list)
    file_paths: list[str] = field(default_factory=list)
    label: str | None = None

    def __post_init__(self) -> None:
        if not self.pkg_names:
            self.pkg_names = [self.app.primary_pkg] if self.app.primary_pkg else []

    @property
    def pkg_name(self) -> str:
        return self.pkg_names[0] if self.pkg_names else (self.app.primary_pkg or "")

    @property
    def display_name(self) -> str:
        if self.label:
            return self.label
        if len(self.file_paths) > 1:
            return f"{len(self.file_paths)} RPM files"
        if len(self.pkg_names) > 1:
            return f"{len(self.pkg_names)} packages"
        return self.app.name

CATEGORY_GROUPS = {
    "system": {
        "news": _("News"),
        "installed": _("Installed"),
        "updates": _("Updates"),
        "queue": _("Queue"),
        "repositories": _("Repositories"),
    },
    "categories": {
        "office": _("Productivity"),
        "graphics": _("Graphics & Photography"),
        "audiovideo": _("Audio & Video"),
        "education": _("Education"),
        "network": _("Networking"),
        "game": _("Games"),
        "development": _("Developer Tools"),
        "science": _("Science"),
        "system": _("System"),
        "utility": _("Utilities"),
    },
}

SUBCATEGORY_GROUPS = {
    "audiovideo": {
        "audiovideoediting": "Audio & Video Editing",
        "discburning": "Disc Burning",
        "midi": "Midi",
        "mixer": "Mixer",
        "player": "Player",
        "recorder": "Recorder",
        "sequencer": "Sequencer",
        "tuner": "Tuner",
        "tv": "TV",
    },
    "development": {
        "building": "Building",
        "database": "Database",
        "debugger": "Debugger",
        "guidesigner": "GUI Designer",
        "ide": "IDE",
        "profiling": "Profiling",
        "revisioncontrol": "Revision Control",
        "translation": "Translation",
        "webdevelopment": "Web Development",
    },
    "game": {
        "actiongame": "Action Games",
        "adventuregame": "Adventure Games",
        "arcadegame": "Arcade Games",
        "blocksgame": "Blocks Games",
        "boardgame": "Board Games",
        "cardgame": "Card Games",
        "emulator": "Emulators",
        "kidsgame": "Kids' Games",
        "logicgame": "Logic Games",
        "roleplaying": "Role Playing",
        "shooter": "Shooter",
        "simulation": "Simulation",
        "sportsgame": "Sports Games",
        "strategygame": "Strategy Games",
    },
    "graphics": {
        "2dgraphics": "2D Graphics",
        "3dgraphics": "3D Graphics",
        "ocr": "OCR",
        "photography": "Photography",
        "publishing": "Publishing",
        "rastergraphics": "Raster Graphics",
        "scanning": "Scanning",
        "vectorgraphics": "Vector Graphics",
        "viewer": "Viewer",
    },
    "network": {
        "chat": "Chat",
        "email": "Email",
        "feed": "Feed",
        "filetransfer": "File Transfer",
        "hamradio": "Ham Radio",
        "instantmessaging": "Instant Messaging",
        "ircclient": "IRC Client",
        "monitor": "Monitor",
        "news": _("News"),
        "p2p": "P2P",
        "remoteaccess": "Remote Access",
        "telephony": "Telephony",
        "videoconference": "Video Conference",
        "webbrowser": "Web Browser",
        "webdevelopment": "Web Development",
    },
    "office": {
        "calendar": "Calendar",
        "chart": "Chart",
        "contactmanagement": "Contact Management",
        "database": "Database",
        "dictionary": "Dictionary",
        "email": "Email",
        "finance": "Finance",
        "presentation": "Presentation",
        "projectmanagement": "Project Management",
        "publishing": "Publishing",
        "spreadsheet": "Spreadsheet",
        "viewer": "Viewer",
        "wordprocessor": "Word Processor",
    },
    "system": {
        "emulator": "Emulators",
        "filemanager": "File Manager",
        "filesystem": "Filesystem",
        "filetools": "File Tools",
        "monitor": "Monitor",
        "security": "Security",
        "terminalemulator": "Terminal Emulator",
    },
    "utility": {
        "accessibility": "Accessibility",
        "archiving": "Archiving",
        "calculator": "Calculator",
        "clock": "Clock",
        "compression": "Compression",
        "filetools": "File Tools",
        "telephonytools": "Telephony Tools",
        "texteditor": "Text Editor",
        "texttools": "Text Tools",
    },
}

CSS = b"""
window {
  background: @window_bg_color;
}
windowhandle > box.top-bar {
  padding: 10px 12px;
  border-bottom: 1px solid alpha(currentColor, 0.12);
  background: mix(@window_bg_color, @headerbar_bg_color, 0.8);
}
.sidebar {
  margin: 12px 10px 12px 12px;
  padding: 0 6px 8px 6px;
  border-radius: 4px;
  background: @sidebar_backdrop_color;
  border: 1px solid mix(currentColor, @window_bg_color, 0.86);
}
.sidebar-section {
  margin: 0;
  font-weight: 700;
  font-size: 1.3rem;
}
.sidebar-section-box {
  margin: 0;
  padding: 10px 8px 2px 8px;
}
.sidebar-section-box image {
  color: @window_fg_color;
  -gtk-icon-style: symbolic;
}
.nav-button {
  border: 0;
  padding: 0;
  margin: 0;
  background: none;
  box-shadow: none;
  min-height: 0;
}
.nav-button > box {
  border-radius: 4px;
  padding: 12px 8px;
  transition: margin-left 0.2s cubic-bezier(0.040, 0.455, 0.215, 0.995), padding 0.2s cubic-bezier(0.040, 0.455, 0.215, 0.995);
}
.nav-button .nav-label {
  font-weight: 400;
}
.nav-button .nav-arrow {
  color: #3584e4;
  font-weight: 700;
}
.nav-button.active > box {
  background: mix(currentColor, @window_bg_color, 0.9);
  margin-left: 4px;
  padding: 12px 14px;
}
.nav-button.active .nav-label {
  font-weight: bold;
}
.content-box {
  margin: 12px 12px 12px 0;
}
.queue-bottom-bar {
  padding: 8px 12px;
  border-top: 1px solid alpha(currentColor, 0.12);
  background: mix(@window_bg_color, @headerbar_bg_color, 0.85);
}
.queue-bottom-status {
  min-width: 180px;
}
.content-header {
  margin: 8px 4px 10px 4px;
  min-width: 0;
}

.updates-action-bar {
  margin: 8px 0 4px 0;
}
.update-check {
  margin-right: 6px;
}
.subcat-strip {
  padding: 4px 0 10px 0;
  min-width: 0;
}
.subcat-strip > box {
  min-width: 0;
}
.subcat-strip-frame {
  margin: 2px;
  padding: 0;
  background: @sidebar_backdrop_color;
  border-radius: 4px;
  min-width: 0;
}
.subcat-page {
  padding: 2px 6px;
}
.pan-button {
  border: 0px;
  padding: 6px;
  margin: 0;
  background: none;
  box-shadow: none;
  color: alpha(currentColor, 0.45);
}
.pan-button.available {
  color: #3584e4;
}
.subcategory-chip {
  margin: 4px 0;
  padding: 8px 14px;
  border-radius: 4px;
  background: transparent;
}
.subcategory-chip.active {
  background-color: @window_bg_color;
}
.subcategory-label {
  font-weight: 400;
}
.subcategory-chip.active .subcategory-label {
  font-weight: 700;
}
.app-list {
  background: transparent;
}
.app-list row,
.app-list row:selected,
.app-list row:hover {
  background: transparent;
  box-shadow: none;
}
.app-list-row {
  margin: 4px 0;
}
.app-card {
  padding: 10px 14px;
  border-radius: 14px;
  background: alpha(@card_bg_color, 0.72);
  border: 1px solid alpha(currentColor, 0.08);
  box-shadow: none;
}
.app-title {
  font-size: 1.02em;
  font-weight: 800;
}
.app-summary {
  margin-top: 0;
  color: alpha(currentColor, 0.78);
}
.app-meta {
  font-size: 0.92em;
  color: alpha(currentColor, 0.68);
  margin-top: 4px;
}
.detail-hero {
  padding: 24px 28px;
  border-radius: 18px;
  background: alpha(@card_bg_color, 0.75);
  border: 1px solid alpha(currentColor, 0.08);
}
.repo-card {
  padding: 14px 16px;
  border-radius: 14px;
  background: alpha(@card_bg_color, 0.7);
  border: 1px solid alpha(currentColor, 0.08);
  margin: 6px 0;
}
.news-card {
  padding: 22px 24px;
  border-radius: 18px;
  background: alpha(@card_bg_color, 0.72);
  border: 1px solid alpha(currentColor, 0.08);
}
.news-heading {
  font-size: 1.18em;
  font-weight: 800;
}
.news-body {
  font-size: 1.02em;
  line-height: 1.45;
}
"""




CACHE_DIR = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "dnf-app-center" / "media"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

DEBUG_ICONS = os.environ.get("APPSTORE_DEBUG_ICONS") == "1"
DEBUG_ICON_FILTER = os.environ.get("APPSTORE_DEBUG_ICON_FILTER", "").strip().casefold()


def _icon_debug_enabled(app: AppEntry | None) -> bool:
    if not DEBUG_ICONS:
        return False
    if app is None:
        return not DEBUG_ICON_FILTER
    haystack = " ".join([
        app.name or "",
        app.appstream_id or "",
        " ".join(app.pkg_names or []),
        " ".join(app.launchables or []),
    ]).casefold()
    return not DEBUG_ICON_FILTER or DEBUG_ICON_FILTER in haystack


def _icon_debug(app: AppEntry | None, message: str) -> None:
    if _icon_debug_enabled(app):
        target = getattr(app, "name", None) or getattr(app, "appstream_id", None) or "<unknown>"
        print(f"[ICON DEBUG] {target}: {message}", file=sys.stderr)


def _cached_media_path(url: str) -> Path:
    suffix = Path(url.split("?", 1)[0]).suffix or ".bin"
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
    return CACHE_DIR / f"{digest}{suffix}"


def _ensure_local_media(ref: str | None) -> str | None:
    if not ref:
        return None
    if ref.startswith(("http://", "https://")):
        target = _cached_media_path(ref)
        if target.exists():
            return str(target)
        try:
            with urllib.request.urlopen(ref, timeout=20) as response:
                data = response.read()
            target.write_bytes(data)
            return str(target)
        except (OSError, urllib.error.URLError, TimeoutError):
            return None
    path = Path(ref)
    if path.is_file():
        return str(path)
    return None


def _crop_transparent_borders(pixbuf: GdkPixbuf.Pixbuf) -> GdkPixbuf.Pixbuf:
    if not pixbuf.get_has_alpha():
        return pixbuf

    width = pixbuf.get_width()
    height = pixbuf.get_height()
    rowstride = pixbuf.get_rowstride()
    n_channels = pixbuf.get_n_channels()
    pixels = pixbuf.get_pixels()

    min_x = width
    min_y = height
    max_x = -1
    max_y = -1

    for y in range(height):
        base = y * rowstride
        for x in range(width):
            alpha = pixels[base + x * n_channels + (n_channels - 1)]
            if alpha > 0:
                if x < min_x:
                    min_x = x
                if y < min_y:
                    min_y = y
                if x > max_x:
                    max_x = x
                if y > max_y:
                    max_y = y

    if max_x < min_x or max_y < min_y:
        return pixbuf

    cropped_w = max_x - min_x + 1
    cropped_h = max_y - min_y + 1
    if cropped_w <= 0 or cropped_h <= 0 or (cropped_w == width and cropped_h == height):
        return pixbuf

    try:
        return pixbuf.new_subpixbuf(min_x, min_y, cropped_w, cropped_h)
    except Exception:
        return pixbuf




def _is_font_like_app(app: AppEntry) -> bool:
    haystacks = [
        app.name or "",
        app.summary or "",
        app.description or "",
        app.primary_pkg or "",
        " ".join(app.pkg_names or []),
        " ".join(app.categories or []),
    ]
    text = " ".join(haystacks).lower()
    if any(token in text for token in [" font", "fonts", "typeface", "typography"]):
        return True
    pkg = (app.primary_pkg or "").lower()
    return pkg.endswith("-fonts") or pkg.endswith("fonts")
def _image_from_ref(ref: str | None, size: int, *, crop_transparency: bool = True, fill_ratio: float = 1.0) -> Gtk.Widget | None:
    local_path = _ensure_local_media(ref)
    if not local_path:
        return None

    try:
        pixbuf = GdkPixbuf.Pixbuf.new_from_file(local_path)
        if crop_transparency:
            pixbuf = _crop_transparent_borders(pixbuf)
        width = pixbuf.get_width()
        height = pixbuf.get_height()
        if width <= 0 or height <= 0:
            return None
        target_size = max(1, int(round(size * fill_ratio)))
        scale = min(target_size / width, target_size / height)
        scaled_w = max(1, int(round(width * scale)))
        scaled_h = max(1, int(round(height * scale)))
        scaled = pixbuf.scale_simple(scaled_w, scaled_h, GdkPixbuf.InterpType.BILINEAR)
        texture = Gdk.Texture.new_for_pixbuf(scaled)
        picture = Gtk.Picture.new_for_paintable(texture)
        picture.set_can_shrink(True)
        picture.set_keep_aspect_ratio(True)
        picture.set_content_fit(Gtk.ContentFit.CONTAIN)
        picture.set_halign(Gtk.Align.CENTER)
        picture.set_valign(Gtk.Align.CENTER)
        picture.set_size_request(size, size)
        return picture
    except Exception:
        picture = Gtk.Picture.new_for_filename(local_path)
        picture.set_can_shrink(True)
        picture.set_keep_aspect_ratio(True)
        picture.set_content_fit(Gtk.ContentFit.CONTAIN)
        picture.set_halign(Gtk.Align.CENTER)
        picture.set_valign(Gtk.Align.CENTER)
        picture.set_size_request(size, size)
        return picture






def _image_from_icon_name(icon_name: str | None, size: int, *, crop_transparency: bool = True, fill_ratio: float = 1.0) -> Gtk.Widget | None:
    if not icon_name:
        return None

    try:
        display = Gdk.Display.get_default()
        if display is not None:
            theme = Gtk.IconTheme.get_for_display(display)
            paintable = None
            try:
                paintable = theme.lookup_icon(
                    icon_name,
                    None,
                    size,
                    1,
                    Gtk.TextDirection.NONE,
                    Gtk.IconLookupFlags.FORCE_SIZE,
                )
            except TypeError:
                try:
                    paintable = theme.lookup_icon(
                        icon_name,
                        [],
                        size,
                        1,
                        Gtk.TextDirection.NONE,
                        Gtk.IconLookupFlags.FORCE_SIZE,
                    )
                except Exception:
                    paintable = None
            except Exception:
                paintable = None

            if paintable is not None:
                try:
                    file = paintable.get_file()
                    if file is not None:
                        local_path = file.get_path()
                        if local_path:
                            image = _image_from_ref(local_path, size, crop_transparency=crop_transparency, fill_ratio=fill_ratio)
                            if image is not None:
                                return image
                except Exception:
                    pass
    except Exception:
        pass

    image = Gtk.Image.new_from_icon_name(icon_name)
    image.set_pixel_size(size)
    image.set_size_request(size, size)
    image.set_halign(Gtk.Align.CENTER)
    image.set_valign(Gtk.Align.CENTER)
    return image

def _picture_from_ref(ref: str | None, width: int, height: int) -> Gtk.Widget | None:
    local_path = _ensure_local_media(ref)
    if not local_path:
        return None

    picture = Gtk.Picture.new_for_filename(local_path)
    picture.set_can_shrink(True)
    picture.set_keep_aspect_ratio(True)
    picture.set_content_fit(Gtk.ContentFit.CONTAIN)
    picture.set_halign(Gtk.Align.CENTER)
    picture.set_valign(Gtk.Align.CENTER)
    picture.set_size_request(width, height)
    return picture

def _icon_names_from_launchables(app: AppEntry) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for launchable in app.launchables:
        if not launchable:
            continue
        desktop_id = Path(str(launchable)).name
        if not desktop_id.endswith('.desktop'):
            _icon_debug(app, f"skip non-desktop launchable {launchable!r}")
            continue
        info = Gio.DesktopAppInfo.new(desktop_id)
        if info is None:
            _icon_debug(app, f"DesktopAppInfo not found for {desktop_id!r}")
            continue
        _icon_debug(app, f"DesktopAppInfo found for {desktop_id!r}")
        icon = info.get_icon()
        icon_key = None
        try:
            icon_key = info.get_string("Icon")
        except Exception:
            icon_key = None
        _icon_debug(app, f"desktop Icon= {icon_key!r}; gicon={icon!r}")
        if icon is None:
            continue
        icon_str = icon.to_string()
        if icon_str and icon_str not in seen:
            names.append(icon_str)
            seen.add(icon_str)
    return names


def _resolve_themed_icon_name(app: AppEntry) -> str | None:
    display = Gdk.Display.get_default()
    if display is None:
        candidates: list[str] = []
        _icon_debug(app, "no display available for icon theme lookup")
    else:
        theme = Gtk.IconTheme.get_for_display(display)
        candidates = []
        if app.icon_name:
            candidates.extend([app.icon_name, Path(app.icon_name).stem])
        candidates.extend(_icon_names_from_launchables(app))
        if app.appstream_id:
            candidates.extend([app.appstream_id, Path(app.appstream_id).stem])
        if app.primary_pkg:
            candidates.extend([app.primary_pkg, Path(app.primary_pkg).stem])
        seen: set[str] = set()
        normalized: list[str] = []
        for candidate in candidates:
            if not candidate:
                continue
            for variant in (candidate, Path(candidate).stem):
                variant = str(variant).strip()
                if not variant or variant in seen:
                    continue
                seen.add(variant)
                normalized.append(variant)
        _icon_debug(app, f"themed icon candidates={normalized!r}")
        for candidate in normalized:
            try:
                if theme.has_icon(candidate):
                    _icon_debug(app, f"theme matched icon name {candidate!r}")
                    return candidate
            except Exception as exc:
                _icon_debug(app, f"theme lookup failed for {candidate!r}: {exc}")
                continue
        candidates = normalized

    if candidates:
        _icon_debug(app, f"no theme match, falling back to first candidate {candidates[0]!r}")
    else:
        _icon_debug(app, "no themed icon candidates available")
    return candidates[0] if candidates else None


class IconWidget(Gtk.Box):
    def __init__(self, app: AppEntry, size: int = 64):
        super().__init__(orientation=Gtk.Orientation.HORIZONTAL, spacing=0)
        self.set_size_request(size, size)
        self.set_halign(Gtk.Align.START)
        self.set_valign(Gtk.Align.CENTER)
        self.set_hexpand(False)
        self.set_vexpand(False)

        holder = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=0)
        holder.set_size_request(size, size)
        holder.set_halign(Gtk.Align.CENTER)
        holder.set_valign(Gtk.Align.CENTER)
        holder.set_hexpand(False)
        holder.set_vexpand(False)
        self.append(holder)

        _icon_debug(app, f"icon_path={app.icon_path!r} icon_url={app.icon_url!r} icon_name={app.icon_name!r}")
        font_like = _is_font_like_app(app)
        crop_transparency = not font_like
        fill_ratio = 0.72 if font_like else 1.0

        image_widget = _image_from_ref(app.icon_path or app.icon_url, size, crop_transparency=crop_transparency, fill_ratio=fill_ratio)
        if image_widget is not None:
            _icon_debug(app, f"using picture from {app.icon_path or app.icon_url!r}")
            holder.append(image_widget)
            return

        themed_name = _resolve_themed_icon_name(app)
        if themed_name:
            _icon_debug(app, f"using themed icon {themed_name!r}")
        else:
            _icon_debug(app, "falling back to generic application-x-executable")

        image = _image_from_icon_name(themed_name or "application-x-executable", size, crop_transparency=crop_transparency, fill_ratio=fill_ratio)
        holder.append(image)


class NavSidebarButton(Gtk.Button):
    def __init__(self, title: str, callback):
        super().__init__()
        self.set_halign(Gtk.Align.START)
        self.set_has_frame(False)
        self.add_css_class("flat")
        self.add_css_class("nav-button")

        inner = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
        inner.set_halign(Gtk.Align.START)
        self.set_child(inner)

        self.label_widget = Gtk.Label(label=title, xalign=0)
        self.label_widget.add_css_class("nav-label")
        inner.append(self.label_widget)

        self.arrow_widget = Gtk.Label(label="❯", xalign=0)
        self.arrow_widget.add_css_class("nav-arrow")
        self.arrow_widget.set_visible(False)
        inner.append(self.arrow_widget)

        self.connect("clicked", lambda *_: callback())

    def set_active(self, active: bool) -> None:
        if active:
            self.add_css_class("active")
        else:
            self.remove_css_class("active")
        self.arrow_widget.set_visible(active)


class SubcategoryButton(Gtk.Box):
    def __init__(self, title: str, callback):
        super().__init__(orientation=Gtk.Orientation.HORIZONTAL, spacing=0)
        self.set_halign(Gtk.Align.START)
        self.set_valign(Gtk.Align.CENTER)
        self.set_hexpand(False)
        self.set_vexpand(False)

        self.chip = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=0)
        self.chip.set_halign(Gtk.Align.START)
        self.chip.set_valign(Gtk.Align.CENTER)
        self.chip.add_css_class("subcategory-chip")
        self.append(self.chip)

        self.label = Gtk.Label(label=title, xalign=0)
        self.label.set_halign(Gtk.Align.START)
        self.label.set_valign(Gtk.Align.CENTER)
        self.label.set_wrap(False)
        self.label.set_single_line_mode(True)
        self.label.add_css_class("subcategory-label")
        self.chip.append(self.label)

        gesture = Gtk.GestureClick()
        gesture.connect("released", lambda *_: callback())
        self.add_controller(gesture)

    def set_active(self, active: bool) -> None:
        if active:
            self.chip.add_css_class("active")
        else:
            self.chip.remove_css_class("active")


class AppCardRow(Gtk.ListBoxRow):
    __gtype_name__ = "DnfAppCenterAppCardRow"

    def __init__(self, app: AppEntry, action_cb, open_cb, queue_state_cb, page_mode: str = "default", update_selected: bool = False, update_toggle_cb=None):
        super().__init__()
        self.app = app
        self._action_cb = action_cb
        self._open_cb = open_cb
        self._queue_state_cb = queue_state_cb
        self._page_mode = page_mode
        self._update_toggle_cb = update_toggle_cb
        self.add_css_class("app-list-row")

        outer = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=12)
        outer.set_valign(Gtk.Align.CENTER)
        outer.add_css_class("app-card")
        outer.set_hexpand(True)
        self.set_child(outer)

        if self._page_mode == "updates":
            self.update_check = Gtk.CheckButton()
            self.update_check.add_css_class("update-check")
            self.update_check.set_active(update_selected)
            if self._update_toggle_cb is not None:
                self.update_check.connect("toggled", lambda btn: self._update_toggle_cb(self.app, btn.get_active()))
            outer.append(self.update_check)
        else:
            self.update_check = None

        outer.append(IconWidget(app, 92))

        text_col = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=4)
        text_col.set_hexpand(True)
        text_col.set_valign(Gtk.Align.CENTER)
        outer.append(text_col)

        self.title_label = Gtk.Label(xalign=0)
        self.title_label.add_css_class("app-title")
        self.title_label.set_wrap(False)
        self.title_label.set_single_line_mode(True)
        self.title_label.set_ellipsize(Pango.EllipsizeMode.END)
        self.title_label.set_text(app.name)
        text_col.append(self.title_label)

        self.summary_label = Gtk.Label(xalign=0)
        self.summary_label.add_css_class("app-summary")
        self.summary_label.set_wrap(True)
        self.summary_label.set_wrap_mode(Pango.WrapMode.WORD_CHAR)
        self.summary_label.set_lines(2)
        self.summary_label.set_max_width_chars(34)
        self.summary_label.set_ellipsize(Pango.EllipsizeMode.END)
        self.summary_label.set_text(app.summary)
        text_col.append(self.summary_label)

        button_box = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=0)
        button_box.set_valign(Gtk.Align.CENTER)
        button_box.set_halign(Gtk.Align.END)
        outer.append(button_box)

        self.action_button = Gtk.Button()
        self.action_button.connect("clicked", lambda *_: self._action_cb(self.app))
        button_box.append(self.action_button)
        self.refresh()

    def refresh(self) -> None:
        queue_state = self._queue_state_cb(self.app)
        if queue_state is not None:
            self.action_button.set_label(queue_state)
            self.action_button.set_sensitive(False)
        else:
            self.action_button.set_sensitive(True)
            if self._page_mode == "updates":
                self.action_button.set_label("Update")
            else:
                self.action_button.set_label(_("Remove") if self.app.installed else _("Install"))
        self.action_button.remove_css_class("destructive-action")
        self.action_button.remove_css_class("suggested-action")
        if self._page_mode == "updates":
            self.action_button.add_css_class("suggested-action")
        else:
            self.action_button.add_css_class("destructive-action" if self.app.installed else "suggested-action")




class AppCardTile(Gtk.Box):
    __gtype_name__ = "DnfAppCenterAppCardTile"

    def __init__(self, app: AppEntry, action_cb, open_cb, queue_state_cb, page_mode: str = "default", update_selected: bool = False, update_toggle_cb=None):
        super().__init__(orientation=Gtk.Orientation.HORIZONTAL, spacing=10)
        self.app = app
        self._action_cb = action_cb
        self._open_cb = open_cb
        self._queue_state_cb = queue_state_cb
        self._page_mode = page_mode
        self._update_toggle_cb = update_toggle_cb
        self.add_css_class("app-card")
        self.set_margin_top(4)
        self.set_margin_bottom(4)
        self.set_margin_start(4)
        self.set_margin_end(4)
        self.set_hexpand(True)
        self.set_size_request(300, 92)
        self.set_valign(Gtk.Align.FILL)

        if self._page_mode == "updates":
            self.update_check = Gtk.CheckButton()
            self.update_check.add_css_class("update-check")
            self.update_check.set_active(update_selected)
            if self._update_toggle_cb is not None:
                self.update_check.connect("toggled", lambda btn: self._update_toggle_cb(self.app, btn.get_active()))
            self.append(self.update_check)
        else:
            self.update_check = None

        self.append(IconWidget(app, 64))

        text_col = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=4)
        text_col.set_hexpand(True)
        text_col.set_valign(Gtk.Align.CENTER)
        self.append(text_col)

        self.title_label = Gtk.Label(xalign=0)
        self.title_label.add_css_class("app-title")
        self.title_label.set_wrap(False)
        self.title_label.set_single_line_mode(True)
        self.title_label.set_ellipsize(Pango.EllipsizeMode.END)
        self.title_label.set_text(app.name)
        text_col.append(self.title_label)

        self.summary_label = Gtk.Label(xalign=0)
        self.summary_label.add_css_class("app-summary")
        self.summary_label.set_wrap(True)
        self.summary_label.set_wrap_mode(Pango.WrapMode.WORD_CHAR)
        self.summary_label.set_lines(2)
        self.summary_label.set_max_width_chars(22)
        self.summary_label.set_ellipsize(Pango.EllipsizeMode.END)
        self.summary_label.set_text(app.summary)
        text_col.append(self.summary_label)

        button_box = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=0)
        button_box.set_valign(Gtk.Align.CENTER)
        button_box.set_halign(Gtk.Align.END)
        self.append(button_box)

        self.action_button = Gtk.Button()
        self.action_button.connect("clicked", lambda *_: self._action_cb(self.app))
        button_box.append(self.action_button)

        gesture = Gtk.GestureClick()
        gesture.connect("released", lambda *_: self._open_cb(self.app))
        self.add_controller(gesture)
        self.refresh()

    def refresh(self) -> None:
        queue_state = self._queue_state_cb(self.app)
        if queue_state is not None:
            self.action_button.set_label(queue_state)
            self.action_button.set_sensitive(False)
        else:
            self.action_button.set_sensitive(True)
            if self._page_mode == "updates":
                self.action_button.set_label("Update")
            else:
                self.action_button.set_label(_("Remove") if self.app.installed else _("Install"))
        self.action_button.remove_css_class("destructive-action")
        self.action_button.remove_css_class("suggested-action")
        if self._page_mode == "updates":
            self.action_button.add_css_class("suggested-action")
        else:
            self.action_button.add_css_class("destructive-action" if self.app.installed else "suggested-action")


class MainWindow(Adw.ApplicationWindow):
    def __init__(self, app: Adw.Application, launch_updates: bool = False):
        super().__init__(application=app)
        self.set_title(_("DNF App Center"))
        self.set_default_size(1280, 800)

        self.catalog: AppStreamCatalog | None = None
        self.backend: DnfBackend | None = None
        self.apps: list[AppEntry] = []
        self.current_items: list[AppEntry] = []
        self.current_page = "updates" if launch_updates else "news"
        self.current_group = "system"
        self.current_subcategory: str | None = None
        self.current_search_text = ""
        self.current_category_filter_text = ""
        self.current_repo_filter = "__all__"
        self.updater_settings = load_updater_settings()
        self.current_app: AppEntry | None = None
        self.update_selection: set[str] = set()
        self.queue_items: list[QueueItem] = []
        self.queue_logs: list[str] = []
        self.queue_log_full: list[str] = []
        self.queue_worker_running = False
        self.current_queue_item: QueueItem | None = None
        self._appstream_pkg_names: set[str] = set()
        self.nav_buttons: dict[str, Gtk.Button] = {}
        self.subcategory_buttons: dict[str, Gtk.Widget] = {}
        self.subcategory_button_pages: dict[str, int] = {}
        self.subcategory_pages: list[list[tuple[str, str]]] = []
        self.current_subcategory_page = 0
        self._page_items_cache: dict[tuple, list[AppEntry]] = {}
        self._data_revision = 0

        provider = Gtk.CssProvider()
        provider.load_from_bytes(GLib.Bytes.new(CSS))
        Gtk.StyleContext.add_provider_for_display(
            self.get_display(), provider, Gtk.STYLE_PROVIDER_PRIORITY_APPLICATION
        )

        self.toast_overlay = Adw.ToastOverlay()
        self.set_content(self.toast_overlay)

        root = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        self.toast_overlay.set_child(root)
        self.connect("close-request", self._on_close_request)
        self._setup_rpm_drop_target()

        root.append(self._build_top_bar())

        body = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=0)
        body.set_vexpand(True)
        root.append(body)

        sidebar_scroll = Gtk.ScrolledWindow()
        sidebar_scroll.set_policy(Gtk.PolicyType.NEVER, Gtk.PolicyType.AUTOMATIC)
        sidebar_scroll.set_min_content_width(308)
        sidebar_scroll.set_size_request(308, -1)
        sidebar_scroll.set_vexpand(True)
        body.append(sidebar_scroll)

        self.sidebar_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        self.sidebar_box.add_css_class("sidebar")
        sidebar_scroll.set_child(self.sidebar_box)
        self._build_sidebar()

        self.content_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        self.content_box.add_css_class("content-box")
        self.content_box.set_hexpand(True)
        self.content_box.set_vexpand(True)
        body.append(self.content_box)

        self.content_header_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        self.content_header_box.add_css_class("content-header")
        self.content_box.append(self.content_header_box)

        self.title_row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=12)
        self.title_row.set_hexpand(True)
        self.title_row.set_halign(Gtk.Align.FILL)
        self.content_header_box.append(self.title_row)

        self.title_label = Gtk.Label(xalign=0)
        self.title_label.add_css_class("title-1")
        self.title_label.set_hexpand(False)
        self.title_label.set_halign(Gtk.Align.START)
        self.title_row.append(self.title_label)

        self.title_row_spacer = Gtk.Box()
        self.title_row_spacer.set_hexpand(True)
        self.title_row.append(self.title_row_spacer)

        self.category_filter_entry = Gtk.SearchEntry()
        self.category_filter_entry.set_placeholder_text(_("Search filter…"))
        self.category_filter_entry.set_hexpand(False)
        self.category_filter_entry.set_halign(Gtk.Align.END)
        self.category_filter_entry.set_valign(Gtk.Align.CENTER)
        self.category_filter_entry.set_size_request(280, -1)
        self.category_filter_entry.set_visible(False)
        self.category_filter_entry.connect("search-changed", self._on_category_filter_changed)
        self.title_row.append(self.category_filter_entry)

        self.status_label = Gtk.Label(xalign=0)
        self.status_label.add_css_class("dim-label")
        self.content_header_box.append(self.status_label)

        self.updates_action_bar = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
        self.updates_action_bar.add_css_class("updates-action-bar")
        self.updates_action_bar.set_visible(False)
        self.content_header_box.append(self.updates_action_bar)

        self.updates_selected_label = Gtk.Label(xalign=0)
        self.updates_selected_label.add_css_class("dim-label")
        self.updates_action_bar.append(self.updates_selected_label)

        self.updates_clear_button = Gtk.Button(label=_("Clear Selection"))
        self.updates_clear_button.connect("clicked", lambda *_: self._clear_update_selection())
        self.updates_action_bar.append(self.updates_clear_button)

        self.update_selected_button = Gtk.Button(label=_("Update Selected"))
        self.update_selected_button.add_css_class("suggested-action")
        self.update_selected_button.connect("clicked", lambda *_: self._queue_selected_updates())
        self.updates_action_bar.append(self.update_selected_button)

        self.update_all_button = Gtk.Button(label=_("Update System"))
        self.update_all_button.connect("clicked", lambda *_: self._queue_system_update())
        self.updates_action_bar.append(self.update_all_button)

        self.subcategory_strip = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
        self.subcategory_strip.add_css_class("subcat-strip")
        self.subcategory_strip.set_hexpand(True)
        self.subcategory_strip.set_halign(Gtk.Align.FILL)
        self.content_header_box.append(self.subcategory_strip)

        self.subcat_left_button = Gtk.Button.new_from_icon_name("pan-start-symbolic")
        self.subcat_left_button.add_css_class("pan-button")
        self.subcat_left_button.connect("clicked", self._on_subcat_pan_start)
        self.subcategory_strip.append(self.subcat_left_button)

        self.subcategory_frame = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=0)
        self.subcategory_frame.add_css_class("subcat-strip-frame")
        self.subcategory_frame.set_hexpand(True)
        self.subcategory_frame.set_halign(Gtk.Align.FILL)
        self.subcategory_frame.set_valign(Gtk.Align.CENTER)
        self.subcategory_strip.append(self.subcategory_frame)

        self.subcategory_stack = Gtk.Stack()
        self.subcategory_stack.set_transition_type(Gtk.StackTransitionType.SLIDE_LEFT_RIGHT)
        self.subcategory_stack.set_transition_duration(180)
        self.subcategory_stack.set_hexpand(True)
        self.subcategory_stack.set_halign(Gtk.Align.FILL)
        self.subcategory_stack.set_valign(Gtk.Align.CENTER)
        self.subcategory_frame.append(self.subcategory_stack)

        self.subcat_right_button = Gtk.Button.new_from_icon_name("pan-end-symbolic")
        self.subcat_right_button.add_css_class("pan-button")
        self.subcat_right_button.connect("clicked", self._on_subcat_pan_end)
        self.subcategory_strip.append(self.subcat_right_button)

        self.stack = Gtk.Stack()
        self.stack.set_hexpand(True)
        self.stack.set_vexpand(True)
        self.content_box.append(self.stack)

        self.bottom_queue_revealer = Gtk.Revealer()
        self.bottom_queue_revealer.set_transition_type(Gtk.RevealerTransitionType.SLIDE_UP)
        self.bottom_queue_revealer.set_reveal_child(False)
        root.append(self.bottom_queue_revealer)

        bottom_bar = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=12)
        bottom_bar.add_css_class("queue-bottom-bar")
        self.bottom_queue_revealer.set_child(bottom_bar)

        self.bottom_queue_progress = Gtk.ProgressBar()
        self.bottom_queue_progress.set_hexpand(True)
        bottom_bar.append(self.bottom_queue_progress)

        self.bottom_queue_status = Gtk.Label(xalign=0)
        self.bottom_queue_status.add_css_class("dim-label")
        self.bottom_queue_status.add_css_class("queue-bottom-status")
        bottom_bar.append(self.bottom_queue_status)

        queue_button = Gtk.Button(label=_("View Queue"))
        queue_button.connect("clicked", lambda *_: self._switch_page("system", "queue"))
        bottom_bar.append(queue_button)

        self._build_list_page()
        self._build_news_page()
        self._build_queue_page()
        self._build_repo_page()
        self._build_detail_page()

        self._show_loading_page(_("Loading AppStream metadata and DNF repositories…"))
        self._load_async()

    def _setup_rpm_drop_target(self) -> None:
        try:
            drop_target = Gtk.DropTarget.new(Gdk.FileList, Gdk.DragAction.COPY)
        except Exception:
            return
        drop_target.connect("drop", self._on_rpm_drop)
        self.add_controller(drop_target)

    def _extract_dropped_rpm_paths(self, value) -> list[str]:
        paths: list[str] = []
        files_obj = None
        try:
            files_obj = value.get_files()
        except Exception:
            files_obj = value

        try:
            n_items = files_obj.get_n_items()
            for i in range(n_items):
                item = files_obj.get_item(i)
                try:
                    path = item.get_path()
                except Exception:
                    path = None
                if path and path.endswith('.rpm'):
                    paths.append(path)
        except Exception:
            try:
                for item in files_obj:  # type: ignore
                    path = item.get_path()
                    if path and path.endswith('.rpm'):
                        paths.append(path)
            except Exception:
                pass
        return paths

    def _on_rpm_drop(self, _target, value, _x, _y) -> bool:
        rpm_paths = self._extract_dropped_rpm_paths(value)
        if not rpm_paths:
            self._show_toast('Drop one or more .rpm files to install them.')
            return False
        self._queue_rpm_file_install(rpm_paths)
        return True

    def queue_rpm_file_install(self, rpm_paths: list[str]) -> None:
        self._queue_rpm_file_install(rpm_paths)

    def _queue_rpm_file_install(self, rpm_paths: list[str]) -> None:
        rpm_paths = [str(Path(path)) for path in rpm_paths if str(path).lower().endswith('.rpm')]
        if not rpm_paths:
            self._show_toast('No RPM files were found in the drop.')
            return
        queued_files = {path for item in self.queue_items for path in getattr(item, 'file_paths', []) if item.status in {'queued', 'running'}}
        unique_paths = [path for path in rpm_paths if path not in queued_files]
        if not unique_paths:
            self._show_toast('Those RPM files are already queued.')
            return
        label = Path(unique_paths[0]).name if len(unique_paths) == 1 else f"{len(unique_paths)} RPM files"
        pseudo_app = AppEntry(
            appstream_id='local-rpm-install',
            name=_('Local RPM Install'),
            summary=_('Install local RPM files'),
            description=_('Install local RPM files'),
            pkg_names=[],
        )
        item = QueueItem(app=pseudo_app, action='install-rpms', message=f"Queued to install {len(unique_paths)} RPM file(s)", file_paths=unique_paths, label=label)
        self.queue_items.append(item)
        self._append_queue_log(f"Queued RPM install for {', '.join(Path(path).name for path in unique_paths[:5])}{'…' if len(unique_paths) > 5 else ''}")
        self.status_label.set_text(self._queue_status_text())
        self._refresh_queue_page()
        self._refresh_main_page()
        self._refresh_detail_action_button()
        self._switch_page('system', 'queue')
        if self.backend and not self.queue_worker_running:
            self._prompt_install()

    def _build_top_bar(self) -> Gtk.Widget:
        handle = Gtk.WindowHandle()
        bar = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=12)
        bar.add_css_class("top-bar")
        handle.set_child(bar)

        self.search_entry = Gtk.SearchEntry()
        self.search_entry.set_placeholder_text(_("Search applications…"))
        self.search_entry.set_hexpand(False)
        self.search_entry.set_size_request(280, -1)
        self.search_entry.connect("search-changed", self._on_search_changed)
        bar.append(self.search_entry)

        self.repo_filter_combo = Gtk.ComboBoxText()
        self.repo_filter_combo.append("__all__", "All repositories")
        self.repo_filter_combo.set_active_id("__all__")
        self.repo_filter_combo.connect("changed", self._on_repo_filter_changed)
        self.repo_filter_combo.set_size_request(220, -1)
        bar.append(self.repo_filter_combo)

        spacer = Gtk.Box()
        spacer.set_hexpand(True)
        bar.append(spacer)

        auth_box = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
        auth_box.set_valign(Gtk.Align.CENTER)

        auth_label = Gtk.Label(label=_("Remember auth"))
        auth_label.set_valign(Gtk.Align.CENTER)
        auth_box.append(auth_label)

        self.cache_auth_check = Gtk.CheckButton()
        self.cache_auth_check.set_active(True)
        self.cache_auth_check.set_tooltip_text("Keep package authorization for this app session until the app closes.")
        self.cache_auth_check.set_valign(Gtk.Align.CENTER)
        self.cache_auth_check.connect("toggled", self._on_cache_auth_toggled)
        auth_box.append(self.cache_auth_check)
        bar.append(auth_box)

        refresh_button = Gtk.Button.new_from_icon_name("view-refresh-symbolic")
        refresh_button.connect("clicked", lambda *_: self._load_async(force=True))
        bar.append(refresh_button)

        menu_button = Gtk.MenuButton(icon_name="open-menu-symbolic")
        menu_button.set_tooltip_text("Updater settings")
        menu_button.set_popover(self._build_app_menu_popover())
        bar.append(menu_button)

        controls = Gtk.WindowControls()
        controls.set_side(Gtk.PackType.END)
        controls.set_decoration_layout(":minimize,maximize,close")
        bar.append(controls)
        return handle

    def _build_app_menu_popover(self) -> Gtk.Popover:
        popover = Gtk.Popover()
        box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=10)
        box.set_margin_top(12)
        box.set_margin_bottom(12)
        box.set_margin_start(12)
        box.set_margin_end(12)

        heading = Gtk.Label(label=_("Update checker"), xalign=0)
        heading.add_css_class("heading")
        box.append(heading)

        self.updater_enabled_check = Gtk.CheckButton(label=_("Enable update checker"))
        self.updater_enabled_check.set_active(bool(self.updater_settings.get("enabled", True)))
        self.updater_enabled_check.connect("toggled", self._on_updater_setting_changed)
        box.append(self.updater_enabled_check)

        self.updater_notifications_check = Gtk.CheckButton(label=_("Enable notifications"))
        self.updater_notifications_check.set_active(bool(self.updater_settings.get("notifications", True)))
        self.updater_notifications_check.connect("toggled", self._on_updater_setting_changed)
        box.append(self.updater_notifications_check)

        interval_row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
        interval_label = Gtk.Label(label=_("Check every"), xalign=0)
        interval_label.set_valign(Gtk.Align.CENTER)
        interval_row.append(interval_label)

        adjustment = Gtk.Adjustment(value=float(self.updater_settings.get("interval_value", 12)), lower=1, upper=999, step_increment=1, page_increment=10)
        self.updater_interval_spin = Gtk.SpinButton(adjustment=adjustment, climb_rate=1, digits=0)
        self.updater_interval_spin.connect("value-changed", self._on_updater_setting_changed)
        interval_row.append(self.updater_interval_spin)

        self.updater_interval_unit = Gtk.ComboBoxText()
        for unit in ("hours", "days", "weeks"):
            self.updater_interval_unit.append(unit, unit.capitalize())
        self.updater_interval_unit.set_active_id(str(self.updater_settings.get("interval_unit", "hours")))
        self.updater_interval_unit.connect("changed", self._on_updater_setting_changed)
        interval_row.append(self.updater_interval_unit)
        box.append(interval_row)

        note = Gtk.Label(label=_("These settings affect the tray updater service."), xalign=0)
        note.add_css_class("dim-label")
        note.set_wrap(True)
        box.append(note)

        popover.set_child(box)
        return popover

    def _on_updater_setting_changed(self, *_args) -> None:
        settings = {
            "enabled": self.updater_enabled_check.get_active(),
            "notifications": self.updater_notifications_check.get_active(),
            "interval_value": int(self.updater_interval_spin.get_value()),
            "interval_unit": self.updater_interval_unit.get_active_id() or "hours",
        }
        save_updater_settings(settings)
        self.updater_settings = load_updater_settings()

    def _build_sidebar(self) -> None:
        self.sidebar_box.append(self._section_label(_("System"), "applications-system-symbolic"))
        for key, title in CATEGORY_GROUPS["system"].items():
            self.sidebar_box.append(self._nav_button(key, title, "system"))

        self.sidebar_box.append(Gtk.Separator(orientation=Gtk.Orientation.HORIZONTAL))
        self.sidebar_box.append(self._section_label(_("Categories"), "starred-symbolic"))
        for key, title in CATEGORY_GROUPS["categories"].items():
            self.sidebar_box.append(self._nav_button(key, title, "categories"))

    def _section_label(self, text: str, icon_name: str) -> Gtk.Widget:
        box = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=7)
        box.add_css_class("sidebar-section-box")
        icon_widget = Gtk.Image.new_from_gicon(Gio.ThemedIcon.new(icon_name))
        icon_widget.set_pixel_size(28)
        icon_widget.set_valign(Gtk.Align.CENTER)
        box.append(icon_widget)
        label = Gtk.Label(label=text, xalign=0)
        label.add_css_class("sidebar-section")
        box.append(label)
        return box

    def _nav_button(self, key: str, title: str, group: str) -> Gtk.Widget:
        button = NavSidebarButton(title, lambda: self._switch_page(group, key))
        self.nav_buttons[f"{group}:{key}"] = button
        return button

    def _build_list_page(self) -> None:
        outer = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        outer.set_vexpand(True)
        self.stack.add_titled(outer, "list", "List")

        self.listbox = Gtk.ListBox()
        self.listbox.add_css_class("app-list")
        self.listbox.set_selection_mode(Gtk.SelectionMode.NONE)
        self.listbox.set_activate_on_single_click(True)
        self.listbox.connect("row-activated", self._on_row_activated)

        self.list_scroller = Gtk.ScrolledWindow()
        self.list_scroller.set_policy(Gtk.PolicyType.NEVER, Gtk.PolicyType.AUTOMATIC)
        self.list_scroller.set_child(self.listbox)
        self.list_scroller.set_vexpand(True)
        outer.append(self.list_scroller)

        self.empty_status = Adw.StatusPage()
        self.empty_status.set_title("No applications found")
        self.empty_status.set_description("Try a different category or search term.")
        outer.append(self.empty_status)
        self.empty_status.set_visible(False)

    def _build_news_page(self) -> None:
        self.news_scroll = Gtk.ScrolledWindow()
        self.news_scroll.set_policy(Gtk.PolicyType.NEVER, Gtk.PolicyType.AUTOMATIC)
        self.stack.add_titled(self.news_scroll, "news", _("News"))

        box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=10)
        box.set_margin_top(8)
        box.set_margin_bottom(20)
        box.set_margin_start(4)
        box.set_margin_end(4)
        self.news_scroll.set_child(box)

        self.news_card = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=14)
        self.news_card.add_css_class("news-card")
        box.append(self.news_card)

        self.news_content_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=12)
        self.news_card.append(self.news_content_box)

        self.news_text = "Loading news…"
        self._refresh_news_page()

    def _build_queue_page(self) -> None:
        scroller = Gtk.ScrolledWindow()
        scroller.set_policy(Gtk.PolicyType.NEVER, Gtk.PolicyType.AUTOMATIC)
        self.stack.add_titled(scroller, "queue", _("Queue"))

        box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=10)
        box.set_margin_top(8)
        box.set_margin_bottom(20)
        box.set_margin_start(4)
        box.set_margin_end(4)
        scroller.set_child(box)

        self.queue_progress = Gtk.ProgressBar()
        self.queue_progress.add_css_class("queue-progress")
        box.append(self.queue_progress)


        queue_title = Gtk.Label(label=_("Queued actions"), xalign=0)
        queue_title.add_css_class("title-4")
        box.append(queue_title)

        self.queue_list_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=8)
        box.append(self.queue_list_box)

        log_title = Gtk.Label(label=_("Transaction log"), xalign=0)
        log_title.add_css_class("title-4")
        box.append(log_title)

        self.queue_log_view = Gtk.TextView()
        self.queue_log_view.set_editable(False)
        self.queue_log_view.set_cursor_visible(False)
        self.queue_log_view.set_monospace(True)
        self.queue_log_view.add_css_class("queue-log-view")
        self.queue_log_view.set_wrap_mode(Gtk.WrapMode.WORD_CHAR)

        log_scroll = Gtk.ScrolledWindow()
        log_scroll.set_policy(Gtk.PolicyType.NEVER, Gtk.PolicyType.AUTOMATIC)
        log_scroll.set_min_content_height(220)
        log_scroll.set_child(self.queue_log_view)
        box.append(log_scroll)

        queue_actions = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
        queue_actions.set_halign(Gtk.Align.START)
        self.view_log_button = Gtk.Button(label=_("View transaction log"))
        self.view_log_button.connect("clicked", self._on_view_transaction_log)
        queue_actions.append(self.view_log_button)
        self.send_paste_button = Gtk.Button(label=_("Send to pastebin"))
        self.send_paste_button.connect("clicked", self._on_send_to_pastebin)
        queue_actions.append(self.send_paste_button)
        box.append(queue_actions)

        self._refresh_queue_page()

    def _build_repo_page(self) -> None:
        self.repo_scroll = Gtk.ScrolledWindow()
        self.repo_scroll.set_policy(Gtk.PolicyType.NEVER, Gtk.PolicyType.AUTOMATIC)
        self.repo_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=8)
        self.repo_box.set_margin_top(6)
        self.repo_box.set_margin_bottom(12)
        self.repo_box.set_margin_start(4)
        self.repo_box.set_margin_end(4)
        self.repo_scroll.set_child(self.repo_box)
        self.stack.add_titled(self.repo_scroll, "repos", "Repositories")

    def _build_detail_page(self) -> None:
        scroller = Gtk.ScrolledWindow()
        scroller.set_policy(Gtk.PolicyType.NEVER, Gtk.PolicyType.AUTOMATIC)
        self.stack.add_titled(scroller, "details", "Details")

        self.detail_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=16)
        self.detail_box.set_margin_top(8)
        self.detail_box.set_margin_bottom(20)
        self.detail_box.set_margin_start(4)
        self.detail_box.set_margin_end(4)
        scroller.set_child(self.detail_box)

        top = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=12)
        self.detail_box.append(top)
        back = Gtk.Button(label=_("← Back"))
        back.connect("clicked", lambda *_: self.stack.set_visible_child_name("list"))
        top.append(back)

        top_spacer = Gtk.Box()
        top_spacer.set_hexpand(True)
        top.append(top_spacer)

        hero = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=18)
        hero.add_css_class("detail-hero")
        self.detail_box.append(hero)

        self.detail_icon_box = Gtk.Box()
        hero.append(self.detail_icon_box)

        hero_text = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=8)
        hero_text.set_hexpand(True)
        hero.append(hero_text)

        self.detail_title = Gtk.Label(xalign=0)
        self.detail_title.add_css_class("title-1")
        self.detail_title.set_wrap(True)
        hero_text.append(self.detail_title)

        self.detail_summary = Gtk.Label(xalign=0)
        self.detail_summary.add_css_class("title-4")
        self.detail_summary.add_css_class("dim-label")
        self.detail_summary.set_wrap(True)
        hero_text.append(self.detail_summary)

        self.detail_meta = Gtk.Label(xalign=0)
        self.detail_meta.set_wrap(True)
        self.detail_meta.set_selectable(True)
        hero_text.append(self.detail_meta)

        action_box = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=10)
        hero_text.append(action_box)

        self.detail_action_button = Gtk.Button(label=_("Install"))
        self.detail_action_button.connect("clicked", lambda *_: self.current_app and self._run_action_for_app(self.current_app, "update" if (self.current_group == "system" and self.current_page == "updates" and self.current_app and self.current_app.installed and self.current_app.candidate_version and self.current_app.candidate_version != self.current_app.installed_version) else None))
        action_box.append(self.detail_action_button)

        self.detail_open_button = Gtk.Button(label=_("Open"))
        self.detail_open_button.connect("clicked", self._on_open_clicked)
        action_box.append(self.detail_open_button)

        self.detail_description_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=10)
        self.detail_box.append(self.detail_description_box)

        self.detail_links = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=8)
        self.detail_box.append(self.detail_links)

        self.detail_screenshots = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=8)
        self.detail_box.append(self.detail_screenshots)

    def _refresh_detail_description(self, raw_text: str) -> None:
        if not hasattr(self, "detail_description_box"):
            return
        child = self.detail_description_box.get_first_child()
        while child is not None:
            next_child = child.get_next_sibling()
            self.detail_description_box.remove(child)
            child = next_child

        blocks = _markup_blocks_from_text(raw_text or "")
        for block in blocks:
            label = Gtk.Label(xalign=0, yalign=0)
            label.add_css_class("body")
            label.set_wrap(True)
            label.set_wrap_mode(Pango.WrapMode.WORD_CHAR)
            label.set_selectable(True)
            label.set_use_markup(True)
            label.set_markup(block)
            label.connect("activate-link", lambda _label, uri: (Gtk.show_uri(None, uri, Gdk.CURRENT_TIME), True)[1])
            self.detail_description_box.append(label)

    def _invalidate_page_caches(self) -> None:
        self._data_revision += 1
        self._page_items_cache.clear()

    def _page_cache_key(self) -> tuple:
        return (
            self._data_revision,
            self.current_group,
            self.current_page,
            self.current_subcategory,
            self.current_search_text,
            self.current_category_filter_text,
            self.current_repo_filter,
        )

    def _show_loading_page(self, message: str) -> None:
        spinner = Gtk.Spinner()
        spinner.start()
        self.status_label.set_text(message)
        self.title_label.set_text(_("Loading…"))
        self._clear_listbox()
        row = Gtk.ListBoxRow()
        box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=12)
        box.set_margin_top(64)
        box.set_margin_bottom(64)
        box.set_margin_start(12)
        box.set_margin_end(12)
        box.set_halign(Gtk.Align.CENTER)
        box.append(spinner)
        label = Gtk.Label(label=message)
        box.append(label)
        row.set_child(box)
        self.listbox.append(row)
        self.stack.set_visible_child_name("list")

    def _load_async(self, force: bool = False) -> None:
        if force:
            self._invalidate_page_caches()
        self._show_loading_page(_("Loading AppStream metadata and DNF repositories…"))

        def worker() -> None:
            try:
                backend = DnfBackend()
                if force:
                    backend.reload_state(force_refresh=True)
                catalog = AppStreamCatalog()
                apps = catalog.load()
                backend.enrich_apps(apps)
                repos = backend.get_repositories()
                news_text = self._fetch_news_text()
            except (AppStreamUnavailable, DnfUnavailable, Exception) as exc:
                GLib.idle_add(self._load_failed, exc, traceback.format_exc())
                return
            GLib.idle_add(self._load_succeeded, catalog, backend, apps, repos, news_text)

        threading.Thread(target=worker, daemon=True).start()

    def _load_succeeded(self, catalog: AppStreamCatalog, backend: DnfBackend, apps: list[AppEntry], repos: list[dict[str, str]], news_text: str) -> bool:
        self.catalog = catalog
        self.backend = backend
        self.apps = apps
        self._appstream_pkg_names = {pkg for app in apps for pkg in app.pkg_names}
        self.repos = repos
        self.news_text = news_text
        self._invalidate_page_caches()
        self._refresh_news_page()
        self.status_label.set_text(f"Loaded {len(apps)} applications from AppStream.")
        self._rebuild_repo_page()
        self._populate_repo_filter_dropdown()
        self._switch_page(self.current_group, self.current_page)
        if self.queue_items and not self.queue_worker_running:
            GLib.idle_add(self._prompt_install)
        return False

    def _fetch_news_text(self) -> str:
        url = str(load_updater_settings().get('update_feed_url') or '').strip()
        if not url:
            return _('No update feed URL is configured.')
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "DNF App Center/1.0"})
            with urllib.request.urlopen(req, timeout=5) as response:
                data = response.read()
            text = data.decode("utf-8", errors="replace").strip()
            return text or "No news available."
        except Exception as exc:
            return f"Failed to load news from {url}.\n\n{exc}"

    def _refresh_news_page(self) -> None:
        if not hasattr(self, "news_content_box"):
            return
        child = self.news_content_box.get_first_child()
        while child is not None:
            next_child = child.get_next_sibling()
            self.news_content_box.remove(child)
            child = next_child

        raw_text = (getattr(self, "news_text", "Loading news…") or "No news available.").strip()
        sections = [part.strip() for part in raw_text.replace("\r\n", "\n").split("\n\n") if part.strip()]
        if not sections:
            sections = ["No news available."]

        for section in sections:
            label = Gtk.Label(xalign=0, yalign=0)
            label.add_css_class("news-body")
            label.set_wrap(True)
            label.set_wrap_mode(Pango.WrapMode.WORD_CHAR)
            label.set_selectable(True)
            label.set_use_markup(True)
            label.set_markup(section)
            label.connect("activate-link", lambda _label, uri: (Gtk.show_uri(None, uri, Gdk.CURRENT_TIME), True)[1])
            self.news_content_box.append(label)

    def _load_failed(self, exc: Exception, tb: str) -> bool:
        self.status_label.set_text(_("Failed to load metadata."))
        self._show_toast(str(exc))
        self.title_label.set_text(_("Startup failed"))
        self._clear_listbox()
        row = Gtk.ListBoxRow()
        label = Gtk.Label(label=f"{exc}\n\n{tb}", xalign=0)
        label.set_wrap(True)
        label.set_selectable(True)
        label.set_margin_top(18)
        label.set_margin_bottom(18)
        label.set_margin_start(18)
        label.set_margin_end(18)
        row.set_child(label)
        self.listbox.append(row)
        return False

    def _switch_page(self, group: str, key: str) -> None:
        if self.current_search_text or self.search_entry.get_text():
            self.search_entry.set_text("")
        self.current_search_text = ""
        if self.current_category_filter_text or self.category_filter_entry.get_text():
            self.category_filter_entry.set_text("")
        self.current_category_filter_text = ""
        self.current_repo_filter = "__all__"
        self.current_group = group
        self.current_page = key
        self.current_subcategory = None
        self._update_nav_buttons()
        self._rebuild_subcategories()
        self._refresh_main_page(preserve_scroll=False)

    def _update_nav_buttons(self) -> None:
        for button in self.nav_buttons.values():
            if hasattr(button, "set_active"):
                button.set_active(False)
            else:
                button.remove_css_class("active")
        active = self.nav_buttons.get(f"{self.current_group}:{self.current_page}")
        if active is not None:
            if hasattr(active, "set_active"):
                active.set_active(True)
            else:
                active.add_css_class("active")

    def _rebuild_subcategories(self) -> None:
        self.subcategory_buttons.clear()
        self.subcategory_button_pages.clear()
        self.subcategory_pages = []
        self.current_subcategory_page = 0

        child = self.subcategory_stack.get_first_child()
        while child is not None:
            next_child = child.get_next_sibling()
            self.subcategory_stack.remove(child)
            child = next_child

        subcats = SUBCATEGORY_GROUPS.get(self.current_page, {}) if self.current_group == "categories" else {}
        has_subcats = bool(subcats)
        self.subcategory_strip.set_visible(has_subcats)
        if not subcats:
            return

        entries: list[tuple[str, str]] = [("__all__", "All")]
        entries.extend((key, title) for key, title in subcats.items())
        self.subcategory_pages = self._paginate_subcategories(entries)

        for page_index, page_entries in enumerate(self.subcategory_pages):
            page = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
            page.add_css_class("subcat-page")
            page.set_hexpand(False)
            page.set_halign(Gtk.Align.CENTER)
            page.set_valign(Gtk.Align.CENTER)

            for key, title in page_entries:
                callback_key = None if key == "__all__" else key
                button = SubcategoryButton(title, lambda s=callback_key: self._select_subcategory(s))
                page.append(button)
                self.subcategory_buttons[key] = button
                self.subcategory_button_pages[key] = page_index

            self.subcategory_stack.add_named(page, f"page-{page_index}")

        self._highlight_subcategory_button()
        self._set_subcategory_page(self.subcategory_button_pages.get("__all__", 0), animate=False)

    def _paginate_subcategories(self, entries: list[tuple[str, str]]) -> list[list[tuple[str, str]]]:
        available_width = self._subcategory_page_width()
        pages: list[list[tuple[str, str]]] = []
        current_page: list[tuple[str, str]] = []
        current_width = 0

        for key, title in entries:
            item_width = self._estimate_subcategory_width(title)
            if current_page and current_width + item_width > available_width:
                pages.append(current_page)
                current_page = [(key, title)]
                current_width = item_width
            else:
                current_page.append((key, title))
                current_width += item_width

        if current_page:
            pages.append(current_page)

        return pages or [entries]

    def _subcategory_page_width(self) -> int:
        window_width = 0
        try:
            window_width = int(self.get_width())
        except Exception:
            window_width = 0

        if window_width <= 0:
            window_width = 1280

        available = window_width - 420
        return max(320, available)

    def _estimate_subcategory_width(self, title: str) -> int:
        return max(74, 32 + (len(title) * 9))

    def _select_subcategory(self, key: str | None) -> None:
        self.current_subcategory = key
        self._highlight_subcategory_button()
        self._refresh_main_page()

    def _highlight_subcategory_button(self) -> None:
        for key, button in self.subcategory_buttons.items():
            if hasattr(button, "set_active"):
                button.set_active(False)
            else:
                button.remove_css_class("active")

        active = "__all__" if self.current_subcategory is None else self.current_subcategory
        button = self.subcategory_buttons.get(active)
        if button is not None:
            if hasattr(button, "set_active"):
                button.set_active(True)
            else:
                button.add_css_class("active")
            page_index = self.subcategory_button_pages.get(active, 0)
            self._set_subcategory_page(page_index)
        else:
            self._set_subcategory_page(0, animate=False)

    def _set_subcategory_page(self, page_index: int, animate: bool = True) -> None:
        if not self.subcategory_pages:
            self.subcat_left_button.set_sensitive(False)
            self.subcat_right_button.set_sensitive(False)
            return

        page_index = max(0, min(len(self.subcategory_pages) - 1, page_index))
        current_index = self.current_subcategory_page
        self.current_subcategory_page = page_index

        try:
            transition_type = Gtk.StackTransitionType.SLIDE_LEFT_RIGHT
            if page_index < current_index:
                transition_type = Gtk.StackTransitionType.SLIDE_RIGHT
            elif page_index > current_index:
                transition_type = Gtk.StackTransitionType.SLIDE_LEFT
            if not animate:
                transition_type = Gtk.StackTransitionType.NONE
            self.subcategory_stack.set_transition_type(transition_type)
        except Exception:
            pass

        self.subcategory_stack.set_visible_child_name(f"page-{page_index}")
        left_available = page_index > 0
        right_available = page_index < len(self.subcategory_pages) - 1
        self.subcat_left_button.set_sensitive(left_available)
        self.subcat_right_button.set_sensitive(right_available)
        self._set_pan_button_available(self.subcat_left_button, left_available)
        self._set_pan_button_available(self.subcat_right_button, right_available)

    def _set_pan_button_available(self, button: Gtk.Button, available: bool) -> None:
        if available:
            button.add_css_class("available")
        else:
            button.remove_css_class("available")

    def _on_search_changed(self, entry: Gtk.SearchEntry) -> None:
        self.current_search_text = entry.get_text().strip()
        self._refresh_main_page()

    def _on_category_filter_changed(self, entry: Gtk.SearchEntry) -> None:
        self.current_category_filter_text = entry.get_text().strip()
        self._refresh_main_page(preserve_scroll=False)

    def _on_repo_filter_changed(self, combo: Gtk.ComboBoxText) -> None:
        self.current_repo_filter = combo.get_active_id() or "__all__"
        self._refresh_main_page()

    def _populate_repo_filter_dropdown(self) -> None:
        current = getattr(self, "current_repo_filter", "__all__") or "__all__"
        combo = self.repo_filter_combo
        combo.remove_all()
        combo.append("__all__", "All repositories")
        for repo in getattr(self, "repos", []):
            repo_id = repo.get("id", "")
            if not repo_id:
                continue
            label = repo.get("name") or repo_id
            combo.append(repo_id, label)
        combo.set_active_id(current if any((repo.get("id") == current) for repo in getattr(self, "repos", [])) else "__all__")
        self.current_repo_filter = combo.get_active_id() or "__all__"

    def _on_subcat_pan_start(self, _button: Gtk.Button) -> None:
        self._set_subcategory_page(self.current_subcategory_page - 1)

    def _on_subcat_pan_end(self, _button: Gtk.Button) -> None:
        self._set_subcategory_page(self.current_subcategory_page + 1)



    def _get_scroll_position_for_visible_page(self) -> tuple[str | None, float]:
        if not hasattr(self, "stack"):
            return None, 0.0
        visible = self.stack.get_visible_child_name()
        scroller = None
        if visible == "list" and hasattr(self, "list_scroller"):
            scroller = self.list_scroller
        elif visible == "news" and hasattr(self, "news_scroll"):
            scroller = self.news_scroll
        elif visible == "queue":
            scroller = self.stack.get_child_by_name("queue")
        elif visible == "repos" and hasattr(self, "repo_scroll"):
            scroller = self.repo_scroll
        elif visible == "details":
            scroller = self.stack.get_child_by_name("details")
        if scroller is None:
            return visible, 0.0
        adj = scroller.get_vadjustment()
        return visible, float(adj.get_value())

    def _restore_scroll_position(self, visible: str | None, value: float) -> bool:
        if not hasattr(self, "stack"):
            return False
        current_visible = self.stack.get_visible_child_name()
        if current_visible != visible:
            return False
        scroller = None
        if visible == "list" and hasattr(self, "list_scroller"):
            scroller = self.list_scroller
        elif visible == "news" and hasattr(self, "news_scroll"):
            scroller = self.news_scroll
        elif visible == "queue":
            scroller = self.stack.get_child_by_name("queue")
        elif visible == "repos" and hasattr(self, "repo_scroll"):
            scroller = self.repo_scroll
        elif visible == "details":
            scroller = self.stack.get_child_by_name("details")
        if scroller is None:
            return False
        adj = scroller.get_vadjustment()
        upper = max(adj.get_lower(), adj.get_upper() - adj.get_page_size())
        adj.set_value(max(adj.get_lower(), min(value, upper)))
        return False

    def _scroll_visible_page_to_top(self) -> bool:
        visible = self.stack.get_visible_child_name()
        scroller = None
        if visible == "list" and hasattr(self, "list_scroller"):
            scroller = self.list_scroller
        elif visible == "news" and hasattr(self, "news_scroll"):
            scroller = self.news_scroll
        elif visible == "queue":
            scroller = self.stack.get_child_by_name("queue")
        elif visible == "repos" and hasattr(self, "repo_scroll"):
            scroller = self.repo_scroll
        elif visible == "details":
            scroller = self.stack.get_child_by_name("details")
        if scroller is None:
            return False
        adj = scroller.get_vadjustment()
        adj.set_value(adj.get_lower())
        return False

    def _refresh_main_page(self, preserve_scroll: bool = True) -> None:
        if preserve_scroll:
            visible_before, scroll_before = self._get_scroll_position_for_visible_page()
        else:
            visible_before, scroll_before = None, 0.0
        in_search_mode = bool(self.current_search_text)
        if not in_search_mode and self.current_group == "system" and self.current_page == "news":
            self.title_label.set_text(CATEGORY_GROUPS["system"]["news"])
            self.status_label.set_text(_("Latest news and announcements."))
            self._refresh_news_page()
            self.stack.set_visible_child_name("news")
            if preserve_scroll:
                GLib.idle_add(self._restore_scroll_position, visible_before, scroll_before)
            else:
                GLib.idle_add(self._scroll_visible_page_to_top)
            return
        if not in_search_mode and self.current_group == "system" and self.current_page == "repositories":
            self.title_label.set_text(CATEGORY_GROUPS["system"]["repositories"])
            self.status_label.set_text(f"Showing {len(getattr(self, 'repos', []))} repositories.")
            self.stack.set_visible_child_name("repos")
            if preserve_scroll:
                GLib.idle_add(self._restore_scroll_position, visible_before, scroll_before)
            else:
                GLib.idle_add(self._scroll_visible_page_to_top)
            return
        if not in_search_mode and self.current_group == "system" and self.current_page == "queue":
            self.title_label.set_text(CATEGORY_GROUPS["system"]["queue"])
            self.status_label.set_text("")
            self._refresh_queue_page()
            self.stack.set_visible_child_name("queue")
            if preserve_scroll:
                GLib.idle_add(self._restore_scroll_position, visible_before, scroll_before)
            else:
                GLib.idle_add(self._scroll_visible_page_to_top)
            return

        show_local_filter = (not in_search_mode and ((self.current_group == "categories") or (self.current_group == "system" and self.current_page in {"installed", "updates"})))
        self.category_filter_entry.set_visible(show_local_filter)

        items = self._filtered_apps_for_current_page()
        self.current_items = items
        self.title_label.set_text(self._page_title())
        repo_note = ''
        if self.current_repo_filter != '__all__':
            repo_name = next((repo.get('name') or repo.get('id') for repo in getattr(self, 'repos', []) if repo.get('id') == self.current_repo_filter), self.current_repo_filter)
            repo_note = f' in {repo_name}'
        if in_search_mode:
            self.status_label.set_text(f'Search results for “{self.current_search_text}” ({len(items)} applications{repo_note}).')
        else:
            extra = ""
            if show_local_filter and self.current_category_filter_text:
                extra = f' matching “{self.current_category_filter_text}”'
            self.status_label.set_text(f"Showing {len(items)} applications{repo_note}{extra}.")
        self._refresh_updates_action_bar(items)
        self._rebuild_listbox(items)
        self.stack.set_visible_child_name("list")
        if preserve_scroll:
            GLib.idle_add(self._restore_scroll_position, visible_before, scroll_before)
        else:
            GLib.idle_add(self._scroll_visible_page_to_top)

    def _filtered_apps_for_current_page(self) -> list[AppEntry]:
        cache_key = self._page_cache_key()
        cached = self._page_items_cache.get(cache_key)
        if cached is not None:
            return list(cached)

        items = list(self.apps)
        needle = self.current_search_text.casefold()
        show_local_filter = ((self.current_group == "categories") or (self.current_group == "system" and self.current_page in {"installed", "updates"})) and not bool(needle)

        if needle:
            items = [
                app for app in items
                if needle in app.name.casefold()
                or needle in app.summary.casefold()
                or needle in app.description.casefold()
                or any(needle in item.casefold() for item in app.categories)
                or any(needle in item.casefold() for item in app.keywords)
                or any(needle in item.casefold() for item in app.pkg_names)
            ]
            if self.current_repo_filter != "__all__":
                items = [app for app in items if self.current_repo_filter in app.repo_ids]

            merged: list[AppEntry] = list(items)
            seen_pkgs = {pkg for app in merged for pkg in app.pkg_names}
            if self.backend is not None:
                try:
                    fallback = self.backend.search_packages(needle, repo_id=self.current_repo_filter)
                except Exception:
                    fallback = []
                for app in fallback:
                    if any(pkg in seen_pkgs for pkg in app.pkg_names):
                        continue
                    merged.append(app)
                    seen_pkgs.update(app.pkg_names)

            merged.sort(key=lambda item: self._search_rank_key(item, needle))
            self._page_items_cache[cache_key] = list(merged)
            return list(merged)
        else:
            if self.current_group == "system":
                if self.current_page == "installed":
                    items = [app for app in items if app.installed]
                    seen_pkgs = {pkg for app in items for pkg in app.pkg_names}
                    if self.backend is not None:
                        try:
                            fallback_installed = self.backend.get_installed_packages(repo_id=self.current_repo_filter)
                        except Exception:
                            fallback_installed = []
                        for app in fallback_installed:
                            if any(pkg in seen_pkgs for pkg in app.pkg_names):
                                continue
                            items.append(app)
                            seen_pkgs.update(app.pkg_names)
                elif self.current_page == "updates":
                    if self.backend is not None:
                        try:
                            items = self.backend.get_upgradable_packages(repo_id=self.current_repo_filter)
                        except Exception:
                            items = []
                    else:
                        items = []
            elif self.current_group == "categories":
                page_key = self.current_page.casefold()
                items = [app for app in items if self._has_category(app, page_key)]
                items = [app for app in items if (app.launchables or app.kind != "PACKAGE") or not should_hide_from_standard_catalog(app)]
                if self.current_subcategory:
                    sub = self.current_subcategory.casefold()
                    items = [app for app in items if self._has_category(app, sub)]

        if show_local_filter and self.current_category_filter_text:
            local_needle = self.current_category_filter_text.casefold()
            items = [
                app for app in items
                if local_needle in app.name.casefold()
                or local_needle in app.summary.casefold()
                or local_needle in app.description.casefold()
                or any(local_needle in item.casefold() for item in app.categories)
                or any(local_needle in item.casefold() for item in app.keywords)
                or any(local_needle in item.casefold() for item in app.pkg_names)
            ]

        if self.current_repo_filter != "__all__":
            items = [app for app in items if self.current_repo_filter in app.repo_ids]

        items.sort(key=lambda item: item.name.casefold())
        self._page_items_cache[cache_key] = list(items)
        return list(items)

    def _search_rank_key(self, app: AppEntry, needle: str) -> tuple[int, int, int, str]:
        pkg_names = [pkg.casefold() for pkg in app.pkg_names if pkg]
        app_name = app.name.casefold()
        summary = app.summary.casefold()

        def _best_pos(values: list[str]) -> int:
            positions = [value.find(needle) for value in values if needle in value]
            return min(positions) if positions else 9999

        exact_pkg = any(pkg == needle for pkg in pkg_names)
        prefix_pkg = any(pkg.startswith(needle) for pkg in pkg_names)
        exact_name = app_name == needle
        prefix_name = app_name.startswith(needle)
        pkg_pos = _best_pos(pkg_names)
        name_pos = app_name.find(needle) if needle in app_name else 9999
        summary_pos = summary.find(needle) if needle in summary else 9999
        best_pos = min(pkg_pos, name_pos, summary_pos)

        # Lower rank wins.
        if exact_pkg:
            rank = 0
        elif prefix_pkg:
            rank = 1
        elif exact_name:
            rank = 2
        elif prefix_name:
            rank = 3
        elif pkg_pos != 9999:
            rank = 4
        elif name_pos != 9999:
            rank = 5
        elif summary_pos != 9999:
            rank = 6
        else:
            rank = 7

        shortest_prefix_len = min((len(pkg) for pkg in pkg_names if pkg.startswith(needle)), default=9999)
        return (rank, best_pos, shortest_prefix_len, app.name.casefold())

    def _has_category(self, app: AppEntry, category: str) -> bool:
        categories = {item.casefold() for item in app.categories}
        aliases = {
            "office": {"office", "productivity"},
            "graphics": {"graphics", "photography"},
            "audiovideo": {"audio", "video", "audiovideo"},
            "network": {"network", "networking", "internet"},
            "development": {"development", "developer tools", "devel", "programming"},
            "game": {"game", "games"},
            "utility": {"utility", "utilities"},
            "system": {"system"},
            "science": {"science"},
            "education": {"education"},
        }
        wanted = aliases.get(category, {category})
        return bool(categories & wanted) or category in categories

    def _page_title(self) -> str:
        if self.current_search_text:
            return "Search"
        if self.current_group == "system":
            return CATEGORY_GROUPS["system"][self.current_page]
        title = CATEGORY_GROUPS["categories"].get(self.current_page, self.current_page.title())
        if self.current_subcategory:
            sub = SUBCATEGORY_GROUPS.get(self.current_page, {}).get(self.current_subcategory, self.current_subcategory)
            return f"{title} » {sub}"
        return title

    def _rebuild_listbox(self, items: list[AppEntry] | None = None) -> None:
        self._clear_listbox()
        items = items or []
        self.empty_status.set_visible(not items)
        update_mode = self.current_group == "system" and self.current_page == "updates" and not self.current_search_text
        compact_grid = True
        cols = 3
        if not compact_grid:
            for app in items:
                self.listbox.append(
                    AppCardRow(
                        app,
                        (lambda entry, mode=("update" if update_mode else None): self._run_action_for_app(entry, mode)),
                        self._open_details,
                        self._queued_state_label,
                        page_mode=("updates" if update_mode else "default"),
                        update_selected=(app.primary_pkg in self.update_selection if app.primary_pkg else False),
                        update_toggle_cb=self._toggle_update_selection if update_mode else None,
                    )
                )
            return

        for start in range(0, len(items), cols):
            chunk = items[start:start + cols]
            row = Gtk.ListBoxRow()
            row.set_activatable(False)
            row.set_selectable(False)
            row_box = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
            row_box.set_homogeneous(True)
            for app in chunk:
                tile = AppCardTile(
                    app,
                    (lambda entry, mode=("update" if update_mode else None): self._run_action_for_app(entry, mode)),
                    self._open_details,
                    self._queued_state_label,
                    page_mode=("updates" if update_mode else "default"),
                    update_selected=(app.primary_pkg in self.update_selection if app.primary_pkg else False),
                    update_toggle_cb=self._toggle_update_selection if update_mode else None,
                )
                row_box.append(tile)
            for _ in range(cols - len(chunk)):
                spacer = Gtk.Box()
                spacer.set_hexpand(True)
                row_box.append(spacer)
            row.set_child(row_box)
            self.listbox.append(row)

    def _clear_listbox(self) -> None:
        child = self.listbox.get_first_child()
        while child is not None:
            next_child = child.get_next_sibling()
            self.listbox.remove(child)
            child = next_child

    def _rebuild_repo_page(self) -> None:
        child = self.repo_box.get_first_child()
        while child is not None:
            next_child = child.get_next_sibling()
            self.repo_box.remove(child)
            child = next_child
        for repo in getattr(self, "repos", []):
            card = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=8)
            card.add_css_class("repo-card")

            top = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=10)
            card.append(top)

            labels = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=4)
            labels.set_hexpand(True)
            top.append(labels)

            name = Gtk.Label(xalign=0)
            name.set_markup(f"<b>{GLib.markup_escape_text(repo['name'] or repo['id'])}</b>")
            labels.append(name)

            rid = Gtk.Label(label=repo["id"], xalign=0)
            rid.add_css_class("dim-label")
            labels.append(rid)

            toggle = Gtk.Switch()
            toggle.set_valign(Gtk.Align.CENTER)
            toggle.set_active(bool(repo.get("enabled", True)))
            toggle.connect("state-set", self._on_repo_toggle_state_set, repo["id"])
            top.append(toggle)

            if repo.get("baseurl"):
                url = Gtk.Label(label=repo["baseurl"], xalign=0)
                url.set_wrap(True)
                url.set_selectable(True)
                card.append(url)
            self.repo_box.append(card)

    def _on_repo_toggle_state_set(self, switch: Gtk.Switch, state: bool, repo_id: str) -> bool:
        switch.set_sensitive(False)

        def worker() -> None:
            ok, message = self.backend.set_repository_enabled(repo_id, state)
            GLib.idle_add(self._repo_toggle_done, switch, repo_id, state, ok, message)

        threading.Thread(target=worker, daemon=True).start()
        return True

    def _repo_toggle_done(self, switch: Gtk.Switch, repo_id: str, state: bool, ok: bool, message: str) -> bool:
        switch.set_sensitive(True)
        if ok:
            self.repos = self.backend.get_repositories()
            self._rebuild_repo_page()
            self._show_toast(message)
            return False
        switch.set_state(not state)
        switch.set_active(not state)
        self._show_toast(message)
        return False

    def _on_row_activated(self, _listbox: Gtk.ListBox, row: Gtk.ListBoxRow) -> None:
        if hasattr(row, "app"):
            self._open_details(row.app)

    def _open_details(self, app: AppEntry) -> None:
        self.current_app = app
        child = self.detail_icon_box.get_first_child()
        while child is not None:
            next_child = child.get_next_sibling()
            self.detail_icon_box.remove(child)
            child = next_child
        self.detail_icon_box.append(IconWidget(app, 128))
        self.detail_title.set_text(app.name)
        self.detail_summary.set_text(app.summary)
        self._refresh_detail_description(app.description)

        lines = []
        if app.pkg_names:
            lines.append(f"Packages: {', '.join(app.pkg_names)}")
        if app.installed_version:
            lines.append(f"Installed: {app.installed_version}")
        if app.candidate_version:
            lines.append(f"Available: {app.candidate_version}")
        if app.repo_ids:
            lines.append(f"Repositories: {', '.join(app.repo_ids)}")
        if app.categories:
            lines.append(f"Categories: {', '.join(app.categories)}")
        self.detail_meta.set_text("\n".join(lines))

        queued_label = self._queued_state_label(app)
        self.detail_action_button.set_label(queued_label or self._default_action_label(app))
        self.detail_action_button.set_sensitive(bool(app.primary_pkg and self.backend) and queued_label is None)
        self.detail_action_button.remove_css_class("destructive-action")
        self.detail_action_button.remove_css_class("suggested-action")
        self.detail_action_button.add_css_class("destructive-action" if app.installed else "suggested-action")
        self.detail_open_button.set_sensitive(bool(app.launchables))

        self._rebuild_detail_links(app)
        self._rebuild_detail_screenshots(app)
        self.stack.set_visible_child_name("details")

    def _refresh_detail_action_button(self) -> None:
        if not getattr(self, "current_app", None):
            return
        app = self.current_app
        queued_label = self._queued_state_label(app)
        self.detail_action_button.set_label(queued_label or self._default_action_label(app))
        self.detail_action_button.set_sensitive(bool(app.primary_pkg and self.backend) and queued_label is None)
        self.detail_action_button.remove_css_class("destructive-action")
        self.detail_action_button.remove_css_class("suggested-action")
        self.detail_action_button.add_css_class("destructive-action" if app.installed else "suggested-action")

    def _default_action_label(self, app: AppEntry) -> str:
        if self.current_group == "system" and self.current_page == "updates" and app.installed and app.candidate_version and app.candidate_version != app.installed_version:
            return "Update"
        return _("Remove") if app.installed else _("Install")

    def _rebuild_detail_links(self, app: AppEntry) -> None:
        child = self.detail_links.get_first_child()
        while child is not None:
            next_child = child.get_next_sibling()
            self.detail_links.remove(child)
            child = next_child
        if app.homepage_url:
            button = Gtk.LinkButton.new_with_label(app.homepage_url, "Homepage")
            self.detail_links.append(button)

    def _rebuild_detail_screenshots(self, app: AppEntry) -> None:
        child = self.detail_screenshots.get_first_child()
        while child is not None:
            next_child = child.get_next_sibling()
            self.detail_screenshots.remove(child)
            child = next_child
        if not app.screenshots:
            return

        title = Gtk.Label(label=_("Screenshots"), xalign=0)
        title.add_css_class("title-4")
        self.detail_screenshots.append(title)

        flow = Gtk.FlowBox()
        flow.set_selection_mode(Gtk.SelectionMode.NONE)
        flow.set_row_spacing(12)
        flow.set_column_spacing(12)
        flow.set_max_children_per_line(2)
        flow.set_min_children_per_line(1)
        self.detail_screenshots.append(flow)

        added_any = False
        for ref in app.screenshots[:6]:
            picture = _picture_from_ref(ref, 560, 315)
            child_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=6)
            child_box.set_margin_bottom(6)
            if picture is not None:
                frame = Gtk.Frame()
                frame.set_child(picture)
                child_box.append(frame)
                added_any = True
            else:
                child_box.append(Gtk.LinkButton.new_with_label(ref, ref))
            flow.insert(child_box, -1)

        if not added_any:
            note = Gtk.Label(label=_("Screenshots could not be loaded locally."), xalign=0)
            note.add_css_class("dim-label")
            self.detail_screenshots.append(note)

    def _queued_state_label(self, app: AppEntry) -> str | None:
        target_pkg = app.primary_pkg
        for item in self.queue_items:
            if target_pkg and target_pkg in item.pkg_names and item.status in {"queued", "running"}:
                return "Queued" if item.status == "queued" else "Running"
        return None

    def _run_action_for_app(self, app: AppEntry, preferred_action: str | None = None) -> None:
        if not self.backend or not app.primary_pkg:
            return
        if self._queued_state_label(app) is not None:
            self._show_toast(f"{app.primary_pkg} is already in the queue.")
            return
        if preferred_action == "update":
            self._enqueue_update_batch([app])
            return
        action = preferred_action or ("remove" if app.installed else "install")
        item = QueueItem(app=app, action=action, message=f"Queued to {action} {app.primary_pkg}")
        self.queue_items.append(item)
        self._append_queue_log(f"Queued {action} for {app.primary_pkg}")
        self._invalidate_page_caches()
        self.status_label.set_text(self._queue_status_text())
        self._refresh_queue_page()
        self._refresh_main_page()
        if self.current_app is app:
            self._open_details(app)
        if not self.queue_worker_running:
            self._start_queue_worker()

    def _toggle_update_selection(self, app: AppEntry, selected: bool) -> None:
        pkg = app.primary_pkg
        if not pkg:
            return
        if selected:
            self.update_selection.add(pkg)
        else:
            self.update_selection.discard(pkg)
        self._refresh_updates_action_bar(self.current_items)

    def _clear_update_selection(self) -> None:
        self.update_selection.clear()
        self._refresh_updates_action_bar(self.current_items)
        self._refresh_main_page()

    def _refresh_updates_action_bar(self, items: list[AppEntry]) -> None:
        is_updates_page = self.current_group == "system" and self.current_page == "updates" and not self.current_search_text
        self.updates_action_bar.set_visible(is_updates_page)
        if not is_updates_page:
            return
        selectable = [app for app in items if app.primary_pkg]
        selected_count = sum(1 for app in selectable if app.primary_pkg in self.update_selection)
        self.updates_selected_label.set_text(f"{selected_count} selected")
        self.updates_clear_button.set_sensitive(selected_count > 0)
        self.update_selected_button.set_sensitive(selected_count > 0)
        self.update_all_button.set_sensitive(bool(selectable))

    def _queue_selected_updates(self) -> None:
        items = [app for app in self.current_items if app.primary_pkg and app.primary_pkg in self.update_selection]
        self._enqueue_update_batch(items)

    def _should_use_nobara_sync(self) -> bool:
        try:
            for raw in Path("/etc/os-release").read_text(encoding="utf-8", errors="replace").splitlines():
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                if key == "ID":
                    return value.strip().strip('"').strip("'").lower() == "nobara"
        except Exception:
            return False
        return False

    def _queue_system_update(self) -> None:
        if self._should_use_nobara_sync():
            if any(item.action == "system-update" and item.status in {"queued", "running"} for item in self.queue_items):
                self._show_toast(_("System update is already in the queue."))
                return
            base_app = self.current_items[0] if self.current_items else AppEntry(
                appstream_id="system-update",
                name="System Update",
                summary="",
                description="",
                pkg_names=[],
            )
            item = QueueItem(base_app, action="system-update", message="Queued system update", pkg_names=[], label=_("System Update"))
            self.queue_items.append(item)
            self._append_queue_log("Queued system update via nobara-sync cli")
            self.status_label.set_text(self._queue_status_text())
            self._refresh_queue_page()
            self._refresh_main_page()
            self._refresh_detail_action_button()
            self._switch_page("system", "queue")
            if not self.queue_worker_running:
                self._start_queue_worker()
            return

        items = [app for app in self.current_items if app.primary_pkg]
        if not items:
            self._show_toast(_("No updates are available."))
            return
        self._enqueue_update_batch(items)

    def _enqueue_update_batch(self, apps: list[AppEntry]) -> None:
        unique: list[AppEntry] = []
        seen: set[str] = set()
        for app in apps:
            pkg = app.primary_pkg
            if not pkg or pkg in seen:
                continue
            if self._queued_state_label(app) is not None:
                continue
            seen.add(pkg)
            unique.append(app)
        if not unique:
            return
        pkg_names = [app.primary_pkg for app in unique if app.primary_pkg]
        label = unique[0].name if len(unique) == 1 else f"{len(unique)} updates"
        item = QueueItem(app=unique[0], action="update", message=f"Queued to update {len(pkg_names)} package(s)", pkg_names=pkg_names, label=label)
        self.queue_items.append(item)
        self._append_queue_log(f"Queued update for {', '.join(pkg_names[:5])}{'…' if len(pkg_names) > 5 else ''}")
        for pkg in pkg_names:
            self.update_selection.discard(pkg)
        self.status_label.set_text(self._queue_status_text())
        self._invalidate_page_caches()
        self._refresh_queue_page()
        self._refresh_main_page()
        self._refresh_detail_action_button()
        if not self.queue_worker_running:
            self._start_queue_worker()

    def _start_queue_worker(self) -> None:
        if self.queue_worker_running or not self.backend:
            return
        self.queue_worker_running = True

        def worker() -> None:
            while True:
                item = next((entry for entry in self.queue_items if entry.status == "queued"), None)
                if item is None:
                    break
                GLib.idle_add(self._queue_item_started, item)

                def on_event(payload: dict) -> None:
                    GLib.idle_add(self._handle_queue_event, item, payload)

                if item.action == "system-update":
                    target = []
                elif item.action == 'install-rpms':
                    target = item.file_paths
                else:
                    target = item.pkg_names if item.action == "update" and len(item.pkg_names) > 1 else (item.pkg_names or [item.pkg_name])
                    if isinstance(target, list) and len(target) == 1:
                        target = target[0]
                ok, message = self.backend.execute_action(item.action, target, on_event)
                GLib.idle_add(self._queue_item_finished, item, ok, message)
            GLib.idle_add(self._queue_worker_done)

        threading.Thread(target=worker, daemon=True).start()

    def _queue_item_started(self, item: QueueItem) -> bool:
        item.status = "running"
        subject = item.display_name if item.action in {"update", "system-update", 'install-rpms'} and (item.action in {'system-update', 'install-rpms'} or len(item.pkg_names) > 1) else item.pkg_name
        item.message = f"Running {item.action} for {subject}"
        self.current_queue_item = item
        self._append_queue_log(f"Starting {item.action} for {subject}")
        self._invalidate_page_caches()
        self._refresh_queue_page()
        self._refresh_main_page()
        if self.current_app is item.app:
            self._open_details(item.app)
        return False

    def _handle_queue_event(self, item: QueueItem, payload: dict) -> bool:
        message = str(payload.get("message") or "").strip()
        if message:
            item.message = message
            self._append_queue_log(f"{item.display_name}: {message}")
        self._refresh_queue_page()
        return False

    def _queue_item_finished(self, item: QueueItem, ok: bool, message: str) -> bool:
        item.status = "done" if ok else "failed"
        item.message = message
        self._append_queue_log(f"{item.display_name}: {message}")
        if self.backend:
            if ok:
                # Update every in-memory representation of the acted-on package.
                related_apps: list[AppEntry] = []
                seen_ids: set[int] = set()

                acted_pkgs = set(item.pkg_names or [item.pkg_name])

                def _collect(candidate: AppEntry | None) -> None:
                    if candidate is None:
                        return
                    ident = id(candidate)
                    if ident in seen_ids:
                        return
                    candidate_pkgs = set(candidate.pkg_names or ([candidate.primary_pkg] if candidate.primary_pkg else []))
                    if candidate_pkgs & acted_pkgs:
                        related_apps.append(candidate)
                        seen_ids.add(ident)

                _collect(item.app)
                _collect(self.current_app)
                for candidate in self.apps:
                    _collect(candidate)
                for candidate in getattr(self, 'current_items', []):
                    _collect(candidate)

                forced_installed = item.action in {"install", "update"}
                forced_installed_version = item.app.candidate_version if forced_installed else None
                for pkg in acted_pkgs:
                    self.update_selection.discard(pkg)

                # First flip the UI optimistically.
                for target in related_apps:
                    target.installed = forced_installed
                    target.installed_version = forced_installed_version if forced_installed else None
                if forced_installed and item.app not in self.apps:
                    self.apps.append(item.app)

                # Then refresh from backend state.
                try:
                    self.backend.refresh_apps(self.apps)
                except Exception:
                    self.backend.reload_state()
                    self.backend.enrich_apps(self.apps)

                for target in related_apps:
                    try:
                        self.backend.refresh_app(target)
                    except Exception:
                        pass

                # Finally, force the acted-on state once more so a slightly stale local
                # libdnf view cannot flip the button back to the old state immediately
                # after a successful transaction.
                for target in related_apps:
                    target.installed = forced_installed
                    if forced_installed:
                        if not target.installed_version:
                            target.installed_version = forced_installed_version or target.candidate_version
                    else:
                        target.installed_version = None
            else:
                self.backend.refresh_app(item.app)
        if ok:
            self._show_toast(message)
        else:
            self._show_toast(f"{item.display_name} failed")
        self._invalidate_page_caches()
        self._refresh_queue_page()
        self._refresh_visible_list()
        self._refresh_detail_action_button()
        if self.current_app is item.app:
            self._open_details(item.app)
        return False

    def _queue_worker_done(self) -> bool:
        self.queue_worker_running = False
        self.current_queue_item = None

        had_items = bool(self.queue_items)
        failed_items = [item for item in self.queue_items if item.status == "failed"]
        done_count = sum(1 for item in self.queue_items if item.status == "done")

        if had_items and done_count:
            self.queue_items = failed_items
            if failed_items:
                self._append_queue_log(
                    f"Queue finished. Cleared {done_count} completed item(s); {len(failed_items)} failed item(s) remain."
                )
            else:
                self._append_queue_log(f"Queue finished. Cleared {done_count} completed item(s).")

        self.status_label.set_text(self._queue_status_text())
        self._refresh_queue_page()
        self._refresh_visible_list()
        self._refresh_detail_action_button()
        return False

    def _on_close_request(self, *_args):
        try:
            if self.backend is not None:
                self.backend.shutdown()
        except Exception:
            pass
        return False

    def _queue_status_text(self) -> str:
        if not self.queue_items:
            return "Queue is empty."
        done = sum(1 for item in self.queue_items if item.status == "done")
        failed = sum(1 for item in self.queue_items if item.status == "failed")
        running = next((item for item in self.queue_items if item.status == "running"), None)
        queued = sum(1 for item in self.queue_items if item.status == "queued")
        if running is not None:
            target = running.display_name if running.action in {"update", "system-update"} and (running.action == "system-update" or len(running.pkg_names) > 1) else running.pkg_name
            return f"Running {running.action} for {target}. {done} done, {failed} failed, {queued} queued."
        return f"{done} completed, {failed} failed, {queued} queued."

    def _append_queue_log(self, line: str) -> None:
        self.queue_log_full.append(line)
        self.queue_logs.append(line)
        if len(self.queue_logs) > 400:
            self.queue_logs = self.queue_logs[-400:]
        if hasattr(self, "queue_log_view"):
            buffer = self.queue_log_view.get_buffer()
            end_iter = buffer.get_end_iter()
            prefix = "" if buffer.get_char_count() == 0 else "\n"
            buffer.insert(end_iter, prefix + line)
            if buffer.get_line_count() > 400:
                start = buffer.get_start_iter()
                end = buffer.get_iter_at_line(1)
                buffer.delete(start, end)
            GLib.idle_add(self._scroll_to_bottom)
            
    def _scroll_to_bottom(self):
        if hasattr(self, "queue_log_view"):
            buffer = self.queue_log_view.get_buffer()
            end_iter = buffer.get_end_iter()
            self.queue_log_view.scroll_to_iter(end_iter, 0.0, False, 0.0, 1.0)
        return False

    def _refresh_visible_list(self) -> None:
        visible, scroll_before = self._get_scroll_position_for_visible_page()
        visible = visible or (self.stack.get_visible_child_name() if hasattr(self, "stack") else "list")
        items = self._filtered_apps_for_current_page()
        self.current_items = items
        self._rebuild_listbox(items)
        if visible != "details":
            self._refresh_main_page()
        else:
            if getattr(self, "current_app", None) is not None and self.current_app not in items and self.current_search_text:
                # keep details open for current search result even if not represented in AppStream list
                pass
            self._refresh_detail_action_button()
        GLib.idle_add(self._restore_scroll_position, visible, scroll_before)

    def _refresh_queue_page(self) -> None:
        if not hasattr(self, "queue_list_box"):
            return
        child = self.queue_list_box.get_first_child()
        while child is not None:
            next_child = child.get_next_sibling()
            self.queue_list_box.remove(child)
            child = next_child

        total = len(self.queue_items)
        done_count = sum(1 for item in self.queue_items if item.status == "done")
        failed_count = sum(1 for item in self.queue_items if item.status == "failed")
        resolved_count = done_count + failed_count
        fraction = (resolved_count / total) if total else 0.0
        if total:
            if failed_count and done_count == 0:
                progress_text = f"0/{total} complete, {failed_count} failed"
            elif failed_count:
                progress_text = f"{done_count}/{total} complete, {failed_count} failed"
            else:
                progress_text = f"{done_count}/{total} complete"
        else:
            progress_text = "Queue empty"
        self.queue_progress.set_fraction(fraction)
        self.queue_progress.set_show_text(True)
        self.queue_progress.set_text(progress_text)

        if self.queue_worker_running and total > resolved_count:
            self.queue_progress.pulse()

        if hasattr(self, "bottom_queue_revealer"):
            self.bottom_queue_revealer.set_reveal_child(total > 0)
            self.bottom_queue_progress.set_fraction(fraction)
            self.bottom_queue_progress.set_show_text(True)
            self.bottom_queue_progress.set_text(progress_text)
            if self.queue_worker_running and total > resolved_count:
                self.bottom_queue_progress.pulse()
            self.bottom_queue_status.set_text(self._queue_status_text())

        if not self.queue_items:
            empty = Gtk.Label(label=_("No queued package actions yet."), xalign=0)
            empty.add_css_class("dim-label")
            self.queue_list_box.append(empty)

        for item in self.queue_items:
            card = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=4)
            card.add_css_class("queue-item-card")
            title = Gtk.Label(xalign=0)
            title.set_markup(f"<b>{GLib.markup_escape_text(item.display_name)}</b> — {GLib.markup_escape_text(item.action)}")
            card.append(title)
            self.queue_list_box.append(card)

    def _get_queue_log_text(self) -> str:
        if getattr(self, "queue_log_full", None):
            return "\n".join(self.queue_log_full)
        buffer = self.queue_log_view.get_buffer()
        start = buffer.get_start_iter()
        end = buffer.get_end_iter()
        return buffer.get_text(start, end, False)

    def _show_text_popup(self, title: str, text: str) -> None:
        window = Gtk.Window(title=title, transient_for=self, modal=True)
        window.set_default_size(900, 600)

        outer = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=8)
        outer.set_margin_top(12)
        outer.set_margin_bottom(12)
        outer.set_margin_start(12)
        outer.set_margin_end(12)

        text_view = Gtk.TextView()
        text_view.set_editable(False)
        text_view.set_cursor_visible(False)
        text_view.set_monospace(True)
        text_view.set_wrap_mode(Gtk.WrapMode.WORD_CHAR)
        text_view.get_buffer().set_text(text)

        scroll = Gtk.ScrolledWindow()
        scroll.set_policy(Gtk.PolicyType.AUTOMATIC, Gtk.PolicyType.AUTOMATIC)
        scroll.set_vexpand(True)
        scroll.set_hexpand(True)
        scroll.set_child(text_view)
        outer.append(scroll)

        buttons = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
        buttons.set_halign(Gtk.Align.END)
        close_btn = Gtk.Button(label=_("Close"))
        close_btn.connect("clicked", lambda *_: window.close())
        buttons.append(close_btn)
        outer.append(buttons)

        window.set_child(outer)
        window.present()

    def _show_info_popup(self, title: str, text: str) -> None:
        window = Gtk.Window(title=title, transient_for=self, modal=True)
        window.set_default_size(560, 160)

        outer = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=12)
        outer.set_margin_top(12)
        outer.set_margin_bottom(12)
        outer.set_margin_start(12)
        outer.set_margin_end(12)

        label = Gtk.Label(xalign=0)
        label.set_wrap(True)
        label.set_selectable(True)
        label.set_text(text)
        outer.append(label)

        buttons = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
        buttons.set_halign(Gtk.Align.END)
        copy_btn = Gtk.Button(label=_("Copy"))
        def _copy(*_args):
            display = Gdk.Display.get_default()
            if display is not None:
                clipboard = display.get_clipboard()
                clipboard.set(text)
                self._show_toast(_("Copied to clipboard"))
        copy_btn.connect("clicked", _copy)
        buttons.append(copy_btn)
        close_btn = Gtk.Button(label=_("Close"))
        close_btn.connect("clicked", lambda *_: window.close())
        buttons.append(close_btn)
        outer.append(buttons)

        window.set_child(outer)
        window.present()

    def _on_cache_auth_toggled(self, button: Gtk.CheckButton) -> None:
        if not self.backend:
            return
        try:
            self.backend.set_cache_authorization(button.get_active())
        except Exception as exc:
            self._show_toast(str(exc))


    def _on_view_transaction_log(self, _button: Gtk.Button) -> None:
        text = self._get_queue_log_text().strip()
        if not text:
            self._show_toast(_("No transaction log available yet."))
            return
        self._show_text_popup(_("Transaction log"), text)

    def _on_send_to_pastebin(self, _button: Gtk.Button) -> None:
        text = self._get_queue_log_text().strip()
        if not text:
            self._show_toast(_("No transaction log available yet."))
            return

        self.send_paste_button.set_sensitive(False)

        def worker() -> None:
            try:
                result = subprocess.run(["pbcli"], input=text, text=True, capture_output=True, check=False)
                stdout = (result.stdout or "").strip()
                stderr = (result.stderr or "").strip()
                if result.returncode != 0 or not stdout:
                    message = stderr or stdout or "pbcli did not return a URL."
                    GLib.idle_add(self._pastebin_done, False, message)
                    return
                url = stdout.splitlines()[-1].strip()
                GLib.idle_add(self._pastebin_done, True, url)
            except FileNotFoundError:
                GLib.idle_add(self._pastebin_done, False, "pbcli was not found on this system.")
            except Exception as exc:
                GLib.idle_add(self._pastebin_done, False, str(exc))

        threading.Thread(target=worker, daemon=True).start()

    def _pastebin_done(self, ok: bool, message: str) -> bool:
        self.send_paste_button.set_sensitive(True)
        if ok:
            self._show_info_popup(_("Pastebin URL"), message)
        else:
            self._show_info_popup(_("Pastebin upload failed"), message)
        return False

    def _on_open_clicked(self, _button: Gtk.Button) -> None:
        if not self.current_app or not self.current_app.launchables:
            return
        desktop_id = self.current_app.launchables[0]
        try:
            subprocess.Popen(["gtk-launch", desktop_id])
        except FileNotFoundError:
            self._show_toast(_("gtk-launch not found on this system."))
        except Exception as exc:
            self._show_toast(str(exc))

    def _show_toast(self, message: str) -> None:
        self.toast_overlay.add_toast(Adw.Toast(title=message[:300]))
        
    def _prompt_install(self) -> bool:
        if not self.queue_items or self.queue_worker_running:
            return False

        dialog = Adw.MessageDialog(
            transient_for=self,
            heading=_("Confirm Installation"),
            body=f"You have {len(self.queue_items)} package(s) ready. Do you want to install them?",
        )

        dialog.add_response("cancel", _("Cancel"))
        dialog.add_response("install", _("Install"))

        try:
            dialog.set_response_appearance("install", Adw.ResponseAppearance.SUGGESTED)
        except AttributeError:
            pass

        def on_response(dlg, response):
            if response == "install":
                self._start_queue_worker()
            else:
                self.queue_items.clear()
                self._refresh_queue_page()

        dialog.connect("response", on_response)
        dialog.present()
        
        return False
