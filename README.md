# DNF App Center

DNF App Center is a GTK4/libadwaita package manager and updater for Fedora-family systems.
It uses AppStream for catalog metadata and libdnf5 for package state and transactions.

## Features

- Browse applications by category
- Search the full package/app catalog
- Install, remove, and update packages with transaction preflight checks
- Drag-and-drop or open local `.rpm` files for installation
- News page backed by a configurable update feed URL
- Background updater tray service with notifications and configurable intervals

## Commands

Main application:

```bash
DNF App Center
dnf-app-center
```

Open directly to the **Updates** page:

```bash
dnf-app-center --update
```

Updater service:

```bash
dnf-app-center-updater
```

Standalone update checks:

```bash
dnf-app-center-updater --check
dnf-app-center-updater --check --refresh
dnf-app-center-updater --check --json
```

Return codes for `--check`:

- `0` = no updates available
- `100` = updates available
- `2` = error

## Configuration

Per-user updater/news settings are stored in:

```text
~/.config/dnf-app-center/updater.json
```

Current keys:

- `enabled`
- `notifications`
- `interval_value`
- `interval_unit`
- `update_feed_url`

Example:

```json
{
  "enabled": true,
  "notifications": true,
  "interval_value": 12,
  "interval_unit": "hours",
  "update_feed_url": "https://updates.nobaraproject.org/updates.txt"
}
```

## Polkit Integration

DNF App Center uses polkit (pkexec) for privilege escalation when installing, removing, or updating packages.

### Nobara Integration

For Nobara Linux systems, a polkit rule is included to allow `nobara-sync` and `nobara-updater` to run without additional authentication when called from the already-authenticated privileged helper. This prevents double password prompts when using the "Select All and Update" feature.

The polkit rule is automatically installed to:
```text
/usr/share/polkit-1/rules.d/50-dnf-app-center-nobara-sync.rules
```

### Manual Installation (Development)

If you're running from source and want to test the polkit rule:

```bash
sudo cp polkit/50-dnf-app-center-nobara-sync.rules /etc/polkit-1/rules.d/
```

### Authentication Caching

The app includes a "Remember auth" checkbox in the top bar that caches your authentication for the session. When enabled, you only need to enter your password once per session rather than for each package operation.

## Building locally

Install typical Fedora build dependencies:

```bash
sudo dnf builddep ./dnf-app-center.spec
```

Build an RPM from the source tarball/spec:

```bash
mkdir -p ~/rpmbuild/{SOURCES,SPECS}
cp dnf-app-center-*.tar.gz ~/rpmbuild/SOURCES/
cp dnf-app-center.spec ~/rpmbuild/SPECS/
rpmbuild -ba ~/rpmbuild/SPECS/dnf-app-center.spec
```

## Build options

The RPM spec supports overriding the default news/update feed URL at build time:

```bash
rpmbuild -ba dnf-app-center.spec --define '_update_feed_url https://example.com/updates.txt'
```

That value becomes the default `update_feed_url` used when the user has not overridden it in their own config.

## License

This project is distributed under the terms of the GNU General Public License version 2.
See `COPYING` for the full license text.
## Proxy connections

App Center uses proxy environment variables inherited when it starts, including
`http_proxy`, `https_proxy`, `all_proxy` and `no_proxy`. For example:

```sh
https_proxy=http://127.0.0.1:3128 dnf-app-center
```

Validated proxy settings are passed through authorization to package operations
and `nobara-sync` over a private connection. They are not included in command
arguments, and the privileged helper does not retain them between operations.
System updates require an updater build with proxy support. Desktop proxy
preferences and PAC files are not imported automatically.
