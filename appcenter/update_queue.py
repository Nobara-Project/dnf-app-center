"""Package progress for one coordinated nobara-sync transaction (no GTK dependency)."""
from __future__ import annotations


class UpdateProgress:
    def __init__(self, requested):
        self.requested = set(requested)
        self.rows = {"request:" + name: dict(name=name, nevra=name, action="Update", stage="request",
                    phase="waiting", fraction=0) for name in requested}
        self.phase = "Preparing"
        self.transactions = {}

    def handle(self, payload):
        transaction = payload.get("transaction")
        if not isinstance(transaction, str):
            return
        if payload.get("event") == "plan":
            stage = payload.get("stage")
            packages = payload.get("packages")
            if stage not in {"main", "early"} or not isinstance(packages, list):
                return
            if any(not isinstance(p, dict) or any(not isinstance(p.get(k), str)
                   for k in ("id", "name", "nevra", "action")) for p in packages):
                return
            # A fresh solve/retry replaces that stage's old plan. Replaying
            # the SAME plan preserves its download/completion information.
            if self.transactions.get(stage) != transaction:
                self.rows = {k: v for k, v in self.rows.items() if v["stage"] != stage}
            self.transactions[stage] = transaction
            for package in packages:
                self.rows.pop("request:" + package["name"], None)
                self.rows.pop("request:" + package["name"] + "." + package.get("arch", ""), None)
                key = transaction + ":" + package["id"]
                self.rows.setdefault(key, dict(package, stage=stage, phase="waiting", fraction=0))
            if stage == "main":
                for row in self.rows.values():
                    if row["stage"] == "request":
                        row.update(phase="unchanged", fraction=1)
            self.phase = "Downloading"
        elif payload.get("event") == "package":
            row = self.rows.get(transaction + ":" + str(payload.get("id")))
            phase, fraction = payload.get("phase"), payload.get("fraction")
            if row is None or phase not in {"downloading", "downloaded", "download-failed", "waiting-install", "applying", "applied"}:
                return
            if not isinstance(fraction, (int, float)) or not 0 <= fraction <= 1:
                return
            if phase == "waiting-install" and self.phase != "Installing":
                for entry in self.rows.values():
                    if entry["stage"] == row["stage"]:
                        entry.update(phase="waiting-install", fraction=0)
            row.update(phase=phase, fraction=fraction)
            self.phase = "Installing" if phase in {"waiting-install", "applying", "applied"} else "Downloading"
            if phase == "applied" and all(entry["phase"] == "applied" for entry in self.rows.values() if entry["stage"] == row["stage"]):
                self.phase = "Validating"
            elif phase == "downloaded" and all(entry["phase"] == "downloaded" or entry["action"] == "Remove" for entry in self.rows.values() if entry["stage"] == row["stage"]):
                self.phase = "Validating downloads"

    def finish(self, ok, result):
        status = result.get("status")
        self.phase = "Failed" if not ok else ("Prepared for restart" if status in {"scheduled", "ready", "awaiting-boot"} else "Finished")
        for row in self.rows.values():
            if row["stage"] == "early" and row["phase"] == "applied":
                row.update(phase="complete", fraction=1)
            elif not ok:
                if row["phase"] != "unchanged":
                    row.update(phase="failed")
            elif row["phase"] == "unchanged":
                continue
            elif status in {"scheduled", "ready", "awaiting-boot"}:
                row.update(phase="staged", fraction=1)
            elif status in {"live-complete", "complete"}:
                row.update(phase="complete", fraction=1)
            elif status == "unchanged":
                row.update(phase="unchanged", fraction=1)
            else:
                # Older/malformed protocol must never falsely claim installed RPMs.
                row.update(phase="unknown", fraction=0)

    @staticmethod
    def label(row):
        labels = {"waiting": "Waiting for preparation", "downloading": "Downloading",
                  "downloaded": "Downloaded; validating", "download-failed": "Download failed",
                  "waiting-install": "Waiting to install", "applying": "Applying",
                  "applied": "Applied; validating update", "complete": "Installed",
                  "staged": "Ready to install on restart", "failed": "Update failed — see log",
                  "unchanged": "No change needed", "unknown": "Check updater result"}
        text = labels.get(row["phase"], row["phase"])
        if row["action"] == "Remove" and row["phase"] == "complete":
            text = "Removed"
        if row["phase"] in {"downloading", "applying"}:
            text += f" ({row['fraction']:.0%})"
        if row["stage"] == "early":
            text += " · updater prerequisite"
        return text

    def summary(self):
        total = len(self.rows)
        fraction = sum(row["fraction"] for row in self.rows.values()) / total if total else 0
        ready = sum(row["fraction"] == 1 for row in self.rows.values())
        return fraction, f"{self.phase}: {ready}/{total} processed items"
