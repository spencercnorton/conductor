#!/usr/bin/env python3
"""Check new public text and paths without printing potentially private values."""
from pathlib import Path
import os
import re
import subprocess
import sys

# Example domains, loopback endpoints and package maintainer contacts belong in
# product documentation; real deployment identities and personal paths do not.
RULES = {
    "personal home path": re.compile(r"(?:/Users/|/home/)(?!demo\b|example\b|user\b|runner\b|test\b|alice\b|bob\b)[a-zA-Z0-9_.-]+"),
    "private repository URL": re.compile(r"(?:https?://|git@)(?:gitlab\.(?!com\b)[\w.-]+|git\.(?!github\.|example\.)[\w.-]+)"),
    "internal tracker reference": re.compile(r"\bOP#\d+\b"),
    "private network address": re.compile(r"(?<![\w.])(?:10\.\d{1,3}\.\d{1,3}\.\d{1,3}|192\.168\.\d{1,3}\.\d{1,3}|172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3}|100\.(?:6[4-9]|[7-9]\d|1[01]\d|12[0-7])\.\d{1,3}\.\d{1,3})(?![\w.])"),
}
FORBIDDEN = re.compile(r"(?:^|/)(?:AGENTS/(?:journal|journal-archive).*|credentials|\.env(?:\.(?!example$)[^/]+)?|id_(?:rsa|ed25519)|[^/]+\.(?:sqlite3?|db|key|p12|pfx|log))$")

def git(*args):
    return subprocess.check_output(["git", *args])

def main():
    base = os.environ.get("BASE_SHA", "")
    if base and set(base) != {"0"}:
        # An invalid base must fail closed rather than skip inspection.
        git("cat-file", "-e", base + "^{commit}")
    else:
        base = git("rev-parse", "HEAD^").decode().strip()
    paths = git("diff", "--name-only", "--diff-filter=ACMR", "-z", base, "HEAD").decode().split("\0")
    failures = []
    for name in filter(None, paths):
        path = Path(name)
        if FORBIDDEN.search(name):
            failures.append((name, "private file path"))
        if path.is_symlink():
            failures.append((name, "symlink requires explicit review"))
            continue
        # Binary pixels need visual review; their metadata and secrets remain
        # covered by the secret scanner and the review checklist.
        data = path.read_bytes()
        if b"\0" in data:
            if path.suffix.lower() not in {".png", ".jpg", ".jpeg", ".gif", ".mp4", ".ico", ".icns"}:
                failures.append((name, "binary requires explicit policy review"))
            continue
        try:
            data.decode("utf-8")
        except UnicodeDecodeError:
            continue
        patch = git("diff", "--unified=0", base, "HEAD", "--", name).decode("utf-8", errors="replace")
        added = "\n".join(line[1:] for line in patch.splitlines() if line.startswith("+") and not line.startswith("+++"))
        for label, pattern in RULES.items():
            if pattern.search(added):
                failures.append((name, label))
    for name, label in failures:
        print(f"{name}: {label}; review locally, do not paste the matched value")
    print(f"Public-content check: {len(failures)} finding(s)")
    return bool(failures)

if __name__ == "__main__":
    sys.exit(main())
