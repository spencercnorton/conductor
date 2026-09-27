#!/usr/bin/env python3
"""Behavior checks for the public content gate in isolated Git repositories."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCANNER = Path(__file__).with_name("check_public_content.py").resolve()

class PublicContentTests(unittest.TestCase):
    def check_change(self, name, content, *, symlink=False):
        with tempfile.TemporaryDirectory() as temp:
            p = Path(temp)
            def git(*args):
                return subprocess.check_output(["git", *args], cwd=p, stderr=subprocess.DEVNULL).decode().strip()
            git("init", "-q")
            git("config", "user.name", "Demo")
            git("config", "user.email", "demo@example.com")
            (p / "README.md").write_text("Public example\n")
            git("add", ".")
            git("commit", "-qm", "baseline")
            base = git("rev-parse", "HEAD")
            target = p / name
            target.parent.mkdir(parents=True, exist_ok=True)
            if symlink:
                target.symlink_to("README.md")
            elif isinstance(content, bytes):
                target.write_bytes(content)
            else:
                target.write_text(content)
            git("add", ".")
            git("commit", "-qm", "change")
            env = dict(os.environ, BASE_SHA=base)
            return subprocess.run(["python3", str(SCANNER)], cwd=p, env=env, capture_output=True, text=True)

    def test_public_examples_pass(self):
        self.assertEqual(self.check_change("docs/setup.md", "Use http://localhost:8409 and demo@example.com\n").returncode, 0)

    def test_environment_file_is_blocked(self):
        self.assertNotEqual(self.check_change(".env", "MODE=demo\n").returncode, 0)

    def test_personal_path_is_blocked_without_echoing_value(self):
        private = "/home/" + "private-person/notes"
        result = self.check_change("docs/setup.md", private)
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn(private, result.stdout)

    def test_internal_tracker_reference_is_blocked(self):
        self.assertNotEqual(self.check_change("docs/setup.md", "OP" + "#1234").returncode, 0)

    def test_private_address_is_blocked(self):
        self.assertNotEqual(self.check_change("docs/setup.md", "192." + "168.17.42").returncode, 0)

    def test_symlink_is_blocked(self):
        self.assertNotEqual(self.check_change("linked", "", symlink=True).returncode, 0)

    def test_binary_archive_is_blocked(self):
        self.assertNotEqual(self.check_change("backup.zip", b"PK\x00\x01").returncode, 0)

    def test_example_environment_file_passes(self):
        self.assertEqual(self.check_change(".env.example", "MODE=demo\n").returncode, 0)

if __name__ == "__main__":
    unittest.main()
