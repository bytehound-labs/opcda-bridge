from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("check-compatibility-catalog.py")


class CompatibilityCatalogTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.repo = Path(self.temp_dir.name)
        self.git("init", "-q")
        self.git("config", "user.email", "test@example.com")
        self.git("config", "user.name", "Compatibility test")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def git(self, *args: str) -> str:
        result = subprocess.run(
            ["git", *args],
            cwd=self.repo,
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()

    def write(self, path: str, contents: str) -> None:
        target = self.repo / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(contents, encoding="utf-8")

    def commit(self, message: str) -> str:
        self.git("add", ".")
        self.git("commit", "-q", "-m", message)
        return self.git("rev-parse", "HEAD")

    def manifests(self, version: str = "0.4.3") -> None:
        for package in (
            "opcda-bridge-proto",
            "opcda-bridge",
            "opcda-bridge-client",
            "opcda-bridge-gateway",
        ):
            self.write(
                f"crates/{package}/Cargo.toml",
                f'[package]\nname = "{package}"\nversion = "{version}"\n',
            )

    def catalog(self, indexed_max: str = "0.999.999", evidence: str = "one") -> None:
        self.write(
            "crates/opcda-bridge-proto/compatibility.toml",
            f"""schema_version = 1
catalog_version = "1"

[protocol]
core = 1
namespace = 2
indexed_search = 1

[[release_lines]]
name = "indexed"
min_version = "0.4.0"
max_version = "{indexed_max}"
status = "supported"
core_protocol = 1
namespace_protocol = 2
indexed_search_protocol = 1
notes = "test"

[[evidence]]
client_line = "indexed"
gateway_line = "indexed"
status = "contract-boundary-tested"
notes = "{evidence}"
""",
        )

    def run_check(
        self, base: str, head: str, *options: str
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--base",
                base,
                "--head",
                head,
                *options,
            ],
            cwd=self.repo,
            capture_output=True,
            text=True,
        )

    def test_release_versions_must_match_one_line(self) -> None:
        self.manifests()
        self.catalog()
        base = self.commit("base")
        result = self.run_check(base, base, "--release-pr")
        self.assertEqual(result.returncode, 0, result.stderr)

        self.manifests("1.0.0")
        head = self.commit("unlisted version")
        result = self.run_check(base, head, "--release-pr")
        self.assertEqual(result.returncode, 1)
        self.assertIn("matches 0 catalog entries", result.stderr)

    def test_breaking_change_requires_boundary_and_evidence(self) -> None:
        self.manifests()
        self.catalog()
        base = self.commit("base")
        self.write("crates/opcda-bridge-proto/proto/bridge.proto", "breaking\n")
        head = self.commit("breaking change")
        result = self.run_check(base, head, "--breaking-protobuf")
        self.assertEqual(result.returncode, 1)
        self.assertIn("release boundary", result.stderr)
        self.assertIn("evidence", result.stderr)

        self.catalog(indexed_max="0.4.999", evidence="two")
        head = self.commit("catalog boundary and evidence")
        result = self.run_check(base, head, "--breaking-protobuf")
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
