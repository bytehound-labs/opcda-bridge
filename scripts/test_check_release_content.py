from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("check-release-content.py")


class ReleaseContentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.repo = Path(self.temp_dir.name)
        self.git("init", "-q")
        self.git("config", "user.email", "test@example.com")
        self.git("config", "user.name", "Release test")

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

    def package_manifest(self, name: str, version: str) -> str:
        return f"[package]\nname = \"{name}\"\nversion = \"{version}\"\n"

    def add_package(self, name: str, version: str = "0.4.3") -> None:
        self.write(f"crates/{name}/Cargo.toml", self.package_manifest(name, version))
        self.write(f"crates/{name}/CHANGELOG.md", "# Changelog\n\n## [Unreleased]\n")

    def bump_package(self, name: str, version: str = "0.4.4") -> None:
        self.write(f"crates/{name}/Cargo.toml", self.package_manifest(name, version))
        self.write(
            f"crates/{name}/CHANGELOG.md",
            "# Changelog\n\n## [Unreleased]\n\n- release metadata\n",
        )

    def run_check(self, base: str, head: str) -> subprocess.CompletedProcess[str]:
        environment = os.environ.copy()
        environment.update(
            {
                "PR_BASE_SHA": base,
                "PR_HEAD_REF": "release-plz-test",
                "PR_HEAD_SHA": head,
            }
        )
        return subprocess.run(
            [sys.executable, str(SCRIPT)],
            cwd=self.repo,
            env=environment,
            capture_output=True,
            text=True,
        )

    def test_gateway_only_release(self) -> None:
        self.add_package("opcda-bridge-gateway")
        self.write("crates/opcda-bridge-gateway/src/logging.rs", "old\n")
        base = self.commit("base")
        self.git("tag", "opcda-bridge-gateway-v0.4.3")

        self.bump_package("opcda-bridge-gateway")
        self.write("crates/opcda-bridge-gateway/src/logging.rs", "new\n")
        head = self.commit("fix(gateway): change logging")

        result = self.run_check(base, head)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("opcda-bridge-gateway", result.stdout)

    def test_dependency_cascade_release(self) -> None:
        self.add_package("opcda-bridge")
        self.add_package("opcda-bridge-client")
        self.write("crates/opcda-bridge/src/lib.rs", "old\n")
        base = self.commit("base")
        self.git("tag", "opcda-bridge-v0.4.3")
        self.git("tag", "opcda-bridge-client-v0.4.3")

        self.bump_package("opcda-bridge")
        self.write("crates/opcda-bridge/src/lib.rs", "new\n")
        self.bump_package("opcda-bridge-client")
        head = self.commit("fix(client): rebuild for library change")

        result = self.run_check(base, head)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("crates/opcda-bridge/src/lib.rs", result.stdout)

    def test_multi_package_release(self) -> None:
        self.add_package("opcda-bridge-gateway")
        self.add_package("opcda-bridge-client")
        self.write("crates/opcda-bridge-gateway/src/lib.rs", "old gateway\n")
        self.write("crates/opcda-bridge-client/src/lib.rs", "old client\n")
        base = self.commit("base")
        self.git("tag", "opcda-bridge-gateway-v0.4.3")
        self.git("tag", "opcda-bridge-client-v0.4.3")

        self.bump_package("opcda-bridge-gateway")
        self.bump_package("opcda-bridge-client")
        self.write("crates/opcda-bridge-gateway/src/lib.rs", "new gateway\n")
        self.write("crates/opcda-bridge-client/src/lib.rs", "new client\n")
        head = self.commit("feat: update both binaries")

        result = self.run_check(base, head)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("opcda-bridge-gateway", result.stdout)
        self.assertIn("opcda-bridge-client", result.stdout)

    def test_generated_metadata_only_release_is_rejected(self) -> None:
        self.add_package("opcda-bridge-gateway")
        self.write("crates/opcda-bridge-gateway/src/lib.rs", "unchanged\n")
        base = self.commit("base")
        self.git("tag", "opcda-bridge-gateway-v0.4.3")

        self.bump_package("opcda-bridge-gateway")
        head = self.commit("chore: generated release metadata")

        result = self.run_check(base, head)
        self.assertEqual(result.returncode, 1)
        self.assertIn("only generated metadata", result.stderr)

    def test_release_without_prior_package_tag(self) -> None:
        self.add_package("opcda-bridge-gateway")
        self.write("crates/opcda-bridge-gateway/src/lib.rs", "old\n")
        base = self.commit("base")

        self.bump_package("opcda-bridge-gateway")
        self.write("crates/opcda-bridge-gateway/src/lib.rs", "new\n")
        head = self.commit("feat: initial gateway release")

        result = self.run_check(base, head)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("since the release PR base", result.stdout)


if __name__ == "__main__":
    unittest.main()
