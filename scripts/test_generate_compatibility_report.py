from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("generate-compatibility-report.py")
SPEC = importlib.util.spec_from_file_location("compatibility_report", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class CompatibilityReportTests(unittest.TestCase):
    def test_renderers_include_protocol_and_evidence(self) -> None:
        document = {
            "schema_version": 1,
            "catalog_version": "test",
            "protocol": {"core": 1, "namespace": 2, "indexed_search": 1},
            "release_lines": [
                {
                    "name": "test",
                    "min_version": "1.0.0",
                    "max_version": "1.9.9",
                    "status": "supported",
                    "core_protocol": 1,
                    "namespace_protocol": 2,
                    "indexed_search_protocol": 1,
                    "notes": "fixture",
                }
            ],
            "evidence": [
                {
                    "client_line": "test",
                    "client_version": "1.2.3",
                    "gateway_line": "test",
                    "gateway_version": "1.2.4",
                    "status": "contract-boundary-tested",
                    "notes": "fixture evidence",
                }
            ],
        }
        markdown, encoded_json = MODULE.generate(document)
        self.assertIn("| test | 1.0.0 - 1.9.9 | supported |", markdown)
        self.assertIn(
            "| test | 1.2.3 | test | 1.2.4 | contract-boundary-tested |",
            markdown,
        )
        self.assertIn("contract-boundary-tested", markdown)
        self.assertIn('"catalog_version": "test"', encoded_json)

    def test_load_catalog_rejects_missing_keys(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "compatibility.toml"
            path.write_text("schema_version = 1\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "missing catalog_version"):
                MODULE.load_catalog(path)

    def test_check_or_write_detects_and_repairs_stale_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original_catalog = MODULE.CATALOG
            original_markdown = MODULE.MARKDOWN
            original_json = MODULE.JSON
            try:
                MODULE.CATALOG = root / "compatibility.toml"
                MODULE.MARKDOWN = root / "COMPATIBILITY.md"
                MODULE.JSON = root / "compatibility.json"
                MODULE.CATALOG.write_text(
                    'schema_version = 1\ncatalog_version = "1"\n'
                    '[protocol]\ncore = 1\nnamespace = 2\nindexed_search = 1\n'
                    '[[release_lines]]\nname = "test"\nmin_version = "1.0.0"\n'
                    'max_version = "1.0.0"\nstatus = "supported"\ncore_protocol = 1\n'
                    'namespace_protocol = 2\nindexed_search_protocol = 1\nnotes = "fixture"\n'
                    '[[evidence]]\nclient_line = "test"\ngateway_line = "test"\n'
                    'status = "unverified"\nnotes = "fixture"\n',
                    encoding="utf-8",
                )
                self.assertEqual(MODULE.check_or_write(write=False), 1)
                self.assertEqual(MODULE.check_or_write(write=True), 0)
                self.assertEqual(MODULE.check_or_write(write=False), 0)
                MODULE.MARKDOWN.write_text("stale\n", encoding="utf-8")
                self.assertEqual(MODULE.check_or_write(write=False), 1)
            finally:
                MODULE.CATALOG = original_catalog
                MODULE.MARKDOWN = original_markdown
                MODULE.JSON = original_json


if __name__ == "__main__":
    unittest.main()
