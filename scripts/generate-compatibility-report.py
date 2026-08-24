#!/usr/bin/env python3
"""Generate the human and machine-readable compatibility catalog."""

from __future__ import annotations

import argparse
import json
import sys
import tomllib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "crates" / "opcda-bridge-proto" / "compatibility.toml"
MARKDOWN = ROOT / "COMPATIBILITY.md"
JSON = ROOT / "compatibility.json"


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def load_catalog(path: Path = CATALOG) -> dict[str, Any]:
    with path.open("rb") as stream:
        document = tomllib.load(stream)
    for key in ("schema_version", "catalog_version", "protocol", "release_lines", "evidence"):
        if key not in document:
            raise ValueError(f"compatibility catalog is missing {key}")
    return document


def render_json(document: dict[str, Any]) -> str:
    return json.dumps(document, indent=2, sort_keys=True) + "\n"


def render_markdown(document: dict[str, Any]) -> str:
    protocol = document["protocol"]
    lines = [
        "# Client/gateway compatibility",
        "",
        "Package versions are independent. Runtime compatibility is negotiated by",
        "protocol feature and advertised capability, not by matching client and",
        "gateway package versions.",
        "",
        "The `opcda-bridge-client compatibility` command checks a deployed pair",
        "without contacting GitHub or crates.io. It reports the gateway package",
        "version, protocol ranges, negotiated features, and whether the exact",
        "pair has test evidence. It distinguishes the client binary version from",
        "the reusable-library version implementing its protocol contract.",
        "",
        "## Protocol features",
        "",
        "| Feature | Current contract | Meaning |",
        "| --- | ---: | --- |",
        f"| Core | {protocol['core']} | Server discovery, reads, and writes |",
        f"| Namespace | {protocol['namespace']} | Capabilities, paged browse, sessions, and live search |",
        f"| Indexed search | {protocol['indexed_search']} | Persistent namespace index operations |",
        "",
        "## Release lines",
        "",
        "| Release line | Package versions | Status | Core | Namespace | Indexed search | Notes |",
        "| --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    for line in document["release_lines"]:
        lines.append(
            "| {name} | {min_version} - {max_version} | {status} | {core_protocol} | "
            "{namespace_protocol} | {indexed_search_protocol} | {notes} |".format(**line)
        )
    lines.extend(
        [
            "",
            "A pair whose required protocol ranges overlap is usable even when its",
            "exact package versions have not been exercised together. Such a pair is",
            "reported as `unverified`, not rejected. Optional features may be",
            "`unsupported` while core read/write compatibility remains available.",
            "",
            "## Evidence",
            "",
            "| Client line | Client version | Gateway line | Gateway version | Evidence | Notes |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
    )
    for evidence in document["evidence"]:
        lines.append(
            "| {client_line} | {client_version} | {gateway_line} | {gateway_version} | "
            "{status} | {notes} |".format(
                client_line=evidence["client_line"],
                client_version=evidence.get("client_version", "-"),
                gateway_line=evidence["gateway_line"],
                gateway_version=evidence.get("gateway_version", "-"),
                status=evidence["status"],
                notes=evidence["notes"],
            )
        )
    lines.extend(
        [
            "",
            "An intentional wire-contract break creates a new protocol boundary.",
            "The affected protocol crate, reusable library, client, and gateway",
            "release independently as needed, while the compatibility catalog and",
            "cross-version evidence are updated together.",
            "",
        ]
    )
    return "\n".join(lines)


def generate(document: dict[str, Any]) -> tuple[str, str]:
    return render_markdown(document), render_json(document)


def check_or_write(write: bool) -> int:
    document = load_catalog()
    markdown, encoded_json = generate(document)
    if write:
        MARKDOWN.write_text(markdown, encoding="utf-8")
        JSON.write_text(encoded_json, encoding="utf-8")
        return 0

    mismatches = []
    if not MARKDOWN.exists() or MARKDOWN.read_text(encoding="utf-8") != markdown:
        mismatches.append(display_path(MARKDOWN))
    if not JSON.exists() or JSON.read_text(encoding="utf-8") != encoded_json:
        mismatches.append(display_path(JSON))
    if mismatches:
        print(
            "Generated compatibility files are stale: " + ", ".join(mismatches),
            file=sys.stderr,
        )
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--write", action="store_true", help="write generated files")
    mode.add_argument("--check", action="store_true", help="check generated files")
    args = parser.parse_args()
    return check_or_write(write=args.write)


if __name__ == "__main__":
    raise SystemExit(main())
