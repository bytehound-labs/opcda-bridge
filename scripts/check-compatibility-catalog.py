#!/usr/bin/env python3
"""Validate release versions and intentional protocol compatibility changes."""

from __future__ import annotations

import argparse
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any

PACKAGES = (
    "opcda-bridge-proto",
    "opcda-bridge",
    "opcda-bridge-client",
    "opcda-bridge-gateway",
)
CATALOG_PATH = "crates/opcda-bridge-proto/compatibility.toml"
MANIFESTS = {package: f"crates/{package}/Cargo.toml" for package in PACKAGES}


def git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def revision_file(revision: str, path: str) -> str | None:
    result = subprocess.run(
        ["git", "show", f"{revision}:{path}"],
        check=False,
        capture_output=True,
        text=True,
    )
    return result.stdout if result.returncode == 0 else None


def document_at(revision: str, path: str) -> dict[str, Any] | None:
    contents = revision_file(revision, path)
    if contents is None:
        return None
    try:
        document = tomllib.loads(contents)
    except tomllib.TOMLDecodeError as error:
        raise ValueError(f"{path} is not valid TOML: {error}") from error
    if not isinstance(document, dict):
        raise ValueError(f"{path} must contain a TOML table")
    return document


def parse_version(value: str) -> tuple[int, int, int]:
    parts = value.split(".")
    if len(parts) != 3 or any(not part.isdigit() for part in parts):
        raise ValueError(f"unsupported package version {value!r}; expected X.Y.Z")
    return tuple(int(part) for part in parts)  # type: ignore[return-value]


def release_lines(document: dict[str, Any]) -> list[dict[str, Any]]:
    lines = document.get("release_lines")
    if not isinstance(lines, list) or not lines:
        raise ValueError("compatibility catalog must define release_lines")
    return [line for line in lines if isinstance(line, dict)]


def matching_lines(version: str, lines: list[dict[str, Any]]) -> list[str]:
    parsed = parse_version(version)
    matches: list[str] = []
    for line in lines:
        try:
            minimum = parse_version(str(line["min_version"]))
            maximum = parse_version(str(line["max_version"]))
            name = str(line["name"])
        except (KeyError, TypeError, ValueError):
            continue
        if minimum <= parsed <= maximum:
            matches.append(name)
    return matches


def package_versions(revision: str) -> dict[str, str]:
    versions: dict[str, str] = {}
    for package, manifest in MANIFESTS.items():
        document = document_at(revision, manifest)
        if document is None:
            raise ValueError(f"{manifest} is missing at {revision}")
        package_table = document.get("package")
        if not isinstance(package_table, dict) or not isinstance(
            package_table.get("version"), str
        ):
            raise ValueError(f"{manifest} does not define a package version")
        versions[package] = package_table["version"]
    return versions


def validate_release_versions(revision: str) -> list[str]:
    catalog = document_at(revision, CATALOG_PATH)
    if catalog is None:
        raise ValueError(f"{CATALOG_PATH} is missing at {revision}")
    lines = release_lines(catalog)
    errors: list[str] = []
    for package, version in package_versions(revision).items():
        matches = matching_lines(version, lines)
        if len(matches) != 1:
            detail = ", ".join(matches) if matches else "none"
            errors.append(
                f"{package} version {version} matches {len(matches)} catalog entries ({detail})"
            )
    return errors


def boundary_signature(line: dict[str, Any]) -> tuple[Any, ...]:
    return (
        line.get("name"),
        line.get("min_version"),
        line.get("max_version"),
        line.get("core_protocol"),
        line.get("namespace_protocol"),
        line.get("indexed_search_protocol"),
    )


def breaking_change_errors(base: str, head: str) -> list[str]:
    base_catalog = document_at(base, CATALOG_PATH)
    head_catalog = document_at(head, CATALOG_PATH)
    if base_catalog is None or head_catalog is None:
        return [f"{CATALOG_PATH} must exist on both sides of a breaking change"]

    base_boundaries = {
        boundary_signature(line) for line in release_lines(base_catalog)
    }
    head_boundaries = {
        boundary_signature(line) for line in release_lines(head_catalog)
    }
    errors: list[str] = []
    if base_boundaries == head_boundaries:
        errors.append(
            "breaking Protobuf changes must add or change a compatibility release boundary"
        )
    if base_catalog.get("evidence") == head_catalog.get("evidence"):
        errors.append("breaking Protobuf changes must update compatibility evidence")
    return errors


def changed_paths(base: str, head: str) -> set[str]:
    return set(git("diff", "--name-only", base, head).splitlines())


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", required=True, help="base Git revision")
    parser.add_argument("--head", required=True, help="head Git revision")
    parser.add_argument(
        "--release-pr",
        action="store_true",
        help="validate every publishable package version against the catalog",
    )
    parser.add_argument(
        "--breaking-protobuf",
        action="store_true",
        help="require a catalog boundary and evidence update",
    )
    args = parser.parse_args()

    errors: list[str] = []
    if args.release_pr:
        errors.extend(validate_release_versions(args.head))
    if args.breaking_protobuf:
        if CATALOG_PATH not in changed_paths(args.base, args.head):
            errors.append(f"{CATALOG_PATH} must change with a breaking Protobuf change")
        errors.extend(breaking_change_errors(args.base, args.head))

    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1

    print("Compatibility catalog validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
