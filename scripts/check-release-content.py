#!/usr/bin/env python3
"""Reject release-plz PRs that contain only generated release metadata."""

from __future__ import annotations

import copy
import os
import subprocess
import sys
import tomllib
from collections.abc import Mapping

WORKSPACE_PACKAGES = {
    "opcda-bridge",
    "opcda-bridge-client",
    "opcda-bridge-gateway",
    "opcda-bridge-proto",
}
RELEASE_TAG_PATTERN = "opcda-bridge-*-v*"


def git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def read_revision_file(revision: str, path: str) -> str | None:
    result = subprocess.run(
        ["git", "show", f"{revision}:{path}"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    return result.stdout


def normalise_manifest(contents: str) -> Mapping[str, object]:
    document = tomllib.loads(contents)
    normalised = copy.deepcopy(document)

    package = normalised.get("package")
    if isinstance(package, dict):
        package.pop("version", None)

    workspace = normalised.get("workspace")
    if isinstance(workspace, dict):
        package_metadata = workspace.get("package")
        if isinstance(package_metadata, dict):
            package_metadata.pop("version", None)

        dependencies = workspace.get("dependencies")
        if isinstance(dependencies, dict):
            for name in WORKSPACE_PACKAGES:
                dependency = dependencies.get(name)
                if isinstance(dependency, dict):
                    dependency.pop("version", None)
                elif isinstance(dependency, str):
                    dependencies[name] = "<workspace-version>"

    return normalised


def normalise_lockfile(contents: str) -> Mapping[str, object]:
    document = tomllib.loads(contents)
    normalised = copy.deepcopy(document)
    packages = normalised.get("package")
    if isinstance(packages, list):
        for package in packages:
            if not isinstance(package, dict):
                continue
            if package.get("name") in WORKSPACE_PACKAGES:
                package.pop("version", None)
    return normalised


def generated_metadata_only(path: str, base: str, head: str) -> bool:
    if path.endswith("CHANGELOG.md"):
        return True

    base_contents = read_revision_file(base, path)
    head_contents = read_revision_file(head, path)
    if base_contents is None or head_contents is None:
        return False

    try:
        if path.endswith("Cargo.toml"):
            return normalise_manifest(base_contents) == normalise_manifest(head_contents)
        if path.endswith("Cargo.lock"):
            return normalise_lockfile(base_contents) == normalise_lockfile(head_contents)
    except tomllib.TOMLDecodeError as error:
        print(f"Unable to parse {path}: {error}", file=sys.stderr)
        return False

    return False


def latest_release_tag(base: str) -> str | None:
    tags = git(
        "tag",
        "--merged",
        base,
        "--list",
        RELEASE_TAG_PATTERN,
        "--sort=-creatordate",
    )
    return tags.splitlines()[0] if tags else None


def main() -> int:
    branch = os.environ.get("PR_HEAD_REF", "")
    if not branch.startswith("release-plz-"):
        print("Not a release-plz PR; release-content check passed.")
        return 0

    base = os.environ.get("PR_BASE_SHA")
    head = os.environ.get("PR_HEAD_SHA")
    if not base or not head:
        print("PR_BASE_SHA and PR_HEAD_SHA are required for a release-plz PR.", file=sys.stderr)
        return 1

    tag = latest_release_tag(base)
    if tag is None:
        print("No reachable release tag was found; refusing to approve the release PR.", file=sys.stderr)
        return 1

    changed_paths = git("diff", "--name-only", tag, head).splitlines()
    meaningful_paths = [
        path for path in changed_paths if not generated_metadata_only(path, tag, head)
    ]

    if meaningful_paths:
        print(f"Release content found since {tag}:")
        for path in meaningful_paths:
            print(f"  {path}")
        return 0

    print(
        f"Release PR contains only generated metadata since {tag}; "
        "a new version would publish unchanged source.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
