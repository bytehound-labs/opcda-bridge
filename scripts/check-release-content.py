#!/usr/bin/env python3
"""Reject release-plz PRs that contain only generated release metadata."""

from __future__ import annotations

import copy
import os
import subprocess
import sys
import tomllib
from collections.abc import Mapping

WORKSPACE_PACKAGES = (
    "opcda-bridge",
    "opcda-bridge-client",
    "opcda-bridge-gateway",
    "opcda-bridge-proto",
)
PACKAGE_MANIFESTS = {
    package: f"crates/{package}/Cargo.toml" for package in WORKSPACE_PACKAGES
}
PACKAGE_DIRECT_DEPENDENCIES = {
    "opcda-bridge": ("opcda-bridge-proto",),
    "opcda-bridge-client": ("opcda-bridge",),
    "opcda-bridge-gateway": ("opcda-bridge-proto",),
    "opcda-bridge-proto": (),
}


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


def package_version(revision: str, package: str) -> str | None:
    contents = read_revision_file(revision, PACKAGE_MANIFESTS[package])
    if contents is None:
        return None

    try:
        document = tomllib.loads(contents)
    except tomllib.TOMLDecodeError as error:
        print(f"Unable to parse {PACKAGE_MANIFESTS[package]}: {error}", file=sys.stderr)
        return None

    package_table = document.get("package")
    if not isinstance(package_table, dict):
        return None
    version = package_table.get("version")
    return version if isinstance(version, str) else None


def released_packages(base: str, head: str) -> tuple[str, ...]:
    released: list[str] = []
    for package in WORKSPACE_PACKAGES:
        base_version = package_version(base, package)
        head_version = package_version(head, package)
        if head_version is not None and base_version != head_version:
            released.append(package)
    return tuple(released)


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


def latest_release_tag(base: str, package: str) -> str | None:
    tags = git(
        "tag",
        "--merged",
        base,
        "--list",
        f"{package}-v*",
        "--sort=-version:refname",
    )
    return tags.splitlines()[0] if tags else None


def release_scope(package: str) -> tuple[str, ...]:
    scope: list[str] = []
    pending = [package]
    while pending:
        current = pending.pop()
        if current in scope:
            continue
        scope.append(current)
        pending.extend(PACKAGE_DIRECT_DEPENDENCIES[current])
    return tuple(scope)


def path_in_scope(path: str, package: str) -> bool:
    return path == f"crates/{package}" or path.startswith(f"crates/{package}/")


def meaningful_paths_for_package(
    package: str,
    comparison: str,
    head: str,
) -> tuple[str, ...]:
    changed_paths = git("diff", "--name-only", comparison, head).splitlines()
    scope = release_scope(package)
    return tuple(
        path
        for path in changed_paths
        if any(path_in_scope(path, scoped_package) for scoped_package in scope)
        and not generated_metadata_only(path, comparison, head)
    )


def check_release_content(base: str, head: str) -> int:
    packages = released_packages(base, head)
    if not packages:
        print("No package version changes detected; release-content check passed.")
        return 0

    failures: list[str] = []
    for package in packages:
        tag = latest_release_tag(base, package)
        comparison = tag or base
        meaningful_paths = meaningful_paths_for_package(package, comparison, head)
        if meaningful_paths:
            source = f"since {tag}" if tag else "since the release PR base"
            print(f"Release content for {package} found {source}:")
            for path in meaningful_paths:
                print(f"  {path}")
        else:
            failures.append(package)

    if failures:
        packages_text = ", ".join(failures)
        print(
            f"Release PR contains only generated metadata for {packages_text}; "
            "a new version would publish unchanged source.",
            file=sys.stderr,
        )
        return 1

    return 0


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

    return check_release_content(base, head)


if __name__ == "__main__":
    raise SystemExit(main())
