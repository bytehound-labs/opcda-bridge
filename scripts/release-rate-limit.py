#!/usr/bin/env python3
"""Fail closed when crates.io publishes indicate a runaway release loop."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

CRATES = (
    "opcda-bridge",
    "opcda-bridge-proto",
    "opcda-bridge-client",
    "opcda-bridge-gateway",
)
HOURLY_LIMIT = int(os.environ.get("RELEASE_RATE_HOURLY_LIMIT", "3"))
DAILY_LIMIT = int(os.environ.get("RELEASE_RATE_DAILY_LIMIT", "12"))
USER_AGENT = "opcda-bridge-release-rate-limit"


def fetch_versions(crate: str) -> list[dict[str, object]]:
    request = Request(
        f"https://crates.io/api/v1/crates/{crate}",
        headers={"User-Agent": USER_AGENT},
    )
    with urlopen(request, timeout=20) as response:
        document = json.load(response)
    versions = document.get("versions")
    if not isinstance(versions, list):
        raise ValueError(f"crates.io returned no versions list for {crate}")
    return [version for version in versions if isinstance(version, dict)]


def publish_times(versions: list[dict[str, object]]) -> list[datetime]:
    times = []
    for version in versions:
        created_at = version.get("created_at")
        if not isinstance(created_at, str):
            raise ValueError("crates.io returned a version without created_at")
        times.append(datetime.fromisoformat(created_at.replace("Z", "+00:00")))
    return times


def main() -> int:
    now = datetime.now(timezone.utc)
    hour_cutoff = now - timedelta(hours=1)
    day_cutoff = now - timedelta(days=1)
    failures = []

    for crate in CRATES:
        try:
            times = publish_times(fetch_versions(crate))
        except (HTTPError, URLError, TimeoutError, ValueError, json.JSONDecodeError) as error:
            print(f"Unable to verify crates.io publish history for {crate}: {error}", file=sys.stderr)
            return 1

        hour_count = sum(created_at >= hour_cutoff for created_at in times)
        day_count = sum(created_at >= day_cutoff for created_at in times)
        print(f"{crate}: {hour_count} publish(es) in the last hour, {day_count} in the last day")

        if hour_count >= HOURLY_LIMIT:
            failures.append(f"{crate} reached the hourly limit ({HOURLY_LIMIT})")
        if day_count >= DAILY_LIMIT:
            failures.append(f"{crate} reached the daily limit ({DAILY_LIMIT})")

    if failures:
        print("Release rate limit exceeded; refusing to run release-plz:", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        return 1

    print("Release rate limits are clear.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
