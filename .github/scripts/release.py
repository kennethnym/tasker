"""Validate container release tags and decide whether to advance latest."""

import json
import os
import re
import subprocess
import sys

NUMBER = r"(0|[1-9][0-9]*)"
IDENTIFIER = r"(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)"
VERSION = re.compile(
    rf"{NUMBER}\.{NUMBER}\.{NUMBER}(?:-({IDENTIFIER}(?:\.{IDENTIFIER})*))?"
)


def parse_version(version):
    match = VERSION.fullmatch(version)
    if not match or any(int(n) > 9007199254740991 for n in match.groups()[:3]):
        raise ValueError(f"Invalid container release version: {version!r}")
    # Docker tags cannot contain '+'. Build metadata is deliberately unsupported.
    if len(version) > 128:
        raise ValueError("Release version exceeds Docker's 128-character tag limit")
    return tuple(int(n) for n in match.groups()[:3]), match.group(4)


def validate_tag(tag):
    if not tag.startswith("v"):
        raise ValueError("Release tags must start with v")
    return parse_version(tag[1:])


def published_version(image):
    result = subprocess.run(
        ["docker", "buildx", "imagetools", "inspect", f"{image}:latest",
         "--format", "{{json .Image}}"],
        capture_output=True, text=True, check=False,
    )
    if result.returncode:
        # Only a missing manifest permits bootstrapping. Auth/network failures fail closed.
        if re.search(rf"(?:docker\.io/)?{re.escape(image)}:latest: not found\s*$", result.stderr):
            return None
        raise RuntimeError(f"Cannot inspect latest: {result.stderr.strip()}")
    config = json.loads(result.stdout)
    if "config" not in config:
        config = config["linux/amd64"]
    version = config["config"]["Labels"]["org.opencontainers.image.version"]
    parsed = parse_version(version)
    if parsed[1] is not None:
        raise ValueError("Existing latest image is not a stable version")
    return parsed[0]


def should_promote(tag, image):
    version, prerelease = validate_tag(tag)
    if prerelease is not None:
        return False
    current = published_version(image)
    return current is None or version > current


def main():
    tag = os.environ["GITHUB_REF_NAME"]
    validate_tag(tag)
    if sys.argv[1] == "promote":
        promote = should_promote(tag, os.environ["IMAGE"])
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            output.write(f"latest={str(promote).lower()}\n")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, RuntimeError, KeyError, TypeError) as error:
        sys.exit(f"Release publication refused: {error}")
