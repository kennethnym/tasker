import json
import subprocess
import unittest
from unittest.mock import patch

import release


class ReleaseTests(unittest.TestCase):
    def test_valid_tags(self):
        for tag in ("v0.0.0", "v1.2.3", "v1.2.3-0", "v1.2.3-rc.1", "v1.2.3-01a"):
            with self.subTest(tag=tag):
                release.validate_tag(tag)

    def test_invalid_tags(self):
        for tag in ("v1.2.3-01", "v1.2.3-rc..1", "v01.2.3", "v1.2.3-",
                    "v1.2", "1.2.3", "v1.2.3-rc.01", "v1.2.3+build",
                    "v1.2.3\n", "v1.2.3-" + "a" * 128):
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                release.validate_tag(tag)

    @patch("release.published_version")
    def test_stable_promotion_never_regresses(self, current):
        current.return_value = (1, 2, 4)
        self.assertFalse(release.should_promote("v1.2.3", "example/tasker"))
        self.assertFalse(release.should_promote("v1.2.4", "example/tasker"))
        self.assertTrue(release.should_promote("v1.2.10", "example/tasker"))
        current.return_value = (1, 9, 0)
        self.assertTrue(release.should_promote("v1.10.0", "example/tasker"))
        current.return_value = None
        self.assertTrue(release.should_promote("v0.1.0", "example/tasker"))

    @patch("release.published_version")
    def test_prerelease_does_not_inspect_or_promote(self, current):
        self.assertFalse(release.should_promote("v2.0.0-rc.1", "example/tasker"))
        current.assert_not_called()

    @patch("release.subprocess.run")
    def test_inspect_single_and_multi_platform_images(self, run):
        config = {"config": {"Labels": {"org.opencontainers.image.version": "1.2.4"}}}
        for image in (config, {"linux/amd64": config}):
            run.return_value = subprocess.CompletedProcess([], 0, json.dumps(image), "")
            self.assertEqual(release.published_version("example/tasker"), (1, 2, 4))

    @patch("release.subprocess.run")
    def test_only_missing_latest_allows_bootstrap(self, run):
        run.return_value = subprocess.CompletedProcess(
            [], 1, "", "ERROR: docker.io/example/tasker:latest: not found\n")
        self.assertIsNone(release.published_version("example/tasker"))
        for error in ("unauthorized", "connection timeout", "host not found", "429 Too Many Requests"):
            run.return_value = subprocess.CompletedProcess([], 1, "", error)
            with self.subTest(error=error), self.assertRaises(RuntimeError):
                release.published_version("example/tasker")

    @patch("release.subprocess.run")
    def test_unknown_current_version_fails_closed(self, run):
        for version in (None, "edge", "1.2.3-rc.1"):
            labels = {} if version is None else {"org.opencontainers.image.version": version}
            run.return_value = subprocess.CompletedProcess(
                [], 0, json.dumps({"config": {"Labels": labels}}), "")
            with self.subTest(version=version), self.assertRaises((KeyError, ValueError)):
                release.published_version("example/tasker")


if __name__ == "__main__":
    unittest.main()
