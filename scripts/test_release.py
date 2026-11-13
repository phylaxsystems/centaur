"""Release contract tests; registry writes are simulated, Helm packaging is real."""

import json
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import release
import yaml

COMMIT = "abcd1234" * 5


class ReleaseTests(unittest.TestCase):
    def test_identity_and_rejected_refs(self):
        for ref, tag, version, kind in [
            (
                "refs/heads/main",
                f"main-sha-{COMMIT}",
                f"0.0.0-main.sha{COMMIT}",
                "main",
            ),
            ("refs/tags/v2.4.1-rc.10", "v2.4.1-rc.10", "2.4.1-rc.10", "rc"),
            ("refs/tags/v2.4.1", "v2.4.1", "2.4.1", "stable"),
        ]:
            with self.subTest(ref=ref):
                result = release.plan(ref, COMMIT)
                self.assertEqual(
                    (result["tag"], result["chart_version"], result["kind"]),
                    (tag, version, kind),
                )
        for ref in [
            "refs/heads/feature",
            "refs/tags/v01.2.3",
            "refs/tags/v1.2.3-rc.01",
            "refs/tags/v1.2.3-beta.1",
            "refs/tags/v1.2.3+build",
            "refs/pull/1/merge",
        ]:
            with self.subTest(ref=ref), self.assertRaises(ValueError):
                release.plan(ref, COMMIT)
        with self.assertRaises(ValueError):
            release.plan("refs/heads/main", COMMIT[:7])

    def test_stable_requires_unambiguous_same_version_rc(self):
        self.assertEqual(
            release.candidate("v2.4.1", ["v2.4.0-rc.9", "v2.4.1-rc.2"]), "v2.4.1-rc.2"
        )
        for tags in [[], ["v2.4.0-rc.9"], ["v2.4.1-rc.2", "v2.4.1-rc.10"]]:
            with self.subTest(tags=tags), self.assertRaises(ValueError):
                release.candidate("v2.4.1", tags)
        self.assertEqual(
            release.candidate("v2.4.1", ["v2.4.1-rc.2", "v2.4.1-rc.10"], "v2.4.1-rc.2"),
            "v2.4.1-rc.2",
        )
        with self.assertRaises(ValueError):
            release.candidate("v2.4.1", ["v2.4.1-rc.2"], "v2.4.1-rc.1")

    def test_privacy_and_api_failures(self):
        for status, stdout, stderr, expected in [
            (0, '{"visibility":"private"}', "", True),
            (1, "", "gh: Not Found (HTTP 404)", False),
            (0, '{"visibility":"public"}', "", ValueError),
            (0, '{"visibility":"internal"}', "", ValueError),
            (1, "", "gh: Forbidden (HTTP 403)", RuntimeError),
            (1, "", "connection refused", RuntimeError),
        ]:
            with (
                self.subTest(stdout=stdout, stderr=stderr),
                patch(
                    "release.subprocess.run",
                    return_value=subprocess.CompletedProcess(
                        [], status, stdout, stderr
                    ),
                ),
            ):
                if isinstance(expected, bool):
                    self.assertIs(release.private_package("charts/centaur"), expected)
                else:
                    with self.assertRaises(expected):
                        release.private_package("charts/centaur")

    def test_packaged_charts_pin_images_and_preserve_policy(self):
        planned = release.plan("refs/heads/main", COMMIT)
        images = {
            key: f"ghcr.io/phylaxsystems/centaur/{name}:{planned['tag']}@sha256:{i:064x}"
            for i, (key, (name, *_)) in enumerate(release.IMAGES.items(), 1)
        }
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            receipt = release.package_charts(planned, images, output)
            for name in ("centaur", "agent-sandbox"):
                package = output / f"{name}-{planned['chart_version']}.tgz"
                with tarfile.open(package) as archive:
                    metadata = yaml.safe_load(archive.extractfile(f"{name}/Chart.yaml"))
                    self.assertEqual(metadata["version"], planned["chart_version"])
                    self.assertEqual(
                        json.load(archive.extractfile(f"{name}/release.json")), receipt
                    )
                    if name == "centaur":
                        values = yaml.safe_load(
                            archive.extractfile("centaur/values.yaml")
                        )
                        for key, image in images.items():
                            self.assertEqual(
                                values[key]["image"]["repository"]
                                + ":"
                                + values[key]["image"]["tag"],
                                image,
                            )
                        self.assertEqual(values["toolServer"]["ref"], COMMIT)
            chart = str(output / f"centaur-{planned['chart_version']}.tgz")
            args = (
                "helm",
                "template",
                "fixture",
                chart,
                "--set",
                "agentSandbox.enabled=false",
                "--set",
                "networkPolicy.apiServer.cidrs={10.1.2.3/32}",
                "--set",
                "postgres.nodeSelector.workload=database",
            )
            objects = list(yaml.safe_load_all(release.run(*args)))
            policies = [o for o in objects if o and o["kind"] == "NetworkPolicy"]
            self.assertTrue(policies)
            for policy in policies:
                for rule in policy["spec"].get("egress", []):
                    self.assertTrue(rule.get("to"), policy["metadata"]["name"])
            database = next(o for o in objects if o and o["kind"] == "StatefulSet")
            self.assertEqual(
                database["spec"]["template"]["spec"]["nodeSelector"],
                {"workload": "database"},
            )
            self.assertFalse(
                any(o and o["kind"] == "CustomResourceDefinition" for o in objects)
            )
            # Missing one digest must fail before any chart is emitted.
            images.pop("sandbox")
            with self.assertRaises(ValueError):
                release.package_charts(planned, images, output / "invalid")
            self.assertFalse((output / "invalid").exists())

    def test_stable_retags_without_build_and_reruns_without_overwrite(self):
        planned = release.plan("refs/tags/v2.4.1", COMMIT)
        packages = [f"centaur/{v[0]}" for v in release.IMAGES.values()]
        registry = {
            f"ghcr.io/phylaxsystems/{package}:v2.4.1-rc.2": f"sha256:{i:064x}"
            for i, package in enumerate(packages, 1)
        }
        before = dict(registry)
        real_run = release.run
        chart_bytes = {}

        def fake_run(*args):
            if args[:2] == ("git", "rev-parse"):
                return COMMIT
            if args[:2] == ("git", "tag"):
                return "v2.4.1-rc.2"
            if args[:2] == ("crane", "ls"):
                return "\n".join(
                    k.split(":", 1)[1] for k in registry if k.startswith(args[2] + ":")
                )
            if args[:2] == ("crane", "digest"):
                return registry[args[2]]
            if args[:2] == ("crane", "config"):
                return json.dumps(
                    {
                        "config": {
                            "Labels": {
                                "org.opencontainers.image.source": release.SOURCE,
                                "org.opencontainers.image.revision": COMMIT,
                            }
                        }
                    }
                )
            if args[:2] == ("crane", "tag"):
                repo, digest = args[2].split("@")
                target = repo + ":" + args[3]
                self.assertNotIn(target, registry, "must not overwrite an existing tag")
                registry[target] = digest
                return ""
            if args[:2] == ("helm", "push"):
                archive = Path(args[2])
                name = archive.name.removesuffix("-2.4.1.tgz")
                target = f"ghcr.io/phylaxsystems/charts/{name}:2.4.1"
                self.assertNotIn(target, registry)
                registry[target] = "sha256:" + "f" * 64
                chart_bytes[name] = archive.read_bytes()
                return ""
            if args[:2] == ("helm", "pull"):
                name = args[2].rsplit("/", 1)[1]
                (Path(args[-1]) / f"{name}-2.4.1.tgz").write_bytes(chart_bytes[name])
                return ""
            # No Docker call is allowed in stable promotion, even if an RC is missing.
            if args[0] == "helm":
                return real_run(*args)
            raise AssertionError(f"unexpected command {args}")

        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict("os.environ", {"CENTAUR_PUBLISH_ENABLED": "true"}),
            patch("release.private_package", return_value=True),
            patch("release.run", side_effect=fake_run),
        ):
            output = Path(directory)
            # Real subprocess.run would build. Reject it while allowing Helm's check_output.
            original = subprocess.run

            def no_build(args, **kwargs):
                self.assertNotEqual(args[0], "docker", "stable must never build")
                return original(args, **kwargs)

            with patch("release.subprocess.run", side_effect=no_build):
                release.publish(planned, output)
                release.publish(planned, output)
                receipt = json.loads((output / "release.json").read_text())
                for key, (name, *_) in release.IMAGES.items():
                    source = f"ghcr.io/phylaxsystems/centaur/{name}:v2.4.1-rc.2"
                    self.assertTrue(
                        receipt["images"][key].endswith("@" + before[source])
                    )
                first = f"ghcr.io/phylaxsystems/{packages[0]}:v2.4.1"
                registry[first] = "sha256:" + "e" * 64
                with self.assertRaisesRegex(ValueError, "different bytes"):
                    release.publish(planned, output)
                self.assertEqual(registry[first], "sha256:" + "e" * 64)
                del registry[first]
                del registry[first + "-rc.2"]
                with self.assertRaisesRegex(ValueError, "missing RC"):
                    release.publish(planned, output)

    def test_publication_is_disabled_by_default(self):
        with (
            patch.dict("os.environ", {}, clear=True),
            self.assertRaisesRegex(ValueError, "disabled"),
        ):
            release.publish(release.plan("refs/heads/main", COMMIT), Path("unused"))


if __name__ == "__main__":
    unittest.main()
