#!/usr/bin/env python3
"""Publish one immutable Centaur bundle. Never writes to a consumer repository."""

import argparse
import json
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path
from urllib.parse import quote

import yaml

ROOT = Path(__file__).resolve().parents[1]
OWNER = "phylaxsystems"
SOURCE = f"https://github.com/{OWNER}/centaur"
IMAGES = {
    "apiRs": ("centaur-api-rs", "services/api-rs/Dockerfile", ".", ""),
    "slackbotv2": ("centaur-slackbotv2", "services/slackbotv2/Dockerfile", ".", ""),
    "linearbot": ("centaur-linearbot", "services/linearbot/Dockerfile", ".", ""),
    "githubbot": ("centaur-githubbot", "services/githubbot/Dockerfile", ".", ""),
    "discordbot": ("centaur-discordbot", "services/discordbot/Dockerfile", ".", ""),
    "teamsbot": ("centaur-teamsbot", "services/teamsbot/Dockerfile", ".", ""),
    "sandbox": ("centaur-agent", "services/sandbox/Dockerfile", ".", "sandbox"),
    "ironProxy": ("centaur-iron-proxy", "services/iron-proxy/Dockerfile", ".", ""),
    "console": (
        "centaur-console",
        "services/console/Dockerfile",
        "services/console",
        "",
    ),
}
SEMVER = r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")


def run(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def plan(ref, commit):
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("source commit must be a full SHA")
    if ref == "refs/heads/main":
        tag, version, kind = f"main-sha-{commit}", f"0.0.0-main.sha{commit}", "main"
    elif re.fullmatch(rf"refs/tags/v{SEMVER}(-rc\.(0|[1-9][0-9]*))?", ref):
        tag = ref.removeprefix("refs/tags/")
        version = tag.removeprefix("v")
        kind = "rc" if "-rc." in tag else "stable"
    else:
        raise ValueError("only main, vX.Y.Z-rc.N and vX.Y.Z may publish")
    return {
        "repo": f"{OWNER}/centaur",
        "commit": commit,
        "tag": tag,
        "chart_version": version,
        "kind": kind,
    }


def candidate(tag, tags, selected=None):
    matches = [
        t for t in tags if re.fullmatch(re.escape(tag) + r"-rc\.(0|[1-9][0-9]*)", t)
    ]
    if selected:
        if selected not in matches:
            raise ValueError("selected RC must match the stable version and commit")
        return selected
    if len(matches) != 1:
        raise ValueError(
            "stable needs one RC at this commit; select the accepted RC explicitly if ambiguous"
        )
    return matches[0]


def private_package(package):
    """Missing packages default to private at GHCR; every other error fails closed."""
    response = subprocess.run(
        ["gh", "api", f"/orgs/{OWNER}/packages/container/{quote(package, safe='')}"],
        capture_output=True,
        text=True,
        check=False,
    )
    if response.returncode:
        if "(HTTP 404)" in response.stderr:
            return False
        raise RuntimeError("cannot verify GHCR package privacy/Actions access")
    if json.loads(response.stdout)["visibility"] != "private":
        raise ValueError(f"refusing non-private package: {package}")
    return True


def existing(package, tag):
    if not private_package(package):
        return None
    repo = f"ghcr.io/{OWNER}/{package}"
    # A failed list is not evidence that a tag is absent.
    if tag not in run("crane", "ls", repo).splitlines():
        return None
    return run("crane", "digest", f"{repo}:{tag}")


def check_revision(repo, digest, commit):
    if not DIGEST.fullmatch(digest):
        raise ValueError("invalid image digest")
    for platform in ("linux/amd64", "linux/arm64"):
        config = json.loads(
            run("crane", "config", "--platform", platform, f"{repo}@{digest}")
        )
        labels = config["config"]["Labels"]
        if (
            labels.get("org.opencontainers.image.revision") != commit
            or labels.get("org.opencontainers.image.source") != SOURCE
        ):
            raise ValueError(
                f"image provenance does not match this release: {repo} ({platform})"
            )


def package_charts(release, images, output):
    if set(images) != set(IMAGES):
        raise ValueError("release must include every Centaur image")
    for key, (name, *_rest) in IMAGES.items():
        expected = f"ghcr.io/{OWNER}/centaur/{name}:{release['tag']}@"
        if not images[key].startswith(expected) or not DIGEST.fullmatch(
            images[key][len(expected) :]
        ):
            raise ValueError(f"invalid pinned image for {key}")
    receipt = {**release, "images": images}
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as directory:
        for name, source in (
            ("centaur", ROOT / "contrib/chart"),
            ("agent-sandbox", ROOT / "contrib/chart/charts/agent-sandbox"),
        ):
            chart = Path(directory) / name
            shutil.copytree(source, chart)
            metadata = yaml.safe_load((chart / "Chart.yaml").read_text())
            metadata.update(version=release["chart_version"], appVersion=release["tag"])
            metadata.setdefault("annotations", {})[
                "org.opencontainers.image.revision"
            ] = release["commit"]
            (chart / "Chart.yaml").write_text(yaml.safe_dump(metadata, sort_keys=False))
            if name == "centaur":
                values = yaml.safe_load((chart / "values.yaml").read_text())
                for key, image in images.items():
                    repo, pin = image.split(":", 1)
                    values[key]["image"].update(repository=repo, tag=pin)
                values["toolServer"].update(
                    repo=f"{OWNER}/centaur", ref=release["commit"]
                )
                (chart / "values.yaml").write_text(
                    yaml.safe_dump(values, sort_keys=False)
                )
                run("helm", "dependency", "build", str(chart))
            (chart / "release.json").write_text(json.dumps(receipt, indent=2) + "\n")
            run("helm", "package", str(chart), "--destination", str(output.resolve()))
    return receipt


def publish(release, output, accepted_rc=None):
    if os.environ.get("CENTAUR_PUBLISH_ENABLED") != "true":
        raise ValueError("publication is disabled until a human enables it")
    if run("git", "rev-parse", "HEAD") != release["commit"]:
        raise ValueError("checkout does not match release commit")
    source_tag = None
    if release["kind"] == "stable":
        source_tag = candidate(
            release["tag"],
            run("git", "tag", "--points-at", "HEAD").splitlines(),
            accepted_rc,
        )
    images = {}
    # Check all destinations before the first write, including charts.
    for package in [f"centaur/{v[0]}" for v in IMAGES.values()] + [
        "charts/centaur",
        "charts/agent-sandbox",
    ]:
        private_package(package)
    for key, (name, dockerfile, context, target) in IMAGES.items():
        package = f"centaur/{name}"
        repo = f"ghcr.io/{OWNER}/{package}"
        digest = existing(package, release["tag"])
        if source_tag:
            accepted = existing(package, source_tag)
            if not accepted:
                raise ValueError(f"missing RC image: {repo}:{source_tag}")
            check_revision(repo, accepted, release["commit"])
            if digest and digest != accepted:
                raise ValueError("stable tag already names different bytes")
            if not digest:
                run("crane", "tag", f"{repo}@{accepted}", release["tag"])
                digest = run("crane", "digest", f"{repo}:{release['tag']}")
            if digest != accepted:
                raise ValueError("stable promotion changed the manifest digest")
        elif not digest:
            args = [
                "docker",
                "buildx",
                "build",
                "--push",
                "--platform",
                "linux/amd64,linux/arm64",
                "--provenance=false",
                "--sbom=false",
                "--file",
                dockerfile,
                "--tag",
                f"{repo}:{release['tag']}",
                "--label",
                f"org.opencontainers.image.revision={release['commit']}",
                "--label",
                f"org.opencontainers.image.source={SOURCE}",
                "--build-arg",
                "RUST_BUILD_PROFILE=release",
            ]
            if target:
                args += ["--target", target]
            subprocess.run([*args, context], cwd=ROOT, check=True)
            digest = run("crane", "digest", f"{repo}:{release['tag']}")
        check_revision(repo, digest, release["commit"])
        if not private_package(package):
            raise ValueError("published package cannot be inspected")
        images[key] = f"{repo}:{release['tag']}@{digest}"
    receipt = package_charts(release, images, output)
    receipt["charts"] = {}
    for name in ("centaur", "agent-sandbox"):
        package = f"charts/{name}"
        repo = f"ghcr.io/{OWNER}/{package}"
        version = release["chart_version"]
        digest = existing(package, version)
        if not digest:
            run(
                "helm",
                "push",
                str(output / f"{name}-{version}.tgz"),
                f"oci://ghcr.io/{OWNER}/charts",
            )
            digest = run("crane", "digest", f"{repo}:{version}")
        if not private_package(package):
            raise ValueError("published chart cannot be inspected")
        with tempfile.TemporaryDirectory() as directory:
            run(
                "helm",
                "pull",
                f"oci://{repo}",
                "--version",
                version,
                "--destination",
                directory,
            )
            with tarfile.open(Path(directory) / f"{name}-{version}.tgz") as archive:
                published = json.load(archive.extractfile(f"{name}/release.json"))
            if published != {**release, "images": images}:
                raise ValueError("immutable chart already contains a different release")
        receipt["charts"][name] = {
            "repository": f"oci://{repo}",
            "version": version,
            "digest": digest,
        }
    (output / "release.json").write_text(json.dumps(receipt, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["plan", "package", "publish"])
    parser.add_argument("--ref", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--images", type=Path)
    parser.add_argument(
        "--accepted-rc", help="explicit RC tag when several RCs share the stable commit"
    )
    parser.add_argument("--output", type=Path, default=ROOT / "dist/release")
    args = parser.parse_args()
    release = plan(args.ref, args.commit)
    if args.action == "plan":
        print(json.dumps(release))
    elif args.action == "package":
        if not args.images:
            parser.error("package requires --images")
        package_charts(release, json.loads(args.images.read_text()), args.output)
    else:
        publish(release, args.output, args.accepted_rc)


if __name__ == "__main__":
    main()
