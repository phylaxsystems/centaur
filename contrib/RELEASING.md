# Fork release bundles

The fork owns `contrib/chart`, its bundled `charts/agent-sandbox`, and the nine
core images listed in `scripts/release.py`. Deployment-specific configuration
and derived sandbox layers belong to their own repositories. This workflow
publishes artifacts only; it neither deploys nor writes to a consumer repository.

## Identity

| Source | Image tag | Chart version |
| --- | --- | --- |
| `main` | `main-sha-<full 40-character commit>` | `0.0.0-main.sha<full 40-character commit>` |
| Human RC tag | `vX.Y.Z-rc.N` | `X.Y.Z-rc.N` |
| Human stable tag, on the accepted RC commit | `vX.Y.Z` | `X.Y.Z` |

Images live at `ghcr.io/phylaxsystems/centaur/<image>`; charts live at
`oci://ghcr.io/phylaxsystems/charts/centaur` and
`oci://ghcr.io/phylaxsystems/charts/agent-sandbox`. Both are **private**, despite
the source repository being public. No `latest`, moving channel, GitHub release
asset or Pages chart is published. Existing releases elsewhere are not deleted.

Main and RC releases build linux/amd64 and linux/arm64. A stable release retags
the existing RC manifests and verifies unchanged digests; it never rebuilds.
If several RC tags for that version point at the same commit, automatic stable
publication fails. A human can dispatch on the stable tag with `accepted_rc`
to identify the accepted RC. A matching commit is not acceptance evidence:
the consumer must still verify its recorded validation before promotion.

Each chart includes `release.json` recording the source commit and image
`tag@sha256` pins. Packaged Centaur defaults use those pins and the same commit
for base tool delivery. The standalone controller chart keeps its upstream
controller interface: consumers must supply their reviewed controller image
tag and digest. Third-party image pins remain a consumer responsibility.
The final run receipt adds exact chart versions and OCI manifest digests after
an authenticated pull and receipt check. Local packaging is not registry proof.

An existing image tag is reused only after checking both platform revision and
source labels. A stable tag pointing at different bytes fails. An existing
chart must contain the same release receipt. Reruns finish missing artifacts;
there is no registry-wide transaction, and a failed run may leave a partial
bundle. Consumers must require a successful final receipt, not just one tag.

## Human setup and activation

Publication is disabled until repository variable `CENTAUR_PUBLISH_ENABLED`
equals `true`. Before enabling it:

1. Configure the `centaur-publish` environment with reviewers and restrict its
   deployment branches/tags to `main` and approved release tags.
2. Grant this repository's Actions token write access to its GHCR packages.
   Verify every existing destination is private. New GHCR packages default to
   private; the publisher checks privacy again after writing and fails on API
   authorization errors. Grant consumers read access separately. Do not make
   a package public to bypass a pull failure.
3. Set repository variable `CENTAUR_BUILD_RUNNER` to an approved Ubuntu runner
   label with Docker, Python, Go and enough disk/memory for nine multi-arch
   builds. Publication also stays disabled while this variable is empty.
   Buildx/QEMU needs no upstream Depot project or credentials. The default
   small GitHub runner is not assumed sufficient; measure capacity and
   duration before approving it. Read-only validation uses standard Ubuntu.
4. Review the source baseline, runtime/security fixes and controller CRD
   compatibility against the intended consumer. This publishing change is not
   a runtime upgrade or a controller migration.
5. Authorize the first publication separately. Record the successful run,
   private-package settings, authenticated consumer pull, full image digests,
   chart versions/digests, and rendered output before changing any consumer.

`GITHUB_TOKEN` is the only publishing credential. PR validation has read-only
permissions and never logs into the registry. Existing application Secrets
are neither read nor changed. Cross-repository notification/write-back is a
separate consumer integration; do not give this publisher Git write access to
deployment repositories. Until that integration and runtime validation are
approved, consumption is a reviewed pin change, not an automatic rollout.

## Local checks

Install Helm 4.1.1 and PyYAML 6.0.3, add the existing 1Password chart repository,
then run:

```bash
helm repo add onepassword https://1password.github.io/connect-helm-charts
python3 -m unittest discover -s scripts -p test_release.py -v
git diff --check
```

The tests use real Helm archives/renders, simulated registry publication and
stable retagging, and negative privacy/ref/pin cases. They do not build the
runtime images, prove GHCR grants, or exercise a live cluster. The chart change
requires API-server endpoint CIDRs when network policy is enabled; leaving
them empty fails rather than granting destination-free API egress. Keep the
runtime-release-label option disabled unless the API binary stamps that label.
