# Container releases

GitHub Actions runs the full Go/E2E suite before building the Dockerfile.
Before publishing, it starts the image with a bind-mounted YAML configuration,
checks the enabled metrics endpoint and SQLite database, and verifies clean shutdown.
Pull requests build without logging in or pushing, including fork PRs.
The E2E workflow can also be run manually.

Publishing policy:

- Push to `main`: publish `edge` and `sha-<full commit SHA>`.
- Push `v1.2.3`: publish `1.2.3` and the commit tag; advance `latest` only
  if this version is newer than the currently published stable image.
- Push `v1.2.3-rc.1`: publish `1.2.3-rc.1` and the commit tag; leave `latest` unchanged.

Images currently target Linux AMD64, matching the GitHub runner and existing
Dockerfile. `edge` is for trying ongoing development; use a numbered release
or image digest for deployments. Release publication is serialized across tags,
with up to 100 pending publication jobs queued rather than replaced. PR runs
still cancel superseded runs separately from the publication queue.
The workflow reads the current `latest` image version from Docker Hub and compares
versions numerically before publishing. Older releases and reruns cannot replace
`latest`. Missing `latest` permits the first release; inspection errors or missing
version labels stop publication. Do not move existing release tags. Publishing an image does not deploy it to a server.

## One-time Docker Hub setup

Create a Docker Hub repository named `tasker`. In the GitHub repository's
Settings → Secrets and variables → Actions, configure:

- Variable `DOCKERHUB_USERNAME`: your Docker Hub login name.
- Secret `DOCKERHUB_TOKEN`: a Docker Hub access token with write access to the repository.
- Optional variable `DOCKERHUB_IMAGE`: full repository name, such as `my-org/tasker`.
  Defaults to `<DOCKERHUB_USERNAME>/tasker`.

Credentials are only used for pushes. The workflow fails with a configuration
message if required settings are missing.

## Publish a release

Tag a tested commit from `main` and push the tag:

```sh
git tag v0.1.0
git push origin v0.1.0
```

The tag push triggers publication; creating a GitHub Release is optional.
Use `v0.1.0-rc.1` for a prerelease. Release tags must use
`vMAJOR.MINOR.PATCH` with a valid SemVer prerelease suffix if needed. Empty
identifiers and numeric identifiers with leading zeroes are rejected. Build
metadata (`+...`) is unsupported because Docker tags cannot contain `+`.

The required test job is now under the **Build and publish** workflow.
If branch protection requires the previous E2E check name, update it to the
new test check after the first run.
