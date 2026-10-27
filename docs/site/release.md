# Release

A release is a tag `vX.Y.Z` on `main`.

## Steps

1. Update `CHANGELOG.md`.
2. Set `__version__` in `src/pysparkonk8s/provider_info.py` and, if the RBAC chart changed, `version` and `appVersion` in `charts/pysparkonk8s-rbac/Chart.yaml`.
3. Merge to `main`, then push the tag:

```bash
git tag vX.Y.Z
git push origin vX.Y.Z
```

## What the workflow does

`.github/workflows/release.yml` runs on the tag, in this order:

1. `test` runs the full CI (`ci.yml` with `full: true`).
2. `publish` runs in the `pypi` environment. It fails unless the tag matches `__version__` in `src/pysparkonk8s/provider_info.py` and `version` in `charts/pysparkonk8s-rbac/Chart.yaml`. It then builds the package, publishes `airflow-provider-pysparkonk8s` to PyPI with trusted publishing, and creates the GitHub release with `dist/*` attached and the matching `CHANGELOG.md` section as notes.
3. `image` and `chart` run only after `publish` succeeded, so nothing is pushed to GHCR when PyPI failed:
    - `image` builds the driver image for each row of `docker/images.json` for linux/amd64 and linux/arm64, from the wheel built by `publish`, and pushes `ghcr.io/sdaberdaku/pysparkonk8s/driver:<version>-airflow<airflow>-python<minor>`. The row with `latest: true` also gets `:<version>` and `:latest`.
    - `chart` pushes the RBAC chart to `oci://ghcr.io/sdaberdaku/charts/pysparkonk8s-rbac`.

## One-time setup

- On PyPI, add a pending publisher for the project `airflow-provider-pysparkonk8s`: owner `sdaberdaku`, repository `pysparkonk8s`, workflow file name `release.yml`, environment `pypi`.
- In the repository settings, create the environment `pypi`, limited to tags `v*`.
- In the repository settings, set Pages to deploy from GitHub Actions.
- After the first release, set the GHCR packages `pysparkonk8s/driver` and `charts/pysparkonk8s-rbac` to public and link them to the repository in the package settings.
