# Release

A release is a tag `vX.Y.Z` on `main`.

## Steps

1. Update `CHANGELOG.md`.
2. Set `__version__` in `src/pysparkonk8s/provider_info.py` and `version` and `appVersion` in `charts/pysparkonk8s-rbac/Chart.yaml`. The chart version must equal the tag on every release, even when the chart did not change, because the workflow checks it.
3. Bump the version literals in the docs and the README: the chart `--version` in the install commands, and the driver image tags in the examples.
4. Optionally dry-run the pre-flight from the Actions tab (workflow `Release`, Run workflow) on the branch. It runs `test`, `build` and `build-check` and skips everything that publishes.
5. Merge to `main`, then push the tag:

```bash
git tag vX.Y.Z
git push origin vX.Y.Z
```

## What the workflow does

`.github/workflows/release.yml` runs on the tag, in this order:

1. `test` runs the full CI (`ci.yml` with `full: true`).
2. `build` fails unless the tag matches `__version__` in `src/pysparkonk8s/provider_info.py` and `version` in `charts/pysparkonk8s-rbac/Chart.yaml`. It builds the package and uploads `dist` as an artifact.
3. `build-check` builds every driver image from `docker/images.json` on a native runner for each of linux/amd64 and linux/arm64, with nothing pushed, and runs `import pysparkonk8s, pyspark, airflow` and `java -version` in it. A broken image therefore fails before anything is published.
4. `publish` needs `build` and `build-check` and runs in the `pypi` environment. It publishes `airflow-provider-pysparkonk8s` to PyPI with trusted publishing (files already on PyPI are skipped, so a rerun is safe) and creates the GitHub release if it does not exist, with `dist/*` attached and the matching `CHANGELOG.md` section as notes.
5. `image` and `chart` run only after `publish` succeeded, so nothing is pushed to GHCR when PyPI failed:
    - `image` builds the driver image for each row of `docker/images.json` for linux/amd64 and linux/arm64, from the wheel built by `build`, and pushes `ghcr.io/sdaberdaku/pysparkonk8s/driver:<version>-airflow<airflow>-python<minor>`. The row with `latest: true` also gets `:<version>` and `:latest`.
    - `chart` pushes the RBAC chart to `oci://ghcr.io/sdaberdaku/charts/pysparkonk8s-rbac`.

`publish`, `image` and `chart` run only for tags starting with `v`. When the workflow is started by hand, only `test`, `build` and `build-check` run, which makes it a dry run.

## One-time setup

- On PyPI, add a pending publisher for the project `airflow-provider-pysparkonk8s`: owner `sdaberdaku`, repository `pysparkonk8s`, workflow file name `release.yml`, environment `pypi`.
- In the repository settings, create the environment `pypi`, limited to tags `v*`.
- In the repository settings, set Pages to deploy from GitHub Actions.
- After the first release, set the GHCR packages `pysparkonk8s/driver` and `charts/pysparkonk8s-rbac` to public and link them to the repository in the package settings.
