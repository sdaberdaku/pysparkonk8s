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

## What the workflow publishes

The release workflow runs on the tag. It runs the full CI, then publishes:

- the package `airflow-provider-pysparkonk8s` to PyPI, using trusted publishing,
- the RBAC chart as an OCI artifact at `oci://ghcr.io/sdaberdaku/charts/pysparkonk8s-rbac`,
- the driver images `ghcr.io/sdaberdaku/pysparkonk8s/driver:<version>-airflow<airflow>-python<minor>` for each row of `docker/images.json`,
- a GitHub release with the changelog entry.

## One-time setup

- On PyPI, add a pending publisher for the project `airflow-provider-pysparkonk8s`: owner `sdaberdaku`, repository `pysparkonk8s`, workflow file name of the release workflow, environment `pypi`.
- In the repository settings, create the environment `pypi`.
- In the repository settings, set Pages to deploy from GitHub Actions.
