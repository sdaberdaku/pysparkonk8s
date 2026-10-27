# Development

## Setup and checks

The project uses [uv](https://docs.astral.sh/uv/).

```bash
uv sync
uv run pytest
uv run ruff check .
uv run ruff format --check .
```

The lock file holds Airflow 3. To test against Airflow 2.11, install it over the lock in a scratch environment:

```bash
uv pip uninstall apache-airflow apache-airflow-core apache-airflow-task-sdk
uv pip install "apache-airflow==2.11.*"
uv run --no-sync pytest -q
```

## Documentation

```bash
pip install -r docs/site/requirements.txt
cp CHANGELOG.md docs/site/changelog.md
mkdocs serve
```

CI builds the site with `mkdocs build --strict`, so any warning fails the build.

## End-to-end test

`e2e/run.sh` creates a kind cluster, builds the driver image from the working tree, installs Airflow 3 with the KubernetesExecutor and the RBAC chart, runs the three example DAGs in `examples/dags` and checks that each succeeds with executors running as pods. It needs docker, kind, kubectl, helm and uv.

```bash
helm repo add apache-airflow https://airflow.apache.org
PYTHON_VERSION=3.12 EXECUTOR_IMAGE=ghcr.io/sdaberdaku/spark:4.2.0-python3.12 bash e2e/run.sh
```

`KEEP_CLUSTER=1` keeps the cluster afterwards, and on failure the run writes cluster dumps to `e2e/out`.

## CI

`.github/workflows/ci.yml` runs on pull requests and can be reused by other workflows.

- `lint` runs ruff.
- `test` runs pytest over the Airflow and Python combinations in `matrix.json`.
- `docs` builds this site strictly.
- `e2e` runs the kind test for Python 3.10 with the stock executor image and for 3.14 with `ghcr.io/sdaberdaku/spark:4.2.0-python3.14`.
- `ci-ok` fails unless every job succeeded and is the single required check of the branch protection.

Draft pull requests skip all jobs, so `ci-ok` stays red until the PR is ready for review.

Renovate opens the dependency update pull requests, on Mondays before 6am. Python packages, the driver image inputs in `docker/images.json`, the Airflow Helm chart and executor images in the e2e test, and GitHub Actions are all tracked. Patch and action updates merge on their own once CI passes; Airflow minor updates, PySpark minor updates and Spark executor patch updates wait for review, and Airflow major updates are never proposed. The Mend Renovate app must be installed on the repository for the configuration to act.
