# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2026-10-27

First release.

- The `@task.pyspark_on_k8s` decorator and the `pyspark_on_k8s` helper for the stock `@task.pyspark`, which run a task as a client-mode Spark driver on Kubernetes. The Airflow worker pod is the driver and the executors are pods in the same namespace.
- The `SparkBaseConf`, `SparkDriverConf` and `SparkExecutorConf` objects that render the driver pod, the executor pod template and the Spark configuration.
- Support for Airflow 2.11 and 3.3 with the KubernetesExecutor, Spark 4 and Python 3.10 to 3.14.
- The Helm chart `pysparkonk8s-rbac`, with the Role and RoleBinding the worker pod needs to create executors.
- Driver images for Airflow 3.3.2 on Python 3.10, 3.12 and 3.14, for linux/amd64 and linux/arm64.
- Documentation site on GitHub Pages.
