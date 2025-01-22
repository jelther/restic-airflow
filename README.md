# Restic Airflow

[![PyPI version](https://badge.fury.io/py/restic-airflow.svg)](https://badge.fury.io/py/restic-airflow)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/restic-airflow)](https://pypi.org/project/restic-airflow/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/restic-airflow)](https://pypi.org/project/restic-airflow/)
[![License](https://img.shields.io/badge/License-Unlicense-blue.svg)](https://unlicense.org)
[![Build Status](https://github.com/jelther/restic-airflow/workflows/CI/badge.svg)](https://github.com/jelther/restic-airflow/actions)

## Introduction
This repository provides Apache Airflow operators for managing [Restic](https://restic.net/) backups. It allows you to integrate Restic backup operations into your Airflow DAGs using Docker-based operators.

## Features

- Docker-based Restic operators for Airflow
- Support for S3 and local repositories
- Built-in notification system for task/DAG success and failure
- Configurable backup retention policies
- Repository health checking capabilities

## Available Operators

- `ResticInitOperator`: Initialize a new Restic repository
- `ResticBackupOperator`: Create backups with configurable tags and paths
- `ResticForgetAndPruneOperator`: Manage backup retention and cleanup
- `ResticCheckOperator`: Verify repository integrity
- `ResticUnlockOperator`: Remove stale repository locks
- `ResticPruneOperator`: Clean up unused data
- `ResticRepositoryExistsOperator`: Check if a repository exists

## Usage Example

```python
from airflow import DAG
from restic_airflow.operators.restic import ResticBackupOperator

with DAG('backup_dag', ...) as dag:
    backup_task = ResticBackupOperator(
        task_id='backup_data',
        repository='/path/to/repo',
        backup_from_path='/data/to/backup',
        cache_directory='/tmp/restic-cache',
        tags=['daily'],
        password='your-repository-password',
        hostname='backup-host'
    )
```

See [sample.py](sample.py) for a complete DAG example including initialization, backup, health checks, and retention management.

## Environment Variables

The operators support configuration through environment variables:

- `RESTIC_PASSWORD`: Repository password
- `AWS_ACCESS_KEY_ID`: For S3 repositories
- `AWS_SECRET_ACCESS_KEY`: For S3 repositories
- `ALERT_EMAIL`: Email address for notifications

## Notifications

The package includes a notification system that can send emails on:
- DAG success/failure
- Individual task success/failure

## License

This project is released into the public domain under the [Unlicense](LICENSE).

