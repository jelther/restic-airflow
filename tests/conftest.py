import os
import subprocess

import pytest
from airflow import DAG
from airflow.utils.dates import days_ago


@pytest.fixture(scope="function")
def init_airflow_db():
    subprocess.run(["airflow", "db", "init"], check=True)
    subprocess.run(["airflow", "db", "reset", "-y"], check=True)


@pytest.fixture
def dag():
    return DAG(dag_id="test_dag", start_date=days_ago(1))


@pytest.fixture(scope="function")
def cache_directory():
    # Get the current file's directory
    current_dir = os.path.dirname(__file__)
    # Construct the full path to the cache_directory inside the tests folder
    cache_dir = os.path.join(current_dir, "cache_directory")
    # Ensure the directory exists
    os.makedirs(cache_dir, exist_ok=True)
    return cache_dir


@pytest.fixture(scope="function")
def backup_from_path():
    current_dir = os.path.dirname(__file__)
    cache_dir = os.path.join(current_dir, "backup_from_path")
    # Ensure the directory exists
    os.makedirs(cache_dir, exist_ok=True)
    return cache_dir
