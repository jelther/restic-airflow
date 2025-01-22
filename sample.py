from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

from restic_airflow.operators.restic import (
    ResticBackupOperator,
    ResticCheckOperator,
    ResticForgetAndPruneOperator,
    ResticInitOperator,
)
from restic_airflow.notifications import (
    dag_failure_callback,
    dag_success_callback,
    task_failure_callback,
    task_success_callback,
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_failure_callback,
    'on_success_callback': task_success_callback,
}

with DAG(
    'restic_backup_example',
    default_args=default_args,
    description='Example DAG for Restic backups',
    schedule_interval='0 1 * * *',  # Run daily at 1 AM
    start_date=days_ago(1),
    catchup=False,
    on_failure_callback=dag_failure_callback,
    on_success_callback=dag_success_callback,
) as dag:

    # Initialize repository if it doesn't exist
    init_repo = ResticInitOperator(
        task_id='init_repository',
        repository='/path/to/backup/repository',
        cache_directory='/tmp/restic-cache',
        tags=['daily'],
        password='your-repository-password',
        hostname='backup-host'
    )

    # Backup important data
    backup_data = ResticBackupOperator(
        task_id='backup_data',
        repository='/path/to/backup/repository',
        backup_from_path='/path/to/important/data',
        cache_directory='/tmp/restic-cache',
        tags=['daily'],
        password='your-repository-password',
        hostname='backup-host'
    )

    # Check repository health
    check_repo = ResticCheckOperator(
        task_id='check_repository',
        repository='/path/to/backup/repository',
        cache_directory='/tmp/restic-cache',
        tags=['daily'],
        password='your-repository-password',
        hostname='backup-host',
        read_data=True
    )

    # Cleanup old backups
    cleanup = ResticForgetAndPruneOperator(
        task_id='cleanup_old_backups',
        repository='/path/to/backup/repository',
        cache_directory='/tmp/restic-cache',
        tags=['daily'],
        password='your-repository-password',
        hostname='backup-host',
        forget_flags=[
            {'operation': 'daily', 'value': '7'},
            {'operation': 'weekly', 'value': '4'},
            {'operation': 'monthly', 'value': '6'}
        ],
        should_prune=True
    )

    # Set task dependencies
    init_repo >> backup_data >> check_repo >> cleanup
