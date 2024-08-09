from airflow.models.taskinstance import TaskInstance
from airflow.utils.dates import days_ago
from airflow.utils.state import State
from airflow.utils.types import DagRunType

from restic_airflow.operators.restic import ResticInitOperator


def test_restic_init_operator_is_ok(dag):
    execution_date = days_ago(1)

    operator = ResticInitOperator(
        task_id="task_id",
        repository="repository",
        cache_directory="cache_directory",
        tags=["tag", "tag2"],
        password="password",
        progress_fps_seconds=30,
        hostname="hostname",
        dag=dag,
    )

    assert (
        operator.command
        == f"-c 'restic cat config --repo repository || restic init --repo repository --cache-dir cache_directory'"
    )


def test_restic_init_operator_execute_is_ok(init_airflow_db, cache_directory, dag):
    execution_date = days_ago(1)

    operator = ResticInitOperator(
        task_id="task_id",
        repository="repository",
        cache_directory=cache_directory,
        tags=["tag", "tag2"],
        password="password",
        progress_fps_seconds=30,
        hostname="hostname",
        dag=dag,
    )
    # Create a DagRun
    dag_run = dag.create_dagrun(
        run_id="test_run_id",
        run_type=DagRunType.MANUAL,
        execution_date=execution_date,
        start_date=days_ago(1),
        state=State.RUNNING,
    )

    # Create the TaskInstance
    task_instance = TaskInstance(task=operator, execution_date=execution_date)
    task_instance.dag_run = dag_run

    # Run the TaskInstance
    task_instance.run(ignore_ti_state=True)

    # Verify the task instance state
    assert task_instance.state == State.SUCCESS
