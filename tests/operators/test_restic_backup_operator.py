from airflow.models import TaskInstance
from airflow.utils.dates import days_ago
from airflow.utils.state import State
from airflow.utils.types import DagRunType

from restic_airflow.operators.restic import ResticBackupOperator


def test_restic_backup_operator_is_ok():
    operator: ResticBackupOperator = ResticBackupOperator(
        task_id="task_id",
        repository="repository",
        backup_from_path="source_path",
        cache_directory="cache_directory",
        tags=["tag", "tag2"],
        password="password",
        progress_fps_seconds=30,
        hostname="hostname",
    )

    expected_command = []
    expected_command.append(f"restic cat config --repo repository")
    expected_command.append(
        f"restic unlock --repo repository --cache-dir cache_directory"
    )
    expected_command.append(
        f"restic backup source_path --repo repository --host hostname --tag tag --tag tag2 --cache-dir cache_directory"
    )

    expected_command = " && ".join(expected_command)

    assert operator.command == f"-c '{expected_command}'"

    assert len(operator.mounts) == 2

    assert operator.mounts[0]["Target"] == "cache_directory"
    assert operator.mounts[0]["Source"] == "cache_directory"
    assert operator.mounts[0]["ReadOnly"] is False
    assert operator.mounts[0]["Type"] == "bind"

    assert operator.mounts[1]["Target"] == "source_path"
    assert operator.mounts[1]["Source"] == "source_path"
    assert operator.mounts[1]["ReadOnly"] is True
    assert operator.mounts[1]["Type"] == "bind"


def test_restic_backup_operator_mount_unix_path():
    operator: ResticBackupOperator = ResticBackupOperator(
        task_id="task_id",
        repository="/destination_path",
        backup_from_path="/source_path",
        cache_directory="/cache_directory",
        tags=["tag", "tag2"],
        password="password",
        progress_fps_seconds=30,
        hostname="hostname",
    )

    assert len(operator.mounts) == 3
    assert operator.mounts[0]["Target"] == "/cache_directory"
    assert operator.mounts[0]["Source"] == "/cache_directory"
    assert operator.mounts[0]["ReadOnly"] is False
    assert operator.mounts[0]["Type"] == "bind"

    assert operator.mounts[1]["Target"] == "/destination_path"
    assert operator.mounts[1]["Source"] == "/destination_path"
    assert operator.mounts[1]["ReadOnly"] is False
    assert operator.mounts[1]["Type"] == "bind"

    assert operator.mounts[2]["Target"] == "/source_path"
    assert operator.mounts[2]["Source"] == "/source_path"
    assert operator.mounts[2]["ReadOnly"] is True
    assert operator.mounts[2]["Type"] == "bind"
