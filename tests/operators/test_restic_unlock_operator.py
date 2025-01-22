from restic_airflow.operators.restic import ResticUnlockOperator


def test_restic_unlock_operator_is_ok():
    operator: ResticUnlockOperator = ResticUnlockOperator(
        task_id="task_id",
        repository="repository",
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

    expected_command = " && ".join(expected_command)

    assert operator.command == f"-c '{expected_command}'"
