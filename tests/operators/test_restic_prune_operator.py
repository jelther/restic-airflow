from restic_airflow.operators.restic import ResticPruneOperator


def test_restic_prune_operator_is_ok():
    operator: ResticPruneOperator = ResticPruneOperator(
        task_id="task_id",
        repository="repository",
        cache_directory="cache_directory",
        tags=["tag", "tag2"],
        password="password",
        progress_fps_seconds=30,
        hostname="hostname",
    )

    exists_repository_command: str = f"restic cat config --repo repository"
    unlock_repository_command: str = (
        f"restic unlock --repo repository --cache-dir cache_directory"
    )

    prune_command: str = f"restic prune --repo repository --cache-dir cache_directory"

    expected_command = " && ".join(
        [exists_repository_command, unlock_repository_command, prune_command]
    )

    assert operator.command == f"-c '{expected_command}'"
