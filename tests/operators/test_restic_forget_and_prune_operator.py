import pytest

from restic_airflow.operators.restic import ResticForgetAndPruneOperator


@pytest.mark.parametrize("should_prune", [True, False])
@pytest.mark.parametrize(
    "forget_operation",
    [
        "last",
        "hourly",
        "daily",
        "weekly",
        "monthly",
        "yearly",
        "tag",
        "within",
        "within-hourly",
        "within-daily",
        "within-weekly",
        "within-monthly",
        "within-yearly",
    ],
)
def test_restic_forget_and_prune_is_ok(should_prune, forget_operation):
    operator: ResticForgetAndPruneOperator = ResticForgetAndPruneOperator(
        task_id="task_id",
        repository="repository",
        cache_directory="cache_directory",
        tags=["tag", "tag2"],
        password="password",
        progress_fps_seconds=30,
        hostname="hostname",
        should_prune=should_prune,
        forget_flags=[
            {"operation": forget_operation, "value": "1"},
            {"operation": forget_operation, "value": "1"},
        ],
    )

    expected_command = []
    expected_command.append(f"restic cat config --repo repository")
    expected_command.append(
        f"restic unlock --repo repository --cache-dir cache_directory"
    )

    if should_prune:
        expected_command.append(
            f"restic forget --host hostname --repo repository --keep-{forget_operation} 1 --keep-{forget_operation} 1 --prune --cache-dir cache_directory"
        )

        expected_command = " && ".join(expected_command)
        assert operator.command == f"-c '{expected_command}'"
    else:
        expected_command.append(
            f"restic forget --host hostname --repo repository --keep-{forget_operation} 1 --keep-{forget_operation} 1 --cache-dir cache_directory"
        )
        expected_command = " && ".join(expected_command)
        assert operator.command == f"-c '{expected_command}'"
