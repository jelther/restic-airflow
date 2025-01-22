from restic_airflow.operators.restic import ResticRepositoryExistsOperator


def test_restic_repository_exists_operator_is_ok():
    operator: ResticRepositoryExistsOperator = ResticRepositoryExistsOperator(
        task_id="task_id",
        repository="repository",
        cache_directory="cache_directory",
        tags=["tag", "tag2"],
        password="password",
        progress_fps_seconds=30,
        hostname="hostname",
    )

    assert (
        operator.command
        == "-c 'restic cat config --repo repository --cache-dir cache_directory'"
    )
