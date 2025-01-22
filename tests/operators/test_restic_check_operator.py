import pytest

from restic_airflow.operators.restic import ResticCheckOperator


@pytest.mark.parametrize("cached", [True, False])
@pytest.mark.parametrize("read_data", [True, False])
@pytest.mark.parametrize("read_data_subset", ["", "1G", "10%"])
def test_restic_check_operator_is_ok(cached, read_data, read_data_subset):
    operator: ResticCheckOperator = ResticCheckOperator(
        task_id="task_id",
        repository="repository",
        cache_directory="cache_directory",
        tags=["tag", "tag2"],
        password="password",
        progress_fps_seconds=30,
        hostname="hostname",
        cached=cached,
        read_data=read_data,
        read_data_subset=read_data_subset,
    )

    check_repository_exists: str = f"restic cat config --repo repository"
    unlock_repository_command: str = (
        f"restic unlock --repo repository --cache-dir cache_directory"
    )

    check_command: str = "restic check --repo repository"

    if read_data and read_data_subset == "":
        check_command += f" --read-data"
    elif not read_data and read_data_subset != "":
        check_command += f" --read-data-subset {read_data_subset}"

    if cached:
        check_command += " --with-cache"

    check_command += f" --cache-dir cache_directory"

    expected_command = " && ".join(
        [check_repository_exists, unlock_repository_command, check_command]
    )

    assert operator.command == f"-c '{expected_command}'"
