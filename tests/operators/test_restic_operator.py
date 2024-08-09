from docker.types import Mount

from restic_airflow.operators.restic import ResticOperator


class TestResticOperator(ResticOperator):

    def _build_command(self):
        return []

    def _build_mounts(self) -> list[Mount]:
        mounts = super()._build_mounts()
        return mounts


def test_restic_operator_is_ok():
    operator: TestResticOperator = TestResticOperator(
        task_id="task_id",
        repository="repository",
        cache_directory="cache_directory",
        tags=["tag", "tag2"],
        password="password",
        progress_fps_seconds=30,
        hostname="hostname",
    )

    assert operator.environment["RESTIC_PASSWORD"] == "password"
    assert operator.environment["RESTIC_CACHE_DIR"] == "cache_directory"
    assert operator.environment["RESTIC_TAG"] == "tag tag2"
    assert operator.environment["RESTIC_PROGRESS_FPS"] == str(1 / 30)
    assert operator.environment["RESTIC_REPOSITORY"] == "repository"
    assert operator.environment["RESTIC_HOSTNAME"] == "hostname"


def test_restic_operator_aws_keys():
    operator: TestResticOperator = TestResticOperator(
        task_id="task_id",
        repository="repository",
        cache_directory="cache_directory",
        tags=["tag", "tag2"],
        password="password",
        progress_fps_seconds=30,
        hostname="hostname",
        aws_access_key_id="aws_access_key_id",
        aws_secret_access_key="aws_secret_access_key",
    )

    assert operator.environment["AWS_ACCESS_KEY_ID"] == "aws_access_key_id"
    assert operator.environment["AWS_SECRET_ACCESS_KEY"] == "aws_secret_access_key"


def test_restic_operator_cache_mount():
    operator: TestResticOperator = TestResticOperator(
        task_id="task_id",
        repository="repository",
        cache_directory="cache_directory",
        tags=["tag", "tag2"],
        password="password",
        progress_fps_seconds=30,
        hostname="hostname",
    )

    assert len(operator.mounts) == 1
    assert operator.mounts[0]["Target"] == "cache_directory"
    assert operator.mounts[0]["Source"] == "cache_directory"
    assert operator.mounts[0]["Type"] == "bind"
    assert operator.mounts[0]["ReadOnly"] is False
