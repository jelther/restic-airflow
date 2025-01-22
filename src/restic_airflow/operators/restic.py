import uuid
from abc import abstractmethod
from typing import Dict, List, Literal, Optional

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount  # type: ignore
from loguru import logger

from restic_airflow.helpers import is_unix_path


class ResticOperator(DockerOperator):

    repository: str
    cache_directory: str
    tags: List[str]
    password: str
    progress_fps_seconds: int
    hostname: str

    def __init__(
        self,
        task_id: str,
        repository: str,
        cache_directory: str,
        tags: List[str],
        password: str,
        hostname: str,
        progress_fps_seconds: int = 30,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        image: str = "restic/restic:latest",
        auto_remove: Literal["never", "success", "force"] = "success",
        # Set to false when using docker-in-docker
        mount_tmp_dir: bool = False,
        *args,
        **kwargs,
    ):
        self.repository = repository
        self.cache_directory = cache_directory
        self.hostname = hostname
        self.tags = tags
        self.task_id = task_id

        command: str = self._build_command()
        command += f" --cache-dir {self.cache_directory}"

        # set RESTIC_PASSWORD environment variable
        env = kwargs.get("env", {})
        env["RESTIC_PASSWORD"] = password
        env["RESTIC_REPOSITORY"] = self.repository

        env["RESTIC_CACHE_DIR"] = self.cache_directory
        env["RESTIC_TAG"] = " ".join(tags)
        env["RESTIC_PROGRESS_FPS"] = str(
            1 / progress_fps_seconds if progress_fps_seconds > 0 else 1 / 30
        )
        env["RESTIC_HOSTNAME"] = self.hostname

        # access keys for AWS S3 or Backblaze B2
        if aws_access_key_id:
            env["AWS_ACCESS_KEY_ID"] = aws_access_key_id

        if aws_secret_access_key:
            env["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key

        mounts = self._build_mounts()

        container_name = f"restic-task-{self.task_id}-{uuid.uuid4().hex[:8]}"

        super().__init__(
            task_id=task_id,
            command=f"-c '{command}'",
            container_name=container_name,
            image=image,
            hostname=hostname,
            auto_remove=auto_remove,
            entrypoint="/bin/sh",
            environment=env,
            mounts=mounts,
            mount_tmp_dir=mount_tmp_dir,
            *args,
            **kwargs,
        )

    def check_repository_exists_command(self) -> str:
        return f"restic cat config --repo {self.repository}"

    def unlock_repository_command(self) -> str:
        return (
            f"restic unlock --repo {self.repository} --cache-dir {self.cache_directory}"
        )

    @abstractmethod
    def _build_command(self) -> str:
        pass

    def _build_mounts(self) -> list[Mount]:
        # cache is always mounted
        mounts: List[Mount] = [
            Mount(
                target=self.cache_directory,
                source=self.cache_directory,
                type="bind",
                read_only=False,
            )
        ]

        # if repo is a unix path, mount it
        if is_unix_path(self.repository):
            mounts.append(
                Mount(
                    target=self.repository,
                    source=self.repository,
                    type="bind",
                    read_only=False,
                ),
            )
        return mounts

    def execute(self, context):
        logger.info(f"Running command `{self.command}`.")
        super().execute(context)


class ResticInitOperator(ResticOperator):

    def _build_command(self) -> str:
        return " || ".join(
            [
                self.check_repository_exists_command(),
                f"restic init --repo {self.repository}",
            ]
        )

    def execute(self, context):
        logger.info(f"Initializing restic repository {self.repository}.")
        super().execute(context)
        logger.info(f"Initialization of restic repository {self.repository} finished.")


class ResticBackupOperator(ResticOperator):

    backup_from_path: str

    def __init__(self, backup_from_path: str, *args, **kwargs):
        self.backup_from_path = backup_from_path
        super().__init__(*args, **kwargs)

    def _build_command(self) -> str:
        backup_command: str = (
            f"restic backup {self.backup_from_path} --repo {self.repository} --host {self.hostname}"
        )
        for tag in self.tags:
            backup_command += f" --tag {tag}"
        return " && ".join(
            [
                self.check_repository_exists_command(),
                self.unlock_repository_command(),
                backup_command,
            ]
        )

    def _build_mounts(self) -> list[Mount]:
        mounts: List[Mount] = super()._build_mounts()  # Get default mounts
        mounts.append(
            Mount(
                target=self.backup_from_path,
                source=self.backup_from_path,
                type="bind",
                read_only=True,
            ),
        )
        return mounts

    def execute(self, context):
        logger.info(f"Backing up {self.backup_from_path} to {self.repository}.")
        super().execute(context)
        logger.info(f"Backup of {self.backup_from_path} to {self.repository} finished.")


class ResticForgetAndPruneOperator(ResticOperator):

    should_prune: bool
    forget_operation: str
    forget_value: str

    allowed_forget_operations = [
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
    ]

    def __init__(
        self,
        forget_flags: List[Dict[str, str]],
        should_prune: bool = True,
        *args,
        **kwargs,
    ):

        self.should_prune = should_prune

        for forget_flag in forget_flags:
            if "operation" not in forget_flag:
                raise ValueError("forget_flags must contain 'operation' key.")
            if "value" not in forget_flag:
                raise ValueError("forget_flags must contain 'value' key.")
            if forget_flag["operation"] not in self.allowed_forget_operations:
                raise ValueError(
                    f"forget_flags operation must be one of {self.allowed_forget_operations}."
                )

        self.forget_flags = forget_flags

        super().__init__(*args, **kwargs)

    def _build_command(self) -> str:

        forget_operations: List[str] = []
        for forget_flag in self.forget_flags:
            forget_operations.append(
                f"--keep-{forget_flag['operation']} {forget_flag['value']}"
            )

        forget_operations_str = " ".join(forget_operations)

        forget_command: str = (
            f"restic forget --host {self.hostname} --repo {self.repository} {forget_operations_str}"
        )
        if self.should_prune:
            forget_command += " --prune"
        return " && ".join(
            [
                self.check_repository_exists_command(),
                self.unlock_repository_command(),
                forget_command,
            ]
        )

    def execute(self, context):
        logger.info(f"Forgetting and pruning {self.repository}.")
        super().execute(context)
        logger.info(f"Forgetting and pruning of {self.repository} finished.")


class ResticCheckOperator(ResticOperator):

    read_data: bool
    read_data_subset: str
    cached: bool

    def __init__(
        self,
        read_data_subset: str = "",
        read_data: bool = False,
        cached: bool = True,
        *args,
        **kwargs,
    ):
        self.read_data = read_data
        self.cached = cached
        self.read_data_subset = read_data_subset
        super().__init__(*args, **kwargs)

    def _build_command(self) -> str:
        check_command: str = f"restic check --repo {self.repository}"
        if self.read_data and self.read_data_subset == "":
            check_command += " --read-data"
        elif not self.read_data and self.read_data_subset != "":
            check_command += f" --read-data-subset {self.read_data_subset}"
        if self.cached:
            check_command += " --with-cache"
        return " && ".join(
            [
                self.check_repository_exists_command(),
                self.unlock_repository_command(),
                check_command,
            ]
        )

    def execute(self, context):
        logger.info(f"Checking {self.repository}.")
        super().execute(context)
        logger.info(f"Check of {self.repository} finished.")


class ResticUnlockOperator(ResticOperator):

    def _build_command(self) -> str:
        unlock_command = f"restic unlock --repo {self.repository}"
        return " && ".join([self.check_repository_exists_command(), unlock_command])

    def execute(self, context):
        logger.info(f"Unlocking {self.repository}.")
        super().execute(context)
        logger.info(f"Unlock of {self.repository} finished.")


class ResticPruneOperator(ResticOperator):

    def _build_command(self) -> str:
        return " && ".join(
            [
                self.check_repository_exists_command(),
                self.unlock_repository_command(),
                f"restic prune --repo {self.repository}",
            ]
        )

    def execute(self, context):
        logger.info(f"Pruning {self.repository}.")
        super().execute(context)
        logger.info(f"Prune of {self.repository} finished.")


class ResticRepositoryExistsOperator(ResticOperator):

    def _build_command(self) -> str:
        return self.check_repository_exists_command()

    def execute(self, context):
        logger.info(f"Checking if repository {self.repository} exists.")
        super().execute(context)
        logger.info(f"Checking if repository {self.repository} exists finished.")
