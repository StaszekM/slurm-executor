import logging

from slurm_executor.models.Context import Context
from slurm_executor.models.RsyncDirection import RsyncDirection
from slurm_executor.models.Step import Step
from slurm_executor.pipeline.compose_rsync_command import compose_rsync_command

logger = logging.getLogger(__name__)


class RSyncWorkspace(Step):
    @property
    def provides(self) -> list[str]:
        return ["remote_workspace_path"]

    @property
    def requires(self) -> list[str]:
        return []

    def __init__(
        self,
        local_root: str,
        remote_root: str,
        direction: RsyncDirection,
        exclude_from: str | None = None,
        include_only: str | None = None,
    ) -> None:
        super().__init__()
        self.local_root = local_root
        self.remote_root = remote_root
        self.direction: RsyncDirection = direction

        if not local_root.endswith("/"):
            logger.warning(
                f"Local root '{local_root}' does not end with '/' which may lead to unexpected behavior. Adding '/' to the end."
            )
            self.local_root = local_root + "/"
        if not remote_root.endswith("/"):
            logger.warning(
                f"Remote root '{remote_root}' does not end with '/' which may lead to unexpected behavior. Adding '/' to the end."
            )
            self.remote_root = remote_root + "/"

        assert not (include_only and exclude_from), (
            "Cannot specify both include_only and exclude_from files."
        )
        assert include_only or exclude_from, (
            "Must specify either include_only or exclude_from file."
        )
        self.exclude_from = exclude_from
        self.include_only = include_only

    def run(self, ctx: Context):
        conn = ctx._connection
        host = ctx.connection_config.host
        user = ctx.connection_config.user
        port = ctx.connection_config.port

        conn.run(
            f"mkdir -p {self.remote_root}",
            pty=False,
        )

        conn.local(  # pyright: ignore[reportUnknownMemberType]
            compose_rsync_command(
                port=port,
                user=user,
                host=host,
                local_root=self.local_root,
                remote_root=self.remote_root,
                exclusion_file=self.exclude_from,
                inclusion_file=self.include_only,
                direction=self.direction,
            ),
            pty=False,
        )

        ctx.remote_workspace_path = self.remote_root

        return ctx
