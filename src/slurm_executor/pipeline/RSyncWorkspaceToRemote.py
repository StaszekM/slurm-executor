from slurm_executor.models.Context import Context
from slurm_executor.models.RsyncDirection import RsyncDirection
from slurm_executor.pipeline.compose_rsync_command import compose_rsync_command
from slurm_executor.pipeline.Step import Step


class RSyncWorkspace(Step):
    def __init__(
        self,
        local_root: str,
        remote_root: str,
        direction: RsyncDirection,
        exclusion_file: str | None = None,
        inclusion_file: str | None = None,
    ) -> None:
        super().__init__()
        self.local_root = local_root
        self.remote_root = remote_root
        self.direction: RsyncDirection = direction

        assert not (inclusion_file and exclusion_file), (
            "Cannot specify both inclusion and exclusion files."
        )
        assert inclusion_file or exclusion_file, (
            "Must specify either inclusion or exclusion file."
        )
        self.exclusion_file = exclusion_file
        self.inclusion_file = inclusion_file

    def run(self, ctx: Context):
        conn = ctx._connection
        host = ctx.connection_config.host
        user = ctx.connection_config.user
        port = ctx.connection_config.port

        conn.run(
            f"mkdir -p {self.remote_root}",
            pty=False,
        )

        conn.local(
            compose_rsync_command(
                port=port,
                user=user,
                host=host,
                local_root=self.local_root,
                remote_root=self.remote_root,
                exclusion_file=self.exclusion_file,
                inclusion_file=self.inclusion_file,
                direction=self.direction,
            ),
            pty=False,
        )

        ctx.remote_workspace_path = self.remote_root

        return ctx
