from slurm_executor.models.Context import Context
from slurm_executor.pipeline.compose_rsync_command import compose_rsync_command
from slurm_executor.pipeline.Step import Step


class RSyncWorkspaceToRemote(Step):
    def __init__(
        self,
        workspace_root: str,
        workspace_destination: str,
        exclusion_file: str | None = None,
    ) -> None:
        super().__init__()
        self.source = workspace_root
        self.destination = workspace_destination
        self.exclusion_file = exclusion_file

    def run(self, ctx: Context):
        conn = ctx._connection
        host = ctx.connection_config.host
        user = ctx.connection_config.user
        port = ctx.connection_config.port

        conn.run(
            f"mkdir -p {self.destination}",
            pty=False,
        )

        conn.local(
            compose_rsync_command(
                port=port,
                user=user,
                host=host,
                source=self.source,
                destination=self.destination,
                exclusion_file=self.exclusion_file,
            ),
            pty=False,
        )

        ctx.remote_workspace_path = self.destination

        return ctx
