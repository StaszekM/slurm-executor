from slurm_executor.models.Context import Context
from slurm_executor.pipeline.compose_rsync_command import compose_rsync_command
from slurm_executor.pipeline.Step import Step


class SendSbatchScript(Step):
    def run(self, ctx: Context):
        conn = ctx._connection
        sbatch_location = ctx.serialized_sbatch_path
        remote_workspace_path = ctx.remote_workspace_path
        assert sbatch_location is not None, (
            f"Sbatch script location must be set in context before executing {type(self).__name__}."
        )
        assert remote_workspace_path is not None, (
            f"Remote workspace location must be set in context before executing \
{type(self).__name__}."
        )

        remote_sbatch_location = remote_workspace_path + "/" + sbatch_location.name

        conn.local(
            compose_rsync_command(
                port=ctx.connection_config.port,
                user=ctx.connection_config.user,
                host=ctx.connection_config.host,
                source=str(sbatch_location),
                destination=remote_sbatch_location,
                exclusion_file=None,
            ),
            pty=False,
        )

        ctx.remote_sbatch_path = remote_sbatch_location

        return ctx
