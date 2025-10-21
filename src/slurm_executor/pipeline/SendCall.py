from slurm_executor.models.Context import Context
from slurm_executor.pipeline.compose_rsync_command import compose_rsync_command
from slurm_executor.pipeline.Step import Step


class SendCall(Step):
    def run(self, ctx: Context):
        conn = ctx._connection
        call_location = ctx.serialized_call_path
        assert call_location is not None, (
            f"Call location must be set in context before executing {type(self).__name__}."
        )
        assert ctx.remote_workspace_path is not None, (
            f"Remote workspace location must be set in context before executing \
{type(self).__name__}."
        )

        remote_call_location = ctx.remote_workspace_path + "/" + call_location.name

        conn.local(
            compose_rsync_command(
                port=ctx.connection_config.port,
                user=ctx.connection_config.user,
                host=ctx.connection_config.host,
                source=str(call_location),
                destination=remote_call_location,
                exclusion_file=None,
            ),
            pty=False,
        )

        ctx.remote_call_path = call_location.name

        return ctx
