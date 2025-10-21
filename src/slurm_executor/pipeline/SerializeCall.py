import pathlib
import tempfile

import cloudpickle

from slurm_executor.models.Context import Context
from slurm_executor.models.SerializableCallData import SerializableCallData
from slurm_executor.pipeline.compose_rsync_command import compose_rsync_command
from slurm_executor.pipeline.Step import Step


class SendCall(Step):
    def __init__(self) -> None:
        super().__init__()

    def run(self, ctx: Context):
        func = ctx.function
        args = ctx.args
        kwargs = ctx.kwargs
        remote_workspace = ctx.remote_workspace_path
        assert remote_workspace is not None, (
            f"Remote workspace location must be set in context before executing {type(self).__name__}."
        )

        call_data = SerializableCallData(
            func=func,
            args=args,
            kwargs=kwargs,
        )

        with tempfile.TemporaryDirectory() as tmp:
            serialized_call_filename = "call.pkl"
            local_job_dir = pathlib.Path(tmp)
            call_file = local_job_dir / serialized_call_filename
            with open(call_file, "wb") as f:
                cloudpickle.dump(call_data, f)
                conn = ctx._connection

            remote_call_location = remote_workspace + "/" + serialized_call_filename

            conn.local(
                compose_rsync_command(
                    port=ctx.connection_config.port,
                    user=ctx.connection_config.user,
                    host=ctx.connection_config.host,
                    source=str(call_file),
                    destination=remote_call_location,
                    exclusion_file=None,
                ),
                pty=False,
            )

            ctx.remote_call_path = remote_call_location

            return ctx
