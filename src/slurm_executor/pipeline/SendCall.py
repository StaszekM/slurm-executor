import pathlib
import tempfile

import cloudpickle

from slurm_executor.models.Context import Context
from slurm_executor.models.SerializableCallData import SerializableCallData
from slurm_executor.models.Step import Step
from slurm_executor.utils.compose_rsync_command import compose_rsync_command
from slurm_executor.utils.append_path_slash_if_missing import (
    append_path_slash_if_missing,
)


class SendCall(Step):
    @property
    def provides(self) -> list[str]:
        return ["remote_call_path"]

    @property
    def requires(self) -> list[str]:
        return ["remote_workspace_path"]

    def __init__(self) -> None:
        super().__init__()

    def run(self, ctx: Context):
        func = ctx.function  # pyright: ignore[reportUnknownMemberType]
        args = ctx.args
        kwargs = ctx.kwargs
        remote_workspace = ctx.remote_workspace_path
        assert remote_workspace is not None, (
            f"Remote workspace location must be set in context before executing {type(self).__name__}."  # noqa: E501
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
                cloudpickle.dump(call_data, f)  # pyright: ignore[reportUnknownMemberType]
                conn = ctx._connection

            remote_call_location = (
                append_path_slash_if_missing(remote_workspace)
                + serialized_call_filename
            )

            conn.local(  # pyright: ignore[reportUnknownMemberType]
                compose_rsync_command(
                    port=ctx.connection_config.port,
                    user=ctx.connection_config.user,
                    host=ctx.connection_config.host,
                    local_root=str(call_file),
                    remote_root=remote_call_location,
                    direction="to_remote",
                ),
                pty=False,
            )

            ctx.remote_call_path = remote_call_location

            return ctx
