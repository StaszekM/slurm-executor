import pathlib
import tempfile

import jinja2

from slurm_executor.models.Context import Context
from slurm_executor.models.Step import Step
from slurm_executor.utils.append_path_slash_if_missing import (
    append_path_slash_if_missing,
)
from slurm_executor.utils.compose_rsync_command import compose_rsync_command


class SendSbatchScript(Step):
    @property
    def provides(self) -> list[str]:
        return ["remote_sbatch_path"]

    @property
    def requires(self) -> list[str]:
        return ["remote_workspace_path", "remote_call_path"]

    def __init__(
        self, partition: str, time: str, sbatch_script_template_location: str
    ) -> None:
        super().__init__()
        self.partition = partition
        self.time = time

        if not pathlib.Path(sbatch_script_template_location).is_file():
            raise FileNotFoundError(
                f"SBATCH script template file not found at {sbatch_script_template_location}."
            )

        with open(sbatch_script_template_location, "r") as f:
            self.sbatch_template = jinja2.Template(f.read())

    def run(self, ctx: Context):
        conn = ctx._connection
        workspace_location = ctx.remote_workspace_path
        remote_call_location = ctx.remote_call_path
        assert workspace_location is not None, (
            f"Remote workspace location must be set in context before executing \
{type(self).__name__}."
        )
        assert remote_call_location is not None, (
            f"Remote call location must be set in context before executing \
{type(self).__name__}."
        )
        composed_file_contents = self.compose_sbatch_script(
            partition=self.partition,
            time=self.time,
            workspace_location=workspace_location,
            remote_call_location=remote_call_location,
        )
        with tempfile.TemporaryDirectory() as tmp:
            job_script_name = "job.sh"
            local_job_dir = pathlib.Path(tmp)
            local_script = local_job_dir / job_script_name
            with open(local_script, "w") as f:
                f.write(composed_file_contents)

            conn = ctx._connection

            remote_sbatch_location = (
                append_path_slash_if_missing(workspace_location) + job_script_name
            )

            conn.local(  # pyright: ignore[reportUnknownMemberType]
                compose_rsync_command(
                    port=ctx.connection_config.port,
                    user=ctx.connection_config.user,
                    host=ctx.connection_config.host,
                    local_root=str(local_script),
                    remote_root=remote_sbatch_location,
                    exclusion_file=None,
                    direction="to_remote",
                ),
                pty=False,
            )

            ctx.remote_sbatch_path = remote_sbatch_location

        return ctx

    def compose_sbatch_script(
        self,
        partition: str,
        time: str,
        workspace_location: str,
        remote_call_location: str,
    ) -> str:
        return self.sbatch_template.render(
            partition=partition,
            time=time,
            workspace_location=workspace_location,
            remote_call_location=remote_call_location,
        )
