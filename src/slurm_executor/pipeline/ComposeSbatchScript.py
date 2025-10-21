import pathlib
import tempfile

import jinja2

from slurm_executor.models.Context import Context
from slurm_executor.pipeline.compose_rsync_command import compose_rsync_command
from slurm_executor.pipeline.Step import Step

with open("src/slurm_executor/executor/sbatch_script.jinja") as f:
    SBATCH_TEMPLATE = jinja2.Template(f.read())


def compose_sbatch_script(
    partition: str, time: str, workspace_location: str, remote_call_location: str
) -> str:
    return SBATCH_TEMPLATE.render(
        partition=partition,
        time=time,
        workspace_location=workspace_location,
        remote_call_location=remote_call_location,
    )


class SendSbatchScript(Step):
    def __init__(
        self,
        partition: str,
        time: str,
    ) -> None:
        super().__init__()
        self.partition = partition
        self.time = time

        with open("src/slurm_executor/executor/sbatch_script.jinja") as f:
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
        composed_file_contents = compose_sbatch_script(
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

            remote_sbatch_location = workspace_location + "/" + job_script_name

            conn.local(
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
