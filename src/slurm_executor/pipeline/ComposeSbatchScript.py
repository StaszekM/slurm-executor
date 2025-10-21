import pathlib
import tempfile
from pathlib import Path
from typing import Optional

import jinja2

from slurm_executor.models.Context import Context
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


class ComposeSbatchScript(Step):
    def __init__(
        self,
        partition: str,
        time: str,
        serialize_to: Optional[Path] = None,
    ) -> None:
        super().__init__()
        self.partition = partition
        self.time = time

        self.serialize_to = serialize_to

        with open("src/slurm_executor/executor/sbatch_script.jinja") as f:
            self.sbatch_template = jinja2.Template(f.read())

    def run(self, ctx: Context):
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

        if self.serialize_to is not None:
            with open(self.serialize_to, "w") as f:
                f.write(composed_file_contents)
            ctx.serialized_sbatch_path = self.serialize_to
        else:
            tmp = tempfile.TemporaryDirectory(delete=False)
            local_job_dir = pathlib.Path(tmp.name)
            job_script_name = "job.sh"
            local_script = local_job_dir / job_script_name
            with open(local_script, "w") as f:
                f.write(composed_file_contents)
            ctx.serialized_sbatch_path = local_script

        return ctx
